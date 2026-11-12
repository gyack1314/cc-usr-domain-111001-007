"""不依赖真实等待的调度演示：虚拟时钟推进，打印完整发送序列。

四个场景覆盖：
  A. 两个域名共用额度 + 紧急修订插队 + 组级 Retry-After 冷却（含在途请求）
  B. 同优先级来源轮转、低优先级老化升级、deadline 过期不占连接
  C. 花费后落盘退出再启动：桶不回满、在途结果转待核实、无超发
  D. 入口见 http_integration.py（真实本地 HTTP 联调）

运行：python demo.py
"""

import json
import os
import sys
import tempfile

# 隔离模式（python -I）不会把脚本目录加入 sys.path，这里显式引导。
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler.clock import ManualClock
from scheduler.model import SendResult, TaskState
from scheduler.scheduler import Scheduler, SchedulerConfig
from scheduler.scripted import ScriptedSender

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = HERE


def load_config():
    with open(os.path.join(ROOT, "sources", "quota.json"),
              encoding="utf-8") as fh:
        raw = json.load(fh)
    return SchedulerConfig(raw["groups"], raw["source_concurrency"],
                           raw["backoff"])


def banner(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def show(scheduler, note=""):
    snap = scheduler.snapshot()
    tokens = ", ".join(f"{g}={v:g}" for g, v in snap["tokens"].items())
    inflight = ", ".join(f"{s}={n}" for s, n in snap["in_flight"].items()) or "-"
    spent = ", ".join(f"{g}={v:g}" for g, v in snap["spent"].items()) or "-"
    cool = ", ".join(f"{g}至{u:g}" for g, u in snap["cooldowns"].items()) or "-"
    print(f"  t={snap['now']:>5g}  {note}")
    print(f"        桶余量[{tokens}]  已花费[{spent}]  "
          f"在途[{inflight}]  冷却[{cool}]")


def print_events(scheduler, since):
    for ev in scheduler.events[since:]:
        kind = ev["kind"]
        line = f"    t={ev['at']:>5g}  {kind:<18} {ev['task_id']}"
        if kind == "dispatch":
            line += (f"  source={ev['source']} cost={ev['cost']:g} "
                     f"第{ev['attempt']}次 发送后桶余{ev['tokens_left']:g}")
        elif kind == "retry_queued":
            line += f"  第{ev['attempt']}次后重排，{ev['retry_ready']:g}可再发"
        elif kind == "cooldown":
            line += f"  组{ev['group']}冷却至 t={ev['until_wall']:g}"
        elif kind == "expired":
            line += f"  deadline={ev['deadline']:g}"
        elif kind == "rejected":
            line += f"  cost={ev['cost']:g} > 容量={ev['capacity']:g}"
        elif kind in ("succeeded", "failed", "in_flight", "recovered_unverified",
                      "verify_retry", "verify_abandon", "submitted",
                      "bad_retry_after", "cancelled"):
            extra = {k: v for k, v in ev.items()
                     if k not in ("at", "kind", "task_id")}
            if extra:
                line += f"  {extra}"
        print(line)


# --------------------------------------------------------------- 场景 A

def scenario_a(config):
    banner("场景 A：双域名共享 vendor 额度 / 紧急修订插队 / 组级冷却 / 在途不中断")
    clock = ManualClock()
    # /north 第一次 429(Retry-After:4)，之后 200；/south 直接 200。
    # t4 额外的 south 任务用于证明冷却期同组另一域名不能偷跑。
    sender = ScriptedSender({
        "t1": [("http", 429, {"Retry-After": "4"}), ("http", 200)],
        "t2": [("http", 200)],
        "t5": [("block",)],
        "t6": [("http", 200)],
    })
    sch = Scheduler(config, sender, clock)
    with open(os.path.join(ROOT, "sources", "tasks.json"),
              encoding="utf-8") as fh:
        tasks = json.load(fh)
    since = 0
    for task in tasks:
        sch.submit(task)
    # t4 才到达的普通 south 请求（与 t1 同组，用于验证冷却封锁）。
    sch.submit({"task_id": "t5", "source": "south", "quota_group": "vendor",
                "path": "/south", "cost": 1, "priority": 0, "not_before": 4})
    # 超容量任务：必须立即不可执行。
    sch.submit({"task_id": "t6", "source": "north", "quota_group": "vendor",
                "path": "/north", "cost": 99, "priority": 0, "not_before": 0})
    print_events(sch, since)
    show(sch, "任务已入队；t6 因 cost>容量 立即拒绝（未扣费）")

    since = len(sch.events)
    sch.tick()
    print_events(sch, since)
    show(sch, "首次派发：t2(优先级3)插队先发，随后同级轮转 t1→t3")

    since = len(sch.events)
    clock.advance(4)
    sch.tick()
    print_events(sch, since)
    show(sch, "t=4：冷却结束，t1 重发成功；t5 同刻就绪，同级轮转轮到 south")

    since = len(sch.events)
    clock.advance(1)
    sch.tick()
    print_events(sch, since)
    show(sch, "t=5：桶补到 1，轮转回到 north -> t3 发出；t5 保持在途，"
              "组冷却不影响在途请求")

    since = len(sch.events)
    print("    —— 在途请求允许结束：显式结算 t5，组冷却不影响在途请求 ——")
    sch.complete("t5", SendResult.http(200))
    print_events(sch, since)
    show(sch, "t5 成功；complete 对重复 task_id 幂等，再调一次会被忽略")
    assert sch.complete("t5", SendResult.http(200)) is False
    print("    重复 complete(t5) -> False（幂等忽略）")


# --------------------------------------------------------------- 场景 B

def scenario_b(config):
    banner("场景 B：来源轮转 / 老化升级 / deadline 过期释放连接")

    # --- B1 轮转 + 老化 ---
    clock = ManualClock()
    sender = ScriptedSender()  # 默认全部 200
    cfg = SchedulerConfig({"vendor": {"capacity": 1, "rate": 0.1}},
                          {"north": 1, "south": 1}, config.backoff)
    sch = Scheduler(cfg, sender, clock)
    for tid, source, prio in [("a", "north", 0), ("b", "south", 0),
                              ("c", "north", 1)]:
        sch.submit({"task_id": tid, "source": source, "quota_group": "vendor",
                    "path": "/" + source, "cost": 1, "priority": prio,
                    "not_before": 0})
    since = len(sch.events)
    sch.tick()
    print_events(sch, since)
    show(sch, "t=0：c(优先级1)先拿唯一个 token；a/b 排队")
    print(f"    队列有效优先级：{sch.snapshot()['effective_priority']}")

    since = len(sch.events)
    clock.advance(10)
    before = sch.snapshot()["effective_priority"]
    sch.tick()
    print_events(sch, since)
    show(sch, "t=10：桶补到 1；a/b 老化到 1 级，同级轮转 north 先 -> a 发出")
    print(f"    t=10 派发前有效优先级：{before}（a、b 已从 0 升到 1）")

    since = len(sch.events)
    clock.advance(10)
    sch.tick()
    print_events(sch, since)
    show(sch, "t=20：轮转轮到 south -> b 发出；a 已老化到 2 级（封顶 3）")

    # --- B2 过期不占连接 ---
    clock2 = ManualClock()
    sender2 = ScriptedSender({"hold": [("block",)]})
    cfg2 = SchedulerConfig({"vendor": {"capacity": 1, "rate": 0}},
                           {"north": 1, "south": 1}, config.backoff)
    sch2 = Scheduler(cfg2, sender2, clock2)
    sch2.submit({"task_id": "hold", "source": "north", "quota_group": "vendor",
                 "path": "/north", "cost": 1, "priority": 0, "not_before": 0})
    sch2.submit({"task_id": "late", "source": "north", "quota_group": "vendor",
                 "path": "/north", "cost": 1, "priority": 0, "not_before": 5,
                 "deadline": 10})
    since = len(sch2.events)
    sch2.tick()
    print_events(sch2, since)
    show(sch2, "t=0：hold 占用 north 唯一连接并保持在途")

    since = len(sch2.events)
    clock2.advance(11)
    sch2.tick()
    print_events(sch2, since)
    show(sch2, "t=11：late 超过 deadline=10 且从未发送 -> 过期，"
               "attempts=0 未扣费、不占连接")
    late = sch2.get_task("late")
    assert late.state == TaskState.EXPIRED and late.attempts == 0
    print(f"    late: state={late.state.value}, attempts={late.attempts}")


# --------------------------------------------------------------- 场景 C

def scenario_c(config):
    banner("场景 C：花费后落盘退出，再启动不回满；在途结果转待核实，无超发")
    state_path = os.path.join(tempfile.mkdtemp(prefix="sched-demo-"),
                              "state.json")
    clock = ManualClock()
    sender = ScriptedSender({"x": [("block",)], "y": [("http", 200)]})
    sch = Scheduler(config, sender, clock, state_path=state_path)
    sch.submit({"task_id": "x", "source": "north", "quota_group": "vendor",
                "path": "/north", "cost": 2, "priority": 0, "not_before": 0})
    sch.submit({"task_id": "y", "source": "south", "quota_group": "vendor",
                "path": "/south", "cost": 1, "priority": 0, "not_before": 0})
    since = len(sch.events)
    sch.tick()
    print_events(sch, since)
    show(sch, f"状态已持久化到 {state_path}")
    print("    模拟进程退出（对象丢弃），随后用同一份状态文件重新启动……")

    # 重新启动：新时钟、新发送器、同路径。
    clock2 = ManualClock(clock.now)
    sender2 = ScriptedSender({"x": [("http", 200)], "y": [("http", 200)]})
    sch2 = Scheduler(config, sender2, clock2, state_path=state_path)
    since = len(sch2.events)
    print_events(sch2, since)
    show(sch2, "重启后：桶保持 0（不回满）；x 结果不明 -> 待核实并释放连接")
    assert sch2.pending_verification() == ["x"]
    print(f"    待核实任务：{sch2.pending_verification()}（不会被自动重发）")

    # 插件核实后决定安全重试。
    since = len(sch2.events)
    assert sch2.verify_decision("x", "retry") is True
    assert sch2.verify_decision("x", "retry") is False  # 幂等
    clock2.advance(3)  # 补 3 个 token
    sch2.tick()
    print_events(sch2, since)
    show(sch2, "插件确认可安全重试后 x 才重发；总花费=2(x)+1(y)+2(x重发)")
    spent = sch2.snapshot()["spent"].get("vendor", 0)
    assert spent == 5, spent
    print(f"    vendor 累计花费={spent:g}，容量 3 从未被单次超发；"
          f"重复 verify 已幂等忽略")

    # 时钟回退保护：把墙钟往回拨后再启动，token 不得凭空增加。
    clock3 = ManualClock(clock2.now - 100)
    sch3 = Scheduler(config, ScriptedSender(), clock3, state_path=state_path)
    before = sch2.snapshot()["tokens"]["vendor"]
    after = sch3.snapshot()["tokens"]["vendor"]
    show(sch3, "墙钟回退 100 秒后再次启动")
    assert after <= before + 1e-9
    print(f"    回退前桶余={before:g}，回退后桶余={after:g}（未凭空补 token）")


def main():
    config = load_config()
    scenario_a(config)
    scenario_b(config)
    scenario_c(config)
    banner("演示结束")
    print("所有场景的额度/并发不变量断言均通过；真实 HTTP 联调见：")
    print("  python http_integration.py")


if __name__ == "__main__":
    main()

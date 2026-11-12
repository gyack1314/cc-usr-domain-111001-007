"""调度层单元测试：不变量、老化、冷却、持久化、幂等与真实 HTTP。"""

import json
import os
import tempfile
import unittest
from urllib.request import urlopen

from scheduler.clock import ManualClock
from scheduler.model import (
    SendResult, TaskState, eff_priority, parse_retry_after,
)
from scheduler.scheduler import Scheduler, SchedulerConfig
from scheduler.scripted import ScriptedSender
from scheduler.sender import PluginSender, ThreadedSender
from source_site.__main__ import running as source_running
import sample_plugin


def config(capacity=3, rate=1, concurrency=None, max_retries=3):
    return SchedulerConfig(
        {"vendor": {"capacity": capacity, "rate": rate}},
        concurrency or {"north": 1, "south": 1},
        {"base": 1, "cap": 16, "max_retries": max_retries})


def task(tid, source="north", cost=1, priority=0, not_before=0,
         deadline=None, group="vendor", path=None):
    out = {"task_id": tid, "source": source, "quota_group": group,
           "path": path or "/" + source, "cost": cost, "priority": priority,
           "not_before": not_before}
    if deadline is not None:
        out["deadline"] = deadline
    return out


class InvariantMixin:
    """任意发送时刻都必须成立的额度与并发不变量。"""

    def assert_invariants(self, sch):
        snap = sch.snapshot()
        for group, bucket in sch.buckets.items():
            self.assertGreaterEqual(bucket.tokens, -1e-9)
            self.assertLessEqual(bucket.tokens, bucket.capacity + 1e-9)
        for source, n in snap["in_flight"].items():
            self.assertLessEqual(n, sch.config.source_concurrency[source])
        # 已花费额度（含失败不退款）+ 当前余量 <= 初始容量 + 累计补充。
        for group, bucket in sch.buckets.items():
            spent = snap["spent"].get(group, 0)
            refill = bucket.rate * snap["now"]
            self.assertLessEqual(
                spent + snap["tokens"][group],
                bucket.capacity + refill + 1e-6,
                msg="发生超发：花费+余量超过容量与补充之和")
        # 冷却中的组不得有在途新请求之外的排队偷发：在途只可能是冷却前发出的。
        for t in sch.tasks.values():
            if t.state == TaskState.IN_FLIGHT:
                self.assertGreaterEqual(t.attempts, 1)


class TokenAndPriorityTests(unittest.TestCase, InvariantMixin):
    def test_cost_over_capacity_rejected_immediately(self):
        sch = Scheduler(config(capacity=3), ScriptedSender(), ManualClock())
        accepted, record = sch.submit(task("big", cost=4))
        self.assertFalse(accepted)
        self.assertEqual(record.state, TaskState.REJECTED)
        self.assertEqual(record.attempts, 0)  # 未扣费
        self.assert_invariants(sch)

    def test_shared_bucket_across_sources_no_charge_while_queued(self):
        sender = ScriptedSender()
        sch = Scheduler(config(capacity=2, rate=0), sender, ManualClock())
        sch.submit(task("n", "north", cost=2))
        sch.submit(task("s", "south", cost=1))
        sch.tick()
        snap = sch.snapshot()
        self.assertEqual(snap["tokens"]["vendor"], 0)  # north 吃掉全部 2
        self.assertEqual(snap["states"]["s"], "queued")  # 同组 south 无法偷发
        self.assertEqual(sender.sent, [("n", "north")])
        self.assert_invariants(sch)

    def test_cancel_while_queued_is_free(self):
        sch = Scheduler(config(capacity=1, rate=0), ScriptedSender(),
                        ManualClock())
        sch.submit(task("a"))
        sch.submit(task("b"))
        sch.tick()
        self.assertTrue(sch.cancel("b"))
        self.assertEqual(sch.get_task("b").state, TaskState.FAILED)
        self.assertEqual(sch.snapshot()["spent"], {"vendor": 1})
        self.assertFalse(sch.cancel("a"))  # 已发送不可取消

    def test_priority_aging(self):
        self.assertEqual(eff_priority(0, 0, 0), 0)
        self.assertEqual(eff_priority(0, 0, 9.999), 0)
        self.assertEqual(eff_priority(0, 0, 10), 1)
        self.assertEqual(eff_priority(0, 0, 35), 3)  # 封顶 3
        self.assertEqual(eff_priority(2, 0, 20), 3)

    def test_urgent_jumps_queue_then_round_robin(self):
        sender = ScriptedSender()
        sch = Scheduler(config(capacity=3), sender, ManualClock())
        sch.submit(task("lo1", "north", priority=0))
        sch.submit(task("lo2", "south", priority=0))
        sch.submit(task("urgent", "south", priority=3))
        sch.tick()
        order = [name for name, _ in sender.sent]
        self.assertEqual(order, ["urgent", "lo1", "lo2"])  # 同级 north 先于 south

    def test_round_robin_alternates_sources(self):
        sender = ScriptedSender()
        sch = Scheduler(config(capacity=4), sender, ManualClock())
        for tid in ("n1", "s1", "n2", "s2"):
            sch.submit(task(tid, "north" if tid.startswith("n") else "south"))
        sch.tick()
        self.assertEqual([s for _, s in sender.sent],
                         ["north", "south", "north", "south"])

    def test_aging_makes_old_low_priority_overtake(self):
        sender = ScriptedSender()
        clock = ManualClock()
        sch = Scheduler(config(capacity=1, rate=0.1), sender, clock)
        sch.submit(task("old", "north", priority=0))
        sch.submit(task("fresh", "south", priority=1))
        sch.tick()  # fresh 先得 token
        clock.advance(10)
        sch.tick()  # old 老化到 1 级且更老，先于未来同级新任务
        self.assertEqual([n for n, _ in sender.sent], ["fresh", "old"])


class TimeAndConcurrencyTests(unittest.TestCase, InvariantMixin):
    def test_not_before_gates_dispatch(self):
        sender = ScriptedSender()
        clock = ManualClock()
        sch = Scheduler(config(capacity=2), sender, clock)
        sch.submit(task("later", not_before=5))
        sch.tick()
        self.assertEqual(sender.sent, [])
        clock.advance(5)
        sch.tick()
        self.assertEqual(sender.sent, [("later", "north")])

    def test_deadline_expires_unsent_and_frees_slot(self):
        sender = ScriptedSender({"hold": [("block",)]})
        clock = ManualClock()
        sch = Scheduler(config(capacity=1, rate=0), sender, clock)
        sch.submit(task("hold", "north"))
        sch.submit(task("late", "north", not_before=5, deadline=10))
        sch.tick()
        clock.advance(11)
        sch.tick()
        late = sch.get_task("late")
        self.assertEqual(late.state, TaskState.EXPIRED)
        self.assertEqual(late.attempts, 0)  # 过期未扣费、未占连接
        self.assert_invariants(sch)

    def test_source_concurrency_limit_with_inflight(self):
        sender = ScriptedSender({"a": [("block",)], "b": [("block",)]})
        sch = Scheduler(config(capacity=5, concurrency={"north": 1}),
                        sender, ManualClock())
        sch.submit(task("a", "north"))
        sch.submit(task("b", "north"))
        sch.tick()
        self.assertEqual(sch.snapshot()["in_flight"], {"north": 1})
        self.assertEqual(sch.get_task("b").state, TaskState.QUEUED)
        sender.resolve("a", SendResult.http(200))
        sch.complete("a", SendResult.http(200))
        sch.tick()
        self.assertEqual(sch.get_task("b").state, TaskState.IN_FLIGHT)
        self.assert_invariants(sch)


class CooldownTests(unittest.TestCase, InvariantMixin):
    def test_retry_after_seconds_cools_whole_group(self):
        sender = ScriptedSender({
            "n": [("http", 429, {"Retry-After": "4"}), ("http", 200)],
            "s": [("http", 200)],
        })
        clock = ManualClock()
        sch = Scheduler(config(capacity=3), sender, clock)
        sch.submit(task("n", "north", cost=1))
        sch.submit(task("s", "south", cost=1))
        sch.tick()
        # n 收到 429 -> 组冷却；冷却期 s 不能偷跑。
        self.assertEqual(sch.get_task("n").state, TaskState.QUEUED)
        clock.advance(3)
        sch.tick()
        self.assertEqual(sch.get_task("s").state, TaskState.QUEUED)
        clock.advance(1)
        sch.tick()  # 冷却结束，n 先重发（重试不重置老化）
        self.assertEqual(sch.get_task("n").state, TaskState.SUCCEEDED)
        sch.tick()
        self.assertEqual(sch.get_task("s").state, TaskState.SUCCEEDED)
        self.assert_invariants(sch)

    def test_retry_after_http_date(self):
        # 2026-01-01T00:00:10Z
        until = parse_retry_after("Thu, 01 Jan 2026 00:00:10 GMT",
                                  1767225600.0)
        self.assertEqual(until, 1767225610.0)
        self.assertIsNone(parse_retry_after("not-a-date", 0.0))
        self.assertIsNone(parse_retry_after(None, 0.0))
        self.assertEqual(parse_retry_after("30", 100.0), 130.0)

    def test_inflight_request_allowed_to_finish_during_cooldown(self):
        sender = ScriptedSender({"n1": [("block",)],
                                 "n2": [("http", 503, {})]})
        clock = ManualClock()
        sch = Scheduler(config(capacity=3, concurrency={"north": 2}),
                        sender, clock)
        sch.submit(task("n1", "north"))
        sch.submit(task("n2", "north"))
        sch.tick()
        # n2 的 503 无 Retry-After -> 指数退避 + 组冷却；n1 仍在途。
        self.assertEqual(sch.get_task("n1").state, TaskState.IN_FLIGHT)
        sch.complete("n1", SendResult.http(200))  # 在途允许结束
        self.assertEqual(sch.get_task("n1").state, TaskState.SUCCEEDED)

    def test_exponential_backoff_and_retry_budget_exhaustion(self):
        sender = ScriptedSender({"x": [("http", 503, {})]})  # 末项保持
        clock = ManualClock()
        sch = Scheduler(config(capacity=5, max_retries=2,
                               concurrency={"north": 1}), sender, clock)
        sch.submit(task("x", "north"))
        for attempt in range(3):  # 首次 + 2 次重试
            sch.tick()
            x = sch.get_task("x")
            if attempt < 2:
                self.assertEqual(x.state, TaskState.QUEUED)
                clock.advance(2 ** attempt)  # base=1: 1, 2
            else:
                self.assertEqual(x.state, TaskState.FAILED)
        self.assertEqual(sch.get_task("x").attempts, 3)
        self.assert_invariants(sch)

    def test_connection_error_shares_budget_but_no_group_cooldown(self):
        sender = ScriptedSender({"c": [("conn", "reset"), ("http", 200)]})
        clock = ManualClock()
        sch = Scheduler(config(capacity=3), sender, clock)
        sch.submit(task("c", "north"))
        sch.tick()
        self.assertEqual(sch.get_task("c").state, TaskState.QUEUED)
        self.assertEqual(sch.snapshot()["cooldowns"], {})  # 连接失败不冷却组
        clock.advance(1)
        sch.tick()
        self.assertEqual(sch.get_task("c").state, TaskState.SUCCEEDED)

    def test_failed_send_is_not_refunded(self):
        sender = ScriptedSender({"x": [("http", 400, {})]})  # 不可重试
        sch = Scheduler(config(capacity=2, rate=0), sender, ManualClock())
        sch.submit(task("x", cost=2))
        sch.tick()
        self.assertEqual(sch.get_task("x").state, TaskState.FAILED)
        self.assertEqual(sch.snapshot()["tokens"]["vendor"], 0)
        self.assertEqual(sch.snapshot()["spent"], {"vendor": 2})

    def test_cooldown_is_scoped_to_quota_group(self):
        cfg = SchedulerConfig(
            {"vendor": {"capacity": 2, "rate": 0},
             "other": {"capacity": 2, "rate": 0}},
            {"north": 2, "south": 2},
            {"base": 1, "cap": 16, "max_retries": 3})
        sender = ScriptedSender({"v": [("http", 429, {"Retry-After": "9"})],
                                 "o": [("http", 200)]})
        clock = ManualClock()
        sch = Scheduler(cfg, sender, clock)
        sch.submit(task("v", "north", group="vendor"))
        sch.submit(task("o", "south", group="other"))
        sch.tick()
        self.assertIn("vendor", sch.snapshot()["cooldowns"])
        # 另一组不受冷却影响，正常发出。
        self.assertEqual(sch.get_task("o").state, TaskState.SUCCEEDED)
        clock.advance(8)
        sch.tick()
        self.assertEqual(sch.get_task("v").state, TaskState.QUEUED)
        clock.advance(1)
        sch.tick()
        self.assertEqual(sch.get_task("v").state, TaskState.QUEUED)  # 等待重试

    def test_complete_is_idempotent(self):
        sender = ScriptedSender({"x": [("block",)]})
        sch = Scheduler(config(), sender, ManualClock())
        sch.submit(task("x"))
        sch.tick()
        self.assertTrue(sch.complete("x", SendResult.http(200)))
        self.assertFalse(sch.complete("x", SendResult.http(500)))
        self.assertFalse(sch.complete("ghost", SendResult.http(200)))


class PersistenceTests(unittest.TestCase, InvariantMixin):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="sched-test-")
        self.path = os.path.join(self.dir, "state.json")

    def test_bucket_not_refilled_after_restart_and_no_overspend(self):
        clock = ManualClock()
        sender = ScriptedSender({"x": [("block",)],
                                 "y": [("http", 200)]})
        sch = Scheduler(config(capacity=3, rate=1), sender, clock,
                        state_path=self.path)
        sch.submit(task("x", "north", cost=2))
        sch.submit(task("y", "south", cost=1))
        sch.tick()
        self.assertEqual(sch.snapshot()["tokens"]["vendor"], 0)

        clock2 = ManualClock(0)
        sch2 = Scheduler(config(capacity=3, rate=1), ScriptedSender(),
                         clock2, state_path=self.path)
        # 重启瞬间桶不回满；x 转待核实并释放连接。
        self.assertEqual(sch2.snapshot()["tokens"]["vendor"], 0)
        self.assertEqual(sch2.pending_verification(), ["x"])
        self.assertEqual(sch2.snapshot()["in_flight"], {})
        clock2.advance(2)
        sch2.verify_decision("x", "retry")
        sch2.tick()  # 补 2 个 token，x 重发恰好够，不超发
        self.assertEqual(sch2.get_task("x").state, TaskState.SUCCEEDED)
        self.assertGreaterEqual(sch2.snapshot()["tokens"]["vendor"], -1e-9)
        self.assert_invariants(sch2)

    def test_clock_rollback_mints_no_tokens(self):
        clock = ManualClock()
        sch = Scheduler(config(capacity=3, rate=1), ScriptedSender(), clock,
                        state_path=self.path)
        sch.submit(task("x", cost=1))
        sch.tick()
        rolled_back = ManualClock(-100)  # 墙钟比落盘时刻早 100 秒
        sch2 = Scheduler(config(capacity=3, rate=1), ScriptedSender(),
                         rolled_back, state_path=self.path)
        self.assertLessEqual(sch2.snapshot()["tokens"]["vendor"],
                             sch.snapshot()["tokens"]["vendor"] + 1e-9)

    def test_pending_verification_requires_plugin_decision(self):
        clock = ManualClock()
        sch = Scheduler(config(), ScriptedSender({"x": [("block",)]}),
                        clock, state_path=self.path)
        sch.submit(task("x"))
        sch.tick()
        sch2 = Scheduler(config(), ScriptedSender(), ManualClock(0),
                         state_path=self.path)
        self.assertEqual(sch2.get_task("x").state,
                         TaskState.PENDING_VERIFICATION)
        sch2.tick()  # 不会自动重发
        self.assertEqual(sch2.get_task("x").state,
                         TaskState.PENDING_VERIFICATION)
        self.assertTrue(sch2.verify_decision("x", "abandon"))
        self.assertEqual(sch2.get_task("x").state, TaskState.FAILED)

    def test_cooldown_survives_restart(self):
        clock = ManualClock()
        sch = Scheduler(config(), ScriptedSender(
            {"n": [("http", 429, {"Retry-After": "40"})]}),
            clock, state_path=self.path)
        sch.submit(task("n"))
        sch.tick()
        sch2 = Scheduler(config(), ScriptedSender(), ManualClock(10),
                         state_path=self.path)
        sch2.submit(task("s", "south"))
        sch2.tick()
        self.assertEqual(sch2.get_task("s").state, TaskState.QUEUED)


class HttpIntegrationTests(unittest.TestCase):
    def test_real_local_http_flow_with_plugin_sender(self):
        # 两个独立来源实例代表两个域名，共享同一 quota_group。
        with source_running() as (north_base, _north_src), \
                source_running() as (south_base, _south_src):
            clock = ManualClock()
            state_dir = tempfile.mkdtemp(prefix="sched-real-")
            sender = ThreadedSender(PluginSender(
                sample_plugin.fetch,
                {"north": north_base, "south": south_base}))
            sch = Scheduler(config(), sender, clock,
                            state_path=os.path.join(state_dir, "s.json"))
            tasks_path = os.path.join(os.path.dirname(__file__), "..",
                                      "sources", "tasks.json")
            with open(tasks_path, encoding="utf-8") as fh:
                for t in json.load(fh):
                    sch.submit(t)
            import time
            handles = {}
            for _ in range(60):
                for item in sch.tick():
                    if item.get("handle") is not None:
                        handles[item["task_id"]] = item["handle"]
                for tid, handle in list(handles.items()):
                    if handle.event.wait(0.02):
                        sch.complete(tid, handle.wait())
                        handles.pop(tid)
                if not any(s in ("in_flight", "queued")
                           for s in sch.snapshot()["states"].values()) \
                        and not handles:
                    break
                time.sleep(0.02)
                clock.advance(1)
            states = sch.snapshot()["states"]
            self.assertEqual(states["t1"], "succeeded")
            self.assertEqual(states["t2"], "succeeded")
            self.assertEqual(states["t3"], "succeeded")
            with urlopen(north_base + "/__calls", timeout=3) as resp:
                north_calls = json.load(resp)
            with urlopen(south_base + "/__calls", timeout=3) as resp:
                south_calls = json.load(resp)
            # north：t1 首次 429 + 冷却后 200，t3 再一次 200，共 3 次。
            self.assertEqual(
                [c["path"] for c in north_calls].count("/north"), 3)
            # south 仅 t2 一次；冷却期没有向同组另一域名偷跑额外请求。
            south_paths = [c["path"] for c in south_calls]
            self.assertEqual(south_paths.count("/south"), 1)
            # 重试事件确实由 Retry-After 驱动。
            retry_events = [e for e in sch.events
                            if e["kind"] == "retry_queued"
                            and e["task_id"] == "t1"]
            self.assertEqual(len(retry_events), 1)
            self.assertEqual(retry_events[0]["retry_ready"], 4)


if __name__ == "__main__":
    unittest.main()

"""与 source_site 的真实本地 HTTP 联调（无外网、无真实凭据）。

启动两个来源实例（不同端口，代表 north / south 两个域名），用
PluginSender 包装现有 sample_plugin.fetch，ThreadedSender 让请求在途；
调度时钟仍是可注入的 ManualClock，Retry-After 冷却由虚拟时钟推进，
脚本本身只在真实 HTTP 往返处做极短等待。

运行：python http_integration.py
"""

import json
import os
import sys
import tempfile
import time

# 隔离模式（python -I）不会把脚本目录加入 sys.path，这里显式引导。
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sample_plugin import fetch
from scheduler.clock import ManualClock
from scheduler.model import SendResult
from scheduler.scheduler import Scheduler, SchedulerConfig
from scheduler.sender import PluginSender, ThreadedSender
from source_site.__main__ import make_server
from source_site.source import Source
from threading import Thread

ROOT = os.path.dirname(os.path.abspath(__file__))


def start_source(port):
    server = make_server(Source(), port=port)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def drain(sch, clock, timeout=10.0):
    """推进虚拟时钟并收在途结果，直到没有在途/可派任务。"""
    deadline_real = time.monotonic() + timeout
    pending = {}
    while time.monotonic() < deadline_real:
        for item in sch.tick():
            handle = item.get("handle")
            if handle is not None:
                pending[item["task_id"]] = handle
        done = []
        for tid, handle in list(pending.items()):
            if handle.event.wait(0.05):
                done.append((tid, handle.wait()))
        for tid, result in done:
            sch.complete(tid, result)
            pending.pop(tid)
        snap = sch.snapshot()
        active = [s for s in snap["states"].values()
                  if s in ("in_flight", "queued", "pending_verification")]
        if not active and not pending:
            return
        clock.advance(1)
    raise TimeoutError("联调任务未在限定真实时间内收敛")


def main():
    with open(os.path.join(ROOT, "sources", "quota.json"),
              encoding="utf-8") as fh:
        raw = json.load(fh)
    config = SchedulerConfig(raw["groups"], raw["source_concurrency"],
                             raw["backoff"])
    north_srv = start_source(8090)
    south_srv = start_source(8091)
    state_dir = tempfile.mkdtemp(prefix="sched-http-")
    try:
        clock = ManualClock()
        plugin = PluginSender(fetch, {
            "north": "http://127.0.0.1:8090",
            "south": "http://127.0.0.1:8091",
        })
        sender = ThreadedSender(plugin)
        sch = Scheduler(config, sender, clock,
                        state_path=os.path.join(state_dir, "state.json"))
        with open(os.path.join(ROOT, "sources", "tasks.json"),
                  encoding="utf-8") as fh:
            for task in json.load(fh):
                sch.submit(task)

        print("真实 HTTP 联调：north=8090（首次 429 Retry-After:4），"
              "south=8091（200）")
        drain(sch, clock)

        for ev in sch.events:
            if ev["kind"] in ("dispatch", "cooldown", "succeeded",
                              "retry_queued"):
                print(f"  t={ev['at']:>5g}  {ev['kind']:<14} "
                      f"{ev['task_id']} {{{', '.join(f'{k}={v}' for k, v in ev.items() if k not in ('at','kind','task_id'))}}}")
        states = sch.snapshot()["states"]
        print("最终状态：", states)
        assert states["t1"] == "succeeded"   # 429 冷却后重试成功
        assert states["t2"] == "succeeded"
        assert states["t3"] == "succeeded"
        print("检查来源 /__calls 计数……")
        import urllib.request
        north_calls = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8090/__calls", timeout=3))
        south_calls = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8091/__calls", timeout=3))
        print(f"  north 实际请求 {len(north_calls)} 次，"
              f"south 实际请求 {len(south_calls)} 次")
        # north：t1 首次 429 + 冷却后 200，t3 再一次 200，共 3 次。
        assert len(north_calls) == 3 and len(south_calls) == 1
        print("联调通过：429 冷却期间没有向同组 south 偷跑，重试预算与"
              "真实 Retry-After 均生效。")
    finally:
        north_srv.shutdown(); north_srv.server_close()
        south_srv.shutdown(); south_srv.server_close()


if __name__ == "__main__":
    main()

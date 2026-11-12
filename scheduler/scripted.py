"""确定性脚本发送器：演示与单测专用，不做任何真实网络 IO。

按 (task_id 或 source) 预置脚本，每个脚本项可以是：
- SendResult：立即返回（同步发送器）；
- ("block",)：返回在途句柄，由测试显式 complete，用于演示冷却期间
  在途请求仍允许正常结束；
- ("http", status, headers) / ("conn", error)：简写形式。
同一脚本多次命中时末项保持，与 source_site 的 responses.json 语义一致。
"""

import threading
from dataclasses import dataclass

from .model import SendResult
from .sender import PendingHandle


@dataclass
class ScriptedHandle(PendingHandle):
    task_id: str
    event: threading.Event
    result: object = None

    def wait(self):
        self.event.wait()
        return self.result


class ScriptedSender:
    def __init__(self, script=None):
        # script: {task_id 或 source: [item, item, ...]}
        self.script = script or {}
        self._pos = {}
        self._lock = threading.Lock()
        self.handles = {}
        self.sent = []  # 实际发送顺序，供断言

    def send(self, task):
        key = task["task_id"] if task["task_id"] in self.script else task["source"]
        with self._lock:
            self.sent.append((task["task_id"], task["source"]))
            items = self.script.get(key, [])
            pos = self._pos.get(key, 0)
            self._pos[key] = pos + 1
            item = items[min(pos, len(items) - 1)] if items else ("http", 200)
        return self._materialize(task["task_id"], item)

    def _materialize(self, task_id, item):
        if isinstance(item, SendResult):
            return item
        kind = item[0]
        if kind == "block":
            handle = ScriptedHandle(task_id, threading.Event())
            self.handles[task_id] = handle
            return handle
        if kind == "http":
            return SendResult.http(item[1], headers=item[2] if len(item) > 2 else {})
        if kind == "conn":
            return SendResult.connection_error(item[1] if len(item) > 1 else "连接失败")
        raise ValueError(f"未知脚本项: {item!r}")

    def resolve(self, task_id, result):
        """结束一个 block 在途请求。"""
        handle = self.handles.pop(task_id)
        handle.result = result
        handle.event.set()

"""发送器接口与现有 sample_plugin 的适配器。

发送器是可注入接口，有两种形态：
- 同步返回 ``SendResult``：发送完成后立即结算；
- 返回 ``PendingHandle``：请求在途，稍后由 ``complete`` 结算，用于演示
  “冷却期间在途请求仍允许结束”。

``PluginSender`` 保持现有插件的调用方式 ``fetch(task, base_url)`` 不变，
插件解析器不需要任何修改；它只负责把插件的返回/异常翻译成 SendResult，
收到 HTTP 响应与不明确连接失败在此区分，但重试预算由调度器统一管理。
"""

import threading
from dataclasses import dataclass, field

from .model import SendKind, SendResult


class PendingHandle:
    """在途请求句柄：complete() 阻塞到发送器给出结果。"""

    def wait(self):
        raise NotImplementedError


@dataclass
class _FutureHandle(PendingHandle):
    event: threading.Event = field(default_factory=threading.Event)
    result: SendResult | None = None

    def complete(self, result):
        self.result = result
        self.event.set()

    def wait(self):
        self.event.wait()
        return self.result


class ThreadedSender:
    """把任意同步发送函数包装成在途句柄，由后台线程执行。"""

    def __init__(self, inner):
        self._inner = inner

    def send(self, task):
        handle = _FutureHandle()

        def run():
            try:
                handle.complete(self._inner.send(task))
            except Exception as exc:  # 发送器约定不抛异常，这里是最后防线
                handle.complete(SendResult.connection_error(exc))

        threading.Thread(target=run, daemon=True).start()
        return handle


class PluginSender:
    """适配 ``sample_plugin.fetch(task, base_url)`` 的发送器。

    base_urls 按 source 名映射到具体域名，例如
    ``{"north": "http://127.0.0.1:8090", "south": "http://127.0.0.1:8091"}``。
    fetch 的 (status, headers, body) 三元组翻译成 HTTP_RESPONSE；
    超时、连接拒绝等没有响应的异常翻译成 CONNECTION_ERROR。
    """

    def __init__(self, fetch, base_urls):
        self._fetch = fetch
        self._base_urls = dict(base_urls)

    def send(self, task):
        base_url = self._base_urls[task["source"]]
        try:
            status, headers, body = self._fetch(task, base_url)
        except Exception as exc:  # 没收到 HTTP 响应：不明确连接失败
            return SendResult.connection_error(exc)
        return SendResult.http(status, headers=headers, body=body)

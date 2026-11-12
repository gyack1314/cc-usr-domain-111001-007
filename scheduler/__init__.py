"""统一请求调度层：共享配额、优先级老化、组级冷却与持久化。"""

from .clock import Clock, ManualClock, SystemClock
from .model import (
    SendResult,
    TaskRecord,
    eff_priority,
    parse_retry_after,
)
from .scheduler import Scheduler, SchedulerConfig
from .sender import PendingHandle, PluginSender, ThreadedSender

__all__ = [
    "Clock",
    "ManualClock",
    "SystemClock",
    "SendResult",
    "TaskRecord",
    "eff_priority",
    "parse_retry_after",
    "Scheduler",
    "SchedulerConfig",
    "PendingHandle",
    "PluginSender",
    "ThreadedSender",
]

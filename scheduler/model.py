"""任务状态机、优先级老化与 Retry-After 解析。"""

import email.utils
from dataclasses import dataclass, field
from enum import Enum

# 原始优先级与有效优先级都在 0..3 之间。
MIN_PRIORITY = 0
MAX_PRIORITY = 3
# 每等待 10 逻辑秒提升一级。
AGE_STEP_SECONDS = 10.0


class TaskState(str, Enum):
    QUEUED = "queued"                  # 排队中，可被调度
    IN_FLIGHT = "in_flight"            # 已扣费、已交给发送器
    PENDING_VERIFICATION = "pending_verification"  # 重启后结果不明
    SUCCEEDED = "succeeded"
    FAILED = "failed"                  # 重试预算耗尽或不可重试响应
    EXPIRED = "expired"                # 超过 deadline 且从未发送
    REJECTED = "rejected"              # cost 超过组容量，入队即终态


class SendKind(str, Enum):
    HTTP_RESPONSE = "http_response"    # 收到了 HTTP 响应（含 4xx/5xx）
    CONNECTION_ERROR = "connection_error"  # 不明确的连接失败，没有响应


@dataclass
class SendResult:
    """发送器的统一结果。两种结果都消耗同一个重试预算。"""

    kind: SendKind
    status: int | None = None
    headers: dict = field(default_factory=dict)
    body: bytes = b""
    error: str = ""

    @classmethod
    def http(cls, status, headers=None, body=b""):
        return cls(SendKind.HTTP_RESPONSE, status=status,
                   headers=headers or {}, body=body or b"")

    @classmethod
    def connection_error(cls, error):
        return cls(SendKind.CONNECTION_ERROR, error=str(error))


@dataclass
class TaskRecord:
    task_id: str
    source: str
    quota_group: str
    path: str
    cost: float
    priority: int
    not_before: float
    deadline: float | None = None
    seq: int = 0                      # 入队序号，来源内 FIFO 依据
    state: TaskState = TaskState.QUEUED
    enqueued_mono: float = 0.0        # 老化计时起点（重试不重置）
    attempts: int = 0                 # 已发送次数（首次为 1）
    retry_ready_mono: float | None = None  # 任务级退避到期
    last_error: str = ""
    last_result: dict | None = None   # 最近一次结果，供插件诊断

    def effective_priority(self, now_mono):
        return eff_priority(self.priority, self.enqueued_mono, now_mono)


def eff_priority(base_priority, enqueued_mono, now_mono):
    """较低优先级每等待 10 秒提升一级，封顶 MAX_PRIORITY。"""
    waited = max(0.0, now_mono - enqueued_mono)
    aged = base_priority + int(waited // AGE_STEP_SECONDS)
    return min(MAX_PRIORITY, max(MIN_PRIORITY, aged))


def parse_retry_after(value, now_wall):
    """解析 Retry-After，支持 delta-seconds 与 HTTP-date，返回墙钟绝对秒。

    非法或缺失时返回 None，由调用方走指数退避。HTTP-date 按 UTC 解析。
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # delta-seconds：纯整数（RFC 9110 要求非负整数）。
    if text.isdigit():
        return now_wall + int(text)
    # HTTP-date，例如 Wed, 21 Oct 2026 07:28:00 GMT。
    try:
        parsed = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    return parsed.timestamp()  # parsedate_to_datetime 对 GMT 给出 aware 时间


def backoff_delay(attempt, base, cap):
    """指数退避：base * 2**(attempt-1)，封顶 cap。attempt 从 1 开始。"""
    return min(cap, base * (2 ** max(0, attempt - 1)))

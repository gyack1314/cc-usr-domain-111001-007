"""组级令牌桶：同 quota_group 跨 source 共享 token。

补充公式为标准令牌桶：

    tokens = min(capacity, tokens + rate * elapsed)

扣费只在请求真正准备发送时发生；排队、取消、过期都不扣费。已发送请求
之后无论成败都不退款（费用计入 ``spent``）。
"""

from dataclasses import dataclass


@dataclass
class TokenBucket:
    group: str
    capacity: float
    rate: float
    tokens: float
    last_refill_mono: float

    @classmethod
    def new(cls, group, capacity, rate, now_mono):
        return cls(group, float(capacity), float(rate), float(capacity), now_mono)

    def refill(self, now_mono):
        """按单调时钟补充 token；时钟回退（elapsed 为负）时不补任何 token。"""
        elapsed = now_mono - self.last_refill_mono
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + self.rate * elapsed)
        # 无论 elapsed 正负都推进基准，保证回退后不会在将来集中补发。
        self.last_refill_mono = now_mono

    def can_afford(self, cost):
        return cost <= self.capacity and self.tokens + 1e-9 >= cost

    def charge(self, cost):
        """仅在请求即将进入发送器时调用。"""
        if cost > self.capacity:
            raise ValueError("cost 超过组容量的任务必须在入队时拒绝")
        if self.tokens + 1e-9 < cost:
            raise ValueError("token 不足，不应扣费")
        self.tokens -= cost

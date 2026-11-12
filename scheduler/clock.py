"""可注入时钟与单调时钟/绝对时间的换算。

调度引擎内部只使用单调时间（逻辑秒），保证同一进程内时间不会倒退；
持久化只写绝对时间（UTC epoch 秒）。两者通过一组锚点换算：

    wall(mono) = anchor_wall + (mono - anchor_mono)

进程重启载入旧状态时，重新取一组锚点，持久化到现在的流逝时间按

    elapsed = max(0, now_wall - stored_wall)

计算并钳为非负：墙钟回退时流逝为 0，token 不会被凭空补充，冷却也只会
保守地延后结束。手册时钟让 wall 与 monotonic 同步推进，便于确定性演示。
"""

import time
from dataclasses import dataclass


class Clock:
    """时钟接口：monotonic 单调（逻辑秒），wall 为 UTC epoch 秒。"""

    def monotonic(self) -> float:
        raise NotImplementedError

    def wall(self) -> float:
        raise NotImplementedError


@dataclass
class ManualClock(Clock):
    """测试与演示用时钟，wall 与 monotonic 从同一逻辑原点一起推进。"""

    now: float = 0.0

    def monotonic(self) -> float:
        return self.now

    def wall(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("时钟只能向前推进")
        self.now += seconds


class SystemClock(Clock):
    """真实时钟。monotonic 用于进程内调度，wall 用于持久化换算。"""

    def monotonic(self) -> float:
        return time.monotonic()

    def wall(self) -> float:
        return time.time()

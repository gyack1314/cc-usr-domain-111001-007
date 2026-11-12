"""统一请求调度引擎。

职责（对应需求逐条实现）：
- 每个 quota_group 一个令牌桶（容量 + 每秒补充速率），同组跨 source 共享；
- 每个 source 有并发上限；只有真正进入发送器的请求才扣费，排队/取消/
  过期不收费，已发送后的失败成本不退；
- cost 大于组容量的任务入队即报告不可执行（REJECTED）；
- 同优先级来源轮转、来源内按入队顺序；低优先级每 10 秒老化一级，封顶 3；
- not_before / deadline 由可注入时钟驱动；过期且未发送即 EXPIRED，
  不再占用连接；
- 429/503 的 Retry-After（秒数或 HTTP-date）作用于整个 quota_group，
  在途请求允许结束；无合法 Retry-After 走配置指数退避，重试耗尽转失败；
- 发送器可注入，HTTP 响应与连接失败区分但共用同一重试预算；
- 队列、已花费额度、冷却期限全部持久化，重启不恢复满桶；
- 结果按 task_id 幂等接收；重启时在途任务转为待核实，由插件决定去留。
"""

import threading
from collections import defaultdict
from dataclasses import asdict

from .bucket import TokenBucket
from .clock import Clock, ManualClock
from .model import (
    MAX_PRIORITY,
    SendKind,
    SendResult,
    TaskRecord,
    TaskState,
    backoff_delay,
    parse_retry_after,
)
from .sender import PendingHandle
from .store import STATE_VERSION, read_state, write_state

RETRYABLE_STATUS = (429, 503)


class SchedulerConfig:
    def __init__(self, groups, source_concurrency, backoff):
        # groups: {group: {"capacity": n, "rate": r}}
        self.groups = groups
        self.source_concurrency = source_concurrency
        self.backoff = backoff  # {"base", "cap", "max_retries"}


class Scheduler:
    def __init__(self, config, sender, clock=None, state_path=None):
        self.config = config
        self.sender = sender
        self.clock: Clock = clock or ManualClock()
        self.state_path = state_path
        self._lock = threading.RLock()
        self._seq = 0
        self.tasks: dict[str, TaskRecord] = {}
        self.buckets: dict[str, TokenBucket] = {}
        self.in_flight: dict[str, int] = defaultdict(int)  # source -> 数量
        # group -> 冷却结束的墙钟绝对秒
        self.cooldown_until_wall: dict[str, float] = {}
        # 每个有效优先级一个来源轮转游标（source 名）
        self.rr_cursor: dict[int, str] = {}
        self.events: list[dict] = []  # 演示与诊断用事件流
        if state_path:
            self._load_state()
        else:
            for group, cfg in config.groups.items():
                self.buckets[group] = TokenBucket.new(
                    group, cfg["capacity"], cfg["rate"], self.clock.monotonic())

    # ------------------------------------------------------------------ 事件

    def _emit(self, kind, task_id, **detail):
        event = {"at": self.clock.monotonic(), "kind": kind,
                 "task_id": task_id, **detail}
        self.events.append(event)
        return event

    # ------------------------------------------------------------------ 提交

    def submit(self, task):
        """插件提交任务。返回 (accepted, TaskRecord)。

        cost 大于组容量时立即不可执行；相同 task_id 重复提交为幂等空操作。
        """
        with self._lock:
            tid = task["task_id"]
            if tid in self.tasks:
                return False, self.tasks[tid]
            group = task["quota_group"]
            if group not in self.config.groups:
                raise ValueError(f"未知 quota_group: {group}")
            if task["source"] not in self.config.source_concurrency:
                raise ValueError(f"未知 source: {task['source']}")
            capacity = self.config.groups[group]["capacity"]
            now = self.clock.monotonic()
            if task["cost"] > capacity:
                record = TaskRecord(
                    task_id=tid, source=task["source"], quota_group=group,
                    path=task.get("path", ""), cost=task["cost"],
                    priority=task["priority"],
                    not_before=task.get("not_before", 0),
                    deadline=task.get("deadline"), seq=self._next_seq(),
                    state=TaskState.REJECTED, enqueued_mono=now,
                    last_error=f"cost {task['cost']} 超过组容量 {capacity}")
                self.tasks[tid] = record
                self._emit("rejected", tid, group=group, cost=task["cost"],
                           capacity=capacity)
                self._save()
                return False, record
            record = TaskRecord(
                task_id=tid, source=task["source"], quota_group=group,
                path=task.get("path", ""), cost=task["cost"],
                priority=task["priority"],
                not_before=task.get("not_before", 0),
                deadline=task.get("deadline"), seq=self._next_seq(),
                enqueued_mono=now)
            self.tasks[tid] = record
            self._emit("submitted", tid, source=record.source,
                       group=group, cost=record.cost,
                       priority=record.priority)
            self._save()
            return True, record

    def _next_seq(self):
        self._seq += 1
        return self._seq

    def cancel(self, task_id):
        """排队中取消不收费；已发送任务不能取消。"""
        with self._lock:
            task = self.tasks.get(task_id)
            if task and task.state == TaskState.QUEUED:
                task.state = TaskState.FAILED
                task.last_error = "排队中取消，未扣费"
                self._emit("cancelled", task_id)
                self._save()
                return True
            return False

    # ------------------------------------------------------------------ 推进

    def tick(self):
        """推进一次调度：过期清理 -> 尽量多地派发就绪任务。返回事件列表。"""
        with self._lock:
            now = self.clock.monotonic()
            for bucket in self.buckets.values():
                bucket.refill(now)
            self._expire_due(now)
            dispatched = []
            while True:
                task = self._pick_next(now)
                if task is None:
                    break
                dispatched.append(self._dispatch(task, now))
            self._save()
            return dispatched

    def _expire_due(self, now):
        for task in self.tasks.values():
            if (task.state == TaskState.QUEUED and task.deadline is not None
                    and now > task.deadline):
                task.state = TaskState.EXPIRED
                task.last_error = f"deadline {task.deadline} 已过，未发送"
                self._emit("expired", task.task_id, deadline=task.deadline)

    def _group_open(self, group):
        until_wall = self.cooldown_until_wall.get(group)
        if until_wall is None:
            return True
        if self.clock.wall() >= until_wall:
            del self.cooldown_until_wall[group]
            return True
        return False

    def _pick_next(self, now):
        """按有效优先级降序选择；同级来源轮转、来源内 FIFO。"""
        queued = [t for t in self.tasks.values()
                  if t.state == TaskState.QUEUED
                  and now >= t.not_before
                  and (t.retry_ready_mono is None
                       or now >= t.retry_ready_mono)]
        if not queued:
            return None
        for level in range(MAX_PRIORITY, -1, -1):
            candidates = [t for t in queued
                          if t.effective_priority(now) == level]
            if not candidates:
                continue
            pick = self._round_robin(candidates, level)
            if pick is not None:
                return pick
        return None

    def _round_robin(self, candidates, level):
        """同级来源轮转：从游标之后第一个有可发任务的来源开始，
        来源内取 seq 最小者；无论该来源本次是否可发，游标都推进到它的
        下一个来源，保证同级下一次派发轮到别的域名。"""
        sources = sorted({t.source for t in candidates})
        start = self.rr_cursor.get(level)
        if start in sources:
            order = (sources[sources.index(start):]
                     + sources[:sources.index(start)])
        else:
            order = sources
        chosen = None
        for source in order:
            if chosen is None:
                same = [t for t in candidates if t.source == source]
                task = min(same, key=lambda t: t.seq)
                if self._is_dispatchable(task):
                    chosen = task
            # 游标推进到当前来源的下一个来源（环形）。
        head = order[0]
        self.rr_cursor[level] = sources[
            (sources.index(head) + 1) % len(sources)]
        return chosen

    def _is_dispatchable(self, task):
        if self.in_flight[task.source] >= \
                self.config.source_concurrency[task.source]:
            return False
        if not self._group_open(task.quota_group):
            return False
        return self.buckets[task.quota_group].can_afford(task.cost)

    def _dispatch(self, task, now):
        bucket = self.buckets[task.quota_group]
        bucket.charge(task.cost)              # 只有真正发送才扣费
        self.in_flight[task.source] += 1
        task.attempts += 1
        task.state = TaskState.IN_FLIGHT
        self._emit("dispatch", task.task_id, source=task.source,
                   group=task.quota_group, cost=task.cost,
                   attempt=task.attempts,
                   tokens_left=round(bucket.tokens, 6),
                   in_flight=self.in_flight[task.source])
        outcome = self.sender.send(self._plugin_view(task))
        if isinstance(outcome, PendingHandle):
            self._emit("in_flight", task.task_id, handle=id(outcome))
            return {"task_id": task.task_id, "handle": outcome}
        self._apply_result(task, outcome)
        return {"task_id": task.task_id, "result": outcome}

    # ------------------------------------------------------------------ 结果

    def complete(self, task_id, result):
        """结算在途任务结果。按 task_id 幂等：未知或非在途一律忽略。"""
        with self._lock:
            task = self.tasks.get(task_id)
            if task is None or task.state != TaskState.IN_FLIGHT:
                self._emit("result_ignored", task_id,
                           reason="未知任务或任务不在途")
                return False
            self._apply_result(task, result)
            self._save()
            return True

    def _apply_result(self, task, result):
        # 此刻一定在途：释放来源连接（在途期间连接一直被占用）。
        self.in_flight[task.source] = max(0, self.in_flight[task.source] - 1)
        task.last_result = {
            "kind": result.kind.value,
            "status": result.status,
            "error": result.error,
        }
        success = (result.kind == SendKind.HTTP_RESPONSE
                   and 200 <= (result.status or 0) < 300)
        if success:
            task.state = TaskState.SUCCEEDED
            self._emit("succeeded", task.task_id, status=result.status,
                       attempt=task.attempts)
            return
        retryable, retry_after = self._retry_plan(task, result)
        max_retries = self.config.backoff["max_retries"]
        if retryable and task.attempts <= max_retries:
            task.state = TaskState.QUEUED
            task.retry_ready_mono = self.clock.monotonic() + self._retry_delay(
                task, result, retry_after)
            self._emit("retry_queued", task.task_id,
                       attempt=task.attempts,
                       retry_ready=task.retry_ready_mono,
                       reason=result.kind.value)
        else:
            task.state = TaskState.FAILED
            task.last_error = (result.error or
                               f"HTTP {result.status}" if result.kind
                               == SendKind.HTTP_RESPONSE else result.error)
            self._emit("failed", task.task_id, attempt=task.attempts,
                       reason=task.last_error)

    def _retry_plan(self, task, result):
        """返回 (是否可重试, Retry-After 绝对墙钟秒或 None)。"""
        if result.kind == SendKind.CONNECTION_ERROR:
            # 不明确连接失败：可重试、共用预算，但不触发组级冷却。
            return True, None
        if result.kind == SendKind.HTTP_RESPONSE \
                and result.status in RETRYABLE_STATUS:
            raw = _header(result.headers, "Retry-After")
            until_wall = parse_retry_after(raw, self.clock.wall())
            if until_wall is None and raw is not None:
                self._emit("bad_retry_after", task.task_id, value=raw)
            # 冷却作用于整个 quota_group，同组另一域名也不能偷跑。
            if until_wall is not None:
                self.cooldown_until_wall[task.quota_group] = until_wall
                self._emit("cooldown", task.task_id,
                           group=task.quota_group, until_wall=until_wall,
                           source="retry-after")
            return True, until_wall
        return False, None

    def _retry_delay(self, task, result, retry_after_wall):
        if retry_after_wall is not None:
            return max(0.0, retry_after_wall - self.clock.wall())
        cfg = self.config.backoff
        return backoff_delay(task.attempts, cfg["base"], cfg["cap"])

    # ---------------------------------------------------------- 待核实与插件

    def verify_decision(self, task_id, action):
        """插件对待核实任务的决定：'retry' 安全重发或 'abandon' 放弃。

        重发任务重新排队，沿用原入队时间（老化不重置）、seq 与成本统计；
        放弃则置 FAILED。已脱离待核实状态的调用幂等忽略。
        """
        with self._lock:
            task = self.tasks.get(task_id)
            if task is None or task.state != TaskState.PENDING_VERIFICATION:
                return False
            if action == "retry":
                task.state = TaskState.QUEUED
                task.retry_ready_mono = None
                self._emit("verify_retry", task_id)
            elif action == "abandon":
                task.state = TaskState.FAILED
                task.last_error = "插件核实后放弃，未重发"
                self._emit("verify_abandon", task_id)
            else:
                raise ValueError("action 必须是 'retry' 或 'abandon'")
            self._save()
            return True

    def pending_verification(self):
        with self._lock:
            return [t.task_id for t in self.tasks.values()
                    if t.state == TaskState.PENDING_VERIFICATION]

    def get_task(self, task_id):
        with self._lock:
            return self.tasks.get(task_id)

    # ------------------------------------------------------------- 只读视图

    def snapshot(self):
        """不变量检查与演示用的一致性快照。"""
        with self._lock:
            now = self.clock.monotonic()
            return {
                "now": now,
                "tokens": {g: round(b.tokens, 9)
                           for g, b in self.buckets.items()},
                "spent": self._spent(),
                "in_flight": dict(self.in_flight),
                "cooldowns": dict(self.cooldown_until_wall),
                "states": {tid: t.state.value for tid, t in self.tasks.items()},
                "effective_priority": {
                    tid: t.effective_priority(now)
                    for tid, t in self.tasks.items()
                    if t.state == TaskState.QUEUED},
            }

    def _spent(self):
        """已花费额度 = 所有真正发送过的请求成本之和（失败不退款）。"""
        totals = defaultdict(float)
        for task in self.tasks.values():
            if task.attempts > 0:
                totals[task.quota_group] += task.cost * task.attempts
        return dict(totals)

    def _plugin_view(self, task):
        return {
            "task_id": task.task_id,
            "source": task.source,
            "quota_group": task.quota_group,
            "path": task.path,
            "cost": task.cost,
            "priority": task.priority,
            "not_before": task.not_before,
            "deadline": task.deadline,
        }

    # ------------------------------------------------------------- 持久化

    def _save(self):
        if not self.state_path:
            return
        now_wall = self.clock.wall()
        state = {
            "version": STATE_VERSION,
            "saved_wall": now_wall,
            "seq": self._seq,
            "rr_cursor": {str(k): v for k, v in self.rr_cursor.items()},
            "cooldown_until_wall": dict(self.cooldown_until_wall),
            "buckets": {
                group: {
                    "capacity": b.capacity,
                    "rate": b.rate,
                    "tokens": b.tokens,
                    "last_refill_wall": _mono_to_wall(
                        self.clock, b.last_refill_mono),
                }
                for group, b in self.buckets.items()},
            "in_flight": dict(self.in_flight),
            "tasks": {
                tid: {
                    **asdict(t),
                    "state": t.state.value,
                    "enqueued_wall": _mono_to_wall(self.clock, t.enqueued_mono),
                    "retry_ready_wall": (
                        _mono_to_wall(self.clock, t.retry_ready_mono)
                        if t.retry_ready_mono is not None else None),
                }
                for tid, t in self.tasks.items()},
        }
        write_state(self.state_path, state)

    def _load_state(self):
        state = read_state(self.state_path)
        now_mono = self.clock.monotonic()
        now_wall = self.clock.wall()
        if state is None:
            for group, cfg in self.config.groups.items():
                self.buckets[group] = TokenBucket.new(
                    group, cfg["capacity"], cfg["rate"], now_mono)
            return

        def elapsed(saved_wall):
            # 墙钟回退 -> 流逝钳为 0：不凭空补 token，不提前结束冷却。
            return max(0.0, now_wall - float(saved_wall))

        self._seq = int(state.get("seq", 0))
        self.rr_cursor = {int(k): v for k, v in
                          state.get("rr_cursor", {}).items()}
        self.cooldown_until_wall = {
            g: float(u) for g, u in
            state.get("cooldown_until_wall", {}).items() if u > now_wall}
        self.in_flight = defaultdict(int)

        for group, cfg in self.config.groups.items():
            saved = state["buckets"].get(group)
            if saved is None:
                self.buckets[group] = TokenBucket.new(
                    group, cfg["capacity"], cfg["rate"], now_mono)
                continue
            bucket = TokenBucket(
                group=group,
                capacity=float(saved["capacity"]),
                rate=float(saved["rate"]),
                tokens=float(saved["tokens"]),
                last_refill_mono=now_mono)
            # 用落盘后真实流逝的时间补一次，回退场景 elapsed=0。
            delta = elapsed(saved["last_refill_wall"])
            if delta > 0:
                bucket.tokens = min(bucket.capacity,
                                    bucket.tokens + bucket.rate * delta)
            self.buckets[group] = bucket

        for tid, raw in state["tasks"].items():
            task = TaskRecord(
                task_id=tid, source=raw["source"],
                quota_group=raw["quota_group"], path=raw.get("path", ""),
                cost=raw["cost"], priority=raw["priority"],
                not_before=raw["not_before"], deadline=raw.get("deadline"),
                seq=raw.get("seq", 0),
                state=TaskState(raw["state"]),
                attempts=raw.get("attempts", 0),
                last_error=raw.get("last_error", ""),
                last_result=raw.get("last_result"))
            task.enqueued_mono = now_mono - elapsed(raw["enqueued_wall"])
            if raw.get("retry_ready_wall") is not None:
                task.retry_ready_mono = now_mono - elapsed(
                    raw["retry_ready_wall"])
            if task.state == TaskState.IN_FLIGHT:
                # 结果未知：不能一律重发，转待核实并释放来源连接。
                task.state = TaskState.PENDING_VERIFICATION
                self._emit("recovered_unverified", tid)
            self.tasks[tid] = task
        self._save()  # 用新锚点重写，立即固化恢复后的状态


def _mono_to_wall(clock, mono):
    if mono is None:
        return None
    return clock.wall() + (mono - clock.monotonic())


def _header(headers, name):
    if not headers:
        return None
    lowered = {str(k).lower(): v for k, v in headers.items()}
    return lowered.get(name.lower())

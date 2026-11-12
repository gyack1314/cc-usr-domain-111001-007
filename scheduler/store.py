"""调度状态的原子 JSON 持久化。

持久化内容：队列、已花费额度、令牌桶余量、组冷却期限、来源轮转游标、
在途/待核实任务、任务结果与序列号。进程退出后重新载入不会得到满桶。

所有时间以 UTC 绝对秒（wall）落盘；单调时钟不能跨进程比较，载入时由
Scheduler 用新时钟锚点换算（见 clock.py 与 scheduler._load_state）。
写入采用临时文件 + os.replace 原子替换，避免半写文件损坏状态。
"""

import json
import os
import tempfile

STATE_VERSION = 1


def write_state(path, state):
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".scheduler-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def read_state(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        state = json.load(fh)
    if state.get("version") != STATE_VERSION:
        raise ValueError(
            f"不支持的状态版本 {state.get('version')!r}，期望 {STATE_VERSION}")
    return state

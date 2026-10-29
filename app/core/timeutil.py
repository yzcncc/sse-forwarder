"""时间工具（避免 models/events/store 互相 import）。"""
from __future__ import annotations

import time


def now_ms() -> float:
    return time.time() * 1000.0

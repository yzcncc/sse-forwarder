"""发布订阅事件总线 + 环形回放缓冲。

Dashboard 的 SSE 连接断线重连后按 after_seq 回放缺失事件；消费太慢的
订阅者直接踢掉（队列满），靠回放补齐，绝不阻塞发布方（转发路径）。
"""
from __future__ import annotations

import asyncio
import itertools
import uuid
from typing import Any

from .timeutil import now_ms


class EventBus:
    def __init__(self, buffer: int = 400) -> None:
        self._subs: dict[str, asyncio.Queue] = {}
        self._buffer: list[dict[str, Any]] = []
        self._buffer_size = buffer
        self._seq = itertools.count(1)

    def subscribe(self) -> tuple[str, asyncio.Queue]:
        sid = uuid.uuid4().hex
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subs[sid] = q
        return sid, q

    def unsubscribe(self, sid: str) -> None:
        self._subs.pop(sid, None)

    @property
    def subscriber_count(self) -> int:
        return len(self._subs)

    def publish(self, kind: str, payload: dict[str, Any]) -> None:
        evt = {"seq": next(self._seq), "kind": kind, "ts": now_ms(), "data": payload}
        self._buffer.append(evt)
        if len(self._buffer) > self._buffer_size:
            del self._buffer[: len(self._buffer) - self._buffer_size]
        dead = []
        for sid, q in self._subs.items():
            try:
                q.put_nowait(evt)
            except asyncio.QueueFull:
                dead.append(sid)
        for sid in dead:
            self._subs.pop(sid, None)

    def replay(self, after_seq: int = 0, limit: int = 200) -> list[dict[str, Any]]:
        out = [e for e in self._buffer if e["seq"] > after_seq]
        return out[-limit:]

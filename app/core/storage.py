"""状态中心：上游管理、会话存储、事件总线。

全内存 + 单事件循环，写入用 asyncio.Lock 保护，1~10 并发毫无压力。
持久化：原子写 .tmp 后 rename，JSON 格式；加载失败忽略使用默认。
抓取字节以 base64 保存，Dashboard 端再解码。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import traceback
import uuid
from typing import Any, Optional

from .analysis import analyze_stream, recent_sample
from .events import EventBus
from .models import (
    ChunkRecord, ErrorInfo, Session, Upstream,
    now_ms, ts_iso,
)

log = logging.getLogger("sse-forwarder.store")

# 逐跳头集合，使用 frozenset
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "trailers", "transfer-encoding", "upgrade",
}


class Store:
    def __init__(self, settings) -> None:
        self.settings = settings
        self.bus = EventBus(settings.event_buffer)
        self._lock = asyncio.Lock()
        self.upstreams: dict[str, Upstream] = {}
        self.active_upstream_id: Optional[str] = None
        self.sessions: dict[str, Session] = {}
        self.order: list[str] = []          # 会话 id，按创建顺序
        self.stats = {"requests": 0, "errors": 0, "bytes_upstream": 0, "started_at": now_ms()}

    # ---------------- 上游 ----------------
    def add_upstream(self, name: str, url: str, headers: dict[str, str] | None = None,
                     note: str = "", verify_tls: Optional[bool] = None,
                     follow_redirects: Optional[bool] = None,
                     upstream_id: Optional[str] = None,
                     rules: list[dict[str, Any]] | None = None) -> Upstream:
        uid = upstream_id or uuid.uuid4().hex[:12]
        up = Upstream(
            id=uid, name=name or url, url=url.rstrip("/"),
            headers=headers or {},
            rules=[dict(r) for r in (rules or [])],
            note=note, verify_tls=verify_tls, follow_redirects=follow_redirects,
        )
        self.upstreams[uid] = up
        if self.active_upstream_id is None:
            self.active_upstream_id = uid
        return up

    def get_active(self) -> Optional[Upstream]:
        if not self.active_upstream_id:
            return None
        return self.upstreams.get(self.active_upstream_id)

    def activate(self, uid: str) -> Upstream:
        up = self.upstreams.get(uid)
        if up is None:
            raise KeyError(f"上游不存在: {uid}")
        self.active_upstream_id = uid
        self.bus.publish("upstream_switched", {"upstream": up.to_dict()})
        self.save()
        return up

    def remove_upstream(self, uid: str) -> None:
        self.upstreams.pop(uid, None)
        if self.active_upstream_id == uid:
            self.active_upstream_id = next(iter(self.upstreams), None)
        self.bus.publish("upstream_removed", {"id": uid})
        self.save()

    # ---------------- 会话 ----------------
    def new_session(self, **kw: Any) -> Session:
        sid = uuid.uuid4().hex
        s = Session(id=sid, created_at=now_ms(), cancel_event=asyncio.Event(), **kw)
        self.sessions[sid] = s
        self.order.append(sid)
        self.stats["requests"] += 1
        self._evict()
        self.bus.publish("session_created", s.summary())
        return s

    def _evict(self) -> None:
        limit = self.settings.max_sessions
        while len(self.order) > limit:
            old = self.order.pop(0)
            s = self.sessions.pop(old, None)
            if s and s.state == "streaming":
                continue
        # 保底：仍有过多已结束会话时按时间清理
        if len(self.sessions) > limit * 2:
            for sid in sorted(self.sessions, key=lambda x: self.sessions[x].created_at)[: len(self.sessions) - limit]:
                if self.sessions[sid].state != "streaming":
                    self.sessions.pop(sid, None)
                    if sid in self.order:
                        self.order.remove(sid)

    def list_sessions(self, limit: int = 100, only_open: bool = False) -> list[dict[str, Any]]:
        items = [self.sessions[i] for i in reversed(self.order) if i in self.sessions]
        if only_open:
            items = [s for s in items if s.state in {"connecting", "streaming"}]
        return [s.summary() for s in items[:limit]]

    def open_sessions(self) -> list[Session]:
        return [s for s in self.sessions.values() if s.state in {"connecting", "streaming"}]

    # ---------------- 事件 ----------------
    def emit(self, kind: str, payload: dict[str, Any]) -> None:
        self.bus.publish(kind, payload)

    def add_chunk(self, s: Session, data: bytes, size: int,
                  src: bytes = b"") -> Optional[dict[str, Any]]:
        last_ts = s.chunks[-1].ts if s.chunks else s.created_at
        rec = ChunkRecord(
            seq=len(s.chunks) + 1,
            ts=now_ms(),
            delta_ms=now_ms() - last_ts,
            size=size,
            data=bytes(data),
            src=bytes(src) if src and src != data else b"",
            truncated=len(data) < size,
        )
        s.bytes_upstream += size
        self.stats["bytes_upstream"] += size
        over_budget = s.captured_bytes + len(data) > self.settings.max_capture_bytes
        if len(s.chunks) >= self.settings.max_chunks_per_session or over_budget:
            s.drop_count += 1
            rec.data = b""
            rec.truncated = True
        else:
            s.captured_bytes += len(rec.data)
            s.chunks.append(rec)

        # SSE 分析（取样）
        sample = recent_sample(s.chunks)
        s.sse_analysis = analyze_stream(sample)

        payload = rec.to_dict(self.settings.max_event_payload)
        payload["session_id"] = s.id
        payload["session_short_id"] = s.id[:8]
        payload["state"] = s.state
        payload["bytes_upstream"] = s.bytes_upstream
        self.emit("chunk", payload)
        return payload

    def add_error(self, s: Session | None, phase: str, exc: BaseException | None = None,
                  *, message: str = "", detail: dict[str, Any] | None = None,
                  fatal: bool = True, type_name: str = "") -> ErrorInfo:
        detail = dict(detail or {})
        tb = ""
        if exc is not None:
            type_name = type_name or type(exc).__name__
            message = message or f"{type(exc).__name__}: {exc}"
            detail.setdefault("exception_module", type(exc).__module__)
            detail.setdefault("exception_args", [repr(a) for a in exc.args])
            for attr in ("errno", "strerror", "filename", "code", "status_code"):
                v = getattr(exc, attr, None)
                if v is not None:
                    detail[attr] = str(v)
            cause = exc.__cause__ or exc.__context__
            if cause is not None and cause is not exc:
                detail["cause"] = f"{type(cause).__name__}: {cause}"
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        else:
            tb = detail.pop("traceback", "") or "".join(traceback.format_stack(limit=12))
        err = ErrorInfo(
            ts=now_ms(), phase=phase, type=type_name or "Error",
            message=message or "unknown error", detail=detail, tb=tb, fatal=fatal,
        )
        self.stats["errors"] += 1
        if s is not None:
            s.errors.append(err)
            if fatal:
                s.state = "error"

        payload = err.to_dict()
        if s is not None:
            payload["session_id"] = s.id
            payload["session_short_id"] = s.id[:8]
            payload["target_url"] = s.target_url
            payload["upstream_name"] = s.upstream_name
        self.emit("error", payload)
        log.error("[%s] %s phase=%s %s", (s.id[:8] if s else "-"), err.type, phase, err.message)
        return err

    def close_session(self, s: Session, reason: str, state: str = "done") -> None:
        if s.closed_at:
            return
        s.closed_at = now_ms()
        s.close_reason = reason
        if state:
            s.state = state
        s.sse_analysis = analyze_stream(recent_sample(s.chunks))
        self.emit("session_closed", s.summary())
        self._evict()

    # ---------------- 持久化 ----------------
    def save(self) -> None:
        path = self.settings.state_file
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            data = {
                "active_upstream_id": self.active_upstream_id,
                "upstreams": [u.to_dict() for u in self.upstreams.values()],
            }
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, path)
        except Exception as exc:
            log.warning("状态持久化失败: %s", exc)

    def load(self) -> None:
        path = self.settings.state_file
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for u in data.get("upstreams", []):
                self.add_upstream(
                    name=u.get("name", ""), url=u.get("url", ""), headers=u.get("headers") or {},
                    note=u.get("note", ""), verify_tls=u.get("verify_tls"),
                    follow_redirects=u.get("follow_redirects"), upstream_id=u.get("id"),
                    rules=u.get("rules") or [],
                )
            act = data.get("active_upstream_id")
            if act in self.upstreams:
                self.active_upstream_id = act
            log.info("已从 %s 恢复 %d 个上游", path, len(self.upstreams))
        except Exception as exc:
            log.warning("状态加载失败（忽略，使用默认配置）: %s", exc)
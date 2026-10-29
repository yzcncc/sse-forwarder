"""领域模型：上游、会话、分片、错误。

这些类型只描述数据，不依赖 Starlette/httpx，序列化（to_dict/summary/detail）
也集中在这里，供 API 层与事件流共用。
"""
from __future__ import annotations

import asyncio
import base64
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from .timeutil import now_ms


def ts_iso(ms: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000.0)) + ".%03d" % (ms % 1000)


# 逐跳头，转发时必须剔除（RFC 7230 6.1）
HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "trailers", "transfer-encoding", "upgrade",
})


@dataclass
class Upstream:
    id: str
    name: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    rules: list[dict[str, Any]] = field(default_factory=list)
    verify_tls: Optional[bool] = None          # None = 跟随全局配置
    follow_redirects: Optional[bool] = None
    note: str = ""
    created_at: float = field(default_factory=now_ms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "url": self.url,
            "headers": dict(self.headers),
            "rules": [dict(r) for r in self.rules],
            "verify_tls": self.verify_tls,
            "follow_redirects": self.follow_redirects,
            "note": self.note,
            "created_at": self.created_at,
            "created_at_text": ts_iso(self.created_at),
        }


@dataclass
class ChunkRecord:
    seq: int
    ts: float
    delta_ms: float                 # 距上一分片间隔
    size: int                       # 真实字节数
    data: bytes                     # 发给客户端的字节（可能被截断）
    src: bytes = b""                # 上游原文，仅在与 data 不同（发生过改写）时保存
    truncated: bool = False

    def to_dict(self, max_payload: int) -> dict[str, Any]:
        payload = self.data
        cut = len(payload) > max_payload
        if cut:
            payload = payload[:max_payload]
        return {
            "seq": self.seq,
            "ts": self.ts,
            "ts_text": ts_iso(self.ts),
            "delta_ms": round(self.delta_ms, 3),
            "size": self.size,
            "captured": len(self.data),
            "truncated": self.truncated or cut,
            "b64": base64.b64encode(payload).decode("ascii"),
            **({"src_b64": base64.b64encode(self.src[:max_payload]).decode("ascii")}
               if self.src else {}),
        }


@dataclass
class ErrorInfo:
    ts: float
    phase: str       # config / send_request / connect_* / read_response_headers / stream / client
    type: str        # 异常类名
    message: str
    detail: dict[str, Any] = field(default_factory=dict)
    tb: str = ""
    fatal: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "ts_text": ts_iso(self.ts),
            "phase": self.phase,
            "type": self.type,
            "message": self.message,
            "detail": self.detail,
            "traceback": self.tb,
            "fatal": self.fatal,
        }


@dataclass
class Session:
    id: str
    created_at: float
    method: str
    path: str
    query: str
    client: str
    upstream_id: str
    upstream_name: str
    target_url: str
    # ---- 请求侧 ----
    request_headers: list[tuple[str, str]] = field(default_factory=list)
    request_body: bytes = b""                  # 实际转发出去的（改写后）
    request_body_raw: bytes = b""              # 客户端原始 body（改写前），仅发生过改写时保存
    request_body_truncated: bool = False
    request_rewrites: list[dict[str, Any]] = field(default_factory=list)
    # ---- 改写侧 ----
    rewrite_rules: list[dict[str, Any]] = field(default_factory=list)   # 本次会话的规则快照
    response_rewrites: list[dict[str, Any]] = field(default_factory=list)
    # ---- 响应侧 ----
    response_status: Optional[int] = None
    response_reason: str = ""
    response_headers: list[tuple[str, str]] = field(default_factory=list)
    http_version: str = ""
    ttfb_ms: Optional[float] = None
    # ---- 流侧 ----
    chunks: list[ChunkRecord] = field(default_factory=list)
    errors: list[ErrorInfo] = field(default_factory=list)
    state: str = "connecting"   # connecting / streaming / done / error / killed
    closed_at: Optional[float] = None
    close_reason: str = ""
    bytes_upstream: int = 0     # 上游 -> 客户端
    bytes_client_body: int = 0  # 客户端 -> 上游（请求体）
    captured_bytes: int = 0
    drop_count: int = 0         # 超出抓取上限后丢弃的分片数
    sse_analysis: dict[str, Any] = field(default_factory=dict)
    cancel_event: Optional[asyncio.Event] = field(default=None, repr=False)

    # ---------------- 序列化 ----------------
    def summary(self) -> dict[str, Any]:
        ended = self.closed_at or now_ms()
        return {
            "id": self.id,
            "short_id": self.id[:8],
            "created_at": self.created_at,
            "created_at_text": ts_iso(self.created_at),
            "method": self.method,
            "path": self.path,
            "query": self.query,
            "client": self.client,
            "upstream_id": self.upstream_id,
            "upstream_name": self.upstream_name,
            "target_url": self.target_url,
            "state": self.state,
            "status": self.response_status,
            "http_version": self.http_version,
            "duration_ms": round(ended - self.created_at, 1),
            "ttfb_ms": round(self.ttfb_ms, 1) if self.ttfb_ms is not None else None,
            "bytes_upstream": self.bytes_upstream,
            "chunks": len(self.chunks),
            "errors": len(self.errors),
            "last_error": self.errors[-1].message if self.errors else None,
            "close_reason": self.close_reason,
            "request_rewrite_count": sum(x.get("count", 0) for x in self.request_rewrites),
            "response_rewrite_count": sum(x.get("count", 0) for x in self.response_rewrites),
            "rewrite_rules": self.rewrite_rules,
            "content_type": dict(self.response_headers).get("content-type", ""),
            "sse_analysis": self.sse_analysis,
        }

    def detail(self, max_payload: int, with_chunks: bool = True, tail: int = 0) -> dict[str, Any]:
        d = self.summary()
        d.update({
            "request_headers": [{"k": k, "v": v} for k, v in self.request_headers],
            "response_headers": [{"k": k, "v": v} for k, v in self.response_headers],
            "request_body": {
                "size": len(self.request_body),
                "truncated": self.request_body_truncated,
                "b64": base64.b64encode(self.request_body).decode("ascii"),
                "raw_size": len(self.request_body_raw),
                "raw_b64": base64.b64encode(self.request_body_raw).decode("ascii"),
            },
            "request_rewrites": self.request_rewrites,
            "response_rewrites": self.response_rewrites,
            "rewrite_rules": self.rewrite_rules,
            "errors": [e.to_dict() for e in self.errors],
            "captured_bytes": self.captured_bytes,
            "drop_count": self.drop_count,
            "closed_at": self.closed_at,
            "closed_at_text": ts_iso(self.closed_at) if self.closed_at else None,
        })
        if with_chunks:
            src = self.chunks[-tail:] if tail > 0 else self.chunks
            d["chunks"] = [c.to_dict(max_payload) for c in src]
        return d

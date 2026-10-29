"""HTTP 传输层：连接池、请求/响应头过滤、目标 URL 拼接。

转发原则：不解析 SSE、不缓冲、不改 Content-Type；逐跳头剔除。
"""
from __future__ import annotations

from typing import Optional, Sequence
from urllib.parse import urlsplit, urlunsplit

import httpx

from ..config import Settings
from ..core.models import HOP_BY_HOP, Upstream


class ClientPool:
    """按 (verify_tls, http2) 复用 AsyncClient，避免每请求握手开销。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._clients: dict[tuple[bool, bool], httpx.AsyncClient] = {}

    def get(self, verify_tls: bool, http2: bool) -> httpx.AsyncClient:
        key = (verify_tls, http2)
        c = self._clients.get(key)
        if c is None or c.is_closed:
            timeout = httpx.Timeout(
                connect=self.settings.connect_timeout,
                read=self.settings.read_timeout_or_none(),
                write=self.settings.write_timeout,
                pool=self.settings.pool_timeout,
            )
            c = httpx.AsyncClient(
                timeout=timeout,
                verify=verify_tls,
                http2=http2,
                follow_redirects=self.settings.follow_redirects,
                limits=httpx.Limits(max_connections=64, max_keepalive_connections=16),
            )
            self._clients[key] = c
        return c

    async def aclose(self) -> None:
        for c in list(self._clients.values()):
            try:
                await c.aclose()
            except Exception:
                pass
        self._clients.clear()


def build_target_url(upstream_url: str, path: str, query: str) -> str:
    """把客户端请求的 path/query 拼到上游 base url 后面（支持上游自带路径前缀）。"""
    b = urlsplit(upstream_url)
    base_path = b.path.rstrip("/")
    req_path = path if path.startswith("/") else "/" + path
    if not req_path.endswith("/") and base_path and path == "":
        req_path = "/"
    full_path = base_path + req_path if req_path != "/" or not base_path else (base_path or "/")
    if not full_path:
        full_path = "/"
    return urlunsplit((b.scheme, b.netloc, full_path, query, ""))


def filter_request_headers(raw: Sequence[tuple[bytes, bytes]], upstream: Upstream,
                           settings: Settings, target_host: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    client_host = ""
    for k, v in raw:
        name = k.decode("latin-1")
        low = name.lower()
        if low == "host":
            client_host = v.decode("latin-1")
            continue
        if low in HOP_BY_HOP or low == "content-length":
            continue
        if low == "accept-encoding" and settings.force_identity_encoding:
            continue
        out.append((name, v.decode("latin-1")))
        seen.add(low)
    # 默认把 Host 改写成上游 Host；PRESERVE_CLIENT_HOST=true 时保留客户端原始 Host
    out.append(("Host", client_host if (settings.preserve_client_host and client_host) else target_host))
    if settings.force_identity_encoding and "accept-encoding" not in seen:
        out.append(("Accept-Encoding", "identity"))
    for k, v in (upstream.headers or {}).items():
        out = [(n, val) for n, val in out if n.lower() != k.lower()]
        out.append((k, v))
    return out


def filter_response_headers(resp: httpx.Response, method: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for k, v in resp.headers.raw:
        name = k.decode("latin-1")
        low = name.lower()
        if low in HOP_BY_HOP:
            continue
        if low == "content-length" and method.upper() != "HEAD":
            continue     # 我们用 chunked 重新分帧，保留会与实际字节数冲突
        out.append((name, v.decode("latin-1")))
    return out


class BodyDecoder:
    """在线解压上游响应，仅当需要对压缩流做反向改写时启用。

    支持 gzip / deflate；br、zstd 等不支持（返回 None，此时放弃改写、原样透传）。
    """

    def __init__(self, obj, encoding: str) -> None:
        self._obj = obj
        self.encoding = encoding
        self.raw_in = 0
        self.raw_out = 0

    @staticmethod
    def make(encoding: str) -> Optional["BodyDecoder"]:
        enc = encoding.strip().lower()
        if enc in ("gzip", "x-gzip"):
            import zlib
            return BodyDecoder(zlib.decompressobj(zlib.MAX_WBITS | 16), enc)
        if enc == "deflate":
            import zlib
            return BodyDecoder(zlib.decompressobj(), enc)
        return None

    def decode(self, data: bytes) -> bytes:
        self.raw_in += len(data)
        out = self._obj.decompress(data)
        self.raw_out += len(out)
        return out

    def flush(self) -> bytes:
        try:
            out = self._obj.flush()
        except Exception:
            out = b""
        self.raw_out += len(out)
        return out
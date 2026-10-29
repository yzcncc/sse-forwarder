"""上游连通性逐层探测：DNS → TCP → TLS → HTTP，逐项报告 OK/FAIL/毫秒与明细。"""
from __future__ import annotations

import asyncio
import ssl
import time
from typing import Any, Optional
from urllib.parse import urlsplit

import httpx

from ..config import Settings
from ..http.transport import ClientPool, build_target_url


async def probe(settings: Settings, pool: ClientPool, url: str, path: str = "/__probe__",
                verify_tls: Optional[bool] = None) -> dict[str, Any]:
    """返回 report dict：{target_url, steps:[{step,ok,ms,detail}], ok, status?, error?}。"""
    target = build_target_url(url, path, "")
    report: dict[str, Any] = {"target_url": target, "steps": [], "ok": False}
    host = urlsplit(target).hostname
    port = urlsplit(target).port or (443 if urlsplit(target).scheme == "https" else 80)

    def step(name: str, ok: bool, ms: float, detail: Any = "") -> None:
        report["steps"].append({"step": name, "ok": ok, "ms": round(ms, 2), "detail": detail})

    # 1) DNS
    t = time.perf_counter()
    try:
        infos = await asyncio.get_event_loop().getaddrinfo(host, port)
        ips = sorted({i[4][0] for i in infos})
        step("dns", True, (time.perf_counter() - t) * 1000, ips)
    except Exception as exc:
        step("dns", False, (time.perf_counter() - t) * 1000, f"{type(exc).__name__}: {exc}")
        report["error"] = f"DNS 解析失败: {exc}"
        return report

    # 2) TCP
    t = time.perf_counter()
    try:
        fut = asyncio.open_connection(host, port)
        reader, writer = await asyncio.wait_for(fut, timeout=settings.connect_timeout)
        step("tcp", True, (time.perf_counter() - t) * 1000, f"{host}:{port} 连接成功")
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    except asyncio.TimeoutError:
        step("tcp", False, (time.perf_counter() - t) * 1000, f"连接超时（>{settings.connect_timeout}s）")
        report["error"] = "TCP 连接超时"
        return report
    except Exception as exc:
        step("tcp", False, (time.perf_counter() - t) * 1000, f"{type(exc).__name__}: {exc}")
        report["error"] = f"TCP 连接失败: {exc}"
        return report

    # 3) TLS（仅 https）
    if urlsplit(target).scheme == "https":
        t = time.perf_counter()
        verify = settings.verify_tls if verify_tls is None else verify_tls
        try:
            ctx = ssl.create_default_context()
            if not verify:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            fut = asyncio.open_connection(host, port, ssl=ctx, server_hostname=host)
            reader, writer = await asyncio.wait_for(fut, timeout=settings.connect_timeout)
            sock = writer.get_extra_info("ssl_object")
            step("tls", True, (time.perf_counter() - t) * 1000,
                 {"version": sock.version() if sock else None,
                  "cipher": sock.cipher() if sock else None,
                  "cert_expiry": sock.getpeercert().get("notAfter") if sock and sock.getpeercert() else None})
            writer.close()
        except Exception as exc:
            step("tls", False, (time.perf_counter() - t) * 1000, f"{type(exc).__name__}: {exc}")
            report["error"] = f"TLS 握手失败: {exc}"
            return report

    # 4) HTTP
    t = time.perf_counter()
    try:
        client = pool.get(settings.verify_tls, settings.http2)
        async with client.stream("GET", target,
                                 timeout=httpx.Timeout(connect=settings.connect_timeout,
                                                       read=5.0, write=5.0, pool=5.0),
                                 follow_redirects=False) as r:
            body = b""
            async for c in r.aiter_raw():
                body += c
                if len(body) > 4096:
                    break
            step("http", True, (time.perf_counter() - t) * 1000,
                 {"status": r.status_code, "reason": r.reason_phrase,
                  "http_version": r.http_version,
                  "content_type": r.headers.get("content-type"),
                  "headers": {k: v for k, v in r.headers.items()},
                  "body_preview": body[:512].decode("utf-8", "replace")})
            report["ok"] = r.status_code < 500
            report["status"] = r.status_code
    except Exception as exc:
        step("http", False, (time.perf_counter() - t) * 1000, f"{type(exc).__name__}: {exc}")
        report["error"] = f"HTTP 请求失败: {type(exc).__name__}: {exc}"
    return report

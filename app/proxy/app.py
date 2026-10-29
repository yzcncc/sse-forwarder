"""代理应用：把客户端请求原样转发到当前激活的上游。

一个请求的处理拆成清晰的阶段：
  入口校验 → 构造会话 → 改写规则 → 请求头/体 → 发上游 → 响应头/解压/反向改写 → 流式转发
连接/流错误的分类见 errors.py，流转发细节见 forward.py。
"""
from __future__ import annotations

import time
from typing import Any, Optional
from urllib.parse import urlsplit

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from ..config import Settings
from ..core.models import Upstream, ts_iso
from ..core.rewrite import RuleSet, StreamRewriter
from ..core.storage import Store
from ..http.transport import (
    BodyDecoder, ClientPool, build_target_url,
    filter_request_headers, filter_response_headers,
)
from .errors import classify_send
from .forward import ResponseStream, connect_fail

log = __import__("logging").getLogger("sse-forwarder.proxy")
ANY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]

# 无 body 的方法
NO_BODY_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "DELETE"})


def make_proxy_app(store: Store, settings: Settings, pool: ClientPool) -> Starlette:
    async def handle(request: Request) -> Response:
        upstream = store.get_active()
        client_addr = (request.headers["x-forwarded-for"].split(",")[0].strip()
                       if request.headers.get("x-forwarded-for")
                       else (request.client.host if request.client else "unknown"))

        # ---- 入口校验 ----
        if upstream is None:
            store.add_error(None, "config", message="未配置任何上游，请先在 Dashboard 添加并激活一个上游",
                            detail={"hint": "POST /api/upstreams 或在页面左侧添加"})
            return JSONResponse(
                {"error": "no_active_upstream",
                 "message": "转发器还没有可用的上游，请打开 Dashboard 配置并激活一个上游",
                 "dashboard_port": settings.dashboard_port},
                status_code=503)

        if request.method.upper() not in settings.allowed_method_set:
            return JSONResponse({"error": "method_not_allowed", "method": request.method}, status_code=405)

        upgrade = request.headers.get("upgrade", "").lower()
        if upgrade == "websocket":
            store.add_error(None, "config", message="收到 WebSocket 升级请求，本转发器只处理 HTTP/SSE 流",
                            detail={"path": request.url.path, "client": client_addr})
            return JSONResponse({"error": "websocket_not_supported"}, status_code=501)

        target = build_target_url(upstream.url, request.url.path, request.url.query)
        sess = store.new_session(
            method=request.method, path=request.url.path, query=request.url.query,
            client=client_addr, upstream_id=upstream.id, upstream_name=upstream.name,
            target_url=target,
        )

        # ---- 改写规则（跟着上游走：每个上游配自己的 key / 模型名映射）----
        ruleset = RuleSet.from_list(upstream.rules) if settings.rewrite_enabled else RuleSet()
        sess.rewrite_rules = ruleset.to_list()

        # ---- 请求头 ----
        raw_headers = [(k, v) for k, v in request.headers.raw]
        sess.request_headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in raw_headers]
        forwarded = filter_request_headers(raw_headers, upstream, settings, urlsplit(target).netloc)
        if ruleset.request_pairs:
            forwarded, h_hits = ruleset.apply_request_headers(forwarded)
            if h_hits:
                sess.request_rewrites.extend(h_hits)

        # ---- 请求体（流式场景通常为空；有体则整体读取后转发，同时抓取副本）----
        body = b""
        try:
            if request.method.upper() not in NO_BODY_METHODS:
                body = await request.body()
                cap = settings.max_request_body_capture
                sess.bytes_client_body = len(body)
                if ruleset.request_pairs and body:
                    if len(body) > settings.rewrite_max_body:
                        store.add_error(
                            sess, "send_request",
                            message=f"请求体 {len(body)} 字节超过改写上限 {settings.rewrite_max_body}，"
                                    f"本次跳过 body 改写（请求头照常改写）",
                            detail={"body_size": len(body), "limit": settings.rewrite_max_body},
                            fatal=False)
                    else:
                        new_body, b_hits = ruleset.apply_request(body)
                        if b_hits:
                            for h in b_hits:
                                h = dict(h)
                                h["in"] = "body"
                                sess.request_rewrites.append(h)
                            sess.request_body_raw = body[:cap]
                            body = new_body
                sess.request_body = body[:cap]
                sess.request_body_truncated = len(body) > cap
        except Exception as exc:
            store.add_error(sess, "send_request", exc,
                            detail={"stage": "读取客户端请求体失败", "target_url": target})
            store.close_session(sess, "request_body_error", "error")
            return JSONResponse({"error": "request_body_error", "detail": str(exc)}, status_code=400)

        # 需要做响应反向改写时，压缩流没法字面替换 —— 强制上游返回明文
        resp_rewrite = bool(ruleset.response_pairs)
        if resp_rewrite and settings.rewrite_force_identity:
            forwarded = [(k, v) for k, v in forwarded if k.lower() != "accept-encoding"]
            forwarded.append(("Accept-Encoding", "identity"))

        if sess.request_rewrites:
            store.emit("request_rewritten", {
                "session_id": sess.id, "session_short_id": sess.id[:8],
                "rewrites": sess.request_rewrites,
                "body_size_before": len(sess.request_body_raw) or sess.bytes_client_body,
                "body_size_after": len(body),
            })

        store.emit("request_sending", {
            "session_id": sess.id, "session_short_id": sess.id[:8], "method": request.method,
            "target_url": target, "upstream_name": upstream.name,
            "headers": [{"k": k, "v": v} for k, v in forwarded],
            "body_size": len(body), "rewrites": sess.request_rewrites,
            "ts_text": ts_iso(sess.created_at),
        })

        # ---- 发请求到上游 ----
        verify = settings.verify_tls if upstream.verify_tls is None else upstream.verify_tls
        follow = settings.follow_redirects if upstream.follow_redirects is None else upstream.follow_redirects
        client = pool.get(verify, settings.http2)
        req = client.build_request(request.method, target, headers=forwarded,
                                   content=body if body else None)

        t0 = time.perf_counter()
        upstream_resp: Optional[httpx.Response] = None
        try:
            upstream_resp = await client.send(req, stream=True, follow_redirects=follow)
        except Exception as exc:
            phase, human = classify_send(exc)
            if phase == "connect_timeout":
                human = f"连接上游超时（>{settings.connect_timeout}s）"
            elif phase == "dns":
                human = f"DNS 解析失败: {urlsplit(target).hostname}"
            return connect_fail(store, sess, settings, exc, phase, target, upstream, human)

        # ---- 响应头 ----
        sess.ttfb_ms = (time.perf_counter() - t0) * 1000
        sess.response_status = upstream_resp.status_code
        sess.response_reason = upstream_resp.reason_phrase or ""
        sess.http_version = upstream_resp.http_version or ""
        resp_headers = filter_response_headers(upstream_resp, request.method)

        # ---- 响应反向改写（上游 key/模型名 -> 本地值）----
        rewriter: Optional[StreamRewriter] = (
            StreamRewriter(ruleset, "response") if resp_rewrite else None)
        if rewriter is not None and not rewriter.enabled:
            rewriter = None

        decoder: Optional[BodyDecoder] = None
        cenc = (upstream_resp.headers.get("content-encoding") or "").strip().lower()
        if rewriter is not None and cenc not in ("", "identity"):
            decoder = BodyDecoder.make(cenc)
            if decoder is None:
                store.add_error(
                    sess, "read_response_headers",
                    message=f"上游响应是 {cenc} 压缩流，无法对其做字面反向替换，本次响应按原始字节透传",
                    detail={"content_encoding": cenc, "target_url": target,
                            "hint": "关闭 FORCE_IDENTITY_ENCODING=false 会让上游有机会返回压缩流；"
                                    "br/zstd 等编码不支持在线解压",
                            "note": "请求方向的改写不受影响，已正常执行"},
                    fatal=False)
                rewriter = None
            else:
                resp_headers = [(k, v) for k, v in resp_headers if k.lower() != "content-encoding"]
                store.add_error(
                    sess, "read_response_headers",
                    message=f"上游返回 {cenc} 压缩响应；为执行反向改写已在线解压，并以明文转发给客户端",
                    detail={"content_encoding": cenc, "target_url": target,
                            "note": "已移除 Content-Encoding 头，客户端收到的是解压后的等价内容"},
                    fatal=False)

        sess.response_headers = list(resp_headers)
        sess.state = "streaming"

        store.emit("response_headers", {
            "session_id": sess.id, "session_short_id": sess.id[:8],
            "status": sess.response_status, "reason": sess.response_reason,
            "http_version": sess.http_version,
            "headers": [{"k": k, "v": v} for k, v in resp_headers],
            "ttfb_ms": round(sess.ttfb_ms, 2), "target_url": target,
        })

        # 非致命告警：非 SSE / 错误状态码
        ctype = upstream_resp.headers.get("content-type", "")
        if "text/event-stream" not in ctype:
            store.add_error(sess, "read_response_headers",
                            message=f"上游 Content-Type 不是 text/event-stream，而是 {ctype or '(缺失)'}",
                            detail={"content_type": ctype, "target_url": target,
                                    "note": "仍然按原始字节透传，不做任何转换"},
                            fatal=False)
        if upstream_resp.status_code >= 400:
            store.add_error(sess, "read_response_headers",
                            message=f"上游返回错误状态码 {upstream_resp.status_code} {sess.response_reason}",
                            detail={"status": upstream_resp.status_code, "target_url": target,
                                    "note": "响应体仍原样透传给客户端"},
                            fatal=False)

        stream = ResponseStream(store, settings, sess, upstream_resp, target, ruleset,
                                rewriter, decoder, cenc)

        headers: dict[str, str] = {}
        for k, v in resp_headers:
            if k.lower() in headers:      # 同名头合并（如 Set-Cookie 用逗号会失真，这里保守处理）
                headers[k] = headers[k] + ", " + v
            else:
                headers[k] = v

        return StreamingResponse(
            stream.run(),
            status_code=upstream_resp.status_code,
            headers=headers,
            media_type=None,
        )

    async def health(_: Request) -> Response:
        up = store.get_active()
        return JSONResponse({
            "ok": True, "service": "sse-forwarder-proxy",
            "active_upstream": up.to_dict() if up else None,
            "open_sessions": len(store.open_sessions()),
            "stats": dict(store.stats),
        })

    routes = [
        Route("/__proxy_health__", health, methods=["GET"]),
        Route("/", handle, methods=ANY_METHODS),
        Route("/{path:path}", handle, methods=ANY_METHODS),
    ]
    app = Starlette(routes=routes)
    app.state.store = store
    return app

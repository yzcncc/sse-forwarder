"""Dashboard 应用：会话查询、上游管理、实时事件流（SSE）、上游探测。

所有路由带可选 token 鉴权（DASHBOARD_TOKEN）。路由清单见 README「HTTP API」。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Optional

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from ..config import Settings
from ..core.models import Session, ts_iso
from ..core.storage import Store
from ..http.transport import ClientPool
from .probe import probe
from .validation import err, norm_rules, validate_url

log = logging.getLogger("sse-forwarder.dashboard")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def make_dashboard_app(store: Store, settings: Settings, pool: ClientPool) -> Starlette:
    token = settings.dashboard_token

    def guard(request: Request) -> Optional[JSONResponse]:
        if not token:
            return None
        got = request.headers.get("x-dashboard-token") or request.query_params.get("token") or ""
        if got == token:
            return None
        return err(401, "Dashboard 鉴权失败：请通过 ?token= 或 X-Dashboard-Token 头访问")

    # ---------------- 页面 ----------------
    async def index(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    # ---------------- 状态 ----------------
    async def state(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        active = store.get_active()
        return JSONResponse({
            "ok": True,
            "settings": {
                "proxy_listen": f"{settings.proxy_host}:{settings.proxy_port}",
                "dashboard_listen": f"{settings.dashboard_host}:{settings.dashboard_port}",
                "force_identity_encoding": settings.force_identity_encoding,
                "http2": settings.http2,
                "follow_redirects": settings.follow_redirects,
                "verify_tls": settings.verify_tls,
                "connect_timeout": settings.connect_timeout,
                "read_timeout": settings.read_timeout,
                "idle_timeout": settings.idle_timeout,
                "max_capture_bytes": settings.max_capture_bytes,
                "max_chunk_capture": settings.max_chunk_capture,
                "preserve_client_host": settings.preserve_client_host,
                "rewrite_enabled": settings.rewrite_enabled,
                "rewrite_force_identity": settings.rewrite_force_identity,
                "rewrite_max_body": settings.rewrite_max_body,
            },
            "upstreams": [u.to_dict() for u in store.upstreams.values()],
            "active_upstream_id": store.active_upstream_id,
            "active_upstream": active.to_dict() if active else None,
            "stats": {**store.stats, "started_at_text": ts_iso(store.stats["started_at"])},
            "open_sessions": len(store.open_sessions()),
            "total_sessions": len(store.sessions),
            "subscribers": store.bus.subscriber_count,
            "server_time": ts_iso(time.time() * 1000),
        })

    # ---------------- 会话 ----------------
    async def list_sessions(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        limit = int(request.query_params.get("limit", "100"))
        only_open = request.query_params.get("open") in {"1", "true"}
        return JSONResponse({"ok": True, "sessions": store.list_sessions(limit=limit, only_open=only_open)})

    def _find(sid: str) -> Optional[Session]:
        if sid in store.sessions:
            return store.sessions[sid]
        for s in store.sessions.values():
            if s.id.startswith(sid):
                return s
        return None

    async def session_detail(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        s = _find(request.path_params["sid"])
        if s is None:
            return err(404, f"会话不存在: {request.path_params['sid']}")
        tail = int(request.query_params.get("tail", "0"))
        with_chunks = request.query_params.get("chunks", "1") not in {"0", "false"}
        return JSONResponse({"ok": True, "session": s.detail(settings.max_event_payload,
                                                             with_chunks=with_chunks, tail=tail)})

    async def kill_session(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        s = _find(request.path_params["sid"])
        if s is None:
            return err(404, f"会话不存在: {request.path_params['sid']}")
        if s.cancel_event:
            s.cancel_event.set()
        store.add_error(s, "stream", message="Dashboard 手动终止该会话",
                        detail={"action": "kill", "operator": request.client.host if request.client else "?"})
        return JSONResponse({"ok": True, "session": s.summary()})

    async def kill_all(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        n = 0
        for s in store.open_sessions():
            if s.cancel_event:
                s.cancel_event.set()
            n += 1
        return JSONResponse({"ok": True, "killed": n})

    async def clear_history(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        keep = {sid: s for sid, s in store.sessions.items() if s.state in {"connecting", "streaming"}}
        store.sessions = keep
        store.order = list(keep.keys())
        store.emit("history_cleared", {"kept_open": len(keep)})
        return JSONResponse({"ok": True, "kept_open": len(keep)})

    # ---------------- 上游 ----------------
    async def _body_json(request: Request) -> dict[str, Any]:
        try:
            data = await request.json()
            if not isinstance(data, dict):
                raise ValueError("请求体必须是 JSON 对象")
            return data
        except Exception as exc:
            raise ValueError(f"请求体不是合法 JSON: {exc}") from exc

    async def create_upstream(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        try:
            data = await _body_json(request)
        except ValueError as exc:
            return err(400, str(exc))
        url = (data.get("url") or "").strip()
        bad = validate_url(url)
        if bad:
            return err(400, bad, url=url)
        try:
            rules = norm_rules(data.get("rules"))
        except ValueError as exc:
            return err(400, str(exc))
        up = store.add_upstream(
            name=(data.get("name") or "").strip() or url,
            url=url,
            headers=data.get("headers") or {},
            note=data.get("note", ""),
            verify_tls=data.get("verify_tls"),
            follow_redirects=data.get("follow_redirects"),
            rules=rules,
        )
        if data.get("activate", True):
            store.activate(up.id)
        else:
            store.save()
        store.emit("upstream_created", up.to_dict())
        return JSONResponse({"ok": True, "upstream": up.to_dict(),
                             "active": store.active_upstream_id == up.id})

    async def update_upstream(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        uid = request.path_params["uid"]
        up = store.upstreams.get(uid)
        if up is None:
            return err(404, f"上游不存在: {uid}")
        try:
            data = await _body_json(request)
        except ValueError as exc:
            return err(400, str(exc))
        if "url" in data:
            bad = validate_url((data["url"] or "").strip())
            if bad:
                return err(400, bad, url=data.get("url"))
            up.url = data["url"].strip().rstrip("/")
        if "name" in data:
            up.name = data["name"] or up.url
        if "headers" in data:
            up.headers = data["headers"] or {}
        if "note" in data:
            up.note = data["note"]
        if "verify_tls" in data:
            up.verify_tls = data["verify_tls"]
        if "follow_redirects" in data:
            up.follow_redirects = data["follow_redirects"]
        if "rules" in data:
            try:
                up.rules = norm_rules(data["rules"])
            except ValueError as exc:
                return err(400, str(exc))
        store.save()
        store.emit("upstream_updated", up.to_dict())
        return JSONResponse({"ok": True, "upstream": up.to_dict()})

    async def delete_upstream(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        uid = request.path_params["uid"]
        if uid not in store.upstreams:
            return err(404, f"上游不存在: {uid}")
        name = store.upstreams[uid].name
        store.remove_upstream(uid)
        return JSONResponse({"ok": True, "deleted": uid, "name": name,
                             "active_upstream_id": store.active_upstream_id})

    async def activate_upstream(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        uid = request.path_params["uid"]
        try:
            data = await _body_json(request)
        except ValueError:
            data = {}
        kill_open = bool(data.get("kill_open", False))
        try:
            up = store.activate(uid)
        except KeyError as exc:
            return err(404, str(exc))
        killed = 0
        if kill_open:
            for s in store.open_sessions():
                if s.upstream_id != uid and s.cancel_event:
                    s.cancel_event.set()
                    killed += 1
        return JSONResponse({"ok": True, "active_upstream": up.to_dict(),
                             "killed_open_sessions": killed,
                             "note": "已建立的流仍走旧上游直到结束；勾选断开可让客户端重连到新上游"})

    async def test_upstream(request: Request) -> Response:
        """主动探测上游连通性，返回逐层（DNS/TCP/TLS/HTTP）的详细结果。"""
        g = guard(request)
        if g:
            return g
        uid = request.path_params.get("uid")
        try:
            data = await _body_json(request)
        except ValueError:
            data = {}
        url = (data.get("url") or "").strip()
        path = data.get("path") or "/__probe__"
        verify = None
        if uid:
            up = store.upstreams.get(uid)
            if up is None:
                return err(404, f"上游不存在: {uid}")
            url = url or up.url
            if up.verify_tls is not None:
                verify = up.verify_tls
        bad = validate_url(url)
        if bad:
            return err(400, bad, url=url)
        report = await probe(settings, pool, url, path, verify_tls=verify)
        store.emit("probe_result", report)
        return JSONResponse({"ok": report["ok"], "report": report})

    # ---------------- 实时事件流 ----------------
    async def events(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        after = int(request.query_params.get("after_seq", "0") or 0)

        async def gen():
            sid, q = store.bus.subscribe()
            try:
                snap = {
                    "kind": "snapshot",
                    "seq": 0,
                    "ts": time.time() * 1000,
                    "data": {
                        "sessions": store.list_sessions(limit=50),
                        "upstreams": [u.to_dict() for u in store.upstreams.values()],
                        "active_upstream_id": store.active_upstream_id,
                        "stats": store.stats,
                    },
                }
                yield "event: snapshot\ndata: " + json.dumps(snap, ensure_ascii=False) + "\n\n"
                for evt in store.bus.replay(after_seq=after, limit=200):
                    yield f"event: {evt['kind']}\nid: {evt['seq']}\ndata: " + \
                        json.dumps(evt, ensure_ascii=False) + "\n\n"
                while True:
                    try:
                        evt = await asyncio.wait_for(q.get(), timeout=15)
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
                        continue
                    yield f"event: {evt['kind']}\nid: {evt['seq']}\ndata: " + \
                        json.dumps(evt, ensure_ascii=False) + "\n\n"
            except asyncio.CancelledError:
                raise
            finally:
                store.bus.unsubscribe(sid)

        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache, no-transform",
                                          "X-Accel-Buffering": "no", "Connection": "keep-alive"})

    async def health(_: Request) -> Response:
        return JSONResponse({"ok": True, "service": "sse-forwarder-dashboard"})

    routes = [
        Route("/", index, methods=["GET"]),
        Route("/api/state", state, methods=["GET"]),
        Route("/api/health", health, methods=["GET"]),
        Route("/api/sessions", list_sessions, methods=["GET"]),
        Route("/api/sessions/clear", clear_history, methods=["POST"]),
        Route("/api/sessions/kill-all", kill_all, methods=["POST"]),
        Route("/api/sessions/{sid}", session_detail, methods=["GET"]),
        Route("/api/sessions/{sid}/kill", kill_session, methods=["POST"]),
        Route("/api/upstreams", create_upstream, methods=["POST"]),
        Route("/api/upstreams/{uid}", update_upstream, methods=["PUT"]),
        Route("/api/upstreams/{uid}", delete_upstream, methods=["DELETE"]),
        Route("/api/upstreams/{uid}/activate", activate_upstream, methods=["POST"]),
        Route("/api/upstreams/{uid}/test", test_upstream, methods=["POST"]),
        Route("/api/upstreams/test", test_upstream, methods=["POST"]),
        Route("/api/events", events, methods=["GET"]),
    ]
    app = Starlette(routes=routes)
    app.state.store = store
    return app

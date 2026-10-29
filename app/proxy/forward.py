"""流式转发：读上游 →（可选解压/反向改写）→ 逐块写给客户端。

核心原则（不可破坏）：
- 用 httpx aiter_raw() 拿未解码原始字节，不重新分帧
- 抓取副本给 Dashboard，但抓取失败绝不影响转发
- 「读上游」与「Dashboard 取消」「空闲超时」三者赛跑，上游静默时 kill 也能生效
- 流结束冲刷解压器/改写器尾巴，不能丢字节
"""
from __future__ import annotations

import asyncio
import socket
from typing import Any, AsyncIterator, Optional
from urllib.parse import urlsplit

import httpx
from starlette.responses import JSONResponse, Response

from ..config import Settings
from ..core.models import Session, Upstream, ts_iso
from ..core.rewrite import RuleSet, StreamRewriter
from ..core.storage import Store
from ..http.transport import BodyDecoder
from .errors import classify_stream, final_state


async def _anext(it: AsyncIterator[bytes]) -> bytes:
    return await it.__anext__()


def connect_fail(store: Store, sess: Session, settings: Settings,
                 exc: BaseException, phase: str, target: str, upstream: Upstream,
                 human: str) -> Response:
    """建连阶段失败：记录详细错误 + 返回 502（带人类可读原因）。"""
    detail: dict[str, Any] = {
        "phase": phase,
        "human_readable": human,
        "target_url": target,
        "upstream_id": upstream.id,
        "upstream_name": upstream.name,
        "upstream_base_url": upstream.url,
        "client": sess.client,
        "request_method": sess.method,
        "connect_timeout": settings.connect_timeout,
        "read_timeout": settings.read_timeout,
        "verify_tls": settings.verify_tls if upstream.verify_tls is None else upstream.verify_tls,
        "proxy_listen": f"{settings.proxy_host}:{settings.proxy_port}",
    }
    host = urlsplit(target).hostname
    detail["resolved_host"] = host
    try:
        infos = socket.getaddrinfo(host, urlsplit(target).port or (443 if urlsplit(target).scheme == "https" else 80))
        detail["dns_result"] = sorted({i[4][0] for i in infos})
    except Exception as dns_exc:
        detail["dns_result"] = f"解析失败: {type(dns_exc).__name__}: {dns_exc}"
    store.add_error(sess, phase, exc, detail=detail)
    store.close_session(sess, phase, "error")
    return JSONResponse({
        "error": "upstream_unreachable",
        "phase": phase,
        "message": human,
        "exception": f"{type(exc).__name__}: {exc}",
        "target_url": target,
        "detail": detail,
    }, status_code=502)


class ResponseStream:
    """一个会话的上游响应流：读取、(解压/改写)、抓取、转发、收尾。

    以异步生成器形式交给 StreamingResponse；生成器被关闭时（客户端断开）
    走 CancelledError/GeneratorExit 分支记录 client 阶段错误并正常收尾。
    """

    def __init__(self, store: Store, settings: Settings, sess: Session,
                 resp: httpx.Response, target: str, ruleset: RuleSet,
                 rewriter: Optional[StreamRewriter], decoder: Optional[BodyDecoder],
                 content_encoding: str) -> None:
        self.store = store
        self.settings = settings
        self.sess = sess
        self.resp = resp
        self.target = target
        self.ruleset = ruleset
        self.rewriter = rewriter          # 固定引用：finally 收集命中用
        self.active_rew = rewriter        # 可能在失败后置空
        self.active_dec = decoder
        self.cenc = content_encoding
        self.idle_to = settings.idle_timeout_or_none()
        self.cancel = sess.cancel_event

    async def run(self) -> AsyncIterator[bytes]:
        store, sess, settings = self.store, self.sess, self.settings
        it = self.resp.aiter_raw()
        closed_reason = "upstream_eof"
        read_task: Optional[asyncio.Task] = None
        cancel_task: Optional[asyncio.Task] = None
        try:
            while True:
                if self.cancel and self.cancel.is_set():
                    closed_reason = "killed_from_dashboard"
                    store.add_error(sess, "stream", message="会话被 Dashboard 手动终止",
                                    detail={"target_url": self.target}, fatal=True)
                    break
                # 「读上游」与「取消」「空闲超时」赛跑；idle_to=None 时无限等
                read_task = asyncio.ensure_future(_anext(it))
                cancel_task = (asyncio.ensure_future(self.cancel.wait())
                               if self.cancel is not None else None)
                watch = {read_task} if cancel_task is None else {read_task, cancel_task}
                done, _ = await asyncio.wait(watch, timeout=self.idle_to,
                                             return_when=asyncio.FIRST_COMPLETED)
                if cancel_task is not None and cancel_task in done:
                    read_task.cancel()
                    try:
                        await read_task
                    except (asyncio.CancelledError, StopAsyncIteration, Exception):
                        pass
                    closed_reason = "killed_from_dashboard"
                    store.add_error(sess, "stream", message="会话被 Dashboard 手动终止",
                                    detail={"target_url": self.target,
                                            "bytes_received": sess.bytes_upstream}, fatal=True)
                    break
                if cancel_task is not None:
                    cancel_task.cancel()
                if not done:      # 只有 read_task 未完成 => 空闲超时
                    read_task.cancel()
                    try:
                        await read_task
                    except (asyncio.CancelledError, StopAsyncIteration, Exception):
                        pass
                    closed_reason = "idle_timeout"
                    store.add_error(sess, "stream",
                                    message=f"上游空闲超过 {self.idle_to}s 没有任何数据，判定为卡死并断开",
                                    detail={"idle_timeout": self.idle_to, "target_url": self.target,
                                            "bytes_received": sess.bytes_upstream,
                                            "elapsed_ms": round(sess.ttfb_ms or 0, 1)})
                    break
                try:
                    chunk = read_task.result()
                except asyncio.TimeoutError:
                    closed_reason = "idle_timeout"
                    store.add_error(sess, "stream",
                                    message=f"上游空闲超过 {self.idle_to}s 没有任何数据，判定为卡死并断开",
                                    detail={"idle_timeout": self.idle_to, "target_url": self.target,
                                            "last_chunk_ago_ms": round(sess.ttfb_ms or 0, 1)})
                    break
                except StopAsyncIteration:
                    break
                except Exception as exc:
                    closed_reason, stage = classify_stream(exc)
                    detail: dict[str, Any] = {"stage": stage, "target_url": self.target,
                                              "bytes_received": sess.bytes_upstream}
                    if closed_reason == "read_timeout":
                        detail["read_timeout"] = settings.read_timeout
                    if isinstance(exc, OSError):
                        detail["errno"] = getattr(exc, "errno", None)
                    store.add_error(sess, "stream", exc, detail=detail)
                    break

                if not chunk:
                    continue
                src = chunk          # 上游原文
                emit = chunk         # 发给客户端的字节
                if self.active_dec is not None or self.active_rew is not None:
                    try:
                        src = self.active_dec.decode(chunk) if self.active_dec is not None else chunk
                        emit = self.active_rew.feed(src) if self.active_rew is not None else src
                    except Exception as exc:
                        store.add_error(
                            sess, "stream", exc,
                            message=f"响应解压/改写失败，本会话后续改为原样透传: {exc}",
                            detail={"target_url": self.target, "bytes_received": sess.bytes_upstream,
                                    "content_encoding": self.cenc,
                                    "rewrite_rules": self.ruleset.to_list()},
                            fatal=False)
                        self.active_dec = self.active_rew = None
                        src = emit = chunk
                if emit:
                    # 抓取副本（先抓后发，抓取异常不能影响转发）
                    try:
                        store.add_chunk(sess, emit[:settings.max_chunk_capture], len(emit),
                                        src=src[:settings.max_chunk_capture] if src != emit else b"")
                    except Exception as exc:
                        store.add_error(sess, "stream", exc,
                                        message=f"抓取分片到 Dashboard 失败（不影响转发）: {exc}",
                                        fatal=False)
                    yield emit

            # 上游读完了：把解压器/改写器里压着的尾巴吐出去，不能丢字节
            tail = b""
            try:
                if self.active_dec is not None:
                    tail += self.active_dec.flush()
                if self.active_rew is not None:
                    tail = ((self.active_rew.feed(tail) + self.active_rew.flush())
                            if tail else self.active_rew.flush())
            except Exception as exc:
                store.add_error(sess, "stream", exc,
                                message=f"冲刷响应改写缓冲失败，可能丢失末尾 {len(tail)} 字节: {exc}",
                                detail={"target_url": self.target})
            if tail:
                try:
                    store.add_chunk(sess, tail[:settings.max_chunk_capture], len(tail), src=b"")
                except Exception:
                    pass
                yield tail

        except (asyncio.CancelledError, GeneratorExit):
            closed_reason = "client_disconnected"
            for task in (read_task, cancel_task):
                if task is not None and not task.done():
                    task.cancel()
            store.add_error(sess, "client",
                            message="客户端提前断开连接（主动关闭 / 取消订阅 / 网络中断）",
                            detail={"bytes_sent": sess.bytes_upstream, "target_url": self.target,
                                    "note": "转发被 Starlette 取消，属正常现象之一"},
                            fatal=False)
            raise
        finally:
            if self.rewriter is not None:
                hits = self.rewriter.take_hits()
                if hits:
                    sess.response_rewrites = hits
                    store.emit("response_rewritten", {
                        "session_id": sess.id, "session_short_id": sess.id[:8],
                        "rewrites": hits, "target_url": self.target})
            try:
                await self.resp.aclose()
            except Exception as exc:
                store.add_error(sess, "stream", exc, message=f"关闭上游连接时出错: {exc}", fatal=False)
            store.close_session(sess, closed_reason, final_state(closed_reason))

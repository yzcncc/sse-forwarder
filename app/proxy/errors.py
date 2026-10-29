"""转发错误分类：异常 → (phase, closed_reason, 是否致命)。

集中在这里，转发路径只负责按分类结果记录与收尾。
"""
from __future__ import annotations

import socket
import ssl
from typing import Optional

import httpx

# closed_reason -> 会话最终 state 是否为 error
ERROR_REASONS = frozenset({
    "read_timeout", "upstream_protocol_error", "upstream_read_error",
    "upstream_network_error", "upstream_http_error", "upstream_unexpected_error",
    "idle_timeout", "killed_from_dashboard",
})


def classify_send(exc: BaseException) -> tuple[str, str]:
    """建立上游连接（client.send）阶段的异常 → (phase, 人类可读原因)。"""
    import asyncio
    if isinstance(exc, httpx.ConnectTimeout):
        return "connect_timeout", "连接上游超时"
    if isinstance(exc, httpx.ConnectError):
        return "connect_error", "无法建立到上游的 TCP/TLS 连接"
    if isinstance(exc, httpx.ReadTimeout):
        return "read_timeout", "等待上游响应头超时"
    if isinstance(exc, (ssl.SSLError, httpx.ProtocolError)):
        return "protocol_tls", "TLS/HTTP 协议错误"
    if isinstance(exc, socket.gaierror):
        return "dns", "DNS 解析失败"
    if isinstance(exc, OSError):
        return "network", "网络层错误（OSError）"
    if isinstance(exc, asyncio.TimeoutError):
        return "connect_timeout", "连接上游超时"
    return "unexpected_connect", "建立上游连接时发生未预期异常"


def classify_stream(exc: BaseException) -> tuple[str, str]:
    """流读取阶段的异常 → (closed_reason, 人类可读阶段名)。"""
    if isinstance(exc, httpx.RemoteProtocolError) or isinstance(exc, httpx.ProtocolError):
        return "upstream_protocol_error", "上游流被异常中断/协议错误"
    if isinstance(exc, httpx.ReadTimeout):
        return "read_timeout", "读取上游数据超时"
    if isinstance(exc, httpx.ReadError):
        return "upstream_read_error", "上游连接被重置"
    if isinstance(exc, httpx.HTTPError):
        return "upstream_http_error", "上游 HTTP 层错误"
    if isinstance(exc, OSError):
        return "upstream_network_error", "上游 socket 层错误"
    return "upstream_unexpected_error", "上游读取未预期异常"


def final_state(closed_reason: str) -> str:
    return "error" if closed_reason in ERROR_REASONS else "done"

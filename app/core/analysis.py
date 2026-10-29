"""非标 SSE 探测：只做"观察"，绝不改动转发内容。

识别常见非标情况：缺空行分隔、无 data: 前缀、CRLF/LF 混用、
裸 JSON 行、BOM、注释心跳行、非 UTF-8 字节等。
"""
from __future__ import annotations

from typing import Any


def analyze_stream(sample: bytes) -> dict[str, Any]:
    res: dict[str, Any] = {
        "sampled_bytes": len(sample),
        "has_bom": sample.startswith(b"\xef\xbb\xbf"),
        "crlf_count": sample.count(b"\r\n"),
        "bare_lf_count": sample.count(b"\n") - sample.count(b"\r\n"),
        "bare_cr_count": sample.count(b"\r") - sample.count(b"\r\n"),
        "blank_line_separators": 0,
        "data_lines": 0,
        "event_lines": 0,
        "id_lines": 0,
        "retry_lines": 0,
        "comment_lines": 0,
        "non_field_lines": 0,
        "json_like_lines": 0,
        "bare_json_lines": 0,
        "data_json_lines": 0,
        "non_utf8": False,
        "nonstandard": [],
    }
    try:
        text = sample.decode("utf-8")
    except UnicodeDecodeError:
        res["non_utf8"] = True
        text = sample.decode("utf-8", errors="replace")

    lines = text.split("\n")
    fields = {"data", "event", "id", "retry"}
    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            res["blank_line_separators"] += 1
            continue
        if line.startswith(":"):
            res["comment_lines"] += 1
            continue
        name = line.split(":", 1)[0].lower() if ":" in line else line.lower()
        if name == "data" and ":" in line:
            res["data_lines"] += 1
            body = line.split(":", 1)[1].lstrip(" ")
            if body[:1] in {"{", "["}:
                res["json_like_lines"] += 1
                res["data_json_lines"] += 1
        elif name == "event":
            res["event_lines"] += 1
        elif name == "id":
            res["id_lines"] += 1
        elif name == "retry":
            res["retry_lines"] += 1
        else:
            res["non_field_lines"] += 1
            stripped = line.strip()
            if stripped[:1] in {"{", "["}:
                res["json_like_lines"] += 1
                res["bare_json_lines"] += 1

    hints = res["nonstandard"]
    if res["bare_lf_count"] and res["crlf_count"]:
        hints.append("行结束符混用（同时存在 CRLF 与 LF）")
    if res["bare_cr_count"]:
        hints.append("存在裸 CR（老式 Mac 行尾），多数 SSE 客户端会解析异常")
    if res["data_lines"] == 0 and res["event_lines"] == 0 and res["non_field_lines"] > 0:
        hints.append("没有任何 data:/event: 字段，疑似裸 JSON / NDJSON / 自定义协议")
    if res["blank_line_separators"] == 0 and (res["data_lines"] + res["non_field_lines"]) > 1:
        hints.append("事件之间没有空行分隔，不符合 SSE 规范")
    if res["bare_json_lines"] > 0:
        hints.append(f"检测到 {res['bare_json_lines']} 行裸 JSON（没有 data: 前缀），标准 EventSource 会直接丢弃")
    if res["has_bom"]:
        hints.append("流开头带 UTF-8 BOM")
    if res["non_utf8"]:
        hints.append("包含非 UTF-8 字节，已用替换字符展示，请以 HEX 视图为准")
    return res


def recent_sample(chunks, limit: int = 256 * 1024) -> bytes:
    """取最近若干字节做非标探测，避免每次全量扫描。"""
    out = bytearray()
    for c in reversed(chunks):
        out[:0] = c.data
        if len(out) >= limit:
            break
    return bytes(out[-limit:])

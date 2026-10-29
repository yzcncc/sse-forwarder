"""Dashboard API 的请求校验工具。"""
from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlsplit

from starlette.responses import JSONResponse


def err(status: int, message: str, **kw: Any) -> JSONResponse:
    return JSONResponse({"ok": False, "error": message, **kw}, status_code=status)


def validate_url(url: str) -> Optional[str]:
    """返回错误描述；合法时返回 None。"""
    if not url:
        return "url 不能为空"
    p = urlsplit(url)
    if p.scheme not in {"http", "https"}:
        return f"只支持 http/https，收到 scheme={p.scheme or '(空)'}"
    if not p.hostname:
        return f"无法从 {url} 解析出主机名"
    return None


def norm_rules(raw: Any) -> list[dict[str, Any]]:
    """校验并规范化改写规则：[{label,src,dst,scope,enabled}]，src=本地值 dst=上游值。"""
    if raw in (None, ""):
        return []
    if not isinstance(raw, list):
        raise ValueError('rules 必须是数组，形如 [{"src":"sk-本地","dst":"sk-上游"}]')
    out: list[dict[str, Any]] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"rules[{i}] 必须是对象")
        src = str(item.get("src") or item.get("from") or item.get("local") or "")
        dst = str(item.get("dst") or item.get("to") or item.get("upstream") or "")
        scope = str(item.get("scope") or "both").lower()
        if scope not in ("both", "request", "response"):
            raise ValueError(f"rules[{i}].scope 只能是 both/request/response，收到 {scope}")
        if not src and not dst:
            continue                      # 整条空规则，忽略
        if bool(src) != bool(dst):
            raise ValueError(f"rules[{i}] 的本地值与上游值必须同时填写（只填了一个无法构成映射）")
        out.append({"label": str(item.get("label") or ""), "src": src, "dst": dst,
                    "scope": scope, "enabled": bool(item.get("enabled", True))})
    return out

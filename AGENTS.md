# AGENTS.md — SSE 转发器（sse-forwarder-cloud）

## 项目目的
调试用 SSE 反向代理（Python / Starlette / httpx）：**不解析 SSE**，按原始字节边界、顺序、编码原样转发；同时把流副本送到实时 Dashboard（事件流、请求/响应头、错误详情、key/模型名改写命中表）。支持大模型接口的本地 key/模型名 ⇄ 上游 key/模型名的双向字面替换。

功能基线见 `DOCS_FUNCTIONS.md`（重构前全部对外行为的验收清单，改功能前先对照它）。

## 目录结构（重构后的分层）
- `app/config.py` — 全部环境变量配置（Settings dataclass）
- `app/core/` — 领域层，不依赖 Starlette/httpx
  - `models.py` 上游/会话/分片/错误 dataclass + 序列化（summary/detail/to_dict）
  - `events.py` 发布订阅事件总线 + 环形回放缓冲
  - `storage.py` Store：上游管理、会话存储、抓取、错误记录、state.json 原子持久化
  - `analysis.py` 非标 SSE 探测（只观察不改动）
  - `rewrite.py` 字面替换规则 + StreamRewriter 滑动窗口（最长模式优先）
- `app/http/transport.py` — httpx ClientPool（按 verify_tls/http2 复用）、请求/响应头过滤（逐跳头剔除）、目标 URL 拼接、gzip/deflate 在线解压器
- `app/proxy/` — 代理应用
  - `app.py` 请求处理管线（入口校验→会话→改写→上游→响应头→流式转发）
  - `errors.py` 异常分类表：异常 → (phase, closed_reason)
  - `forward.py` ResponseStream：读上游/取消/空闲超时三者赛跑 + 尾巴冲刷
- `app/dashboard/` — Dashboard 应用
  - `app.py` 全部 API 路由 + SSE 事件流端点
  - `probe.py` DNS→TCP→TLS→HTTP 逐层探测
  - `validation.py` url/规则校验
  - `static/index.html` 前端（单文件 HTML，零依赖）
- `app/main.py` — 入口与装配（create_apps / serve），启动横幅
- `tests/run_tests.py` 端到端自测（86 项断言，不需要 Docker）
- `tests/test_rewrite.py` 改写引擎单测（33 项）
- `tools/fake_upstream.py` 裸 socket 假上游（精确控制字节与分片时机）
- `data/` 运行时状态，勿提交

## 常用命令
- 测试：`make test`（即 `python3 tests/run_tests.py`；改完代码必跑，再补跑 `python3 tests/test_rewrite.py`）
- 本地运行：`make run`（需先装 `requirements.txt`；端口默认代理 8788 / Dashboard 8787）
- 假上游：`make mock`（127.0.0.1:9000）
- Docker：`make build` / `make up`（compose = 转发器 + mock 上游；镜像默认端口 6778/6779）
- CI：`.github/workflows/docker.yml` 推 main 自动构建双架构（amd64/arm64）镜像到 GHCR

## 架构与改代码红线
- **绝对不要引入 SSE 解析/重新分帧**：转发路径用 `httpx` `aiter_raw()` 取未解码字节（不解 gzip、不分帧），用 Starlette `StreamingResponse` 逐块写回、不缓冲、不改 `Content-Type`。非标流必须逐字节原样透传。
- **改写是纯字面替换，不解析 JSON**：请求方向对全部请求头 + body 做 `src → dst`；响应方向自动反向。URL 的 path/query 原样透传，不做任何替换。
- **流式安全**：替换必须用滑动窗口保留「最长模式 - 1」字节尾巴，保证拆成 TCP 分片的模式也能命中、且不攒数据加延迟。
- **压缩流**：配了响应改写规则时强制 `Accept-Encoding: identity`；上游仍回 gzip/deflate 则在线解压后改写、移除 `Content-Encoding`；br/zstd 不支持时放弃响应改写、原样透传并记录原因。请求方向改写不受压缩影响。
- **抓取副本绝不影响转发**：抓取/分析/持久化的一切异常都要 catch 住。
- 规则与上游绑定：每个上游各配一套，切换上游即切换规则。初始规则用 `LOCAL_API_KEY` / `UPSTREAM_API_KEY` / `LOCAL_MODEL` / `UPSTREAM_MODEL` 种入。
- **流转发循环里「读上游 / Dashboard 取消 / 空闲超时」必须赛跑**（asyncio.wait），上游静默时 kill 才能生效；流结束要冲刷解压器/改写器尾巴。
- Dashboard 按「明文显示、不遮蔽」设计，改 UI 时不要加遮蔽。

## 约定
- 代码与注释、commit message 均为中文，保持一致。
- 无 linter/formatter 配置；遵循现有风格（dataclass 配置、类型注解、`from __future__ import annotations`）。
- 配置一律走环境变量（见 `app/config.py` 与 README 环境变量表），不要硬编码端口或路径。
- 分层依赖方向：`core` 不 import starlette/httpx；`proxy`/`dashboard` 依赖 `core` 与 `http`；`main` 只做装配。

## 敏感区
改 `app/proxy/forward.py`、`app/core/rewrite.py`、`app/http/transport.py`（头过滤）前先读 `README.md` 与 `DOCS_FUNCTIONS.md`（解释了每条红线的设计动机），改完必跑两套测试。

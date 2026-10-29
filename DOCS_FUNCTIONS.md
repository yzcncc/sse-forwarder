# 原项目功能说明（重构前的功能基线）

> 本文档描述重构前系统的**全部对外功能与行为**，是重构的验收基线：重构后所有条目必须保持等价。
> 来源：README、`tests/run_tests.py`（86 项断言）、`tests/test_rewrite.py`（33 项断言）、源码。

## 1. 项目定位

调试用 SSE 反向代理（"转发流而非转发 SSE"）：

- 客户端把请求打到**代理端口**，转发器把上游返回的字节**按原始边界、顺序、编码**写回客户端
- 同时抓一份副本送到**Dashboard 端口**，实时看到每个分片、请求/响应头、以及尽量详细的错误
- 面向大模型接口的核心能力：本地 key/模型名 ⇄ 上游 key/模型名 的双向字面改写
- 上游可在 Dashboard 随时增删、切换（点一下立即生效，对后续请求生效；已建立的流走旧上游直到结束）

```
客户端 ──HTTP/SSE──▶ 代理端口 ──原样字节流──▶ 上游（可随时切换）
                        └──抓取副本──▶ Dashboard（实时事件流 + 会话详情 + 错误详情）
```

## 2. 转发核心（不可破坏的红线）

1. **不解析 SSE**：`httpx` `aiter_raw()` 拿未解码原始字节（不解 gzip、不重新分帧），`StreamingResponse` 逐块写回，不缓冲、不改 `Content-Type`。SSE 规范一律不校验、不修正，只在 Dashboard 上给"观察结论"。
2. **帧边界保真**：上游一个 TCP 段吐 1 字节，客户端就收到 1 字节（自测第 4 项验证）。
3. **非 SSE 同样透传**：普通 JSON、二进制下载都行；非 `text/event-stream` 给一条非致命警告。
4. **抓取副本绝不影响转发**：抓取异常被 catch，超限只计数不存内容。
5. URL 的 path/query **原样透传**，支持上游 base url 自带路径前缀。

## 3. 请求改写（大模型接口用）

- 一条规则 = 一对值：`src`（本地值）→ `dst`（上游值），可选 `label` / `scope`（both/request/response）/ `enabled`
- **请求方向**：全部请求头 + body 做字面替换（`Authorization: Bearer sk-本地…` → 上游 key，body 里 model 名、嵌在 content 里的 key 都换）；改写后 Content-Length 与新 body 一致
- **响应方向**：自动反向 `dst → src`，客户端全程看不到上游凭据
- **纯字节字面替换，不解析 JSON**；最长模式优先；二进制安全（非法 UTF-8 也能替换）
- **流式安全**：滑动窗口保留「最长模式 - 1」字节尾巴，模式被拦腰拆成两个分片、甚至逐字节滴也能正确替换，且不会无限攒数据（无匹配时及时吐出）
- 规则跟着上游走：每个上游各配一套，切换上游即切换规则，互不影响
- 请求体超过 `REWRITE_MAX_BODY`（8MB）时跳过 body 改写（头照常），记录非致命告警
- `REWRITE_ENABLED=false` 完全透传不改写
- Dashboard 上 key 与 body 全部明文显示，不做遮蔽

## 4. 压缩流处理

- 配了响应改写规则时强制 `Accept-Encoding: identity`
- 上游仍返回 gzip/deflate → 在线解压后改写、以明文转发（移除 `Content-Encoding` 头，内容等价，Dashboard 记录原因）
- br/zstd 不支持在线解压 → 放弃响应改写、原样透传并记录原因；**请求方向改写始终不受影响**
- `FORCE_IDENTITY_ENCODING=false` 时 100% 字节级透传（可能是 gzip 流，Dashboard 用 HEX 看）

## 5. 请求头处理

- 逐跳头剔除（Connection/Keep-Alive/Transfer-Encoding/Upgrade 等，RFC 7230）
- 默认把 `Host` 改写为上游 Host；`PRESERVE_CLIENT_HOST=true` 保留客户端原始 Host
- `Content-Length` 剔除（转发侧重分帧）
- 上游附加请求头（JSON 对象配置）注入并覆盖同名头
- 客户端真实 IP 取 `X-Forwarded-For` 首段

## 6. 会话观测（Dashboard 后端数据）

每个客户端请求记录一个会话：

- 请求侧：方法/路径/query/客户端 IP、请求头（原始 + 转发的）、请求体（改写前 raw + 改写后，明文）
- 响应侧：状态码/reason/HTTP 版本、响应头、TTFB
- 流侧：逐分片记录 seq/时间戳/Δms/真实字节数/内容（base64 出 JSON），改写过的分片同时保存上游原文 `src_b64`
- 改写侧：本会话规则快照、请求方向命中表（规则/位置 headers 或 body/替换前后/次数）、响应方向命中表、请求体前后对照
- 错误侧：phase（config/send_request/connect_*/read_response_headers/stream/client）+ 异常类型 + 消息 + 结构化 detail + 完整 Python 堆栈 + fatal 标记
- 状态机：connecting → streaming → done / error（killed_from_dashboard 也算终止原因）
- 非标 SSE 探测（仅观察结论，不影响转发）：CRLF/LF/CR 计数、空行分隔数、data/event/id/retry/注释行数、非字段行、裸 JSON 行、BOM、非 UTF-8、以及非标提示列表（行尾混用/裸 CR/无 data: 字段/无空行分隔/裸 JSON/BOM/非 UTF-8）

**内存保护**：每会话最多 5000 分片 / 8MB 抓取，超出只累加 `drop_count`；会话历史上限 300（进行中的永不淘汰）。

## 7. 实时事件流（Dashboard 的 SSE）

- `EventBus` 发布订阅 + 环形回放缓冲（400 条），断线重连按 `after_seq` 回放不丢事件；消费太慢的订阅者被踢掉靠回放补齐
- 事件类型：`snapshot` `session_created` `request_sending` `request_rewritten` `response_headers` `chunk` `error` `session_closed` `response_rewritten` `upstream_created/updated/removed/switched` `probe_result` `history_cleared`
- 15s keepalive 注释行

## 8. HTTP API（Dashboard 背后的接口，全部带鉴权守卫）

```
GET    /                        Dashboard 页面（单文件 HTML）
GET    /api/state               配置、上游列表、激活项、统计
GET    /api/health              健康检查
GET    /api/sessions?limit&open 会话列表（摘要）
GET    /api/sessions/{id}?tail&chunks   会话详情（支持前缀匹配 id）
POST   /api/sessions/{id}/kill  终止会话
POST   /api/sessions/kill-all   终止所有进行中
POST   /api/sessions/clear      清历史（保留进行中）
POST   /api/upstreams           新增 {name,url,headers,note,rules,activate,verify_tls,follow_redirects}
PUT    /api/upstreams/{uid}     修改（部分字段）
DELETE /api/upstreams/{uid}     删除（删的是激活项则自动切到剩余第一个）
POST   /api/upstreams/{uid}/activate  切换 {kill_open}
POST   /api/upstreams/{uid}/test      逐层探测
GET    /api/events?after_seq    实时事件流
GET    /__proxy_health__        代理端口侧健康检查
```

- 鉴权：`DASHBOARD_TOKEN` 非空时需要 `?token=` 或 `X-Dashboard-Token` 头
- 规则校验：scope 只能 both/request/response；src/dst 必须成对；空规则忽略
- 逐层探测：DNS（getaddrinfo 解析出的 IP 列表）→ TCP（裸连接 + 耗时）→ TLS（仅 https，版本/密码套件/证书有效期）→ HTTP（GET /__probe__，状态码/响应头/512B body 预览），逐项 OK/FAIL/毫秒，结果同时推 `probe_result` 事件
- 上游不可达时代理返回 502 JSON（含 phase/message/exception/target_url/detail），detail 含 `dns_result`、`resolved_host`、上游名、超时配置等

## 9. 流生命周期管理

- `read_task`（读上游）与 `cancel_event`（Dashboard kill）、`idle_timeout` 三者赛跑——上游静默时 kill 也能立即生效
- 空闲超时（`IDLE_TIMEOUT`，0=关）：连接还在但不再吐数据 → 判定卡死断开
- 客户端提前断开（GeneratorExit）→ 记 `client` phase 非致命错误，取消后台读任务
- 流结束冲刷解压器/改写器尾巴（不能丢字节）
- 已建立连接走旧上游直到自然结束；`activate {kill_open:true}` 可主动断旧流

## 10. 配置（全部环境变量，容器内无需改代码）

端口、初始上游、超时（connect/read/write/pool/idle）、转发行为（force_identity/http2/follow_redirects/verify_tls/preserve_client_host/allowed_methods）、改写（enabled/force_identity/max_body/两组 seed 值）、抓取上限（sessions/chunks/bytes/chunk_capture/request_body_capture）、事件（buffer/max_event_payload）、持久化 `STATE_FILE`（原子写 .tmp 后 rename，加载失败忽略用默认）、鉴权 `DASHBOARD_TOKEN`、`LOG_LEVEL`。

初始规则可用 `LOCAL_API_KEY`/`UPSTREAM_API_KEY`/`LOCAL_MODEL`/`UPSTREAM_MODEL` 种入 state 文件。

## 11. 前端（单文件 HTML，零依赖）

三栏：左（上游增删改切 + 规则可视化编辑 + 逐层探测结果）· 中（会话列表，状态灯/错误数/字节数）· 右（详情 6 页签：数据流/请求/响应头/改写/错误/概览）。数据流页签支持文本/HEX 视图、合并连续小分片、自动滚动、暂停渲染、改写分片的「上游原文」对照。SSE 实时更新 + 断线重连；token 走 `?token=`。

## 12. 交付物

- Dockerfile：多阶段、非 root（uid 10001）、HEALTHCHECK（打 `/api/health`）、`/data` 卷；镜像端口 6778/6779
- docker-compose：转发器 + mock 假上游（同镜像跑 `tools.fake_upstream`）
- GitHub Actions：推 main 自动构建 amd64/arm64 双架构镜像推 GHCR
- `tools/fake_upstream.py`：裸 socket 假上游，路由 `/std /nonstd /binary /drip /plain /gzip /500 /404 /die /hang /echo /echo-b64 /llm /__probe__`（精确控制字节与分片时机，含把 model 拦腰拆成两个分片）

## 13. 行为级验收断言（重构必须保持）

来自 `tests/run_tests.py`（86 项）与 `tests/test_rewrite.py`（33 项），关键行为：

- 标准/非标/二进制/滴流/JSON 响应：**字节完全一致**
- 非标特征识别（缺空行/CRLF 混用/裸 JSON/非 UTF-8）
- 请求头透传、Host 改写、附加头注入
- 上游掐断 → fatal 错误 + phase=stream + bytes_received + 堆栈 + 已收数据完整转发
- 上游不可达 → 502 + 人类可读原因 + exception + dns_result + target_url/upstream_name/connect_timeout
- 切换上游立即生效；探测步骤序列为 dns/tcp/http
- 并发 6 路互不干扰
- kill 会话后客户端连接被关闭，close_reason=killed_from_dashboard
- gzip 字节级透传（force_identity=false 时）
- key/模型名双向改写（头 2 处 + body 2 处命中、Content-Length 一致、JSON 仍合法、跨分片拆断的模型名也换回、SSE 结构不破坏、上游原文分片保留）
- 上游之间规则隔离、`REWRITE_ENABLED=false` 纯透传
- Dashboard 首页可访问（含「SSE 转发器」字样）、事件流以 snapshot 开头

# 迁移设计文档：pi-a2a → Google A2A (Agent2Agent) 开放协议

> 状态：**已确认（决策已拍板）** — 据此实现。
> 规范依据：A2A Protocol v1.0.0（https://a2aproject.github.io/A2A/v1.0.0/specification/ ，normative: `spec/a2a.proto`）。
> 目标：用 A2A 的 **JSON-RPC 2.0 over HTTP** 传输取代当前自定义 wire protocol；保留 pi 使用体验（LAN-only、零配置发现、委派注入闭环、Node+Bun 双运行时、仅 `bonjour-service` 依赖）。

---

## 0. 已确认的四个关键决策

| # | 决策 | 结论 |
|---|------|------|
| DP0 | 协议绑定 | **JSON-RPC**（spec §9）。单端点 `POST /rpc`，PascalCase 方法名（`SendMessage`/`GetTask`）。 |
| DP2 | 数据模型 | **全部走 Task 模型**：每个 `SendMessage` 都返回 `Task`（wire 统一）。行为仍由 `metadata.kind` 决定注入策略。 |
| DP3 | 结果回传 | **push-notification（webhook）**，零延迟，语义最接近现状（入站推）。`GetTask` 保留作兜底（防 webhook 丢失）。 |
| DP7 | 兼容 | **硬切**：只支持 A2A v1.0，不兼容旧 proto=1。 |

其余（DP1 发现保留 mDNS、DP4 共享密钥作 Bearer、DP5 五工具签名不变、DP6 注入闭环保留、DP8 JSON store 保留）均按初稿结论。

---

## 1. 一句话定位

**每个 pi agent 同时是「A2A Server」和「A2A Client」**：
- **Server**：本地起 HTTP，暴露 `/.well-known/agent-card.json` + `POST /rpc`（JSON-RPC：`SendMessage` / `GetTask`）+ `POST /a2a/notify`（push-notification webhook 接收）。
- **Client**：要发消息/委派时，作为 client 调对方 server 的 JSON-RPC 方法。
- 对称性天然成立（双方都跑 server、都能当 client）。发现层（mDNS + presence）负责把 peerName → A2A endpoint URL。

---

## 2. 数据模型映射（全 Task 模型）

### 2.1 现状 `Message` 字段 → A2A 落点

| 现状字段 | A2A 落点 | 说明 |
|---|---|---|
| `id` | `Message.messageId` | 创建方生成 |
| `thread_id` | `Task.contextId` | 同一线程=同一 contextId（多 task 共享） |
| `reply_to` | contextId 续接 / `referenceTaskIds` | A2A 靠 contextId 续接 |
| `from_id`/`from_name` | Agent Card `name` + Bearer 鉴权身份 | from_name 来自对方 Card/mDNS |
| `to_name` | 对方 server endpoint URL（mDNS/presence 解析） | 非 wire 字段，=「调哪个 server」 |
| `subject` | `Message.metadata.subject` | A2A Message 无原生 subject |
| `body` | `Part{ text: body }`（单 TextPart） | parts 数组一个 TextPart |
| `kind` | `Message.metadata.kind` ∈ {message,request,result} | **server 据此决定注入 + 是否注册 push** |
| `direction` | 本地视角，不进 wire | store 保留 |
| `created_at` | `TaskStatus.timestamp`（ISO 8601 UTC） | A2A 要求 ISO 8601 |

### 2.2 `kind` × Task 行为矩阵（核心）

每个 `SendMessage` 都返回 Task，但生命周期由 `metadata.kind` 决定：

| `kind` | server 建 task 后 | 注入收件人会话？ | 注册 push（回传给发送方）？ | 发送方等待结果？ |
|---|---|---|---|---|
| **`message`**（闲聊） | 立即 **COMPLETED**（存 inbox + toast，task 作为「送达记录」） | ❌ | ❌ | ❌（同步 ack 即返回） |
| **`request`**（委派） | **WORKING**（存 inbox + 注入让 agent 处理） | ✅ | ✅（发送方注册 webhook） | ✅（等 push 回来的结果） |
| **`result`**（交付结果） | 立即 **COMPLETED**（存 inbox + 注入=自动接收） | ✅ | ❌ | ❌ |

> **要点**：全 Task 统一了 wire 形态（响应恒为 `{task}`），但 UX 分流仍在：只有 `request` 打扰+等待，`result` 自动接收，`message` 只通知。push-notification 仅 `request` 注册（发送方才需要等结果），因此「全 Task」不会让闲聊产生延迟或噪音。

### 2.3 结果回传（request → result）在 push 模型下

现状：BE 处理完 → 主动推 result 到 FE。
A2A push 模型：BE 是当初 request 的 **server**（task 由 BE 持有）。「回结果」= BE 把 task 置 COMPLETED + 挂 Artifact，然后 **POST push-notification 到 FE 在请求里注册的 webhook** → FE 接收 → 注入。零延迟、语义=现状入站推。

> 闭环（FE 发 request → BE 自动处理 → BE 回 result → FE 自动接收注入）**完全保留**，且延迟=现状（即时推）。

---

## 3. 时序图

### 3.1 闲聊消息（kind=message）— 同步 ack，无 push

```
FE (client)                              BE (server)
  POST /rpc SendMessage                    │
    message.metadata.kind="message"        │
    message.parts=[{text:body}]  ────────► │  校验 Bearer
                                           │  建 Task(COMPLETED)  ← 立即终态
                                           │  store inbox + inboundTask
                                           │  toast("📨 ...")   ← 仅通知，不注入
  {result:{ task:{ id, status:COMPLETED}}}◄│  返回 COMPLETED task（无 push 注册）
工具返回 "✅ 已发送给 BE"
```

### 3.2 委派闭环（kind=request → result，push 回传）★核心

```
FE (client)                BE (server)                 FE webhook (/a2a/notify)
  POST /rpc SendMessage       │
    metadata.kind="request"   │
    configuration.             │
      pushNotificationConfig:{ │
        url:"http://FE:port/a2a/notify",
        token:<secret>,        │
        authentication:{schemes:["Bearer"]} }
    parts=[{text:请求体}]  ───►│  建 Task(SUBMITTED→WORKING)
                              │  inboundTask.taskId + 存 pushConfig
                              │  store inbox
                              │  ★ pi.sendUserMessage(注入请求) ──► BE LLM 立即处理
  {result:{ task:{id,WORKING}}}◄│
                              │
工具返回 "✅ 已委派，task=XXX"   │  ...BE 处理中... (task=WORKING)
store.outboundTask[taskId]    │
  = {peer:BE, awaitPush:true} │  BE LLM 处理完 → a2a_reply(msgId, 结果)
                              │  → 查 inboundTask[msgId]
                              │  → Task 置 COMPLETED + Artifact{parts:[{text:结果}]}
                              │  → store sent(kind=result)
                              │
                              │  POST /a2a/notify       │
                              │   Authorization: Bearer │
                              │   X-A2A-Notification-Token
                              │   body:{ task:{ id,     │
                              │     status:COMPLETED,   │
                              │     artifacts:[{parts:[{text:结果}]}] } }
                              │  ─────────────────────► │  FE 校验 token
                              │                         │  ← 查 outboundTask[task.id]
                              │                         │  取 Artifact 文本
                              │                         │  本地构造 inbox(result)
                              │  ◄── 200 OK             │  ★ 注入 FE 会话
                              │                         │  移除 outboundTask
                              │  ◄──────────────────────│  （FE 无需手动 a2a_read）
```

### 3.3 a2a_reply 的两种分支

```
分支 A：原消息是我收到的 request（我是 server，持有 task 且 pushConfig 已存）
  → Task 置 COMPLETED + Artifact(body)
  → 按 pushConfig POST 到对方 webhook（对方自动接收注入）
  → 无需主动 SendMessage

分支 B：原消息是 message / 是我发出的（无我侧 task）
  → 我当 client，SendMessage 给对方 server
  → metadata.kind = (原是 request ? result : message)
  → 对方按 §2.2 矩阵处理
```

### 3.4 兜底：GetTask 防丢失

webhook 是 at-least-once、可能丢（FE 重启/网络抖动）。FE 维护 `outboundTasks`（awaitPush）。低频兜底扫描（默认每 15s）对**超过 30s 仍未收到 push** 的 task 主动 `GetTask`：若已 COMPLETED 则补注入、移除；避免结果永久丢失。正常路径不走 GetTask，零额外流量。

---

## 4. 各文件改动计划

### 4.1 `extensions/net.ts`（重写）

`Network` 类，三块职责：

**A2A Server 侧**（`node:http` 路由）：
- `GET /.well-known/agent-card.json` → Agent Card（§5）。
- `GET /health` → 保留（mDNS 续命探测，非 spec，无害）。
- `POST /rpc` → JSON-RPC 分发：
  - `SendMessage`：校验 Bearer → 读 `metadata.kind` + `configuration.pushNotificationConfig` → 建 Task（按 §2.2 决定 state + 注入 + 存 pushConfig）→ 返回 `{task}`。
  - `GetTask`：校验 Bearer → 返回本地 task 状态（兜底用）。
  - 未知方法 → `-32601`。
  - 错误用 spec §5.4 A2A 错误码（`-32001` TaskNotFound 等）+ `google.rpc.ErrorInfo` data。
- `POST /a2a/notify` → **push-notification 接收**：校验 token → 解析 `{task:{id,status,artifacts}}` → 查 `outboundTasks` → 终态则本地构造 inbox(result) + 注入 + 移除 → 200。

**A2A Client 侧**（`fetch`）：
- `send(peer, msg, cfg?)` → JSON-RPC `SendMessage`，POST `peer/rpc`，返回 Task。
- `getTask(peer, taskId)` → `GetTask`，返回 Task（兜底）。
- `getAgentCard(peer)` → GET `peer/.well-known/agent-card.json`（发现后缓存，校验 protocolVersion）。
- `notifyPeer(peerEndpoint, token, task)` → BE 完成 task 时 POST push 到对方 webhook。

**发现层**（基本不动）：
- mDNS TXT：`proto` 从 `"1"` → `"a2a"`，新增 `a2aver=1.0`。
- presence 文件同上。
- `onPeerUp` 后台 GET 一次 Agent Card（硬切：protocolVersion 非 1.0 直接忽略该 peer）。

**push 等待 + 兜底**：
- `outboundTasks`（store 里）记录 awaitPush 的 task。
- 兜底扫描器（默认 15s）：对 >30s 未收 push 的 task 调 `getTask` 补救。
- 离线 outbox 保留：对方离线时 `send` 失败 → 入 outbox，上线重投（语义同现状）。

### 4.2 `extensions/index.ts`（适配，签名不变）

- `startEngine` 的 Network hooks：
  - `onMessageReceived`（message/result 的通知+注入）几乎不变。
  - 新增 `onTaskReceived`（request 的注入）——抽出现有 request 分支。
- 5 工具 `execute` 内部改 A2A：
  - `a2a_send`：构造 A2A Message（subject→metadata、body→TextPart、kind→metadata）；`request` 额外附 `pushNotificationConfig`（url=自己 /a2a/notify、token=workspaceSecret）并把返回 taskId 存 `outboundTasks`。
  - `a2a_reply`：按 §3.3 分支——本地有 task 则 `completeInboundTask`+`notifyPeer`；否则 `send`。
  - `a2a_inbox`/`a2a_read`/`a2a_peers`：**不动**（读本地 store/peer 表）。

### 4.3 `extensions/store.ts`（小扩展）

- `Message` 加可选：`taskId?`、`contextId?`、`taskState?`。
- 新增两集合（落盘）：
  - `inboundTasks: { taskId, msgId, contextId, state, fromName, createdAt, artifactText?, pushConfig?:{url,token} }[]`
  - `outboundTasks: { taskId, msgId, contextId, peerName, peerEndpoint, createdAt, awaitPush, lastPushAt? }[]`
- 新方法：`addInboundTask / getInboundTask(taskId|msgId) / completeInboundTask(taskId, artifactText) / addOutboundTask / resolveOutboundTask(taskId) / getStaleOutbound(olderThanMs)`。
- 其余原样。

### 4.4 `extensions/config.ts` + `config.example.json`

`A2aConfig` 新增（均可选，有默认）：
- `rpcPath?`（默认 `/rpc`）、`agentCardPath?`（默认 `/.well-known/agent-card.json`）
- `notifyPath?`（默认 `/a2a/notify`）
- `pushBackstopMs?`（默认 30000）、`pushSweepMs?`（默认 15000）
- 其余字段不变。

### 4.5 `README.md` / `SKILL.md`

架构图重画（A2A JSON-RPC + Agent Card + push webhook）。HTTP endpoint 表换 A2A 方法表。消息类型表加「Task 行为」列。说明 result 现经 push 即时回传。

### 4.6 `scripts/be-daemon.ts`

复用新 `Network`。收到 `request` → 建 task → 立即 `completeInboundTask` + `notifyPeer`（罐头回复经 push 回 FE）。心跳/退出不变。

---

## 5. Agent Card（server 返回）

```jsonc
GET /.well-known/agent-card.json  →  200 application/json
{
  "name": "backend",                       // peerName
  "description": "writes the API",         // role
  "version": "1.0.0",
  "supportedInterfaces": [
    { "url": "http://<host>:<port>/rpc", "protocolBinding": "JSONRPC", "protocolVersion": "1.0" }
  ],
  "capabilities": { "streaming": false, "pushNotifications": true, "extendedAgentCard": false },
  "securitySchemes": {
    "ws": { "httpAuthSecurityScheme": { "scheme": "Bearer", "description": "shared workspace secret" } }
  },
  "securityRequirements": [ { "ws": [] } ],
  "defaultInputModes": ["text/plain"],
  "defaultOutputModes": ["text/plain"],
  "skills": [
    { "id": "pi-a2a", "name": "backend", "description": "writes the API", "tags": ["pi","agent","a2a"] }
  ]
}
```

> workspace 隔离仍由共享密钥在 Bearer 校验时保证；Card 不暴露 workspace。

---

## 6. JSON-RPC wire 示例

**SendMessage（委派，附 push 配置）：**
```jsonc
POST /rpc   Authorization: Bearer <secret>   Content-Type: application/json
{ "jsonrpc":"2.0", "id":1, "method":"SendMessage",
  "params": {
    "message": { "messageId":"msg_abc", "role":"ROLE_USER", "contextId":"ctx_fe1",
      "parts":[{"text":"<请求体>"}],
      "metadata": { "kind":"request", "subject":"登录接口签名", "from":"FE" } },
    "configuration": {
      "pushNotificationConfig": {
        "url": "http://192.168.1.5:54321/a2a/notify",
        "token": "<workspaceSecret>",
        "authentication": { "schemes": ["Bearer"] }
      }
    }
  } }
```
**响应（task WORKING）：**
```jsonc
{ "jsonrpc":"2.0", "id":1,
  "result": { "task": { "id":"task_007", "contextId":"ctx_be1",
    "status": { "state":"TASK_STATE_WORKING", "timestamp":"2026-06-19T10:00:00.000Z" } } } }
```

**push-notification（BE 完成后 POST 给 FE webhook）：**
```jsonc
POST /a2a/notify   Authorization: Bearer <secret>   X-A2A-Notification-Token: <secret>
{ "task": { "id":"task_007", "contextId":"ctx_be1",
    "status": { "state":"TASK_STATE_COMPLETED", "timestamp":"..." },
    "artifacts": [ { "artifactId":"art_1", "parts":[{"text":"<结果正文>"}] } ] } }
```
→ FE 校验 token → 查 outboundTasks[task_007] → 注入 → `200 OK`。

**GetTask（兜底）：**
```jsonc
POST /rpc   { "jsonrpc":"2.0", "id":2, "method":"GetTask", "params": { "id":"task_007" } }
```

**错误：**
```jsonc
{ "jsonrpc":"2.0", "id":2,
  "error": { "code":-32001, "message":"Task not found",
    "data":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"TASK_NOT_FOUND","domain":"a2a-protocol.org"}] } }
```

> v1.0 方法名 PascalCase。不兼容旧 v0.3 `message/send`（硬切）。

---

## 7. 约束达成核对

| 约束 | 达成 |
|---|---|
| 依赖最少（仅 bonjour-service） | A2A 纯 JSON-RPC，`node:http` + 全局 `fetch` 手写，**不引入任何 SDK**。 |
| Node + Bun 双运行时 | `createRequire` 取 bonjour-service；`fetch`/`node:http` 两运行时都支持。 |
| LAN-only（`0.0.0.0`、不加固） | 保持。push-notification webhook 走 LAN 内对端地址，不暴露公网。 |
| 不破坏委派注入闭环 | §3.2 时序证明闭环保留；result 经 push 即时回传，延迟≈现状。 |

---

## 8. 风险与取舍

1. **R1 push 丢失**：webhook at-least-once、可能丢（FE 重启/抖动）。用 §3.4 兜底扫描（>30s 未收 push 则 GetTask 补救）覆盖。正常路径零额外流量。
2. **R2 metadata.kind 扩展**：用 metadata 让 server 决定注入，属合法扩展但偏离"A2A server 必须处理每条 message"。仅影响 pi-a2a↔pi-a2a；对第三方 A2A agent 退化为"全当 request"。
3. **R3 单 JSON-RPC binding**：只保证 pi-a2a 互通；Agent Card 如实声明 binding，未来加 REST 是增量。
4. **R4 硬切**：旧 proto=1 peer 静默失效（发现层忽略）。团队需同时升级。

---

## 附录：实现顺序

1. `store.ts`：Message 可选字段 + inboundTasks/outboundTasks 两本账 + 方法。
2. `net.ts`：Server（agent-card + /rpc: SendMessage/GetTask + /a2a/notify 接收）→ Client（send/getTask/notifyPeer/getAgentCard）→ push 兜底扫描 → mDNS TXT 调整。
3. `config.ts` + `config.example.json`：补字段。
4. `index.ts`：工具适配 + 注入 hook（onMessageReceived/onTaskReceived）接线。
5. `be-daemon.ts`：适配新 Network。
6. 端到端验证：be-daemon 起 BE → FE 发 request → BE 自动处理 → BE complete task + push → FE webhook 接收 → 自动注入结果。
7. README/SKILL 更新。

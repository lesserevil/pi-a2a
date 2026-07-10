---
name: pi-a2a
description: Agent-to-agent communication — send messages, reply in threads, and discuss code with other pi agents on the same local network (decentralized P2P, no server)
---

# pi-a2a — Agent 间消息总线（A2A / 局域网 P2P）

当用户想和**另一个 pi agent** 沟通、协作、讨论代码时，使用 pi-a2a。每个 agent 有一个名字（如 backend / frontend / reviewer），消息按线程组织，支持已读/未读。

**协议**：实现 Google [A2A (Agent2Agent) 开放协议 v1.0](https://a2aproject.github.io/A2A/v1.0.0/specification/)（JSON-RPC over HTTP）。同一局域网内、同一 `workspace` + 共享密钥 的 agent 通过 mDNS 自动互相发现，点对点收发。无中心服务器、无需部署、每个 agent 本地各自存储。对方离线时消息进本地发件箱，对方上线自动送达；委派任务的结果经 push-notification **即时**回传。

## 何时使用

- 用户说"问问另一个 agent"、"让 X 来帮忙看看"、"和另一个终端讨论一下"
- 你需要把代码片段、设计决定、问题发给另一个 agent
- 另一个 agent 问了你问题（你会在 widget 上看到未读消息提示）

## 工具

| 工具 | 用途 |
|------|------|
| `a2a_peers` | **先调用它**，列出当前在线的 agent 和他们的角色，拿到可用的 `to` 名字 |
| `a2a_send` | 开始一个新对话 / 发消息 / 广播。`to` 是 peer_name 或 `*`（广播）|
| `a2a_inbox` | 查看发给我的消息（传 `unread=true` 只看未读）|
| `a2a_read` | 用 message_id 读完整正文（含完整代码），并标记已读 |
| `a2a_reply` | 在线程内回复一条消息（收件人自动取原消息发送者）|

## 共享记忆（workspace memory）

同一 workspace 的所有 agent 共享一份 KV 记忆。A 写入，B/C 立刻可见；新加入或离线过的 peer 上线时自动从其他 peer 拉全量对齐。适合放**每个 agent 都该知道的上下文**：架构总览、API 契约、技术决策、关键约定。

| 工具 | 用途 |
|------|------|
| `a2a_mem_keys` | 列出所有共享记忆 key（已删除的不显示）|
| `a2a_mem_get` | 按 key 读值（附作者和时间）|
| `a2a_mem_set` | 写入/更新一个 key，实时广播给在线 peer |
| `a2a_mem_delete` | 删除一个 key（写 tombstone，防迟到写入复活）|

**语义**：last-write-wins（按时间戳）；离线 peer 重连后自动从对方拉 snapshot 对齐；删除会传播。**不要**把一次性/临时信息塞进去（会稀释信号），优先放持久的、架构性的认知。

**典型流程**：
1. `a2a_mem_keys()` → 看已有上下文
2. `a2a_mem_get("api_schema")` → 读具体值
3. 学到新东西 → `a2a_mem_set("api_schema", "...")` → 其他 agent 立刻能用


## 典型流程

**发起讨论：**
1. `a2a_peers` → 确认对方在线、拿到名字（同局域网、同 workspace 才能看到）
2. `a2a_send(to="frontend", subject="登录接口签名", body="<代码+问题>")` → kind 用 `request` 表示想让对方做事
3. 对方处理后会 `a2a_reply` 回来，你在 widget 看到 `📨 N 未读`

**回应别人：**
1. `a2a_inbox` 或 `a2a_read(message_id)` → 看对方问了什么
2. `a2a_reply(message_id, body="<回答/代码>")` → 自动接在同一线程

## 代码讨论建议

- 把**完整代码**放进 `body`（接收端单条消息上限 512KB，对方用 a2a_read 能看到全文）
- `subject` 写一句话总结（如 "auth.ts 的 token 续期逻辑"）
- 回复时引用对方的具体问题，再给答案
- 如果对方不在线（a2a_peers 没列出），消息会进你的本地发件箱，对方下次上线自动收到——异步协作，不需要同时在线

## 约定

- agent 名字建议语义化：`backend`、`frontend`、`reviewer`、`tester`、`docs` 等
- 同一工作区的所有 agent 必须共享同一个 `workspace` 名 + `workspaceSecret` 密钥；**持有密钥即被完全信任**（可冒充任意 from_name、可向任意成员收件箱注入），只与你完全信任、且在同一局域网内的 agent 共享
- 未配置时工具会返回提示让用户跑 `/a2a-setup`（填 workspace、密钥、agent 名字）
- `a2a_read` / `a2a_reply` 的 message_id 可以是线程里的任意一条（根或回复），会自动定位到所属线程

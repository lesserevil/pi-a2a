# pi-a2a

> Agent-to-agent (A2A) message bus for pi — **decentralized LAN P2P**. Agents on the same network discover each other via mDNS and talk point-to-point. No server, no database to deploy, no cloud.

## What It Does

pi-a2a lets two (or more) pi agents communicate with each other:

- 📨 **Send messages** — start a conversation, ask a question, share a code snippet
- 💬 **Threaded replies** — `a2a_reply` keeps conversations in threads
- 📥 **Inbox + unread tracking** — widget shows live unread count, notifies on new messages
- 👥 **Presence** — see which agents are online and their roles
- 📡 **Auto-discovery** — agents find each other on the LAN via mDNS/Bonjour, zero config
- 🔌 **No server required** — every agent runs its own tiny HTTP listener + local store

This is **not** human chat or a shared-blackboard (that's pi-sync). It's a direct agent-to-agent channel: agent A asks agent B about an API signature, agent B replies with the code; agent A delegates a task, agent B delivers the result.

## Architecture

pi-a2a speaks the open **[Google A2A (Agent2Agent) Protocol v1.0](https://a2aproject.github.io/A2A/v1.0.0/specification/)** — JSON-RPC 2.0 over HTTP. Each agent is simultaneously an A2A **server** (exposes an Agent Card + JSON-RPC endpoint) and an A2A **client** (calls other agents' endpoints). Symmetry comes for free.

```
局域网 LAN（无中心服务器）
┌──────────────┐  mDNS(发现)  ┌──────────────┐
│   Agent A     │◄───────────►│   Agent B     │
│ A2A server:   │  JSON-RPC   │ A2A server:   │
│  Agent Card   │ ◄─────────► │  Agent Card   │
│  POST /rpc    │ (SendMessage│  POST /rpc    │
│  POST /notify │  + push)    │  POST /notify │
│ 内嵌 mDNS广告  │             │ 内嵌 mDNS广告  │
│ 本地 DB       │             │ 本地 DB       │
└──────────────┘             └──────────────┘
```

- **Discovery** — each agent advertises a `_pi-a2a._tcp` mDNS service (via [`bonjour-service`](https://www.npmjs.com/package/bonjour-service), TXT `proto=a2a`) and browses for others, filtered to the same `workspace`. A same-machine presence-file backstop covers mDNS missed packets.
- **Transport (A2A JSON-RPC)** — each agent runs a local `node:http` server on `0.0.0.0` (port auto-assigned), announced via mDNS. It serves `GET /.well-known/agent-card.json`, `POST /rpc` (JSON-RPC `SendMessage` / `GetTask`), and `POST /a2a/notify` (push-notification webhook receiver). Sending = acting as A2A client calling the peer's `/rpc`.
- **Storage** — each agent keeps its own local JSON file (`pi-a2a.db.json`) with both received (`inbox`) and sent copies, so a full thread can be reconstructed locally without asking anyone.
- **Async delivery** — if a recipient is offline, the message is queued in the sender's local **outbox** and re-delivered automatically when that peer comes back online. For delegations (`kind=request`), the result is pushed back **instantly** via A2A push-notification (the sender registers a webhook when sending); a `GetTask` backstop catches any missed push.

> Full migration design (decisions, data-model mapping, sequence diagrams): see [`docs/MIGRATION-TO-A2A.md`](./docs/MIGRATION-TO-A2A.md).

### Trust model

The shared `workspaceSecret` is the only credential. It is carried as an A2A `Bearer` token (declared in the Agent Card's `securitySchemes`) and validated on **every** JSON-RPC and push-notification request (checked together with the `workspace` name). **Anyone on the same LAN holding the secret is fully trusted** — they can send messages as any `from_name` and inject into any member's inbox. So:

- Share the secret only with agents you trust.
- This is a **LAN-only** tool — it binds to `0.0.0.0` and is not hardened for the public internet. For cross-internet use, put the agents behind a VPN.

## Quick Start

### 1. Install the extension + dependency

```bash
# from the pi-a2a directory
npm install        # installs bonjour-service (pure JS, no native build)

# Option A: install locally into pi
pi install /path/to/pi-a2a

# Option B: symlink for development
ln -s /path/to/pi-a2a ~/.pi/agent/extensions/pi-a2a
```

> **Linux note:** mDNS discovery needs an mDNS daemon running on the system (e.g. **Avahi**: `apt install avahi-daemon` and ensure `nss-mdns` is configured). macOS has Bonjour built in.

### 2. Configure (on each machine)

In pi, run:

```
/a2a-setup
```

This prompts for:

- **Workspace name** — every agent in your team uses the *same* name (this is how peers isolate from other pi-a2a users on the same LAN)
- **Shared secret** — also identical across the team; used for mutual authentication
- **Your agent name** — e.g. `backend`, `frontend`, `reviewer`
- Optional role

Config is saved **per-project** to `.pi/pi-a2a.json` (there is no global config). Each directory gets its own config with a unique `agentId`. Different projects can share the same `peerName` (e.g. two dirs both named `FE`) and still communicate — they're distinguished by `agentId`, discovered via the shared presence dir + mDNS as long as `workspace` + `workspaceSecret` match.

> There is no "create / join workspace" step — a workspace is simply a name + secret everyone agrees on. Two agents with the same `workspace` + `workspaceSecret` on the same LAN automatically find each other.

### 3. Talk

```
> List the other agents online
→ a2a_peers()

> Ask the frontend agent what props the Auth component needs
→ a2a_send(to="frontend", subject="Auth component props",
           body="What props does <Auth/> need? I'm writing the store...", kind="request")

# (frontend agent's widget shows: 📨 1 unread)
> frontend: check my inbox and reply
→ a2a_inbox() → a2a_read(msg_xxx) → a2a_reply(msg_xxx, body="It needs userId and ...")
```

## Tools

| Tool | Description |
|------|-------------|
| `a2a_send` | Send a message (or broadcast with `to="*"`) |
| `a2a_inbox` | List messages to me (`unread=true` for unread only) |
| `a2a_read` | Read full message body by id (any message in a thread), marks as read |
| `a2a_reply` | Reply in a thread (recipient auto-detected; accepts any message id) |
| `a2a_peers` | List online agents and their roles |

Tool signatures are unchanged from the previous (server-backed) version — only the implementation moved to P2P.

## Commands

| Command | Description |
|---------|-------------|
| `/a2a-setup` | Interactive setup wizard |
| `/a2a` | Show status (inbox + online agents) |

## Widget

A small status panel renders below the editor. It refreshes on events (new message, peer up/down) plus a low-frequency backstop:

```
🟢 a2a·backend·写API
  📨 2 未读
  在线: frontend·写UI reviewer·审查
```

New messages trigger a toast notification.

## Configuration

```json
{
  "workspace": "my-team",
  "workspaceSecret": "shared-secret-passphrase",
  "agentId": "auto-generated",
  "peerName": "backend",
  "role": "writes the API",
  "listenPort": 0,

  "agentCardPath": "/.well-known/agent-card.json",
  "rpcPath": "/rpc",
  "notifyPath": "/a2a/notify",
  "pushSweepMs": 15000,
  "pushBackstopMs": 30000
}
```

| Field | Meaning |
|-------|---------|
| `workspace` | Team name; peers only see each other if this matches |
| `workspaceSecret` | Shared passphrase; must match across the team; carried as A2A `Bearer` token |
| `agentId` | Auto-generated on first run, then persisted |
| `peerName` | Your display name (used as the `to` address) |
| `role` | Optional capability description |
| `listenPort` | Local HTTP port, `0` = OS-assigned (default) |
| `agentCardPath` | Agent Card path (default `/.well-known/agent-card.json`) |
| `rpcPath` | JSON-RPC endpoint path (default `/rpc`) |
| `notifyPath` | push-notification webhook receiver path (default `/a2a/notify`) |
| `pushSweepMs` | push-notification backstop sweep interval (default `15000`) |
| `pushBackstopMs` | after this long without a push, actively `GetTask` (default `30000`) |

Environment variable references are supported: `"workspaceSecret": "$PI_A2A_SECRET"`.

## How messages flow

1. **Send** — `a2a_send` writes a local copy (`direction=sent`) and looks up the recipient in the in-memory peer table.
   - **Online** → acts as A2A client: `POST /rpc {method:"SendMessage"}` to the recipient's JSON-RPC endpoint; recipient creates a `Task` and notifies. For `kind=request` the sender also registers a push-notification webhook so the result comes back instantly.
   - **Offline** → queues a delivery task in the local **outbox** (retried when the peer reappears).
   - **Broadcast (`to="*"`)** → `SendMessage` to every currently-online peer.
2. **Receive** — the local A2A server validates the `Bearer` token, creates a `Task` (per `metadata.kind`: `message`/`result` → `COMPLETED`, `request` → `WORKING`+inject), stores the message as `inbox`, fires a toast, and redraws the widget. Duplicate `messageId`s are ignored (idempotent retries).
3. **Result push-back** — when a delegated `request` is fulfilled (`a2a_reply`), the recipient completes its `Task` (`COMPLETED` + Artifact) and POSTs a push-notification to the requester's `/a2a/notify`; the requester extracts the Artifact and auto-injects the result. A `GetTask` backstop covers any missed push.
4. **Threads** — `thread_id` = A2A `contextId`; both sides keep a local copy, so `a2a_read` reconstructs the whole thread from local storage alone.

## Message Kinds

Every `SendMessage` produces an A2A `Task` (uniform wire format); behavior is keyed off `metadata.kind`:

| Kind | Task lifecycle on recipient | Recipient injected? | Sender awaits result? |
|------|------------------------------|--------------------|-----------------------|
| `message` | immediately `COMPLETED` (stored + toast) | ❌ only notified | ❌ synchronous ack |
| `request` | `WORKING` → recipient processes → `COMPLETED`+Artifact | ✅ delegated | ✅ via push-notification |
| `result` | immediately `COMPLETED` (stored + **auto-received**) | ✅ injected | ❌ synchronous ack |

> `request` 与 `result` 都会被**自动注入收件人当前会话**（见下节）。普通 `message` 只 toast 通知，不打扰，需手动 `a2a_read` 查看。

### Delegation（主动调动对方 agent）

`kind=request` 不只是个标签——它是真正的任务委派：

1. **A 发请求** — `a2a_send(to="backend", kind="request", subject="...", body="...")`，异步发出（fire-and-forget）。
2. **B 被调动** — B 收到后，pi-a2a 自动调 `pi.sendUserMessage(...)` 把任务**注入 B 的当前会话**，B 的 LLM 立刻着手处理（B 正忙则排队到当前 turn 之后，不中断）。普通 `message` 不会注入，只 toast。
3. **B 回结果** — 注入的提示已指引 B 处理完调 `a2a_reply(message_id, body)` 回复；若原消息是 request，回复自动标为 `result`。
4. **A 自动收结果** — 该 `result` 回执**经 A2A push-notification 即时送达 A**（A 发 request 时已注册 webhook）：B 完结 task 后 POST `{task:{status:COMPLETED, artifacts:[...]}}` 到 A 的 `/a2a/notify`，A 校验 token、取出 Artifact、同样**自动注入 A 的当前会话**，A 无需手动 `a2a_inbox` / `a2a_read`。委派全流程因此形成无人值守闭环。（push 万一丢失，A 的兑底扫描器会在超时后主动 `GetTask` 补取结果，不会永久丢失。）

> 注入到 B 的当前会话会写入 B 的对话历史（适合同机协作）。想要完全隔离的任务处理，可在 B 端用 `newSession` fork——本实现默认注入当前会话。

## A2A endpoints

Each agent exposes a tiny A2A-compliant HTTP server (the sending side is an outbound `fetch` JSON-RPC client):

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/.well-known/agent-card.json` | A2A Agent Card (identity, capabilities, JSON-RPC URL, Bearer security scheme) |
| `POST` | `/rpc` | JSON-RPC 2.0 endpoint: `SendMessage`, `GetTask` (validates `Authorization: Bearer <secret>`) |
| `POST` | `/a2a/notify` | push-notification webhook receiver (validates `X-A2A-Notification-Token` / Bearer) |
| `GET` | `/health` | Liveness + name/workspace (mDNS keep-alive probe; non-spec) |

Bodies are capped at 512KB. Methods are v1.0 PascalCase (`SendMessage`/`GetTask`).

## Storage backend

Local JSON file (`pi-a2a.db.json`), kept in memory and persisted on change + on shutdown. This is deliberately dependency-free and portable across the Node and Bun runtimes pi ships as. All storage access is isolated in `extensions/store.ts`, so it can be swapped for SQLite later without touching the tools or network layer.

## Development

Two dev scripts live in `scripts/` (not shipped in the published package — `files` only includes `extensions`, `skills`, `config.example.json`, `LICENSE`, `README.md`):

```bash
npm run typecheck   # auto-links global pi types into node_modules, then tsc (0 errors expected)
npm run test:e2e    # spins up a BE daemon + headless FE, verifies the full delegation loop:
                    #   FE sends kind=request → BE completes its Task → result pushed back
                    #   instantly via A2A push-notification (zero-latency, no polling)
```

`npm run typecheck` is self-bootstrapping: it locates your global `@earendil-works/pi-coding-agent` install and symlinks its type packages (plus `@types/node` and `typebox`) into the project's `node_modules`, so no devDependencies on the full agent are needed. `npm run test:e2e` cleans up its presence/DB artifacts on exit.

## License

MIT

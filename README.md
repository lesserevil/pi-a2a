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
LAN (no central server)
┌──────────────┐  mDNS(discover) ┌──────────────┐
│   Agent A     │◄───────────►│   Agent B     │
│ A2A server:   │  JSON-RPC   │ A2A server:   │
│  Agent Card   │ ◄─────────► │  Agent Card   │
│  POST /rpc    │ (SendMessage│  POST /rpc    │
│  POST /notify │  + push)    │  POST /notify │
│ embedded mDNS │             │ embedded mDNS │
│ local DB      │             │ local DB      │
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
| `a2a_session` | Read (or name) the current pi session id, for remote-exec session continuity |
| `a2a_put` | Copy a local file to another agent's sandbox (peer-to-peer, bytes bypass the model) |
| `a2a_get` | Fetch a file from another agent's sandbox into the local one |
| `a2a_file_root` | Show the local file sandbox root |
| `a2a_mem_set` / `a2a_mem_get` / `a2a_mem_keys` / `a2a_mem_delete` | Workspace shared memory (KV, replicated to peers) |

Tool signatures are unchanged from the previous (server-backed) version — only the implementation moved to P2P.

## File transfer

`a2a_put` / `a2a_get` move files **directly between agents** over the A2A
HTTP channel. The bytes never enter the model context, so this is cheap for
large or binary files and has no 512 KB message limit.

- Each host has a **file sandbox**, by default `~/.pi/a2a-files` (config
  `fileRoot`). Every path is **relative to the sandbox** and resolved inside it;
  `..` and absolute paths are rejected, so nothing outside the root can be read
  or written.
- The sender computes a **sha256**; the receiver recomputes it after writing and
  fails the transfer on mismatch.
- Single-transfer size cap is `fileMaxBytes` (default 64 MiB).
- New endpoints on each agent's HTTP server: `POST /file` (receive) and
  `GET /file?path=<rel>` (send), both requiring the shared Bearer secret.

```
# push  ~/.pi/a2a-files/report.pdf  on this host  ->  peer's sandbox
→ a2a_put(to="godspeed", path="report.pdf", remotePath="incoming/report.pdf")

# pull  incoming/report.pdf  from the peer  ->  ~/.pi/a2a-files/report.pdf
→ a2a_get(from="godspeed", path="incoming/report.pdf", localPath="report.pdf")
```

> For small text snippets you can instead use a `remote-exec` request with the
> content inline (or base64), but `a2a_put`/`a2a_get` are the right tool for
> anything sizable or binary.

## Bundled skills

The package ships skills under `skills/`; pi loads them automatically once the
package is installed, on every peer.

| Skill | Purpose |
|-------|---------|
| `pi-a2a` | General agent-to-agent messaging: send/reply in threads, shared memory, when to use the channel. |
| `remote-exec` | Run a command/script/query on **another host** and get its output back over the a2a channel — no SSH, no shared filesystem. |

### remote-exec — cross-host command delegation

Use this when a task needs the OS, network, filesystem, or hardware of another
agent (e.g. Linux-only tools, a headless server, a different machine's data).
It is plain a2a delegation, just with a defined message contract:

1. **Caller** discovers peers with `a2a_peers`, then sends a `kind=request`
   message whose subject starts with `exec:`. The body has an optional header
   block followed by a fenced `sh` block. In prose, the body looks like:

       workdir: /home/me/project
       session: 01a0ff96-...

       Run this on your host and reply with the result.

       ```sh
       uname -a
       ```

   ...sent as:

       a2a_send(to="plaz", subject="exec: uname -a",
                kind="request", body="<the text above>")

   - `workdir: <path>` (optional) — run the command in this directory.
   - `session: <id>` (optional) — continue a previous exec session. Omit it on
     the first call; pass the id returned by the reply on later calls to keep
     the remote agent's context across requests. Sessions are persisted and
     resumable across restarts, and are one-at-a-time per peer.

2. **Remote agent** reads the headers, runs the command with its own `bash`
   tool (in `workdir` if given), and replies with
   `a2a_reply(message_id, body=<result>)`. For a new session it calls
   `a2a_session` to obtain the id to return. The reply body is a machine-readable
   block — nothing else:

       host: plaz
       workdir: /home/me/project
       session: 01a0ff96-7f3f-727f-b998-c54c65f53fcc
       exit: 0
       stdout:
       Linux plaz 7.0.0-34-generic #34-Ubuntu SMP x86_64 GNU/Linux
       stderr:

3. **Caller** receives the pushed `result` (auto-injected), parses
   `host`/`workdir`/`session`/`exit`/`stdout`/`stderr`, verifies `host` matches
   the peer it asked, remembers `session` for the next call, and treats non-zero
   `exit` (including `-1`) as failure.

The skill covers the request/reply conventions, the exact reply format,
truncation rules, and safety guidance (non-interactive commands only; no
credentials over the plaintext LAN channel; only delegate to trusted peers).

> The remote host executes the command itself — requests are never forwarded
> onward. As with all pi-a2a traffic, holders of the workspace secret are fully
> trusted, so only delegate to peers you trust.

## Commands

Slash commands let you use pi-a2a **without involving the AI** — they run directly and show results as a notification.

| Command | Description |
|---------|-------------|
| `/a2a-setup` | Interactive setup wizard (workspace, secret, name, role) |
| `/a2a` | Show status (unread count + online agents) |
| `/a2a-peers` | List online agents and their roles |
| `/a2a-inbox` | List recent inbox messages (● unread / ○ read) |
| `/a2a-send` | Send a message to another agent directly |
| `/a2a-clear` | Clear inbox (archives & sent history kept) |

### Usage examples

```
/a2a                          # status: unread count + who's online
/a2a-peers                    # who's online right now
/a2a-peers all                # include offline (previously seen) agents
/a2a-inbox                    # recent 20 messages (● unread, ○ read)
/a2a-inbox unread             # only unread
/a2a-inbox msg_xxxxx          # read full message + mark as read
/a2a-send frontend hello      # quick message to @frontend
/a2a-send                     # interactive: pick peer → subject → body
/a2a-clear                    # clear inbox (archives & sent history kept)
```

> For threaded replies and delegations (`kind=request`), use the AI tools (`a2a_reply`, `a2a_send` with `kind`) — they handle thread tracking and result push-back automatically.

## Widget

A small status panel renders below the editor. It refreshes on events (new message, peer up/down) plus a low-frequency backstop:

```
🟢 a2a·backend·api
  📨 2 unread
  online: frontend·ui reviewer·review
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

> Both `request` and `result` are **auto-injected into the recipient's current session** (see below). A plain `message` only shows a toast — it does not interrupt and must be read with `a2a_read`.

### Delegation (actively activating the other agent)

`kind=request` is not just a label — it is genuine task delegation:

1. **A sends a request** — `a2a_send(to="backend", kind="request", subject="...", body="...")`, sent asynchronously (fire-and-forget).
2. **B is activated** — on receipt, pi-a2a automatically calls `pi.sendUserMessage(...)` to **inject the task into B's current session**, and B's LLM starts working on it immediately (if B is busy it queues after the current turn without interrupting). A plain `message` is not injected; it only shows a toast.
3. **B returns the result** — the injected prompt instructs B to reply with `a2a_reply(message_id, body)` when done; if the original message was a request, the reply is automatically marked `result`.
4. **A receives the result automatically** — the `result` receipt is **delivered to A immediately via A2A push-notification** (A registered a webhook when sending the request): after completing the task, B POSTs `{task:{status:COMPLETED, artifacts:[...]}}` to A's `/a2a/notify`; A validates the token, extracts the Artifact and likewise **auto-injects it into A's current session**, so A needs no manual `a2a_inbox` / `a2a_read`. The whole delegation flow is therefore an unattended loop. (If a push is ever lost, A's fallback scanner actively calls `GetTask` after the timeout to recover the result, so it is never lost permanently.)

> Injection into B's current session is written to B's conversation history (suitable for same-machine collaboration). For fully isolated task handling, fork with `newSession` on B — this implementation injects into the current session by default.

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

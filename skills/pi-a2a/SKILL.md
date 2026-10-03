---
name: pi-a2a
description: Agent-to-agent communication — send messages, reply in threads, and discuss code with other pi agents on the same local network (decentralized P2P, no server)
---

# pi-a2a — agent-to-agent message bus (A2A / LAN P2P)

Use pi-a2a when the user wants to talk to, collaborate with, or discuss code with **another pi agent**. Each agent has a name (e.g. backend / frontend / reviewer). Messages are organised into threads and track read/unread state.

**Protocol**: implements Google's [A2A (Agent2Agent) open protocol v1.0](https://a2aproject.github.io/A2A/v1.0.0/specification/) (JSON-RPC over HTTP). Agents on the same LAN with the same `workspace` and shared secret discover each other automatically over mDNS and exchange messages peer-to-peer. No central server, no deployment, and each agent stores its own data locally. Messages to an offline peer go to a local outbox and are delivered automatically when the peer comes online; results of delegated tasks are pushed back **immediately** via push-notification.

## When to use it

- The user says "ask the other agent", "have X take a look", or "discuss this with the other terminal"
- You need to send a code snippet, a design decision, or a question to another agent
- Another agent has asked you something (you will see an unread-message indicator in the widget)
- The work needs another host's OS / hardware / repo: use the `cluster-workspaces` skill to provision a working copy and open a session there, or `remote-exec` for a single command

## Tools

| Tool | Purpose |
|------|---------|
| `a2a_peers` | **Call this first** — lists the agents currently online and their roles, giving you valid `to` names |
| `a2a_send` | Start a new conversation / send a message / broadcast. `to` is a peer_name or `*` (broadcast) |
| `a2a_inbox` | View messages addressed to me (pass `unread=true` for unread only) |
| `a2a_read` | Read a full message body by message_id (including complete code) and mark it read |
| `a2a_reply` | Reply to a message within its thread (the recipient is taken from the original sender automatically) |

## Shared memory (workspace memory)

All agents in the same workspace share one KV memory store. If A writes, B and C see it immediately; a newly joined or previously offline peer pulls a full snapshot from another peer on reconnect to catch up. Use it for **context every agent should know**: architecture overviews, API contracts, technical decisions, key conventions.

| Tool | Purpose |
|------|---------|
| `a2a_mem_keys` | List all shared-memory keys (deleted ones are hidden) |
| `a2a_mem_get` | Read a value by key (with author and timestamp) |
| `a2a_mem_set` | Write/update a key and broadcast it to online peers in real time |
| `a2a_mem_delete` | Delete a key (writes a tombstone so a late write cannot resurrect it) |

**Semantics**: last-write-wins (by timestamp); offline peers pull a snapshot to realign on reconnect; deletions propagate. Do **not** put one-off or temporary information in here (it dilutes the signal) — prefer durable, architectural knowledge.

**Typical flow**:
1. `a2a_mem_keys()` → see what shared context already exists
2. `a2a_mem_get("api_schema")` → read a specific value
3. Learned something new → `a2a_mem_set("api_schema", "...")` → other agents can use it immediately

## Typical flows

**Starting a discussion:**
1. `a2a_peers` → confirm the peer is online and get their name (only visible if on the same LAN and workspace)
2. `a2a_send(to="frontend", subject="Login endpoint signature", body="<code + question>")` → use kind `request` if you want them to do something
3. They reply via `a2a_reply`; you will see `📨 N unread` in the widget

**Responding to someone:**
1. `a2a_inbox` or `a2a_read(message_id)` → see what they asked
2. `a2a_reply(message_id, body="<answer/code>")` → automatically continues the same thread

## Tips for discussing code

- Put **complete code** in the `body` (the per-message limit on the receiving side is 512KB; they can see the full text with a2a_read)
- Write a one-line summary in `subject` (e.g. "token refresh logic in auth.ts")
- When replying, quote their specific question before giving the answer
- If the peer is offline (not listed by a2a_peers), the message goes to your local outbox and is delivered automatically the next time they come online — asynchronous collaboration, no need to be online simultaneously

## Conventions

- Prefer semantic agent names: `backend`, `frontend`, `reviewer`, `tester`, `docs`, etc.
- All agents in a workspace must share the same `workspace` name + `workspaceSecret`. **Holding the secret means full trust** (you can impersonate any from_name and inject into any member's inbox), so only share it with agents you fully trust and that are on the same LAN
- When unconfigured, the tools return a message telling the user to run `/a2a-setup` (workspace, secret, agent name)
- The message_id for `a2a_read` / `a2a_reply` can be any message in a thread (root or reply); it will resolve to the correct thread automatically

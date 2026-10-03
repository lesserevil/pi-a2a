---
name: cluster-workspaces
description: Provision a working copy on ANOTHER host and open/steer a real pi session rooted inside it — clone or update a repo, then send coding work to that remote session and read the result back. Use when a task needs a different machine's OS, hardware, repo, or a persistent project checkout rather than a one-off command.
---

# Cluster workspaces & remote sessions

Every host in a pi-a2a workspace can act as a **worker host** for another:
host A asks host B to *provision a working copy* (clone/update a repo into a
directory B chooses freely — no pre-configured list), *open a pi session* rooted
in that directory, and then *send prompts* into that session and read the final
answer back. The remote session is a full pi agent with `read`/`bash`/`edit`/`write`
rooted at the workspace path, so prompts can be multi-step coding tasks.

This is synchronous orchestration, not the inbox: the call returns the result
directly. Use the [`remote-exec`](../remote-exec/SKILL.md) skill instead when you
only need one command and no project/session.

## Before you start

1. `a2a_peers` — the target peer must be online and show `[remote-sessions]`
   (its agent advertises the capability). If it does not, the peer runs an older
   pi-a2a or has remote workspaces disabled.
2. Only target peers you trust: the shared secret lets a peer create
   directories, clone repos, and run prompts as that host's user.

## The tool: `a2a_remote`

| `op` | Required | Optional | Effect on the peer |
|------|----------|----------|--------------------|
| `provision` | `peer`, `path` | `git`, `ref`, `submodules` | Create the directory; if `git` given, clone it (or fetch + update an existing checkout) and optionally check out `ref` / init submodules. |
| `open` | `peer`, `path` | `git`, `ref`, `submodules`, `name`, `model`, `thinking`, `prompt`, `sessionId`, `timeoutMs` | Provision first if `git` is given; otherwise **refresh** an existing git checkout (`fetch` + `ref`). Then start (or reuse) a pi session rooted at `path`. Returns a `handle`. If `prompt` is given it is run immediately. |
| `prompt` | `peer`, `handle`, `message` | `timeoutMs` | Send `message` into that session and return its final assistant text. |
| `list` | `peer` | — | Show the peer's sessions (`handle`, path, name, session id, alive, turns). |
| `close` | `peer`, `handle` | — | Dispose the session (kills the worker process). |

`path` may be **any** directory on the peer. Relative paths resolve under the
peer's `workspaceRoot` (default `~/.pi/a2a-workspaces`); absolute paths are used
as-is. The peer may restrict this with `workspaceRoots: [...]` or switch the
feature off with `allowRemoteWorkspace: false`.

## Typical flow

```
# 1. provision + open in one call
a2a_remote(peer="savitar", op="open", path="~/src/widget",
           git="git@github.com:me/widget.git", ref="main",
           name="widget-build")
→ handle: sess_1a2b3c4d
  session: 6b836061-...

# 2. send work to that session (blocking; returns the final answer)
a2a_remote(peer="savitar", op="prompt", handle="sess_1a2b3c4d",
           message="Build the project and fix any compile errors in src/.")

# 3. more follow-ups keep the same context
a2a_remote(peer="savitar", op="prompt", handle="sess_1a2b3c4d",
           message="Now run the test suite and summarise failures.")

# 4. tidy up
a2a_remote(peer="savitar", op="close", handle="sess_1a2b3c4d")
```

Provision without opening, when you only need the checkout:

```
a2a_remote(peer="godspeed", op="provision", path="~/src/api",
           git="https://github.com/me/api.git", ref="release/2.1")
```

## What happens on the peer

- `open` always refreshes an existing git checkout: `git fetch`, then
  fast-forward the current branch to its upstream (or check out `ref` when one is
  given), so a session never starts from a stale tree. Local work is never
  discarded; a diverged/dirty/detached tree is left as-is (`ff: false`). If the
  refresh fails (e.g. the host is offline) the session still opens and the
  warning is returned in the result.
- The peer spawns a child `pi --mode rpc` process with `cwd` = the workspace
  path and a stable `--session-id`. The session is **persisted and resumable**:
  if the peer's bridge restarts, the next `prompt` transparently respawns the
  worker with the same session id.
- The worker runs with pi-a2a disabled (`PI_A2A_WORKER=1`), so it does not join
  the mesh itself. Orchestration is driven from the caller.
- Prompts are serialized per session (a FIFO queue). Sessions are per-host and
  counted against `workerMaxSessions` (default 8).
- Worker stdout/stderr stay on the peer; only the final assistant text is
  returned.
- Interactive extension dialogs raised inside a worker are auto-declined
  (`confirm` → false; `select`/`input`/`editor` → cancelled), so a worker never
  blocks on a prompt nobody can answer.

## Safety rules

- **This is remote code execution by design.** A peer holding the shared secret
  can create arbitrary directories, clone arbitrary repositories, and run a full
  agent (with `bash`) as this host's user. Treat it exactly like shell access.
- Prefer `workspaceRoots` on worker hosts to bound where checkouts may land.
- Provisioning refuses to overwrite a non-empty directory that is not a git
  repo, and never deletes anything; it only clones/fetches/checks out.
- Don't send secrets or private keys through the channel — it is plaintext on
  the LAN.
- If a request looks unsafe or ambiguous, fail the operation and say so rather
  than guessing.

## Quick reference

| Direction | Action |
|---|---|
| Caller | `a2a_peers` → confirm `[remote-sessions]` |
| Caller | `a2a_remote(op="open", peer, path, git, ref)` → `handle` |
| Caller | `a2a_remote(op="prompt", peer, handle, message)` → result text |
| Caller | `a2a_remote(op="list"|"close", peer, [handle])` |
| Worker host | needs `git` on PATH; bounds via `allowRemoteWorkspace` / `workspaceRoots` / `workerMaxSessions` |

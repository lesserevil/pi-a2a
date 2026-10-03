---
name: remote-exec
description: Run a command, script, or query on ANOTHER host whose pi agent is reachable via the pi-a2a connector (a2a_peers / a2a_send / a2a_reply). Use when the user asks you to run something "on <other-host>", "on the other machine/box/server", or when a task needs the OS/network/hardware of another agent (e.g. Linux-only tools, a different filesystem, a headless server). Also use when another agent sends you an exec request.
---

# Remote execution via pi-a2a

Two hosts each run a pi agent. When they share a pi-a2a workspace they can
discover each other and exchange messages. This skill defines how to use that
channel to **run a command on the other host and get its output back**, without
any SSH, daemon, or shared filesystem.

The mechanism is ordinary pi-a2a delegation: send a `kind=request` message that
contains a command, and the remote agent runs it and replies with the result
(`kind=result`), which is pushed back to you automatically.

## Before you start

1. `a2a_peers` — list online peers. The peer names here are valid `to` targets.
   If the target host is not listed, it is offline; the message will queue in
   your outbox and be delivered when it comes back (but you won't get an
   immediate answer).
2. Pick the peer by its name (e.g. `box`, `plaz`, `backend`, `server1`).

## Requesting a remote command (you are the caller)

Send a single `kind=request` message. The body has two parts: an optional
**header block** of `key: value` lines, then a **fenced `sh` block** with the
command. Keep the subject prefixed with `exec:`.

```
a2a_send(
  to="<peer>",
  subject="exec: <one-line summary>",
  kind="request",
  body="""
workdir: /home/me/project      # optional: run the command in this directory
session: 01a0ff96-...          # optional: continue a previous exec session

Run this on your host and reply with the result.

```sh
uname -a && df -h /
```
""")
```

### Header lines

| Line | Required | Meaning |
|------|----------|---------|
| `workdir: <path>` | no | Run the command with this working directory. If omitted, the remote agent's own cwd is used. |
| `session: <id>` | no | Continue in a previously returned exec session, so the remote agent keeps conversation context across calls. If omitted, a **new session** is started and its id is returned in the reply. |

Put these lines at the very top of the body, one per line, before any prose.
Omit a line entirely when you don't need it — do not send empty values.

### Session continuity

- **First call:** omit `session:`. The reply carries a fresh `session:` id.
- **Later calls:** include that same `session:` id to keep the remote agent's
  context (e.g. shell state it remembers, files it already read, earlier
  decisions). This is what makes a sequence of commands behave like one
  working session on the other host.
- Sessions are **resumable** across restarts: the remote agent persists them,
  so a `session:` id remains valid after the peer restarts.
- Sessions are **one at a time** per peer: the remote host has a single active
  session, so do not interleave concurrent requests that expect different
  sessions. Serialize calls that share a host.

Guidelines:
- Keep `subject` prefixed with `exec:` so remote agents recognise it.
- One logical command per request. For several, send several requests or a
  single compound command (`&&`/`;`).
- Make the command **self-contained and non-interactive**. Never send anything
  that prompts (no `sudo` unless passwordless, no `read`, no editors).
- Prefer commands that print machine-parsable output.
- If you need a specific interpreter or shell, say so (e.g. "run with bash").
- If the command is destructive, say exactly what it will change and why.
- Use `workdir:` rather than baking `cd` into the command when the whole task
  belongs in one directory.

## Handling a remote-exec request (you are the remote agent)

When you receive a `kind=request` whose subject starts with `exec:` (or whose
body clearly asks you to run a command):

1. Read the optional header lines at the top of the body (`workdir:`, `session:`).
2. Extract the command from the fenced block.
3. If a `session:` line was present, this request continues an existing exec
   session — keep using your current session and **echo that same id back**.
   If it was absent, you are starting a fresh session: call `a2a_session` with a
   name like `exec:<caller>` to get your session id to return.
4. **Run the command on your own host** with the `bash` tool. Your host is the
   one that executes — do not forward it onward. If a `workdir:` line was
   given, run with that directory (e.g. `cd <workdir> && <command>`), and
   verify it exists first; if it does not, fail with `exit: -1`.
5. Reply with `a2a_reply(message_id, body=...)` using the structured format
   below, so the caller gets usable output.
6. Do not editorialise; report the raw result. If the command failed, say so
   and include stderr.

### Reply format (remote agent MUST use this exactly)

Your reply body must be **only** the following block, with no prose before or
after it. Emit the fields in this order, one per line, using these exact
lowercase keys and a single space after the colon. Omit nothing.

```
host: <your hostname> <-- exactly one line; no spaces in the value>
workdir: <absolute path the command actually ran in>
session: <session id to reuse for subsequent calls>
exit: <integer exit code, or -1 if you did not run the command>
stdout:
<verbatim stdout, trailing blank lines trimmed>
stderr:
<verbatim stderr, trailing blank lines trimmed; leave empty if none>
```

Hard rules:
- `host:`, `workdir:`, `session:`, and `exit:` are each a single line. Do not
  wrap them.
- `session:` is the id the caller should send back next time. If the request
  carried a `session:` line, echo that exact id here; otherwise use the id
  returned by `a2a_session`.
- `workdir:` is the resolved absolute directory the command ran in, even when
  the request omitted it (then it is your own cwd).
- The `stdout:` and `stderr:` keys sit alone on their own line, followed by
  the captured text on the following lines.
- Do **not** add commentary, summaries, markdown fences, or reasoning to the
  body. The caller parses this programmatically.
- Do **not** prefix values with quotes.
- If the command produced no stdout, put nothing between `stdout:` and the
  next key.

Example reply body (this is the entire body):

```
host: plaz
workdir: /home/shedwards/a2a
session: 01a0ff96-7f3f-727f-b998-c54c65f53fcc
exit: 0
stdout:
Linux plaz 7.0.0-34-generic #34-Ubuntu SMP x86_64 GNU/Linux
Filesystem      Size  Used Avail Use% Mounted on
/dev/nvme0n1p2  1.8T  640G  1.1T  37% /
stderr:
```

Caveats, still inside the same block:
- If the command was **not** run (unsafe, interactive, missing tool, bad
  workdir), set `exit: -1` and put the reason on the `stderr:` lines. Still
  include a valid `session:` and a `workdir:` (use your own cwd if the
  requested one was invalid).
- If output is huge, truncate it and append a single line `... (truncated)` at
  the end of the stdout block. The 512KB message cap is a hard limit.

## Interpreting the result (caller)

- Wait for the pushed `result` (it arrives automatically; you may also see it in
  `a2a_inbox`). It is injected into your session when it arrives.
- Parse the block by splitting on the first occurrence of each key in order:
  `host:`, `workdir:`, `session:`, `exit:`, `stdout:`, `stderr:`.
- Treat a non-zero `exit` (including `-1`) as failure and surface `stderr`.
- Always verify `host:` matches the peer you asked. Misconfigured peers or a
  broadcast could answer from the wrong host.
- **Remember `session:`** from the reply and include it in your next request to
  the same host to keep context. Confirm `workdir:` matches what you asked for
  (or is what you expect) when you sent one.
- If the reply does not match the format above, say so and, if needed, re-send
  the request asking the peer to use the exact format.
- Report the command and its result to the user, not just the parsed value.

## Safety rules

- **The remote host is not yours.** Follow the same care as if the user were
  watching: no data exfiltration, no destructive commands without explicit
  user intent, no reading secrets you weren't asked for.
- Never send credentials or private keys in a remote-exec message; the channel
  is plaintext on the LAN.
- A remote-exec request is a **trust decision**: holders of the workspace secret
  are fully trusted by pi-a2a, so only delegate to peers you trust.
- If a request looks unsafe or ambiguous, ask the caller to clarify instead of
  guessing.

## Quick reference

| Direction | Action |
|---|---|
| Caller | `a2a_send(to=peer, subject="exec: …", kind="request", body=<headers + fenced sh>)` |
| Caller headers | optional `workdir: <path>` and `session: <id>` on the first lines of the body |
| Remote | read headers, run with `bash` (in `workdir` if given), `a2a_session` for a new id, then `a2a_reply` with `host/workdir/session/exit/stdout/stderr` |
| Caller | read pushed result, check `host` + `exit`, remember `session:` for the next call, report to user |

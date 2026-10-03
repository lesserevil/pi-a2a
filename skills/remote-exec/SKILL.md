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

Send a single `kind=request` message. Put a **fenced `sh` block** in the body so
the remote agent has an unambiguous command, and state the expected host:

```
a2a_send(
  to="<peer>",
  subject="exec: <one-line summary>",
  kind="request",
  body="""
Run this on your host and reply with the result.

```sh
uname -a && df -h /
```
""")
```

Guidelines:
- Keep `subject` prefixed with `exec:` so remote agents recognise it.
- One logical command per request. For several, send several requests or a
  single compound command (`&&`/`;`).
- Make the command **self-contained and non-interactive**. Never send anything
  that prompts (no `sudo` unless passwordless, no `read`, no editors).
- Prefer commands that print machine-parsable output.
- If you need a specific interpreter or shell, say so (e.g. "run with bash").
- If the command is destructive, say exactly what it will change and why.

## Handling a remote-exec request (you are the remote agent)

When you receive a `kind=request` whose subject starts with `exec:` (or whose
body clearly asks you to run a command):

1. Extract the command from the fenced block.
2. **Run it on your own host** with the `bash` tool. Your host is the one that
   executes — do not forward it onward.
3. Reply with `a2a_reply(message_id, body=...)` using the structured format
   below, so the caller gets usable output.
4. Do not editorialise; report the raw result. If the command failed, say so
   and include stderr.

### Reply format (remote agent must use this)

Use this exact shape so the caller can parse it:

```
host: <your hostname>
exit: <exit code>
stdout:
<verbatim stdout, trimmed of trailing blank lines>
stderr:
<verbatim stderr, or empty>
```

Example reply body:

```
host: plaz
exit: 0
stdout:
Linux plaz 7.0.0-34-generic #34-Ubuntu SMP x86_64 GNU/Linux
Filesystem      Size  Used Avail Use% Mounted on
/dev/nvme0n1p2  1.8T  640G  1.1T  37% /
stderr:
```

Caveats to include when relevant:
- If the command was **not** run (unsafe, interactive, missing tool), reply with
  `exit: -1` and a `stderr:` line explaining why.
- If output is huge, truncate to a reasonable size and note
  `stdout: (truncated)`. The 512KB message cap is a hard limit.

## Interpreting the result (caller)

- Wait for the pushed `result` (it arrives automatically; you may also see it in
  `a2a_inbox`). It is injected into your session when it arrives.
- Parse `host`, `exit`, `stdout`, `stderr`. Treat non-zero `exit` as failure.
- Always verify `host:` matches the peer you asked. Misconfigured peers or a
  broadcast could answer from the wrong host.
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
| Caller | `a2a_send(to=peer, subject="exec: …", kind="request", body=<fenced sh>)` |
| Remote | run with `bash`, then `a2a_reply(message_id, host/exit/stdout/stderr)` |
| Caller | read pushed result, check `host` + `exit`, report to user |

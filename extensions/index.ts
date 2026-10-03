/**
 * pi-a2a — Agent-to-agent message bus for pi coding agent (LAN P2P edition)
 *
 * pi agents on the same LAN with the same workspace + shared secret discover each other automatically and exchange messages peer-to-peer.
 * No central server, no cloud dependency, no deployment. Each agent stores its own data locally.
 *
 * Discovery: mDNS/Bonjour (bonjour-service) advertise + browse _pi-a2a._tcp
 * Transport: each agent runs a local HTTP server (node:http) and pushes peer-to-peer
 * Storage: local JSON files (store.ts); inbox + sent copies reconstruct the full thread locally
 * Async: peer offline → queued in the local outbox; redelivered automatically when the peer comes online (no need to be online simultaneously)
 *
 * Tools:    a2a_send, a2a_inbox, a2a_read, a2a_reply, a2a_peers
 * Commands: /a2a-setup, /a2a, /a2a-clear, /a2a-send, /a2a-inbox, /a2a-peers
 * Widget:   local unread count + online peers (event-driven refresh + low-frequency fallback)
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { StringEnum } from "@earendil-works/pi-ai";
import * as crypto from "node:crypto";
import {
  loadConfig,
  saveConfig,
  genAgentId,
  dbPathFor,
  type A2aConfig,
} from "./config.ts";
import { Store, type Message, type MsgKind } from "./store.ts";
import { Network, type Peer } from "./net.ts";
import { resolveFileRoot } from "./files.ts";
import { WorkerManager } from "./workers.ts";

// ── state ───────────────────────────────────────────────────

interface LiveState {
  config: A2aConfig | null;
  configPath: string | null;
  store: Store | null;
  net: Network | null;
  workers: WorkerManager | null;
  unread: number;
  lastCtx: any; // most recent ctx, used for widget redraw / toast
  widgetTimer: ReturnType<typeof setInterval> | null;
}

const state: LiveState = {
  config: null,
  configPath: null,
  store: null,
  net: null,
  workers: null,
  unread: 0,
  lastCtx: null,
  widgetTimer: null,
};

const NO_CONFIG_MSG =
  "⚠️ pi-a2a is not configured. Run `/a2a-setup` to set the workspace name, shared secret and agent name.";

const WIDGET_REFRESH_MS = 5000; // low-frequency fallback refresh (new messages / presence are mostly event-driven)

// ── helpers ─────────────────────────────────────────────────

function genMsgId(): string {
  return "msg_" + crypto.randomBytes(5).toString("hex"); // 10 hex
}
function nowSec(): number {
  return Math.floor(Date.now() / 1000);
}

function notReady(): string {
  if (!state.config) return NO_CONFIG_MSG;
  return "⏳ pi-a2a is not ready (network service not started). Retry shortly, or run /a2a-setup again.";
}

/** Standard text tool result. The SDK requires AgentToolResult.details (for logs/UI); default to an empty object. */
function textResult(
  text: string,
  details: Record<string, unknown> = {},
): { content: { type: "text"; text: string }[]; details: Record<string, unknown> } {
  return { content: [{ type: "text", text }], details };
}

// ── Widget ──────────────────────────────────────────────────

function recalcUnread(): void {
  if (state.store && state.config) {
    state.unread = state.store.unreadCount(state.config.peerName);
  }
  redrawWidget();
}

function redrawWidget(): void {
  const ctx = state.lastCtx;
  try {
    if (!ctx?.hasUI) return;
    ctx.ui.setWidget(
      "pi-a2a",
      (_tui: any, theme: any) => ({
        render(width: number) {
          if (!state.config) {
            return [theme.fg("muted", "📨 pi-a2a not configured (/a2a-setup)")];
          }
          const me = state.config.peerName;
          const peers = (state.net?.getOnlinePeers() ?? []).filter((p) => p.peerName !== me);
          const peersTxt =
            peers.length === 0
              ? "(no other agents online)"
              : peers.map((p) => p.peerName + (p.role ? `·${p.role}` : "")).join(" ");
          const unreadTxt =
            state.unread > 0
              ? theme.bold(theme.fg("warning", `📨 ${state.unread} unread`))
              : theme.fg("muted", "📭 inbox empty");
          const line1 =
            `🟢 a2a·${theme.bold(me)}` + (state.config.role ? `·${state.config.role}` : "");
          const line2 = `  ${unreadTxt}`;
          const line3 = `  online: ${peersTxt}`;
          return [line1, line2, line3].map((l) => (l.length > width ? l.slice(0, width - 1) + "…" : l));
        },
        invalidate() {},
      }),
      { placement: "belowEditor" },
    );
  } catch {
    // ctx may be stale (timers still fire after session switch/exit); skip silently,
    // a new session's session_start rebuilds the widget.
  }
}

// ── engine start/stop ───────────────────────────────────────

async function startEngine(ctx: any): Promise<void> {
  const cfg = state.config;
  if (!cfg) return;
  // storage shares scope with ctx; inject onOutboxOverflow so overflow surfaces in the UI (avoids silent message loss)
  const store = new Store(dbPathFor(ctx.cwd), {
    onOutboxOverflow: (peer, droppedId, subject) => {
      try {
        ctx.ui.notify(
          `⚠️ outbox overflow: messages queued for @${peer} hit the limit; dropping oldest "${subject}"`,
          "warning",
        );
      } catch {
        /* ignore */
      }
      console.log(`[pi-a2a] ⚠️ outbox overflow: peer=${peer} dropping ${droppedId} "${subject}"`);
    },
  });
  store.load();
  store.startAutoFlush();
  state.store = store;

  // Remote workspaces / sessions (cluster): a peer may ask this host to provision
  // a working copy and open a pi session inside it. Workers are child pi processes
  // rooted at the requested path, controlled over JSON-RPC.
  const workers = new WorkerManager({
    config: cfg,
    // scope the worker registry to this project (next to the a2a DB), so two
    // bridges on one host (different projects) do not clobber each other.
    registryPath: dbPathFor(ctx.cwd).replace(/pi-a2a\.db\.json$/, "pi-a2a-workers.json"),
    log: (msg) => {
      try {
        console.log(`[pi-a2a] ${msg}`);
      } catch {
        /* ignore */
      }
    },
  });
  state.workers = workers;

  const net = new Network(cfg, {
    store,
    workers,
    onMessageReceived: (m) => {
      recalcUnread();
      try {
        ctx.ui.notify(`📨 ${m.from_name}: ${m.subject || "(no subject)"}`, "info");
      } catch {
        /* ignore */
      }
      // request / result → always inject into the current session, closing the delegation loop:
      //   FE sends request → BE handles it automatically → BE returns result → FE receives it automatically (no manual a2a_read)
      // Plain messages notify without interrupting by default; also injected when autoInjectMessage=true.
      // deliverAs=followUp: queue after the current turn when busy without interrupting; trigger a new turn immediately when idle.
      const autoInject =
        m.kind === "request" || m.kind === "result" || (m.kind === "message" && cfg.autoInjectMessage === true);
      if (autoInject) {
        try {
          const subj = m.subject ? `Subject: ${m.subject}\n\n` : "";
          let prompt: string;
          if (m.kind === "request") {
            const isExec = (m.subject || "").trim().toLowerCase().startsWith("exec:") ||
              /^\s*workdir:/m.test(m.body || "") || /^\s*session:/m.test(m.body || "");
            if (isExec) {
              // Remote-exec: force an exact, machine-parsable reply. The remote
              // agent must run the command and return ONLY the block below.
              prompt =
                `📥 Remote-exec request from @${m.from_name}\n` +
                subj +
                `${m.body}\n\n` +
                `——\n` +
                `This is a remote-exec request. Handle it per the remote-exec skill: parse the optional \`workdir:\` / \`session:\` lines at the top of the body, ` +
                `run the commands in the fenced code block on this host with the bash tool (inside workdir if given), ` +
                `then call a2a_session to obtain this session's id (echo back the provided id if the request supplied one).\n\n` +
                `Reply with a2a_reply(message_id="${m.id}", body=...); the body must contain ONLY the block below, ` +
                `with the same field order and lowercase key names exactly, and no extra prose, markdown fences or surrounding text:\n` +
                `host: <your hostname>\n` +
                `workdir: <absolute directory the command actually ran in>\n` +
                `session: <session id echoed back to the caller>\n` +
                `exit: <integer exit code; -1 if not executed>\n` +
                `stdout:\n` +
                `<stdout verbatim>\n` +
                `stderr:\n` +
                `<stderr verbatim, empty if none>`;
            } else {
              prompt =
                `📥 Task request from @${m.from_name}\n` +
                subj +
                `${m.body}\n\n` +
                `——\nHandle this request, then call a2a_reply(message_id="${m.id}", body="<result or explanation>") to return the result to @${m.from_name}.`;
            }
          } else if (m.kind === "result") {
            // result: the peer delivered a previously delegated task's result; inject so this agent receives/knows about it automatically.
            prompt =
              `📬 Result receipt from @${m.from_name}\n` +
              subj +
              `${m.body}\n\n` +
              `——\nThis is the result of a task you previously delegated; it has been delivered automatically. To follow up, use a2a_reply(message_id="${m.id}", body="..."); otherwise no action is needed.`;
          } else {
            // message: plain message, injected because autoInjectMessage=true.
            prompt =
              `📨 Message from @${m.from_name}\n` +
              subj +
              `${m.body}\n\n` +
              `——\nTo reply, use a2a_reply(message_id="${m.id}", body="..."); otherwise no action is needed.`;
          }
          api?.sendUserMessage(prompt, { deliverAs: "followUp" });
          // auto-inject means the agent has already seen the full text; mark read immediately or unread never decreases
          // (the agent won't call a2a_read again because it already saw the body via injection)
          // note: pi's sendUserMessage is fire-and-forget (the internal promise isn't returned,
          // it always returns undefined), so delivery can't be detected via the return value; api?. already guards against a missing api.
          store.markRead(m.id, cfg.peerName);
          store.persist();
          recalcUnread();
        } catch {
          /* ignore */
        }
      }
    },
    onPeersChanged: () => redrawWidget(),
    log: (msg) => {
      try {
        console.log(`[pi-a2a] ${msg}`);
      } catch {
        /* ignore */
      }
    },
  });
  await net.start();
  state.net = net;
  recalcUnread();
  state.widgetTimer = setInterval(() => {
    recalcUnread();
    redrawWidget();
  }, WIDGET_REFRESH_MS);
}

async function stopEngine(): Promise<void> {
  if (state.widgetTimer) {
    clearInterval(state.widgetTimer);
    state.widgetTimer = null;
  }
  try {
    await state.workers?.shutdown();
  } catch {
    /* ignore */
  }
  state.workers = null;
  try {
    await state.net?.stop();
  } catch {
    /* ignore */
  }
  try {
    state.store?.persist();
    state.store?.stopAutoFlush();
  } catch {
    /* ignore */
  }
  state.net = null;
  state.store = null;
}

// ── extension entry point ───────────────────────────────────

// Extension API reference (factory argument) so the network layer can, on receiving kind=request,
// call pi.sendUserMessage to inject the task into the current session → genuinely "activating" this agent.
// Plain messages are not injected, only notified; only request triggers active handling.
let api: ExtensionAPI | null = null;

export default function (pi: ExtensionAPI) {
  // Worker sessions spawned by another host's pi-a2a must not themselves join the
  // mesh (that would create a duplicate peer and discovery loops). They run with
  // PI_A2A_WORKER=1 and pi-a2a is a no-op inside them.
  if (process.env.PI_A2A_WORKER === "1") return;

  api = pi;
  pi.on("session_start", async (_event, ctx) => {
    if (!ctx.hasUI) return; // only activate in interactive TUI sessions (avoids spare services in forks/sub-sessions)
    state.lastCtx = ctx;
    const loaded = loadConfig(ctx.cwd);
    if (loaded) {
      state.config = loaded.config;
      state.configPath = loaded.path;
      await startEngine(ctx);
    }
    redrawWidget();
  });

  pi.on("session_shutdown", async () => {
    await stopEngine();
  });

  // ── Tool: a2a_send ────────────────────────────────────────
  pi.registerTool({
    name: "a2a_send",
    label: "A2A Send",
    description:
      "Send a message to another pi agent (by peer_name) or broadcast to all ('*'). Used to start a conversation, ask a question, or discuss code. Use a2a_reply instead if responding to an existing message.",
    promptSnippet: "Message another pi agent to discuss code, ask questions, or coordinate",
    promptGuidelines: [
      "Use a2a_send to start a conversation or ask another agent a question about the code.",
      "Find available recipients first with a2a_peers; the 'to' field is a peer_name or '*' for broadcast.",
      "Put the actual code/question in 'body'; keep 'subject' short.",
      "LAN P2P: messages to an offline peer go to the local outbox and are delivered automatically when the peer comes online; no need to be online simultaneously.",
      "kind=request actively activates the recipient: once delivered it is injected into their current session and their agent starts working on it and replies — use it to delegate tasks, not for small talk.",
    ],
    parameters: Type.Object({
      to: Type.String({ description: "Recipient agent peer_name, or '*' to broadcast to all agents" }),
      subject: Type.String({ description: "Short subject line" }),
      body: Type.String({ description: "Full message body — can include code, questions, explanations" }),
      kind: StringEnum(["message", "request", "result"] as const, {
        description:
          "message=ordinary conversation (notifies the recipient only, does not trigger handling); request=task request (the recipient's agent is injected into its current session and handles it immediately, then returns a result via a2a_reply); result=delivery of a requested result (also auto-injected into the recipient's current session so they receive it without a manual a2a_read)",
      }),
    }),
    async execute(_id, params, _signal, onUpdate) {
      if (!state.config || !state.store || !state.net) {
        return textResult(notReady());
      }
      const cfg = state.config;
      const kind: MsgKind = Store.validKind(params.kind) ? params.kind : "message";
      const id = genMsgId();
      const msg: Message = {
        id,
        thread_id: id,
        reply_to: null,
        from_id: cfg.agentId,
        from_name: cfg.peerName,
        to_name: params.to,
        subject: params.subject ?? "",
        body: params.body,
        kind,
        direction: "sent",
        created_at: nowSec(),
      };
      state.store.addMessage(msg);
      state.store.persist();
      onUpdate?.(textResult(`Sending to ${params.to}…`));
      const result = await state.net.deliver(msg);
      state.store.persist();

      const who = params.to === "*" ? "everyone (broadcast)" : params.to;
      let text = `✅ Sent to ${who}\n   thread: ${msg.thread_id}\n   message id: ${msg.id}`;
      if (params.to === "*") {
        if (result.delivered.length) text += `\n   delivered: ${result.delivered.join(", ") || "(no peers online)"}`;
        if (result.failed.length) text += `\n   ⚠️ not delivered: ${result.failed.join(", ")}`;
      } else if (result.delivered.length) {
        text += `\n   delivered`;
      } else if (result.queued.length) {
        text += `\n   ⏳ ${params.to} is currently offline; queued in the outbox and will be delivered when they come online`;
      }
      return textResult(text);
    },
  });

  // ── Tool: a2a_inbox ───────────────────────────────────────
  pi.registerTool({
    name: "a2a_inbox",
    label: "A2A Inbox",
    description:
      "Check messages addressed to me. Returns a list of messages (newest first) with sender, subject, preview, and id. Pass unread=true for only unread, or leave default for all. Inbox listing does not mark messages as read — use a2a_read to read the full body and mark read.",
    promptSnippet: "Check my inbox for messages from other agents",
    promptGuidelines: [
      "Use a2a_inbox to check for new messages before deciding to reply.",
      "Reply to a specific message with a2a_reply using its id.",
    ],
    parameters: Type.Object({
      unread: Type.Optional(Type.Boolean({ description: "Only unread messages (default false)" })),
      limit: Type.Optional(Type.Number({ description: "Max messages to return (default 20)" })),
    }),
    async execute(_id, params) {
      if (!state.config || !state.store) return textResult(notReady());
      const me = state.config.peerName;
      const msgs = state.store.getInbox(me, { unread: !!params.unread, limit: params.limit ?? 20 });
      if (msgs.length === 0) return textResult("📭 inbox is empty");
      const lines = msgs.map((m) => {
        const tag = state.store!.isRead(m.id, me) ? "○" : "●";
        const subj = m.subject ? `"${m.subject}"` : '"(no subject)"';
        const preview = m.body.replace(/\s+/g, " ").slice(0, 60);
        return `${tag} [${m.id}] ${m.from_name} → ${subj} ${preview}`;
      });
      return textResult(`📨 inbox (${msgs.length}):\n` + lines.join("\n"));
    },
  });

  // ── Tool: a2a_read ────────────────────────────────────────
  pi.registerTool({
    name: "a2a_read",
    label: "A2A Read",
    description:
      "Read the full body of a message by id and mark it as read. Accepts any message id in a thread (root or reply). Use this to see the complete content (including full code) of a message from a2a_inbox.",
    promptSnippet: "Read a full message by id",
    promptGuidelines: ["Use a2a_read with a message id from a2a_inbox to see its full content."],
    parameters: Type.Object({
      message_id: Type.String({ description: "Message id (e.g. msg_xxxxx); may be the thread root or any reply" }),
      thread: Type.Optional(
        Type.Boolean({
          description: "If true (default), read the whole thread; if false, return only the single message",
        }),
      ),
    }),
    async execute(_id, params) {
      if (!state.config || !state.store) return textResult(notReady());
      const me = state.config.peerName;
      const wholeThread = params.thread !== false;
      // deep variant: on a main-db miss → scan the archive so historical (archived) messages remain readable.
      // getInbox never uses deep (hot-path performance limit); a2a_read is low-frequency manual reading where scan cost is acceptable.
      const threadMsgs = state.store.getThreadDeep(params.message_id);
      if (threadMsgs.length === 0) return textResult("message not found");

      const shown = wholeThread ? threadMsgs : threadMsgs.filter((m) => m.id === params.message_id);
      const target = shown.length ? shown : threadMsgs;
      const lines = target.map((m) => {
        const t = new Date(m.created_at * 1000).toLocaleTimeString();
        return `[${t}] ${m.from_name} → ${m.to_name} ${m.subject ? `「${m.subject}」` : ""} (${m.kind})\n${m.body}`;
      });

      // mark as read (per-reader)
      if (wholeThread) state.store.markThreadRead(threadMsgs[0].thread_id, me);
      else state.store.markRead(params.message_id, me);
      state.store.persist();
      recalcUnread();
      return textResult(lines.join("\n\n---\n\n"));
    },
  });

  // ── Tool: a2a_reply ───────────────────────────────────────
  pi.registerTool({
    name: "a2a_reply",
    label: "A2A Reply",
    description:
      "Reply to a message, keeping the conversation thread. The recipient is auto-detected from the original message (if someone messaged you, your reply goes back to them). Accepts any message id in the thread.",
    promptSnippet: "Reply to a message, continuing the thread",
    promptGuidelines: ["Use a2a_reply(message_id, body) to answer a message in its thread."],
    parameters: Type.Object({
      message_id: Type.String({ description: "The id of the message you are replying to (root or reply)" }),
      body: Type.String({ description: "Your reply — can include code, answers, explanations" }),
    }),
    async execute(_id, params, _signal, onUpdate) {
      if (!state.config || !state.store || !state.net) {
        return textResult(notReady());
      }
      const cfg = state.config;
      const orig = state.store.getMessage(params.message_id);
      if (!orig) {
        return {
          content: [
            { type: "text", text: "❌ Original message not found; cannot determine the reply target (no local copy of that message)" },
          ],
          details: {}
        };
      }
      // Branch A: replying to a request I received (I hold a WORKING task as the A2A server)
      // → complete the task + push the result back to the requester immediately (without sending a new message)
      const workingTask = state.store.getWorkingInboundTaskByMsgId(params.message_id);
      if (workingTask) {
        const subject = orig.subject
          ? orig.subject.startsWith("Re:")
            ? orig.subject
            : `Re: ${orig.subject}`
          : "";
        // keep a local sent(result) copy so the thread stays complete
        const sent: Message = {
          id: genMsgId(),
          thread_id: orig.thread_id,
          reply_to: params.message_id,
          from_id: cfg.agentId,
          from_name: cfg.peerName,
          to_name: orig.from_name,
          subject,
          body: params.body,
          kind: "result",
          direction: "sent",
          created_at: nowSec(),
          taskId: workingTask.taskId,
          contextId: orig.contextId,
          taskState: "TASK_STATE_COMPLETED",
        };
        state.store.addMessage(sent);
        state.store.persist();
        onUpdate?.(textResult(`Completing task and returning result to ${orig.from_name}…`));
        const ok = await state.net.completeInboundTask(params.message_id, params.body);
        state.store.persist();
        return {
          content: [
            {
              type: "text",
              text: ok
                ? `✅ Delivered result to ${orig.from_name} (task ${workingTask.taskId} complete, pushed immediately)`
                : `⚠️ Task completion failed (task ${workingTask.taskId}); please check`,
            },
          ],
          details: {}
        };
      }

      // Branch B: ordinary reply (original was not a request / already complete / plain chat) → send a new message
      const recipient = orig.from_name; // reply to the original sender
      const subject = orig.subject
        ? orig.subject.startsWith("Re:")
          ? orig.subject
          : `Re: ${orig.subject}`
        : "";
      const msg: Message = {
        id: genMsgId(),
        thread_id: orig.thread_id,
        reply_to: params.message_id,
        from_id: cfg.agentId,
        from_name: cfg.peerName,
        to_name: recipient,
        subject,
        body: params.body,
        // replying to a request → deliver a result, closing the semantics; otherwise keep it a message.
        kind: orig.kind === "request" ? "result" : "message",
        direction: "sent",
        created_at: nowSec(),
      };
      state.store.addMessage(msg);
      state.store.persist();
      onUpdate?.(textResult(`Replying to ${recipient}…`));
      await state.net.deliver(msg);
      state.store.persist();
      return textResult(`✅ Replied to ${recipient} (thread ${msg.thread_id})`);
    },
  });

  // ── Tool: a2a_peers ───────────────────────────────────────
  pi.registerTool({
    name: "a2a_peers",
    label: "A2A Peers",
    description:
      "List other agents in the workspace — who's online (active within 60s) and their roles. Use before messaging to discover valid recipient names.",
    promptSnippet: "List online agents and their roles",
    promptGuidelines: [
      "Use a2a_peers to find who you can message; valid 'to' names come from here.",
      "LAN P2P: shows only mDNS-discovered peers in the same workspace; make sure the peer has run /a2a-setup and is on the same LAN.",
    ],
    parameters: Type.Object({
      all: Type.Optional(Type.Boolean({ description: "Include offline agents too (default false)" })),
    }),
    async execute(_id, params) {
      if (!state.config || !state.net) return textResult(notReady());
      const me = state.config.peerName;
      const peers: Peer[] = params.all ? state.net.getPeers() : state.net.getOnlinePeers();
      const others = peers.filter((p) => p.peerName !== me);
      if (others.length === 0) {
        return {
          content: [
            {
              type: "text",
              text: `🤷 No other agents ${params.all ? "known" : "online"} right now (you are ${me}). Have the peer run /a2a-setup to join the same workspace + secret, and ensure the same LAN and an installed bonjour-service.`,
            },
          ],
          details: {}
        };
      }
      const lines = others.map((p) => {
        const status = params.all && !state.net!.isOnline(p.peerName) ? "⚪" : "🟢";
        const cap = p.caps?.includes("sessions") ? "  [remote-sessions]" : "";
        return `${status} ${p.peerName}${p.role ? ` — ${p.role}` : ""}${cap}`;
      });
      return textResult(`👥 Agents:\n` + lines.join("\n"));
    },
  });

  // ── Tool: a2a_session ─────────────────────────────────────
  pi.registerTool({
    name: "a2a_session",
    label: "A2A Session",
    description:
      "Report the current pi session identity (id + name) used to give remote-exec callers a stable session to continue in. Optionally set the session name. Use when a remote-exec request carries a session: line or when you must return a session id to a caller.",
    promptSnippet: "Read or name the current session for remote-exec continuity",
    promptGuidelines: [
      "Use a2a_session to get the current session id when replying to a remote-exec request.",
      "If the caller sent a session: line, echo it back; otherwise reply with the id a2a_session returns so the caller can reuse it.",
    ],
    parameters: Type.Object({
      name: Type.Optional(
        Type.String({ description: "Optional: set the session display name (e.g. 'exec:box') before returning the id." }),
      ),
    }),
    async execute(_id, params) {
      const ctx = state.lastCtx;
      if (!ctx) return textResult("⚠️ Session context unavailable.");
      try {
        if (params.name) ctx.setSessionName?.(params.name);
        // Session id/file live on the read-only session manager, not on ctx itself.
        const sm = ctx.sessionManager;
        const sessionId: string | undefined =
          sm?.getSessionId?.() ?? ctx.sessionId ?? ctx.getSessionId?.();
        const sessionName: string | undefined =
          sm?.getSessionName?.() ?? ctx.getSessionName?.() ?? params.name;
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({ session: sessionId ?? null, name: sessionName ?? null }),
            },
          ],
          details: { sessionId: sessionId ?? null, sessionName: sessionName ?? null },
        };
      } catch (e) {
        return textResult(`⚠️ Could not read session: ${e instanceof Error ? e.message : String(e)}`);
      }
    },
  });

  // ── Tool: a2a_remote ────────────────────────────────────
  pi.registerTool({
    name: "a2a_remote",
    label: "A2A Remote Workspace/Session",
    description:
      "Provision a working copy on another host and open a real pi session rooted inside it, then send prompts into that session and read the replies. Not limited to pre-configured folders: `path` may be any directory on the peer (relative paths resolve under its workspaceRoot). Ops: provision (git clone/fetch/checkout into `path`), open (create/reuse a session at `path`, optionally provisioning first via `git`/`ref`), prompt (send work into a session `handle` and wait for the final answer), list (show remote sessions), close (dispose a session).",
    promptSnippet: "Provision a working copy and open/prompt a pi session on another host",
    promptGuidelines: [
      "Use a2a_remote when the work must happen on another host (different OS/hardware/repo) and needs its own project checkout and session rather than a one-off command.",
      "Flow: a2a_remote(op='open', peer, path, git=<url>, ref=<branch>) provisions the checkout and opens a session in one step; then a2a_remote(op='prompt', peer, handle, message) sends work to it; use op='close' when done.",
      "op='provision' only sets up the working copy; op='open' requires `path`, and provisions first when `git` is given.",
      "op='open' always refreshes an existing git checkout (fetch + optional ref checkout) before starting, so sessions never run on a stale tree; if the refresh fails (e.g. offline) the session still opens and the warning is reported.",
      "Remote sessions run a full pi agent with file/bash tools rooted at `path`, so prompts can be multi-step coding tasks; the reply is the session's final assistant text.",
      "The peer must advertise [remote-sessions] in a2a_peers; the shared secret is the only credential, so only target peers you trust.",
      "For a single command on another host, prefer a remote-exec (a2a_send kind=request, subject 'exec: …') instead of a session.",
    ],
    parameters: Type.Object({
      peer: Type.String({ description: "Target agent peer_name (see a2a_peers)" }),
      op: StringEnum(["provision", "open", "prompt", "list", "close"] as const, {
        description:
          "provision=clone/update a working copy; open=create/reuse a session at path (provisions first if git given); prompt=send a message to a session handle; list=show sessions; close=dispose a session",
      }),
      path: Type.Optional(
        Type.String({
          description: "Workspace directory on the peer. Relative paths resolve under the peer's workspaceRoot; absolute paths are allowed. Required for provision/open.",
        }),
      ),
      git: Type.Optional(
        Type.String({ description: "Git remote URL to clone (or fetch if the workspace already exists). Optional for open/provision." }),
      ),
      ref: Type.Optional(Type.String({ description: "Branch, tag or commit to check out after clone/fetch" })),
      submodules: Type.Optional(Type.Boolean({ description: "Run `git submodule update --init --recursive` (default false)" })),
      name: Type.Optional(Type.String({ description: "Display name for the session (default remote:<dirname>)" })),
      model: Type.Optional(Type.String({ description: "Optional model pattern for the session (e.g. 'anthropic/*')" })),
      thinking: Type.Optional(
        Type.String({ description: "Optional thinking level: off|minimal|low|medium|high|xhigh|max" }),
      ),
      handle: Type.Optional(Type.String({ description: "Session handle returned by op=open. Required for prompt/close." })),
      message: Type.Optional(Type.String({ description: "Prompt text to send into the session. Required for op=prompt." })),
      prompt: Type.Optional(
        Type.String({ description: "Optional initial prompt to run immediately after op=open (convenience)." }),
      ),
      timeoutMs: Type.Optional(
        Type.Number({ description: "Optional wall-clock timeout for open/prompt (ms); defaults to the peer's workerPromptTimeoutMs" }),
      ),
    }),
    async execute(_id, params, _signal, onUpdate) {
      if (!state.config || !state.net) return textResult(notReady());
      const peer = String(params.peer ?? "").trim();
      if (!peer) return textResult("❌ a2a_remote requires `peer` (see a2a_peers).");
      const op = String(params.op ?? "");
      try {
        switch (op) {
          case "provision": {
            if (!params.path) return textResult("❌ op=provision requires `path`.");
            onUpdate?.(textResult(`⏳ provisioning ${params.path} on ${peer}…`));
            const r = await state.net.provisionWorkspace(peer, {
              path: params.path,
              git: params.git,
              ref: params.ref,
              submodules: params.submodules,
            });
            return textResult(
              `✅ workspace ${r.action} on ${peer}\n   path: ${r.path}\n   head: ${r.head ?? "(n/a)"}\n   branch: ${r.branch || "(n/a)"}`,
              { peer, ...r },
            );
          }
          case "open": {
            if (!params.path) return textResult("❌ op=open requires `path`.");
            onUpdate?.(textResult(`⏳ opening session on ${peer} at ${params.path}…`));
            const r = await state.net.openSession(peer, {
              path: params.path,
              git: params.git,
              ref: params.ref,
              submodules: params.submodules,
              name: params.name,
              model: params.model,
              thinking: params.thinking,
              prompt: params.prompt,
              timeoutMs: params.timeoutMs,
            });
            let text = `✅ session ${r.reused ? "reused" : "opened"} on ${peer}\n   handle: ${r.handle}\n   path: ${r.path}\n   session: ${r.sessionId}\n   name: ${r.name}`;
            if (r.provision) text += `\n   git: ${r.provision.action} (head ${r.provision.head ?? "n/a"})`;
            if (r.refreshError) text += `\n   ⚠️ refresh failed, opened the existing tree as-is: ${r.refreshError}`;
            if (typeof r.reply === "string") text += `\n\n── reply ──\n${r.reply}`;
            return textResult(text, { peer, ...r });
          }
          case "prompt": {
            if (!params.handle) return textResult("❌ op=prompt requires `handle` (from op=open).");
            if (!params.message) return textResult("❌ op=prompt requires `message`.");
            onUpdate?.(textResult(`⏳ prompting session ${params.handle} on ${peer}…`));
            const r = await state.net.promptSession(peer, {
              handle: params.handle,
              message: params.message,
              timeoutMs: params.timeoutMs,
            });
            const secs = typeof r.elapsedMs === "number" ? (r.elapsedMs / 1000).toFixed(1) : "?";
            return textResult(
              `✅ ${r.handle} (${r.path}) replied in ${secs}s · turn ${r.turns}\n\n${r.reply || "(empty reply)"}`,
              { peer, ...r },
            );
          }
          case "list": {
            const r = await state.net.listSessions(peer);
            const sessions: any[] = Array.isArray(r.sessions) ? r.sessions : [];
            if (sessions.length === 0) return textResult(`📭 no remote sessions on ${peer}`);
            const lines = sessions.map(
              (s) =>
                `${s.alive ? "🟢" : "⚪"} ${s.handle} · ${s.path} · name=${s.name} · session=${s.sessionId} · turns=${s.turns}`,
            );
            return textResult(`🗂️ remote sessions on ${peer} (${sessions.length}/${r.maxSessions ?? "?"}):\n` + lines.join("\n"), {
              peer,
              ...r,
            });
          }
          case "close": {
            if (!params.handle) return textResult("❌ op=close requires `handle`.");
            const r = await state.net.closeSession(peer, { handle: params.handle });
            return textResult(`🗑️ closed ${r.handle} on ${peer} (${r.path})`, { peer, ...r });
          }
          default:
            return textResult(`❌ unknown op: ${op} (expected provision|open|prompt|list|close)`);
        }
      } catch (e) {
        return textResult(`⚠️ a2a_remote ${op} failed: ${e instanceof Error ? e.message : String(e)}`);
      }
    },
  });

  // ── Tool: a2a_put ─────────────────────────────────────────
  pi.registerTool({
    name: "a2a_put",
    label: "A2A Put File",
    description:
      "Copy a local file to another agent's file sandbox over the a2a channel (peer-to-peer HTTP, bytes do not go through the model). Paths are relative to each side's file root; use a2a_file_root to see it.",
    promptSnippet: "Send a file to another agent",
    promptGuidelines: [
      "Use a2a_put to hand a file to another host; both paths are relative to the a2a file root, not arbitrary filesystem paths.",
      "Use a2a_file_root to discover the local root if unsure.",
      "For text/config snippets under ~300KB, an exec: request with base64 also works, but a2a_put is cheaper and handles large/binary files.",
    ],
    parameters: Type.Object({
      to: Type.String({ description: "Recipient agent peer_name" }),
      path: Type.String({ description: "Source path relative to the local file root" }),
      remotePath: Type.Optional(
        Type.String({ description: "Destination path relative to the peer's file root (default: same as path)" }),
      ),
      overwrite: Type.Optional(Type.Boolean({ description: "Overwrite if it exists on the peer (default false)" })),
    }),
    async execute(_id, params) {
      if (!state.config || !state.net) return textResult(notReady());
      try {
        const res = await state.net.putFile(
          params.to,
          params.path,
          params.remotePath ?? params.path,
          params.overwrite === true,
        );
        return textResult(`✅ Sent to ${params.to}:\n  path: ${res.path}\n  bytes: ${res.bytes}\n  sha256: ${res.sha256}`);
      } catch (e) {
        return textResult(`⚠️ a2a_put failed: ${e instanceof Error ? e.message : String(e)}`);
      }
    },
  });

  // ── Tool: a2a_get ─────────────────────────────────────────
  pi.registerTool({
    name: "a2a_get",
    label: "A2A Get File",
    description:
      "Fetch a file from another agent's file sandbox into the local one over the a2a channel (peer-to-peer HTTP, bytes do not go through the model). Paths are relative to each side's file root.",
    promptSnippet: "Fetch a file from another agent",
    promptGuidelines: [
      "Use a2a_get to pull a file from another host; both paths are relative to the a2a file root.",
      "The transferred file lands inside the local file root, never outside it.",
    ],
    parameters: Type.Object({
      from: Type.String({ description: "Source agent peer_name" }),
      path: Type.String({ description: "Path relative to the peer's file root" }),
      localPath: Type.Optional(
        Type.String({ description: "Destination path relative to the local file root (default: same as path)" }),
      ),
      overwrite: Type.Optional(Type.Boolean({ description: "Overwrite if it exists locally (default false)" })),
    }),
    async execute(_id, params) {
      if (!state.config || !state.net) return textResult(notReady());
      try {
        const res = await state.net.getFile(
          params.from,
          params.path,
          params.localPath ?? params.path,
          params.overwrite === true,
        );
        return textResult(`✅ Fetched from ${params.from}:\n  path: ${res.path}\n  bytes: ${res.bytes}\n  sha256: ${res.sha256}`);
      } catch (e) {
        return textResult(`⚠️ a2a_get failed: ${e instanceof Error ? e.message : String(e)}`);
      }
    },
  });

  // ── Tool: a2a_file_root ───────────────────────────────────
  pi.registerTool({
    name: "a2a_file_root",
    label: "A2A File Root",
    description:
      "Report the local sandbox directory used by a2a_put / a2a_get. All file transfers are confined to this directory on each host.",
    promptSnippet: "Show the local a2a file sandbox root",
    promptGuidelines: ["Use a2a_file_root to learn where a2a_put/a2a_get place files locally."],
    parameters: Type.Object({}),
    async execute() {
      if (!state.config) return textResult(notReady());
      const root = resolveFileRoot((state.config as any).fileRoot);
      return textResult(`📁 a2a file root: ${root}\n(all a2a_put / a2a_get paths are relative to this directory)`);
    },
  });

  // ── Tool: a2a_mem_set ─────────────────────────────────────
  pi.registerTool({
    name: "a2a_mem_set",
    label: "A2A Mem Set",
    description:
      "Write a key-value pair to the workspace shared memory. Replicates to all online peers immediately; offline peers sync on reconnect. Use for cross-agent shared context: architecture notes, API contracts, decisions, config that every agent in the workspace should know.",
    promptSnippet: "Write to shared memory",
    promptGuidelines: [
      "Use a2a_mem_set for things EVERY agent in the workspace should know (architecture, contracts, decisions).",
      "Pick descriptive keys (e.g. 'auth_strategy', 'db_schema_v2'); avoid overwriting blindly.",
    ],
    parameters: Type.Object({
      key: Type.String({ description: "Memory key (e.g. 'api_schema')" }),
      value: Type.String({ description: "Value to store" }),
    }),
    async execute(_id, params) {
      if (!state.config || !state.store) return textResult(notReady());
      const me = state.config.peerName;
      const entry = state.store.memSetLocal(params.key, params.value, me);
      state.store.persist();
      state.net?.broadcastMemUpdate(params.key, entry);
      return textResult(`✅ Shared memory "${params.key}" set (${params.value.length} chars, broadcast to online peers)`);
    },
  });

  // ── Tool: a2a_mem_get ─────────────────────────────────────
  pi.registerTool({
    name: "a2a_mem_get",
    label: "A2A Mem Get",
    description:
      "Read a value from the workspace shared memory by key. Returns the value, who wrote it, and when. Returns 'not found' if the key doesn't exist or was deleted.",
    promptSnippet: "Read a shared memory key",
    promptGuidelines: ["Use a2a_mem_get to recall a previously stored value before asking peers."],
    parameters: Type.Object({
      key: Type.String({ description: "Memory key" }),
    }),
    async execute(_id, params) {
      if (!state.config || !state.store) return textResult(notReady());
      const e = state.store.memGet(params.key);
      if (!e) return textResult(`🤷 Shared memory "${params.key}" does not exist (or was deleted)`);
      const when = new Date(e.ts).toLocaleString();
      return textResult(`📦 ${params.key}（by @${e.author} @ ${when}）:\n${e.value}`);
    },
  });

  // ── Tool: a2a_mem_keys ────────────────────────────────────
  pi.registerTool({
    name: "a2a_mem_keys",
    label: "A2A Mem Keys",
    description: "List all keys currently in the workspace shared memory (excluding deleted). Use to see what shared context exists before reading specific values.",
    promptSnippet: "List shared memory keys",
    promptGuidelines: ["Use a2a_mem_keys first to discover what shared context exists, then a2a_mem_get for specifics."],
    parameters: Type.Object({}),
    async execute() {
      if (!state.config || !state.store) return textResult(notReady());
      const keys = state.store.memKeys();
      if (keys.length === 0) return textResult("📭 shared memory is empty");
      return textResult(`🗝️ shared memory (${keys.length}):\n${keys.map((k) => `- ${k}`).join("\n")}`);
    },
  });

  // ── Tool: a2a_mem_delete ──────────────────────────────────
  pi.registerTool({
    name: "a2a_mem_delete",
    label: "A2A Mem Delete",
    description:
      "Delete a key from workspace shared memory. Writes a tombstone that replicates to peers, preventing late-arriving writes from resurrecting the key. Use when a key is obsolete or wrong.",
    promptSnippet: "Delete a shared memory key",
    promptGuidelines: ["Use a2a_mem_delete when a key is obsolete; the deletion propagates to all peers."],
    parameters: Type.Object({
      key: Type.String({ description: "Memory key to delete" }),
    }),
    async execute(_id, params) {
      if (!state.config || !state.store) return textResult(notReady());
      const me = state.config.peerName;
      const entry = state.store.memDeleteLocal(params.key, me);
      state.store.persist();
      state.net?.broadcastMemUpdate(params.key, entry);
      return textResult(`🗑️ Shared memory "${params.key}" deleted (tombstone broadcast)`);
    },
  });

  // ── Command: /a2a-setup ───────────────────────────────────
  pi.registerCommand("a2a-setup", {
    description: "Configure pi-a2a (workspace / secret / agent identity)",
    handler: async (_args, ctx) => {
      if (!ctx.hasUI) {
        ctx.ui.notify("a2a-setup must run inside the TUI", "warning");
        return;
      }
      const workspace = (await ctx.ui.input("Workspace name (agents on the same team share one name)", "my-team"))?.trim();
      if (!workspace) {
        ctx.ui.notify("Cancelled", "info");
        return;
      }
      const workspaceSecret = (
        await ctx.ui.input("Shared secret (must be identical for all agents in the workspace; used for LAN mutual authentication)", "")
      )?.trim();
      if (!workspaceSecret) {
        ctx.ui.notify("A shared secret is required", "warning");
        return;
      }
      const peerName = (await ctx.ui.input("Your agent name (e.g. backend / frontend / reviewer)", ""))?.trim();
      if (!peerName) {
        ctx.ui.notify("An agent name is required", "warning");
        return;
      }
      const role = (await ctx.ui.input("Role / capability (optional, e.g. API / frontend / review)", ""))?.trim();

      const injectChoice = await ctx.ui.select("How should plain messages be handled?", [
        "Notify only (default) — show a toast; inspect manually with a2a_inbox/read",
        "Auto-read and handle — inject into the current session on arrival",
      ]);
      const autoInjectMessage = injectChoice?.startsWith("Auto-read") === true;

      const cfg: A2aConfig = {
        workspace,
        workspaceSecret,
        agentId: genAgentId(),
        peerName,
        role: role || undefined,
        autoInjectMessage,
      };
      const fp = saveConfig(ctx.cwd, cfg);
      ctx.ui.notify(`✅ Configuration saved: ${fp}`, "info");

      // activate: stop the old engine first, then start with the new config
      await stopEngine();
      state.config = cfg;
      state.configPath = fp;
      state.lastCtx = ctx;
      await startEngine(ctx);
      ctx.ui.notify(`🎉 pi-a2a is ready! workspace "${workspace}" · you are ${peerName}`, "info");
    },
  });

  // ── Command: /a2a ─────────────────────────────────────────
  pi.registerCommand("a2a", {
    description: "Show pi-a2a status (inbox + online agents)",
    handler: async (_args, ctx) => {
      if (!state.config || !state.net || !state.store) {
        ctx.ui.notify(NO_CONFIG_MSG, "warning");
        return;
      }
      recalcUnread();
      const me = state.config.peerName;
      const peers = state.net.getOnlinePeers().filter((p) => p.peerName !== me);
      const peersTxt =
        peers.map((p) => `🟢 ${p.peerName}${p.role ? `·${p.role}` : ""}`).join("\n  ") ||
        "(no other agents online)";
      ctx.ui.notify(`pi-a2a · ${me}\n📨 ${state.unread} unread\nOnline:\n  ${peersTxt}`, "info");
    },
  });

  // ── Command: /a2a-clear ───────────────────────────────────
  pi.registerCommand("a2a-clear", {
    description: "Clear the inbox (main-db inbox messages only; archive and sent history untouched)",
    handler: async (_args, ctx) => {
      if (!state.config || !state.store) {
        ctx.ui.notify(NO_CONFIG_MSG, "warning");
        return;
      }
      if (!ctx.hasUI) {
        ctx.ui.notify("a2a-clear must run inside the TUI", "warning");
        return;
      }
      const me = state.config.peerName;
      const inbox = state.store.getInbox(me, { limit: 999999 });
      if (inbox.length === 0) {
        ctx.ui.notify("📭 Inbox already empty; nothing to clear", "info");
        return;
      }
      const unread = state.store.unreadCount(me);
      const confirm = await ctx.ui.select(
        `Clear the inbox? This deletes ${inbox.length} messages (${unread} unread). Archive and sent history are unaffected.`,
        ["Clear", "Cancel"],
      );
      if (confirm !== "Clear") {
        ctx.ui.notify("Cancelled", "info");
        return;
      }
      const removed = state.store.clearInbox();
      state.store.persist();
      recalcUnread();
      ctx.ui.notify(`🗑️ Inbox cleared; deleted ${removed} messages`, "info");
    },
  });

  // ── Command: /a2a-send ────────────────────────────────────
  pi.registerCommand("a2a-send", {
    description: "Send a message to another agent directly (bypassing the AI). Usage: /a2a-send <peer> <message> or /a2a-send interactively",
    handler: async (args, ctx) => {
      if (!state.config || !state.store || !state.net) {
        ctx.ui.notify(notReady(), "warning");
        return;
      }
      const cfg = state.config;
      let to: string;
      let body: string;
      let subject = "";

      const trimmed = (args ?? "").trim();
      if (trimmed) {
        // /a2a-send <peer> <body...>  —— before the first space is the peer; everything after is the message body
        const sp = trimmed.indexOf(" ");
        if (sp === -1) {
          ctx.ui.notify("Usage: /a2a-send <peer> <message> (space after the peer, then the body)", "warning");
          return;
        }
        to = trimmed.slice(0, sp).replace(/^@/, "");
        body = trimmed.slice(sp + 1).trim();
      } else {
        // no arguments → interactively pick a peer + enter subject/body
        if (!ctx.hasUI) {
          ctx.ui.notify("Interactive a2a-send requires the TUI, or use /a2a-send <peer> <message>", "warning");
          return;
        }
        const peers = state.net.getOnlinePeers().filter((p) => p.peerName !== cfg.peerName);
        if (peers.length === 0) {
          ctx.ui.notify("🤷 No agents online right now (try /a2a)", "warning");
          return;
        }
        const picked = await ctx.ui.select("Send to whom?", peers.map((p) => p.peerName));
        if (!picked) {
          ctx.ui.notify("Cancelled", "info");
          return;
        }
        to = picked;
        subject = (await ctx.ui.input("Subject (optional, Enter to skip)", ""))?.trim() || "";
        body = (await ctx.ui.input("Message body", ""))?.trim() || "";
      }

      if (!body) {
        ctx.ui.notify("Message body is empty; cancelled", "warning");
        return;
      }

      const id = genMsgId();
      const msg: Message = {
        id,
        thread_id: id,
        reply_to: null,
        from_id: cfg.agentId,
        from_name: cfg.peerName,
        to_name: to,
        subject,
        body,
        kind: "message",
        direction: "sent",
        created_at: nowSec(),
      };
      state.store.addMessage(msg);
      state.store.persist();
      const result = await state.net.deliver(msg);
      state.store.persist();

      let text = `✅ Sent to ${to === "*" ? "everyone" : to}`;
      if (result.delivered.length) text += " (delivered)";
      else if (result.queued.length) text += " (⏳ peer offline; queued in the outbox, delivered automatically when online)";
      else if (result.failed.length) text += " (⚠️ not delivered)";
      ctx.ui.notify(text, "info");
    },
  });

  // ── Command: /a2a-inbox ───────────────────────────────────
  pi.registerCommand("a2a-inbox", {
    description: "View the inbox (bypassing the AI). Usage: /a2a-inbox [unread|<msg_id>]",
    handler: async (args, ctx) => {
      if (!state.config || !state.store) {
        ctx.ui.notify(NO_CONFIG_MSG, "warning");
        return;
      }
      const me = state.config.peerName;
      const trimmed = (args ?? "").trim();

      // /a2a-inbox <msg_id>  → read the full message + mark as read
      if (trimmed && trimmed !== "unread") {
        const threadMsgs = state.store.getThreadDeep(trimmed);
        if (threadMsgs.length === 0) {
          ctx.ui.notify(`Message not found: ${trimmed}`, "warning");
          return;
        }
        const lines = threadMsgs.map((m) => {
          const t = new Date(m.created_at * 1000).toLocaleTimeString();
          return `[${t}] ${m.from_name} → ${m.to_name}${m.subject ? ` 「${m.subject}」` : ""}\n${m.body}`;
        });
        state.store.markThreadRead(threadMsgs[0].thread_id, me);
        state.store.persist();
        recalcUnread();
        ctx.ui.notify(lines.join("\n\n---\n\n"), "info");
        return;
      }

      // /a2a-inbox [unread]  → list
      const unreadOnly = trimmed === "unread";
      const msgs = state.store.getInbox(me, { unread: unreadOnly, limit: 20 });
      if (msgs.length === 0) {
        ctx.ui.notify(unreadOnly ? "📭 No unread messages" : "📭 Inbox is empty", "info");
        return;
      }
      const totalUnread = state.store.unreadCount(me);
      const lines = msgs.map((m) => {
        const tag = state.store!.isRead(m.id, me) ? "○" : "●";
        const subj = m.subject ? `"${m.subject}"` : '"(no subject)"';
        const preview = m.body.replace(/\s+/g, " ").slice(0, 60);
        return `${tag} [${m.id}] ${m.from_name} → ${subj} ${preview}`;
      });
      const header = `📨 inbox (${msgs.length}${unreadOnly ? " unread" : ""} · ${totalUnread} unread total)`;
      ctx.ui.notify(`${header}\n` + lines.join("\n"), "info");
    },
  });

  // ── Command: /a2a-peers ───────────────────────────────────
  pi.registerCommand("a2a-peers", {
    description: "View online agents (bypassing the AI). Usage: /a2a-peers [all]",
    handler: async (args, ctx) => {
      if (!state.config || !state.net) {
        ctx.ui.notify(NO_CONFIG_MSG, "warning");
        return;
      }
      const me = state.config.peerName;
      const includeAll = (args ?? "").trim() === "all";
      const peers = includeAll ? state.net.getPeers() : state.net.getOnlinePeers();
      const others = peers.filter((p) => p.peerName !== me);
      if (others.length === 0) {
        ctx.ui.notify(`🤷 No other agents ${includeAll ? "known" : "online"} right now`, "info");
        return;
      }
      const lines = others.map((p) => {
        const status = includeAll && !state.net!.isOnline(p.peerName) ? "⚪" : "🟢";
        const cap = p.caps?.includes("sessions") ? "  [remote-sessions]" : "";
        return `${status} ${p.peerName}${p.role ? ` — ${p.role}` : ""}${cap}`;
      });
      const onlineCount = others.filter((p) => state.net!.isOnline(p.peerName)).length;
      const header = includeAll
        ? `👥 Agents (${onlineCount} online / ${others.length} known)`
        : `👥 Online Agents (${others.length})`;
      ctx.ui.notify(`${header}\n` + lines.join("\n"), "info");
    },
  });
}

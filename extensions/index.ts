/**
 * pi-a2a — Agent-to-agent message bus for pi coding agent (局域网 P2P 版)
 *
 * 同一局域网内、同一 workspace + 共享密钥 的 pi agent 自动互相发现、点对点收发。
 * 无中心服务器、无云端依赖、无需部署。每个 agent 本地各自存储。
 *
 * 发现: mDNS/Bonjour (bonjour-service) 广告 + 浏览 _pi-a2a._tcp
 * 传输: 每个 agent 本地起 HTTP server（node:http），点对点直推
 * 存储: 本地 JSON 文件（store.ts），inbox + sent 副本，本地还原完整线程
 * 异步: 对方离线 → 本地 outbox 暂存；对方上线自动重投（无需同时在线）
 *
 * Tools:    a2a_send, a2a_inbox, a2a_read, a2a_reply, a2a_peers
 * Commands: /a2a-setup, /a2a, /a2a-clear, /a2a-send, /a2a-inbox, /a2a-peers
 * Widget:   本地未读数 + 在线 peer（事件驱动刷新 + 低频兜底）
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

// ── 状态 ────────────────────────────────────────────────────

interface LiveState {
  config: A2aConfig | null;
  configPath: string | null;
  store: Store | null;
  net: Network | null;
  unread: number;
  lastCtx: any; // 最近一次 ctx，供 widget 重绘 / toast
  widgetTimer: ReturnType<typeof setInterval> | null;
}

const state: LiveState = {
  config: null,
  configPath: null,
  store: null,
  net: null,
  unread: 0,
  lastCtx: null,
  widgetTimer: null,
};

const NO_CONFIG_MSG =
  "⚠️ pi-a2a 尚未配置。请运行 `/a2a-setup` 设置工作区名、共享密钥和 agent 名字。";

const WIDGET_REFRESH_MS = 5000; // 低频兜底刷新（新消息/上下线主要由事件驱动）

// ── 小工具 ──────────────────────────────────────────────────

function genMsgId(): string {
  return "msg_" + crypto.randomBytes(5).toString("hex"); // 10 hex
}
function nowSec(): number {
  return Math.floor(Date.now() / 1000);
}

function notReady(): string {
  if (!state.config) return NO_CONFIG_MSG;
  return "⏳ pi-a2a 尚未就绪（网络服务未启动）。稍候重试，或重新运行 /a2a-setup。";
}

/** 标准 text 工具结果。SDK 要求 AgentToolResult 必填 details（供日志/UI），这里默认空对象。 */
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
            return [theme.fg("muted", "📨 pi-a2a 未配置（/a2a-setup）")];
          }
          const me = state.config.peerName;
          const peers = (state.net?.getOnlinePeers() ?? []).filter((p) => p.peerName !== me);
          const peersTxt =
            peers.length === 0
              ? "（无其他 agent 在线）"
              : peers.map((p) => p.peerName + (p.role ? `·${p.role}` : "")).join(" ");
          const unreadTxt =
            state.unread > 0
              ? theme.bold(theme.fg("warning", `📨 ${state.unread} 未读`))
              : theme.fg("muted", "📭 收件箱空");
          const line1 =
            `🟢 a2a·${theme.bold(me)}` + (state.config.role ? `·${state.config.role}` : "");
          const line2 = `  ${unreadTxt}`;
          const line3 = `  在线: ${peersTxt}`;
          return [line1, line2, line3].map((l) => (l.length > width ? l.slice(0, width - 1) + "…" : l));
        },
        invalidate() {},
      }),
      { placement: "belowEditor" },
    );
  } catch {
    // ctx 可能已 stale（会话切换/退出后定时器仍触发）；静默跳过，
    // 新会话 session_start 会重建 widget。
  }
}

// ── 引擎启停 ────────────────────────────────────────────────

async function startEngine(ctx: any): Promise<void> {
  const cfg = state.config;
  if (!cfg) return;
  // 存储与 ctx 同 scope；注入 onOutboxOverflow 让溢出能冒泡到 UI（防静默丢消息）
  const store = new Store(dbPathFor(ctx.cwd), {
    onOutboxOverflow: (peer, droppedId, subject) => {
      try {
        ctx.ui.notify(
          `⚠️ outbox 溢出：给 @${peer} 的消息积压达上限，丢弃最旧「${subject}」`,
          "warning",
        );
      } catch {
        /* ignore */
      }
      console.log(`[pi-a2a] ⚠️ outbox 溢出: peer=${peer} 丢弃 ${droppedId}「${subject}」`);
    },
  });
  store.load();
  store.startAutoFlush();
  state.store = store;

  const net = new Network(cfg, {
    store,
    onMessageReceived: (m) => {
      recalcUnread();
      try {
        ctx.ui.notify(`📨 ${m.from_name}: ${m.subject || "(无主题)"}`, "info");
      } catch {
        /* ignore */
      }
      // request / result → 总是主动注入当前会话，形成委派闭环：
      //   FE 发 request → BE 自动处理 → BE 回 result → FE 自动接收（无需手动 a2a_read）
      // 普通消息(message)默认只通知不打扰；配置 autoInjectMessage=true 时也注入。
      // deliverAs=followUp：本 agent 正忙时排队到当前 turn 之后，不中断；空闲时立即触发新 turn。
      const autoInject =
        m.kind === "request" || m.kind === "result" || (m.kind === "message" && cfg.autoInjectMessage === true);
      if (autoInject) {
        try {
          const subj = m.subject ? `主题: ${m.subject}\n\n` : "";
          let prompt: string;
          if (m.kind === "request") {
            prompt =
              `📥 来自 @${m.from_name} 的任务请求\n` +
              subj +
              `${m.body}\n\n` +
              `——\n请处理这个请求，完成后调用 a2a_reply(message_id="${m.id}", body="<结果或处理说明>") 把结果回给 @${m.from_name}。`;
          } else if (m.kind === "result") {
            // result：对方交付了之前委派任务的返回结果，注入让本 agent 自动接收/知晓。
            prompt =
              `📬 来自 @${m.from_name} 的结果回执\n` +
              subj +
              `${m.body}\n\n` +
              `——\n这是你之前委派任务的返回结果，已自动送达。如需继续追问，可用 a2a_reply(message_id="${m.id}", body="...")；否则无需任何操作。`;
          } else {
            // message：普通消息，因 autoInjectMessage=true 而注入。
            prompt =
              `📨 来自 @${m.from_name} 的消息\n` +
              subj +
              `${m.body}\n\n` +
              `——\n如需回复，可用 a2a_reply(message_id="${m.id}", body="...")；否则无需任何操作。`;
          }
          api?.sendUserMessage(prompt, { deliverAs: "followUp" });
          // 自动注入=agent 已看到全文，立即标记已读，否则 unread 永不下降
          // （agent 不会再调 a2a_read，因为它已经通过注入看到了 body）
          // 注意：pi 的 sendUserMessage 是 fire-and-forget（内部 promise 不 return，
          // 永远返回 undefined），无法用返回值判断投递成功；api?. 已挡住 api 缺失。
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

// ── 扩展入口 ────────────────────────────────────────────────

// 扩展 API 引用（工厂入参），供网络层回调在收到 kind=request 时
// 调 pi.sendUserMessage 把任务注入当前会话 → 真正“调动”本 agent。
// 普通消息(message)不注入，只通知；只有 request 会触发主动处理。
let api: ExtensionAPI | null = null;

export default function (pi: ExtensionAPI) {
  api = pi;
  pi.on("session_start", async (_event, ctx) => {
    if (!ctx.hasUI) return; // 仅在交互式 TUI 会话激活（避免 fork/子会话起多余服务）
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
      "局域网 P2P：对方离线时消息会进本地发件箱，对方上线自动送达，无需同时在线。",
      "kind=request 会主动调动对方：消息送达后会被注入对方当前会话，对方 agent 会立即着手处理并回复——用于委派任务，不要用于闲聊。",
    ],
    parameters: Type.Object({
      to: Type.String({ description: "Recipient agent peer_name, or '*' to broadcast to all agents" }),
      subject: Type.String({ description: "Short subject line" }),
      body: Type.String({ description: "Full message body — can include code, questions, explanations" }),
      kind: StringEnum(["message", "request", "result"] as const, {
        description:
          "message=普通对话(仅通知收件人，不主动触发处理); request=任务请求(收件人 agent 会被主动注入其当前会话立即处理，处理完用 a2a_reply 回 result); result=交付被请求的结果(同样会自动注入收件人当前会话，使其无需手动 a2a_read 即可接收)",
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
      onUpdate?.(textResult(`发送给 ${params.to}…`));
      const result = await state.net.deliver(msg);
      state.store.persist();

      const who = params.to === "*" ? "所有人(broadcast)" : params.to;
      let text = `✅ 已发送给 ${who}\n   线程: ${msg.thread_id}\n   消息ID: ${msg.id}`;
      if (params.to === "*") {
        if (result.delivered.length) text += `\n   已送达: ${result.delivered.join(", ") || "（无在线 peer）"}`;
        if (result.failed.length) text += `\n   ⚠️ 未送达: ${result.failed.join(", ")}`;
      } else if (result.delivered.length) {
        text += `\n   已送达`;
      } else if (result.queued.length) {
        text += `\n   ⏳ ${params.to} 当前离线，已进入发件箱，对方上线后自动送达`;
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
      if (msgs.length === 0) return textResult("📭 收件箱为空");
      const lines = msgs.map((m) => {
        const tag = state.store!.isRead(m.id, me) ? "○" : "●";
        const subj = m.subject ? `「${m.subject}」` : "「(无主题)」";
        const preview = m.body.replace(/\s+/g, " ").slice(0, 60);
        return `${tag} [${m.id}] ${m.from_name} → ${subj} ${preview}`;
      });
      return textResult(`📨 收件箱 (${msgs.length}):\n` + lines.join("\n"));
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
      // deep 版本：主 db miss → 扫 archive，保证历史消息（已归档）也能读回。
      // getInbox 绝不调 deep（高频性能红线）；a2a_read 是低频主动阅读，可接受扫描开销。
      const threadMsgs = state.store.getThreadDeep(params.message_id);
      if (threadMsgs.length === 0) return textResult("未找到该消息");

      const shown = wholeThread ? threadMsgs : threadMsgs.filter((m) => m.id === params.message_id);
      const target = shown.length ? shown : threadMsgs;
      const lines = target.map((m) => {
        const t = new Date(m.created_at * 1000).toLocaleTimeString();
        return `[${t}] ${m.from_name} → ${m.to_name} ${m.subject ? `「${m.subject}」` : ""} (${m.kind})\n${m.body}`;
      });

      // 标记已读（per-reader）
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
            { type: "text", text: "❌ 找不到原消息，无法确定回复对象（本地没有该消息副本）" },
          ],
          details: {}
        };
      }
      // 分支 A：回复的是我收到的 request（我作为 A2A server 持有 WORKING task）
      // → 完结 task + 经 push 把结果即时回给请求方（不主动发新消息）
      const workingTask = state.store.getWorkingInboundTaskByMsgId(params.message_id);
      if (workingTask) {
        const subject = orig.subject
          ? orig.subject.startsWith("Re:")
            ? orig.subject
            : `Re: ${orig.subject}`
          : "";
        // 本地留一份 sent(result) 让线程完整
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
        onUpdate?.(textResult(`完结任务并回结果给 ${orig.from_name}…`));
        const ok = await state.net.completeInboundTask(params.message_id, params.body);
        state.store.persist();
        return {
          content: [
            {
              type: "text",
              text: ok
                ? `✅ 已交付结果给 ${orig.from_name}（task ${workingTask.taskId} 完成，经 push 即时送达）`
                : `⚠️ 任务完结异常（task ${workingTask.taskId}），请检查`,
            },
          ],
          details: {}
        };
      }

      // 分支 B：普通回复（原消息非 request / 已完结 / 是闲聊）→ 发新消息
      const recipient = orig.from_name; // 回复给原消息发送者
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
        // 回复 request → 交付 result，语义闭环；其余保持 message。
        kind: orig.kind === "request" ? "result" : "message",
        direction: "sent",
        created_at: nowSec(),
      };
      state.store.addMessage(msg);
      state.store.persist();
      onUpdate?.(textResult(`回复给 ${recipient}…`));
      await state.net.deliver(msg);
      state.store.persist();
      return textResult(`✅ 已回复 ${recipient}（线程 ${msg.thread_id}）`);
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
      "局域网 P2P：只显示 mDNS 发现到的、同一 workspace 的 peer；确保对方已 /a2a-setup 且在同一局域网。",
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
              text: `🤷 当前${params.all ? "已知" : "在线"}没有其他 agent（你是 ${me}）。让对方也跑 /a2a-setup 加入同一个 workspace + 密钥，并确保同一局域网、已安装 bonjour-service。`,
            },
          ],
          details: {}
        };
      }
      const lines = others.map((p) => {
        const status = params.all && !state.net!.isOnline(p.peerName) ? "⚪" : "🟢";
        return `${status} ${p.peerName}${p.role ? ` — ${p.role}` : ""}`;
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
      if (!ctx) return textResult("⚠️ 会话上下文不可用（session context unavailable）。");
      try {
        if (params.name) ctx.setSessionName?.(params.name);
        const sessionId: string | undefined = ctx.sessionId ?? ctx.getSessionId?.();
        const sessionName: string | undefined = ctx.getSessionName?.() ?? params.name;
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
        return textResult(`⚠️ 无法读取会话: ${e instanceof Error ? e.message : String(e)}`);
      }
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
      return textResult(`✅ 已设置共享记忆「${params.key}」（${params.value.length} 字符，已广播给在线 peer）`);
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
      if (!e) return textResult(`🤷 共享记忆「${params.key}」不存在（或已被删除）`);
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
      if (keys.length === 0) return textResult("📭 共享记忆为空");
      return textResult(`🗝️ 共享记忆 (${keys.length}):\n${keys.map((k) => `- ${k}`).join("\n")}`);
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
      return textResult(`🗑️ 已删除共享记忆「${params.key}」（tombstone 已广播）`);
    },
  });

  // ── Command: /a2a-setup ───────────────────────────────────
  pi.registerCommand("a2a-setup", {
    description: "配置 pi-a2a（工作区/密钥/agent 身份）",
    handler: async (_args, ctx) => {
      if (!ctx.hasUI) {
        ctx.ui.notify("a2a-setup 需要在 TUI 中运行", "warning");
        return;
      }
      const workspace = (await ctx.ui.input("工作区名称（同一团队的 agent 用同一个名字）", "my-team"))?.trim();
      if (!workspace) {
        ctx.ui.notify("已取消", "info");
        return;
      }
      const workspaceSecret = (
        await ctx.ui.input("共享密钥（同一工作区所有 agent 必须相同；用于局域网内互验）", "")
      )?.trim();
      if (!workspaceSecret) {
        ctx.ui.notify("需要共享密钥", "warning");
        return;
      }
      const peerName = (await ctx.ui.input("你的 agent 名字（如 backend / frontend / reviewer）", ""))?.trim();
      if (!peerName) {
        ctx.ui.notify("需要 agent 名字", "warning");
        return;
      }
      const role = (await ctx.ui.input("角色/能力（可选，如 写API / 写前端 / 审查）", ""))?.trim();

      const injectChoice = await ctx.ui.select("收到普通消息时怎么处理？", [
        "只通知（默认）—— 弹 toast，手动 a2a_inbox/read 查看",
        "自动读并处理 —— 收到即注入当前会话",
      ]);
      const autoInjectMessage = injectChoice?.startsWith("自动读") === true;

      const cfg: A2aConfig = {
        workspace,
        workspaceSecret,
        agentId: genAgentId(),
        peerName,
        role: role || undefined,
        autoInjectMessage,
      };
      const fp = saveConfig(ctx.cwd, cfg);
      ctx.ui.notify(`✅ 已保存配置: ${fp}`, "info");

      // 激活：先停旧引擎，再用新配置启动
      await stopEngine();
      state.config = cfg;
      state.configPath = fp;
      state.lastCtx = ctx;
      await startEngine(ctx);
      ctx.ui.notify(`🎉 pi-a2a 已就绪！工作区「${workspace}」· 你是 ${peerName}`, "info");
    },
  });

  // ── Command: /a2a ─────────────────────────────────────────
  pi.registerCommand("a2a", {
    description: "显示 pi-a2a 状态（收件箱 + 在线 agent）",
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
        "（无其他在线 agent）";
      ctx.ui.notify(`pi-a2a · ${me}\n📨 ${state.unread} 条未读\n在线:\n  ${peersTxt}`, "info");
    },
  });

  // ── Command: /a2a-clear ───────────────────────────────────
  pi.registerCommand("a2a-clear", {
    description: "清空收件箱（仅主 db 的 inbox 消息；归档与发件历史不动）",
    handler: async (_args, ctx) => {
      if (!state.config || !state.store) {
        ctx.ui.notify(NO_CONFIG_MSG, "warning");
        return;
      }
      if (!ctx.hasUI) {
        ctx.ui.notify("a2a-clear 需要在 TUI 中运行", "warning");
        return;
      }
      const me = state.config.peerName;
      const inbox = state.store.getInbox(me, { limit: 999999 });
      if (inbox.length === 0) {
        ctx.ui.notify("📭 收件箱已空，无需清理", "info");
        return;
      }
      const unread = state.store.unreadCount(me);
      const confirm = await ctx.ui.select(
        `确认清空收件箱？将删除 ${inbox.length} 条消息（其中 ${unread} 条未读）。归档与发件历史不受影响。`,
        ["确认清空", "取消"],
      );
      if (confirm !== "确认清空") {
        ctx.ui.notify("已取消", "info");
        return;
      }
      const removed = state.store.clearInbox();
      state.store.persist();
      recalcUnread();
      ctx.ui.notify(`🗑️ 已清空收件箱，删除 ${removed} 条消息`, "info");
    },
  });

  // ── Command: /a2a-send ────────────────────────────────────
  pi.registerCommand("a2a-send", {
    description: "直接给其他 agent 发消息（不经过 AI）。用法: /a2a-send <peer> <消息> 或 /a2a-send 交互式",
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
        // /a2a-send <peer> <body...>  —— 第一个空格前是 peer，其后全部是消息内容
        const sp = trimmed.indexOf(" ");
        if (sp === -1) {
          ctx.ui.notify("用法: /a2a-send <peer> <消息内容>（peer 后空格接内容）", "warning");
          return;
        }
        to = trimmed.slice(0, sp).replace(/^@/, "");
        body = trimmed.slice(sp + 1).trim();
      } else {
        // 无参数 → 交互式选 peer + 输入主题/内容
        if (!ctx.hasUI) {
          ctx.ui.notify("a2a-send 交互式需在 TUI 中运行，或用 /a2a-send <peer> <消息>", "warning");
          return;
        }
        const peers = state.net.getOnlinePeers().filter((p) => p.peerName !== cfg.peerName);
        if (peers.length === 0) {
          ctx.ui.notify("🤷 当前没有在线 agent（可 /a2a 查看）", "warning");
          return;
        }
        const picked = await ctx.ui.select("发给谁？", peers.map((p) => p.peerName));
        if (!picked) {
          ctx.ui.notify("已取消", "info");
          return;
        }
        to = picked;
        subject = (await ctx.ui.input("主题（可选，回车跳过）", ""))?.trim() || "";
        body = (await ctx.ui.input("消息内容", ""))?.trim() || "";
      }

      if (!body) {
        ctx.ui.notify("消息内容为空，已取消", "warning");
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

      let text = `✅ 已发送给 ${to === "*" ? "所有人" : to}`;
      if (result.delivered.length) text += "（已送达）";
      else if (result.queued.length) text += "（⏳ 对方离线，进发件箱，上线自动送达）";
      else if (result.failed.length) text += "（⚠️ 未送达）";
      ctx.ui.notify(text, "info");
    },
  });

  // ── Command: /a2a-inbox ───────────────────────────────────
  pi.registerCommand("a2a-inbox", {
    description: "查看收件箱（不经过 AI）。用法: /a2a-inbox [unread|<msg_id>]",
    handler: async (args, ctx) => {
      if (!state.config || !state.store) {
        ctx.ui.notify(NO_CONFIG_MSG, "warning");
        return;
      }
      const me = state.config.peerName;
      const trimmed = (args ?? "").trim();

      // /a2a-inbox <msg_id>  → 读取完整消息 + 标记已读
      if (trimmed && trimmed !== "unread" && trimmed !== "未读") {
        const threadMsgs = state.store.getThreadDeep(trimmed);
        if (threadMsgs.length === 0) {
          ctx.ui.notify(`未找到消息 ${trimmed}`, "warning");
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

      // /a2a-inbox [unread]  → 列表
      const unreadOnly = trimmed === "unread" || trimmed === "未读";
      const msgs = state.store.getInbox(me, { unread: unreadOnly, limit: 20 });
      if (msgs.length === 0) {
        ctx.ui.notify(unreadOnly ? "📭 没有未读消息" : "📭 收件箱为空", "info");
        return;
      }
      const totalUnread = state.store.unreadCount(me);
      const lines = msgs.map((m) => {
        const tag = state.store!.isRead(m.id, me) ? "○" : "●";
        const subj = m.subject ? `「${m.subject}」` : "「(无主题)」";
        const preview = m.body.replace(/\s+/g, " ").slice(0, 60);
        return `${tag} [${m.id}] ${m.from_name} → ${subj} ${preview}`;
      });
      const header = `📨 收件箱 (${msgs.length}${unreadOnly ? " 未读" : ""} · 共 ${totalUnread} 未读)`;
      ctx.ui.notify(`${header}\n` + lines.join("\n"), "info");
    },
  });

  // ── Command: /a2a-peers ───────────────────────────────────
  pi.registerCommand("a2a-peers", {
    description: "查看在线 agent（不经过 AI）。用法: /a2a-peers [all]",
    handler: async (args, ctx) => {
      if (!state.config || !state.net) {
        ctx.ui.notify(NO_CONFIG_MSG, "warning");
        return;
      }
      const me = state.config.peerName;
      const includeAll = (args ?? "").trim() === "all" || (args ?? "").trim() === "全部";
      const peers = includeAll ? state.net.getPeers() : state.net.getOnlinePeers();
      const others = peers.filter((p) => p.peerName !== me);
      if (others.length === 0) {
        ctx.ui.notify(`🤷 当前${includeAll ? "已知" : "在线"}没有其他 agent`, "info");
        return;
      }
      const lines = others.map((p) => {
        const status = includeAll && !state.net!.isOnline(p.peerName) ? "⚪" : "🟢";
        return `${status} ${p.peerName}${p.role ? ` — ${p.role}` : ""}`;
      });
      const onlineCount = others.filter((p) => state.net!.isOnline(p.peerName)).length;
      const header = includeAll
        ? `👥 Agents (${onlineCount} 在线 / ${others.length} 已知)`
        : `👥 在线 Agents (${others.length})`;
      ctx.ui.notify(`${header}\n` + lines.join("\n"), "info");
    },
  });
}

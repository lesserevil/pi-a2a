/**
 * pi-a2a — network layer (A2A / LAN P2P)
 *
 * Implements the JSON-RPC binding of Google's A2A Protocol v1.0 (spec §9).
 *
 * Every agent is both an A2A server and client:
 *   Server (node:http):
 *     GET  /.well-known/agent-card.json  — Agent Card
 *     GET  /health                        — mDNS keep-alive probe (not in spec, harmless)
 *     POST /rpc                           — JSON-RPC: SendMessage / GetTask
 *     POST /a2a/notify                    — push-notification webhook receiver
 *     POST /file                          — receive a file (body=raw bytes, metadata in query)
 *     GET  /file?path=<rel>               — send a file (response body=raw bytes)
 *   Client (fetch):
 *     send(peer, msg)     — SendMessage (attaches pushNotificationConfig for requests)
 *     getTask(peer, id)   — GetTask (push fallback)
 *     notifyPeer(url, t)  — push the result to the requester when an inbound task completes
 *     putFile(peer, ...)  — push a local sandbox file to a peer
 *     getFile(peer, ...)  — pull a file from a peer into the local sandbox
 *
 * Discovery: mDNS/Bonjour advertise + browse _pi-a2a._tcp (TXT proto=a2a) + same-host presence fallback.
 * Auth: the shared workspaceSecret as a Bearer token (validated on every JSON-RPC request).
 * Async: peer offline → queued in the local outbox, redelivered on reconnect; outbound requests wait for a push result with a GetTask fallback on timeout.
 */
import * as http from "node:http";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as crypto from "node:crypto";
import { createRequire } from "node:module";
import type { A2aConfig } from "./config.ts";
import { Store, type Message, type MemEntry } from "./store.ts";
import {
  resolveFileRoot,
  writeReceivedFile,
  readOutgoingFile,
  DEFAULT_FILE_MAX_BYTES,
} from "./files.ts";

const nativeRequire = createRequire(import.meta.url);

export interface Peer {
  peerName: string;
  agentId: string;
  role: string;
  host: string;
  port: number;
  lastSeen: number; // epoch ms
}

export interface DeliverResult {
  delivered: string[]; // peerNames delivered successfully
  queued: string[]; // peerNames queued in the outbox (targeted, offline)
  failed: string[]; // online peers not reached during a broadcast
}

export interface NetHooks {
  store: Store;
  onMessageReceived: (m: Message) => void; // new message → toast + (request/result) injection
  onPeersChanged: () => void; // peer online/offline → refresh widget
  log?: (msg: string) => void;
}

// ── constants ───────────────────────────────────────────
const SERVICE_TYPE = "pi-a2a";
const PROTO = "a2a"; // A2A protocol marker (hard cut: legacy proto=1 is no longer recognised)
const A2AVER = "1.0";
const PEER_TTL_MS = 60_000;
const REFRESH_INTERVAL_MS = 10_000;
const POST_TIMEOUT_MS = 3_000;
const MAX_BODY_BYTES = 512 * 1024;
const PRESENCE_TTL_MS = 35_000;
const DEFAULT_PUSH_SWEEP_MS = 15_000;
const DEFAULT_PUSH_BACKSTOP_MS = 30_000;

/** presence file record (this host: `~/.pi/agent/pi-a2a-presence/<agentId>-<pid>.json`). */
export interface PresenceRec {
  peerName: string;
  agentId: string;
  role?: string;
  workspace: string; // plaintext, back-compat with older versions (dual-sent during transition; to be removed)
  wsh?: string; // workspace hash; isolation relies on it (plaintext ws leaks, so it has been migrated here)
  host: string;
  port: number;
  proto: string;
  ts: number;
  pid: number;
}

/**
 * workspace hash：sha256(len(ws):ws:len(secret):secret)[:16]（64bit）。
 * Use length prefixes rather than plain delimiters — plain `:` separators collide: `ws=a:b,secret=c` and `ws=a,secret=b:c` produce the same input string (both concatenate to "a:b:c").
 * Length prefixes are the standard way to make concatenation collision-resistant: `3:a:b:1:c` ≠ `1:a:3:b:c`.
 */
export function wsHash(workspace: string, secret: string): string {
  const payload = `${workspace.length}:${workspace}:${secret.length}:${secret}`;
  return crypto.createHash("sha256").update(payload).digest("hex").slice(0, 16);
}

/**
 * workspace matching: prefer wsh (hash), fall back to plaintext ws only when absent.
 * Compatibility window: new versions send both ws+wsh, old versions only ws. New→old uses the ws fallback; old→new matches on the dual-sent ws.
 */
export function wsMatch(
  their: { ws?: string; wsh?: string },
  mine: { workspace: string; workspaceSecret: string },
): boolean {
  if (their.wsh) return their.wsh === wsHash(mine.workspace, mine.workspaceSecret);
  // peer is an older version (no wsh); fall back to plaintext ws matching
  return !!their.ws && their.ws === mine.workspace;
}

/**
 * presence dedup: the same agentId may have several pid files (two processes in one directory); keep the newest by ts.
 * Production and tests share this function, eliminating helper-drift at the root.
 */
export function dedupePresence<T extends { agentId?: string; ts?: number }>(records: T[]): T[] {
  const byAgent = new Map<string, { rec: T; ts: number }>();
  for (const rec of records) {
    const ts = typeof rec.ts === "number" ? rec.ts : 0;
    const agentId = String(rec.agentId ?? "");
    const prev = byAgent.get(agentId);
    if (!prev || ts > prev.ts) byAgent.set(agentId, { rec, ts });
  }
  return [...byAgent.values()].map((v) => v.rec);
}

// JSON-RPC error codes (spec §5.4 / §9.5)
const ERR_PARSE = -32700;
const ERR_INVALID_REQ = -32600;
const ERR_METHOD_NOT_FOUND = -32601;
const ERR_INVALID_PARAMS = -32602;
const ERR_INTERNAL = -32603;
const ERR_TASK_NOT_FOUND = -32001;

// ── JSON-RPC / A2A helpers ──────────────────────────────

function isoNow(): string {
  return new Date().toISOString();
}
function nowSec(): number {
  return Math.floor(Date.now() / 1000);
}
function genId(prefix: string): string {
  return prefix + "_" + crypto.randomBytes(5).toString("hex");
}
function rpcResult(id: any, result: unknown) {
  return { jsonrpc: "2.0", id, result };
}
function rpcError(id: any, code: number, message: string, data?: unknown) {
  const err: any = { code, message };
  if (data !== undefined) err.data = data;
  return { jsonrpc: "2.0", id, error: err };
}

/** Virtual NIC name markers (Docker / WSL / Hyper-V / VMware / VirtualBox / VPN / tunnel, etc.).
 *  Addresses on these NICs are only reachable inside the host, not across machines; advertising them makes peers unable to connect. */
const VIRTUAL_IFACE_RE =
  /^(docker|br-|veth|vEthernet|WSL|Hyper-V|VMware|VMnet|VirtualBox|TAP|tun|utun|llw|awdl|bridge|tap|p2p|anpi)/i;

function isLikelyVirtualIface(name: string): boolean {
  return VIRTUAL_IFACE_RE.test(name);
}

/** Get this host's LAN IPv4 (used in the push webhook URL; reachable across machines and from itself).
 *  Prefer a real physical/wireless NIC; skip virtual NICs (Docker/WSL/Hyper-V, etc.),
 *  falling back to a virtual NIC address only when there is no real NIC at all. */
function getLocalLanIp(): string {
  try {
    const ifaces = os.networkInterfaces();
    let virtualFallback = "";
    for (const [name, list] of Object.entries(ifaces)) {
      for (const iface of list ?? []) {
        if (iface.internal || iface.family !== "IPv4") continue;
        if (isLikelyVirtualIface(name)) {
          if (!virtualFallback) virtualFallback = iface.address;
          continue;
        }
        return iface.address;
      }
    }
    if (virtualFallback) return virtualFallback;
  } catch {
    /* ignore */
  }
  return "127.0.0.1";
}

/** Extract text from A2A Message.parts (concatenating all TextParts). */
function extractText(parts: any[]): string {
  if (!Array.isArray(parts)) return "";
  return parts
    .map((p) => (typeof p?.text === "string" ? p.text : ""))
    .filter(Boolean)
    .join("\n");
}

export class Network {
  private bonjour: any = null;
  private service: any = null;
  private browser: any = null;
  private server: http.Server | null = null;
  private listenPort = 0;
  private lanIp = "127.0.0.1";
  private peers = new Map<string, Peer>();
  private snapshottedPeers = new Set<string>(); // peers whose mem snapshot has already been pulled (prevents repeats within a process; re-pulled on restart, idempotent)
  private refreshTimer: ReturnType<typeof setInterval> | null = null;
  private pushSweepTimer: ReturnType<typeof setInterval> | null = null;
  private refreshing = false;
  private reqIdCounter = 0;

  constructor(private config: A2aConfig, private hooks: NetHooks) {}

  getListenPort(): number {
    return this.listenPort;
  }

  private get rpcPath(): string {
    return this.config.rpcPath ?? "/rpc";
  }
  private get notifyPath(): string {
    return this.config.notifyPath ?? "/a2a/notify";
  }
  private get agentCardPath(): string {
    return this.config.agentCardPath ?? "/.well-known/agent-card.json";
  }
  private get filePath(): string {
    return (this.config as any).filePath ?? "/file";
  }
  private fileRoot(): string {
    return resolveFileRoot((this.config as any).fileRoot);
  }
  private fileMaxBytes(): number {
    const n = Number((this.config as any).fileMaxBytes);
    return Number.isFinite(n) && n > 0 ? n : DEFAULT_FILE_MAX_BYTES;
  }
  private nextReqId(): number {
    return ++this.reqIdCounter;
  }

  // ── lifecycle ───────────────────────────────────────────

  async start(): Promise<void> {
    this.lanIp = this.config.advertiseHost?.trim() || getLocalLanIp();
    await this.startHttp();
    this.writePresence();
    this.startMdns();
    void this.refresh();
    this.refreshTimer = setInterval(() => void this.refresh(), REFRESH_INTERVAL_MS);
    const sweepMs = this.config.pushSweepMs ?? DEFAULT_PUSH_SWEEP_MS;
    this.pushSweepTimer = setInterval(() => void this.pushSweep(), sweepMs);
  }

  async stop(): Promise<void> {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = null;
    }
    if (this.pushSweepTimer) {
      clearInterval(this.pushSweepTimer);
      this.pushSweepTimer = null;
    }
    this.clearPresence();
    try {
      this.browser?.stop?.();
    } catch {
      /* ignore */
    }
    try {
      this.service?.stop?.(() => {});
    } catch {
      /* ignore */
    }
    try {
      this.bonjour?.destroy?.();
    } catch {
      /* ignore */
    }
    this.bonjour = null;
    this.service = null;
    this.browser = null;
    this.peers.clear();
    if (this.server) {
      const srv = this.server;
      this.server = null;
      await new Promise<void>((res) => srv.close(() => res()));
    }
  }

  // ── peer table queries ──────────────────────────────────

  getPeers(): Peer[] {
    return [...this.peers.values()];
  }
  getOnlinePeers(): Peer[] {
    const cutoff = Date.now() - PEER_TTL_MS;
    return [...this.peers.values()].filter((p) => p.lastSeen >= cutoff);
  }
  getPeer(name: string): Peer | undefined {
    return this.peers.get(name);
  }
  isOnline(name: string): boolean {
    const p = this.peers.get(name);
    return !!p && p.lastSeen >= Date.now() - PEER_TTL_MS;
  }

  // ── HTTP server (A2A server side) ───────────────────────

  private startHttp(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => this.handle(req, res));
      this.server.on("error", (e) => this.hooks.log?.(`HTTP server error: ${e?.message ?? e}`));
      const port = this.config.listenPort ?? 0;
      this.server.listen(port, "0.0.0.0", () => {
        const addr = this.server!.address();
        this.listenPort = typeof addr === "object" && addr ? addr.port : port;
        this.hooks.log?.(
          `A2A HTTP listening on 0.0.0.0:${this.listenPort} (lanIp=${this.lanIp}${this.config.advertiseHost ? " [manual]" : ""})`,
        );
        resolve();
      });
      this.server.once("error", reject);
    });
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = req.url ?? "";
    try {
      // Agent Card (no auth; the spec requires public discoverability)
      if (req.method === "GET" && url === this.agentCardPath) {
        return this.json(res, 200, this.agentCard(), { "Cache-Control": "max-age=60" });
      }
      // health probe (used for mDNS keep-alive; not in spec)
      if (req.method === "GET" && url === "/health") {
        return this.json(res, 200, {
          status: "ok",
          peer: this.config.peerName,
          workspace: this.config.workspace, // plaintext, back-compat (dual-sent during transition)
          wsh: wsHash(this.config.workspace, this.config.workspaceSecret), // workspace hash
          proto: PROTO,
        });
      }
      // operational stats (per-peer outbox backlog + cumulative drops; auth required so silent message loss is observable)
      if (req.method === "GET" && url === "/stats") {
        if (!this.checkAuth(req)) return this.json(res, 401, { error: "unauthorized" });
        return this.json(res, 200, this.hooks.store.getStats());
      }
      // JSON-RPC endpoint
      if (req.method === "POST" && url === this.rpcPath) {
        return this.handleRpc(req, res);
      }
      // push-notification webhook receiver
      if (req.method === "POST" && url === this.notifyPath) {
        return this.handleNotify(req, res);
      }
      // file transfer: dispatch by path (with query)
      if (url === this.filePath || url.startsWith(this.filePath + "?")) {
        if (req.method === "POST") return this.handleFileReceive(req, res);
        if (req.method === "GET") return this.handleFileSend(req, res);
      }
      this.json(res, 404, { error: "Not found" });
    } catch (e) {
      this.hooks.log?.(`handle exception: ${e instanceof Error ? e.message : String(e)}`);
      try {
        this.json(res, 500, { error: "internal" });
      } catch {
        /* ignore */
      }
    }
  }

  /** Validate the Bearer shared secret. */
  private checkAuth(req: http.IncomingMessage): boolean {
    const auth = req.headers["authorization"];
    const secret = typeof auth === "string" && auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
    return secret !== "" && secret === this.config.workspaceSecret;
  }

  private async handleRpc(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    let body: any;
    try {
      body = await this.readBody(req);
    } catch {
      return this.rpcJson(res, null, rpcError(null, ERR_PARSE, "Invalid JSON payload"));
    }
    const id = body?.id ?? null;
    if (!this.checkAuth(req)) {
      return this.rpcJson(res, id, rpcError(id, ERR_INVALID_REQ, "Unauthorized: bad or missing Bearer token"));
    }
    const method = String(body?.method ?? "");
    const params = (body?.params ?? {}) as any;
    switch (method) {
      case "SendMessage":
        return this.handleSendMessage(params, res, id);
      case "GetTask":
        return this.handleGetTask(params, res, id);
      case "MemUpdate":
        return this.handleMemUpdate(params, res, id);
      case "MemSnapshot":
        return this.handleMemSnapshot(res, id);
      default:
        return this.rpcJson(res, id, rpcError(id, ERR_METHOD_NOT_FOUND, `Method not found: ${method}`));
    }
  }

  /** SendMessage: a message arrives → create a task → decide inject vs notify by kind. */
  private handleSendMessage(params: any, res: http.ServerResponse, id: any): void {
    const msg = params?.message;
    if (!msg || !Array.isArray(msg.parts) || msg.parts.length === 0) {
      return this.rpcJson(res, id, rpcError(id, ERR_INVALID_PARAMS, "Invalid params: message.parts required"));
    }
    const meta = msg.metadata ?? {};
    const kind = Store.validKind(String(meta.kind ?? "message")) ? (String(meta.kind) as Message["kind"]) : "message";
    const fromName = String(meta.from ?? "");
    const subject = String(meta.subject ?? "");
    const body = extractText(msg.parts);
    const incomingMsgId = String(msg.messageId ?? genId("msg"));
    const contextId = String(msg.contextId ?? genId("ctx"));
    const pushCfg = params?.configuration?.pushNotificationConfig;

    const taskId = genId("task");
    const state = kind === "request" ? "TASK_STATE_WORKING" : "TASK_STATE_COMPLETED";
    const pushConfig =
      kind === "request" && pushCfg && typeof pushCfg.url === "string"
        ? { url: String(pushCfg.url), token: String(pushCfg.token ?? "") }
        : undefined;

    const inboxMsg: Message = {
      id: incomingMsgId,
      thread_id: contextId, // A2A contextId = local thread
      reply_to: null,
      from_id: "",
      from_name: fromName,
      to_name: this.config.peerName,
      subject,
      body,
      kind,
      direction: "inbox",
      created_at: nowSec(),
      taskId,
      contextId,
      taskState: state,
    };
    const added = this.hooks.store.addMessage(inboxMsg);
    this.hooks.store.addInboundTask({
      taskId,
      msgId: incomingMsgId,
      contextId,
      state,
      fromName,
      createdAt: nowSec(),
      pushConfig,
    });
    this.hooks.store.persist();
    if (added) this.hooks.onMessageReceived({ ...inboxMsg });

    const task = this.buildTask(taskId, contextId, state);
    this.rpcJson(res, id, rpcResult(id, { task }));
  }

  /** GetTask: return local task state (used as the push fallback). */
  private handleGetTask(params: any, res: http.ServerResponse, id: any): void {
    const taskId = String(params?.id ?? "");
    const t = this.hooks.store.getInboundTaskByTaskId(taskId);
    if (!t) {
      return this.rpcJson(
        res,
        id,
        rpcError(id, ERR_TASK_NOT_FOUND, "Task not found", [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            reason: "TASK_NOT_FOUND",
            domain: "a2a-protocol.org",
            metadata: { taskId },
          },
        ]),
      );
    }
    this.rpcJson(res, id, rpcResult(id, this.buildTask(t.taskId, t.contextId, t.state, t.artifactText)));
  }

  /** MemUpdate: receive a single remote entry delta → LWW merge. Not re-broadcast (the sender already broadcast). */
  private handleMemUpdate(params: any, res: http.ServerResponse, id: any): void {
    const key = String(params?.key ?? "");
    const entry = params?.entry;
    if (!key || !entry || typeof entry.ts !== "number" || typeof entry.author !== "string") {
      return this.rpcJson(res, id, rpcError(id, ERR_INVALID_PARAMS, "Invalid params: key + entry{ts,author} required"));
    }
    const changed = this.hooks.store.memApplyRemote(key, entry as MemEntry);
    if (changed) this.hooks.store.persist();
    this.rpcJson(res, id, rpcResult(id, { applied: changed }));
  }

  /** MemSnapshot: return the full local mem (including tombstones) so new peers can bootstrap. */
  private handleMemSnapshot(res: http.ServerResponse, id: any): void {
    this.rpcJson(res, id, rpcResult(id, { entries: this.hooks.store.memGetAll() }));
  }

  /** push-notification receiver: the peer completed a task I delegated → parse → inject. */
  private async handleNotify(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    // auth: either Bearer or X-A2A-Notification-Token must equal the shared secret
    const auth = req.headers["authorization"];
    const bearer = typeof auth === "string" && auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
    const xToken = req.headers["x-a2a-notification-token"];
    const token = typeof xToken === "string" ? xToken.trim() : "";
    if ((bearer !== this.config.workspaceSecret && token !== this.config.workspaceSecret) || !this.config.workspaceSecret) {
      return this.json(res, 401, { error: "unauthorized" });
    }
    let body: any;
    try {
      body = await this.readBody(req);
    } catch {
      return this.json(res, 400, { error: "bad payload" });
    }
    // StreamResponse: {task} or {statusUpdate} (take its task)
    const task = body?.task ?? body?.statusUpdate;
    if (task) this.handleIncomingTaskUpdate(task);
    this.json(res, 200, {}); // idempotent ack
  }

  /** Handle a state update for an outbound task (shared by push and the GetTask fallback). */
  private handleIncomingTaskUpdate(task: any): void {
    const taskId = String(task?.id ?? "");
    const state = String(task?.status?.state ?? "");
    const ob = this.hooks.store.getOutboundTaskByTaskId(taskId);
    if (!ob) return; // unknown/duplicate → ignore (idempotent)
    if (!Store.isTerminalState(state)) return; // not finished yet; keep waiting

    const arts = Array.isArray(task?.artifacts) ? task.artifacts : [];
    const text =
      extractText(arts.flatMap((a: any) => a?.parts ?? [])) ||
      extractText(task?.status?.message?.parts ?? []) ||
      "";
    this.hooks.store.resolveOutboundTask(taskId);

    const success = state === "TASK_STATE_COMPLETED";
    const body = success
      ? text || "(empty result)"
      : `[delegation incomplete: ${state.replace("TASK_STATE_", "")}]${text ? "\n" + text : ""}`;

    const resultMsg: Message = {
      id: genId("msg"),
      thread_id: ob.contextId, // backfill into the original request's thread
      reply_to: ob.msgId,
      from_id: "",
      from_name: ob.peerName,
      to_name: this.config.peerName,
      subject: "",
      body,
      kind: "result",
      direction: "inbox",
      created_at: nowSec(),
      taskId,
      contextId: ob.contextId,
      taskState: state,
    };
    this.hooks.store.addMessage(resultMsg);
    this.hooks.store.persist();
    this.hooks.onMessageReceived(resultMsg); // kind=result → auto-inject
  }

  // ── A2A construction ────────────────────────────────────

  private agentCard(): any {
    return {
      name: this.config.peerName,
      description: this.config.role || "pi agent",
      version: "1.0.0",
      supportedInterfaces: [
        {
          url: `http://${this.lanIp}:${this.listenPort}${this.rpcPath}`,
          protocolBinding: "JSONRPC",
          protocolVersion: A2AVER,
        },
      ],
      capabilities: {
        streaming: false,
        pushNotifications: true,
        extendedAgentCard: false,
      },
      securitySchemes: {
        ws: {
          httpAuthSecurityScheme: {
            scheme: "Bearer",
            description: "shared workspace secret",
          },
        },
      },
      securityRequirements: [{ ws: [] }],
      defaultInputModes: ["text/plain"],
      defaultOutputModes: ["text/plain"],
      skills: [
        {
          id: "pi-a2a",
          name: this.config.peerName,
          description: this.config.role || "pi agent",
          tags: ["pi", "agent", "a2a"],
        },
      ],
    };
  }

  private buildTask(taskId: string, contextId: string, state: string, artifactText?: string): any {
    const task: any = {
      id: taskId,
      contextId,
      status: { state, timestamp: isoNow() },
    };
    if (artifactText != null && artifactText !== "") {
      task.artifacts = [
        {
          artifactId: "art_" + taskId.slice(-8),
          name: "result",
          parts: [{ text: artifactText }],
        },
      ];
    }
    return task;
  }

  // ── A2A client side ─────────────────────────────────────

  /** Make a JSON-RPC call. Returns {result} or {error} (throws on network failure). */
  private async rpcCall(peer: Peer, method: string, params: any, timeoutMs = POST_TIMEOUT_MS): Promise<any> {
    const url = `http://${peer.host}:${peer.port}${this.rpcPath}`;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.config.workspaceSecret}`,
        },
        body: JSON.stringify({ jsonrpc: "2.0", id: this.nextReqId(), method, params }),
        signal: ctrl.signal,
      });
      return await resp.json();
    } finally {
      clearTimeout(timer);
    }
  }

  /** Deliver an outgoing message (already written to the local store with direction=sent). */
  async deliver(msg: Message): Promise<DeliverResult> {
    // broadcast: push to all currently online peers
    if (msg.to_name === "*") {
      const online = this.getOnlinePeers().filter((p) => p.peerName !== this.config.peerName);
      const delivered: string[] = [];
      const failed: string[] = [];
      for (const p of online) {
        if (await this.sendToOne(p, msg)) delivered.push(p.peerName);
        else failed.push(p.peerName);
      }
      return { delivered, queued: [], failed };
    }
    // targeted: push directly when online; queue in the outbox when offline/failed
    if (this.isOnline(msg.to_name)) {
      const peer = this.getPeer(msg.to_name)!;
      if (await this.sendToOne(peer, msg)) return { delivered: [peer.peerName], queued: [], failed: [] };
    }
    this.hooks.store.queueOutbox(msg.id, msg.to_name);
    this.hooks.store.persist();
    return { delivered: [], queued: [msg.to_name], failed: [] };
  }

  /** Send an A2A SendMessage to a peer. On success, register an outbound task awaiting push if it was a request. */
  private async sendToOne(peer: Peer, msg: Message): Promise<boolean> {
    const params: any = {
      message: {
        messageId: msg.id,
        role: "ROLE_USER",
        contextId: msg.thread_id, // local thread = A2A contextId
        parts: [{ text: msg.body }],
        metadata: {
          kind: msg.kind,
          subject: msg.subject,
          from: this.config.peerName,
        },
      },
    };
    // request → register push so the result comes back immediately
    if (msg.kind === "request") {
      params.configuration = {
        pushNotificationConfig: {
          url: `http://${this.lanIp}:${this.listenPort}${this.notifyPath}`,
          token: this.config.workspaceSecret,
          authentication: { schemes: ["Bearer"] },
        },
      };
    }
    try {
      const j = await this.rpcCall(peer, "SendMessage", params);
      if (j.error) {
        this.hooks.log?.(`SendMessage error ← ${peer.peerName}: ${j.error.message}`);
        return false;
      }
      const task = j.result?.task;
      if (msg.kind === "request" && task?.id) {
        this.hooks.store.addOutboundTask({
          taskId: task.id,
          msgId: msg.id,
          contextId: msg.thread_id,
          peerName: peer.peerName,
          peerEndpoint: `http://${peer.host}:${peer.port}`,
          createdAt: nowSec(),
        });
        this.hooks.store.persist();
      }
      return true;
    } catch (e) {
      this.hooks.log?.(`sendToOne failed → ${peer.peerName}: ${e instanceof Error ? e.message : String(e)}`);
      return false;
    }
  }

  /** Push a completed inbound task's result to the requester (called when a2a_reply answers a request). */
  async completeInboundTask(msgId: string, body: string, state = "TASK_STATE_COMPLETED"): Promise<boolean> {
    const t = this.hooks.store.getWorkingInboundTaskByMsgId(msgId);
    if (!t) return false;
    this.hooks.store.completeInboundTask(t.taskId, state, body);
    this.hooks.store.persist();
    if (t.pushConfig) {
      const task = this.buildTask(t.taskId, t.contextId, state, body);
      const ok = await this.notifyPeer(t.pushConfig.url, t.pushConfig.token, task);
      if (!ok) this.hooks.log?.(`push result failed → ${t.pushConfig.url} (task=${t.taskId})`);
    }
    return true;
  }

  /** POST a push-notification (StreamResponse {task}) to the peer's webhook. */
  private async notifyPeer(url: string, token: string, task: any): Promise<boolean> {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), POST_TIMEOUT_MS);
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
          "X-A2A-Notification-Token": token,
        },
        body: JSON.stringify({ task }),
        signal: ctrl.signal,
      });
      return resp.ok;
    } catch {
      return false;
    } finally {
      clearTimeout(timer);
    }
  }

  /** push fallback: actively GetTask for outbound tasks whose push never arrived. */
  private async pushSweep(): Promise<void> {
    const backstop = this.config.pushBackstopMs ?? DEFAULT_PUSH_BACKSTOP_MS;
    const now = Date.now();
    const pending = this.hooks.store.getPendingOutbound();
    for (const ob of pending) {
      if (now - ob.createdAt * 1000 < backstop) continue; // still fresh; keep waiting for the push
      if (!this.isOnline(ob.peerName)) continue; // peer offline; cannot poll
      const peer = this.getPeer(ob.peerName);
      if (!peer) continue;
      try {
        const j = await this.rpcCall(peer, "GetTask", { id: ob.taskId });
        const task = j?.result; // GetTask result = Task object
        if (task && Store.isTerminalState(String(task.status?.state ?? ""))) {
          this.handleIncomingTaskUpdate(task);
        }
      } catch {
        /* ignore; retry next time */
      }
    }
  }

  // ── mDNS advertise + browse ─────────────────────────────
  // (largely unchanged from older versions; TXT proto is now "a2a" and non-a2a peers are ignored by hard cut)

  private startMdns(): void {
    let BonjourCtor: any = null;
    try {
      const mod: any = nativeRequire("bonjour-service");
      BonjourCtor = mod.Bonjour ?? mod.default ?? mod;
    } catch {
      this.hooks.log?.(
        "⚠️ mDNS unavailable: bonjour-service is not installed (cannot auto-discover LAN peers). Run npm install in the pi-a2a directory.",
      );
      return;
    }
    try {
      // Optional: pin the mDNS/multicast socket to one interface. Only set this
      // via config `mdnsInterface` on a host whose default multicast route goes
      // out the wrong adapter (e.g. Windows with Tailscale up, where the VPN
      // interface has a lower route metric and multicast never reaches the LAN).
      // Multi-homed hosts with several addresses on the same LAN should leave it
      // unset, otherwise they stop hearing peers advertised on the other address.
      const bindIface = this.config.mdnsInterface?.trim();
      this.bonjour = bindIface ? new BonjourCtor({ interface: bindIface }) : new BonjourCtor();
      const instanceName = `${this.config.peerName}@${this.config.agentId.slice(0, 8)}@${process.pid}`;
      this.service = this.bonjour.publish({
        name: instanceName,
        type: SERVICE_TYPE,
        port: this.listenPort,
        txt: {
          name: this.config.peerName,
          role: this.config.role ?? "",
          agent: this.config.agentId,
          ws: this.config.workspace, // plaintext, back-compat (dual-sent during transition; to be removed)
          wsh: wsHash(this.config.workspace, this.config.workspaceSecret), // workspace hash; isolation relies on it
          proto: PROTO,
          a2aver: A2AVER,
        },
      });
      this.service?.on?.("error", (e: unknown) => {
        this.hooks.log?.(`⚠️ mDNS publish failed (service name conflict?): ${e instanceof Error ? e.message : String(e)}`);
      });
      this.browser = this.bonjour.find({ type: SERVICE_TYPE });
      this.browser.on("up", (svc: any) => this.onPeerUp(svc));
      this.browser.on("down", (svc: any) => this.onPeerDown(svc));
      this.hooks.log?.(`mDNS advertise + browse started (ws=${this.config.workspace}, proto=a2a, instance=${instanceName})`);
    } catch (e) {
      this.hooks.log?.(`⚠️ mDNS start failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  private isIpv4(s: unknown): s is string {
    return typeof s === "string" && /^\d+\.\d+\.\d+\.\d+$/.test(s);
  }
  private pickHost(svc: any): string {
    const referer = svc?.referer?.address;
    const addrs: string[] = [];
    if (this.isIpv4(referer)) addrs.push(referer);
    for (const a of Array.isArray(svc?.addresses) ? svc.addresses : []) {
      if (this.isIpv4(a) && !addrs.includes(a)) addrs.push(a);
    }
    if (addrs.length === 0) return "";
    // prefer an address on the same subnet as this host (reachable across machines); avoid virtual NICs (Docker/WSL 172.x, etc.)
    if (!this.lanIp.startsWith("127.")) {
      const prefix = this.lanIp.split(".").slice(0, 3).join(".") + ".";
      const same = addrs.find((a) => a.startsWith(prefix));
      if (same) return same;
    }
    // fallback: skip link-local (169.254) and loopback (127)
    return addrs.find((a) => !a.startsWith("169.254.") && !a.startsWith("127.")) ?? addrs[0];
  }
  private betterHost(prev: string | undefined, next: string): string {
    if (this.isIpv4(next)) return next;
    if (this.isIpv4(prev)) return prev;
    return next || prev || "";
  }

  private onPeerUp(svc: any): void {
    const txt = svc?.txt ?? {};
    if (!wsMatch(txt, this.config)) return; // workspace isolation (prefer wsh hash, fall back to plaintext ws)
    if (txt.proto !== PROTO) {
      // hard cut: ignore anything that is not the A2A protocol (including legacy proto=1 or missing proto)
      return;
    }
    const peerName = String(txt.name ?? svc?.name ?? "");
    if (!peerName || peerName === this.config.peerName) return;
    const port = Number(svc?.port ?? 0);
    const existing = this.peers.get(peerName);
    const host = this.betterHost(existing?.host, this.pickHost(svc));
    if (!host || !port) {
      if (existing) existing.lastSeen = Date.now();
      return;
    }
    const isNew = !existing;
    const changed = existing && (existing.host !== host || existing.port !== port);
    this.peers.set(peerName, {
      peerName,
      agentId: String(txt.agent ?? ""),
      role: String(txt.role ?? ""),
      host,
      port,
      lastSeen: Date.now(),
    });
    if (isNew) {
      this.hooks.log?.(`peer online: ${peerName} (${host}:${port})`);
      this.hooks.onPeersChanged();
      void this.flushOutbox(peerName);
      void this.pullMemSnapshot(peerName);
    } else if (changed) {
      this.hooks.onPeersChanged();
    }
  }

  private onPeerDown(svc: any): void {
    const txt = svc?.txt ?? {};
    const peerName = String(txt.name ?? svc?.name ?? "");
    // don't delete immediately: mDNS "down" frequently misfires on WiFi / multi-NIC / firewalled setups,
    // real offline is decided by refresh()'s TTL + healthCheck fallback (delete only when the network is unreachable).
    // this avoids a peer being wrongly deleted by sporadic multicast loss and then unrecoverable via healthCheck.
    if (this.peers.has(peerName)) {
      this.hooks.log?.(`peer mDNS signal lost: ${peerName} (kept; falling back to healthCheck)`);
    }
  }

  // ── same-host presence fallback discovery ───────────────
  private presenceDir(): string {
    return path.join(os.homedir(), ".pi", "agent", "pi-a2a-presence");
  }
  private presenceFile(): string {
    // filenames carry the pid so two processes in one directory (same agentId) no longer overwrite each other's presence file.
    // scanPresence dedups by agentId and uses the newest ts for discovery, without deleting older pid files (the live process is still writing).
    return path.join(this.presenceDir(), `${this.config.agentId}-${process.pid}.json`);
  }
  private writePresence(): void {
    if (!this.listenPort) return;
    try {
      const dir = this.presenceDir();
      fs.mkdirSync(dir, { recursive: true });
      const rec: PresenceRec = {
        peerName: this.config.peerName,
        agentId: this.config.agentId,
        role: this.config.role ?? "",
        workspace: this.config.workspace, // plaintext, back-compat (dual-sent during transition; to be removed)
        wsh: wsHash(this.config.workspace, this.config.workspaceSecret), // workspace hash; isolation relies on it
        host: "127.0.0.1",
        port: this.listenPort,
        proto: PROTO,
        ts: Date.now(),
        pid: process.pid,
      };
      fs.writeFileSync(this.presenceFile(), JSON.stringify(rec));
    } catch {
      /* a failed write is not fatal */
    }
  }
  private clearPresence(): void {
    try {
      fs.unlinkSync(this.presenceFile());
    } catch {
      /* ignore */
    }
  }
  private scanPresence(): void {
    let files: string[] = [];
    try {
      files = fs.readdirSync(this.presenceDir());
    } catch {
      return;
    }
    const now = Date.now();
    // stage 1: read + filter + TTL cleanup (deleting files is a side effect that stays in scanPresence; use dedupePresence for pure dedup)
    const valid: PresenceRec[] = [];
    for (const f of files) {
      if (!f.endsWith(".json")) continue;
      let rec: any;
      try {
        rec = JSON.parse(fs.readFileSync(path.join(this.presenceDir(), f), "utf8"));
      } catch {
        continue;
      }
      // workspace isolation: prefer wsh hash, fall back to plaintext ws (older-version compatibility)
      if (!rec || !wsMatch(rec, this.config)) continue;
      if (rec.proto !== PROTO) continue; // hard cut: must be the A2A protocol
      const peerName = String(rec.peerName ?? "");
      if (!peerName || peerName === this.config.peerName) continue;
      const port = Number(rec.port);
      if (!Number.isFinite(port) || port <= 0) continue;
      const ts = typeof rec.ts === "number" ? rec.ts : 0;
      // TTL expired → the process is dead; safe to delete (this is the only path allowed to delete files)
      if (ts > 0 && now - ts > PRESENCE_TTL_MS) {
        try {
          fs.unlinkSync(path.join(this.presenceDir(), f));
        } catch {
          /* ignore */
        }
        continue;
      }
      valid.push(rec);
    }
    // stage 2: dedup multiple pids for one agentId (take the newest ts) — extracted as the exported dedupePresence, shared by production/tests
    const deduped = dedupePresence(valid);
    // stage 3: add to peers
    for (const rec of deduped) {
      const peerName = String(rec.peerName);
      const port = Number(rec.port);
      const existing = this.peers.get(peerName);
      const isNew = !existing;
      this.peers.set(peerName, {
        peerName,
        agentId: String(rec.agentId ?? ""),
        role: String(rec.role ?? ""),
        host: "127.0.0.1",
        port,
        lastSeen: Date.now(),
      });
      if (isNew) {
        this.hooks.log?.(`peer online (presence): ${peerName} (127.0.0.1:${port})`);
        this.hooks.onPeersChanged();
        void this.flushOutbox(peerName);
      }
    }
  }

  // ── health-probe keep-alive ────────────────────────────
  private async healthCheck(peer: Peer): Promise<boolean> {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), POST_TIMEOUT_MS);
    try {
      const r = await fetch(`http://${peer.host}:${peer.port}/health`, { signal: ctrl.signal });
      if (!r.ok) return false;
      const j = await r.json().catch(() => null);
      // workspace check: prefer wsh hash, fall back to plaintext ws (older-peer compatibility)
      return !!j && wsMatch(j, this.config);
    } catch {
      return false;
    } finally {
      clearTimeout(timer);
    }
  }
  private async probePeers(): Promise<void> {
    const cutoff = Date.now() - PEER_TTL_MS;
    for (const [, p] of this.peers) {
      if (p.lastSeen >= cutoff) continue;
      if (await this.healthCheck(p)) p.lastSeen = Date.now();
    }
  }

  private async refresh(): Promise<void> {
    if (this.refreshing) return;
    this.refreshing = true;
    try {
      this.writePresence();
      this.scanPresence();
      try {
        this.browser?.update?.();
      } catch {
        /* ignore */
      }
      await this.probePeers();
      const cutoff = Date.now() - PEER_TTL_MS;
      let changed = false;
      for (const [name, p] of this.peers) {
        if (p.lastSeen < cutoff) {
          this.peers.delete(name);
          changed = true;
        }
      }
      if (changed) this.hooks.onPeersChanged();
      for (const p of this.getOnlinePeers()) {
        void this.flushOutbox(p.peerName);
        if (!this.snapshottedPeers.has(p.peerName)) void this.pullMemSnapshot(p.peerName);
      }
    } finally {
      this.refreshing = false;
    }
  }

  // ── offline outbox redelivery ──────────────────────────
  private async flushOutbox(peerName: string): Promise<void> {
    const entries = this.hooks.store.getOutboxFor(peerName);
    if (entries.length === 0) return;
    for (const e of entries) {
      const msg = this.hooks.store.getMessage(e.id);
      if (!msg) {
        this.hooks.store.removeOutbox(e.id, peerName);
        continue;
      }
      if (!this.isOnline(peerName)) break;
      if (await this.sendToOne(this.getPeer(peerName)!, msg)) {
        this.hooks.store.removeOutbox(e.id, peerName);
        this.hooks.log?.(`redelivery succeeded: ${e.id} → ${peerName}`);
      }
    }
    this.hooks.store.persist();
  }

  // ── shared-memory (mem) sync ───────────────────────────
  /**
   * Pull a full mem snapshot from a peer once and LWW-merge it locally. Each peer is pulled once per process
   * (snapshottedPeers prevents repeats); the Set clears on restart and re-pulls, which is idempotent. Failures are silent and retried.
   */
  private async pullMemSnapshot(peerName: string): Promise<void> {
    if (this.snapshottedPeers.has(peerName)) return;
    const peer = this.getPeer(peerName);
    if (!peer) return;
    this.snapshottedPeers.add(peerName); // mark first: don't retry the same peer even on failure, avoiding hammering on a flaky network
    try {
      const resp = await this.rpcCall(peer, "MemSnapshot", {});
      const entries: any[] = resp?.result?.entries ?? [];
      let changed = false;
      for (const { key, entry } of entries) {
        if (key && entry && typeof entry.ts === "number") {
          if (this.hooks.store.memApplyRemote(key, entry as MemEntry)) changed = true;
        }
      }
      if (changed) {
        this.hooks.store.persist();
        this.hooks.log?.(`mem snapshot aligned: ${peerName} (${entries.length} entries)`);
      }
    } catch (e) {
      this.snapshottedPeers.delete(peerName); // on failure allow retries (refresh re-pulls periodically until it succeeds)
      this.hooks.log?.(`mem snapshot pull failed: ${peerName} ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  /**
   * Broadcast a single mem entry delta to all online peers (best-effort, silent on failure).
   * Offline peers are not queued in the outbox — pullMemSnapshot realigns from them at the next onPeerUp.
   */
  broadcastMemUpdate(key: string, entry: MemEntry): void {
    const peers = this.getOnlinePeers().filter((p) => p.peerName !== this.config.peerName);
    for (const p of peers) {
      this.rpcCall(p, "MemUpdate", { key, entry }).catch(() => {
        /* one peer's delivery failure doesn't affect others; the snapshot heals it when they come online */
      });
    }
  }

  // ── file transfer ───────────────────────────────────────
  /** POST /file?path=<rel>&sha256=<hex>&overwrite=1  + raw byte body */
  private async handleFileReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    if (!this.checkAuth(req)) return this.json(res, 401, { error: "unauthorized" });
    const q = this.queryOf(req);
    const rel = q.get("path") ?? "";
    const wantSha = q.get("sha256") ?? undefined;
    const overwrite = q.get("overwrite") === "1" || q.get("overwrite") === "true";
    let data: Buffer;
    try {
      data = await this.readRawBody(req, this.fileMaxBytes());
    } catch (e) {
      return this.json(res, 413, { error: e instanceof Error ? e.message : "read failed" });
    }
    try {
      const got = writeReceivedFile(this.fileRoot(), rel, data, wantSha, overwrite);
      this.hooks.log?.(`file received: ${got.rel} (${got.bytes} bytes)`);
      return this.json(res, 200, {
        ok: true,
        path: got.rel,
        bytes: got.bytes,
        sha256: got.sha256,
        root: this.fileRoot(),
      });
    } catch (e) {
      return this.json(res, 400, { error: e instanceof Error ? e.message : String(e) });
    }
  }

  /** GET /file?path=<rel>  → response body = raw bytes, headers carry sha256/byte count */
  private handleFileSend(req: http.IncomingMessage, res: http.ServerResponse): void {
    if (!this.checkAuth(req)) return this.json(res, 401, { error: "unauthorized" });
    const q = this.queryOf(req);
    const rel = q.get("path") ?? "";
    let out;
    try {
      out = readOutgoingFile(this.fileRoot(), rel);
    } catch (e) {
      return this.json(res, 404, { error: e instanceof Error ? e.message : String(e) });
    }
    if (out.bytes > this.fileMaxBytes()) {
      return this.json(res, 413, { error: `file exceeds fileMaxBytes (${out.bytes})` });
    }
    this.hooks.log?.(`file sent: ${out.rel} (${out.bytes} bytes)`);
    res.writeHead(200, {
      "Content-Type": "application/octet-stream",
      "Content-Length": String(out.bytes),
      "X-A2A-File-Path": out.rel,
      "X-A2A-File-Sha256": out.sha256,
    });
    res.end(out.data);
  }

  private queryOf(req: http.IncomingMessage): URLSearchParams {
    const u = new URL(req.url ?? "", "http://localhost");
    return u.searchParams;
  }

  /** Read the raw body (non-JSON) with a size cap. */
  private readRawBody(req: http.IncomingMessage, maxBytes: number): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const len = parseInt(req.headers["content-length"] ?? "0", 10);
      if (Number.isFinite(len) && len > maxBytes) return reject(new Error("too large"));
      const chunks: Buffer[] = [];
      let total = 0;
      req.on("data", (c) => {
        const buf = c as Buffer;
        total += buf.length;
        if (total > maxBytes) {
          reject(new Error("too large"));
          req.destroy();
          return;
        }
        chunks.push(buf);
      });
      req.on("end", () => resolve(Buffer.concat(chunks)));
      req.on("error", reject);
    });
  }

  /** client: push a local sandbox file to a peer. */
  async putFile(
    peerName: string,
    localRel: string,
    remoteRel: string,
    overwrite: boolean,
  ): Promise<{ path: string; bytes: number; sha256: string }> {
    const peer = this.getPeer(peerName);
    if (!peer) throw new Error(`peer not found: ${peerName}`);
    const out = readOutgoingFile(this.fileRoot(), localRel);
    if (out.bytes > this.fileMaxBytes()) throw new Error(`file exceeds fileMaxBytes (${out.bytes})`);
    const q = new URLSearchParams({
      path: remoteRel,
      sha256: out.sha256,
      overwrite: overwrite ? "1" : "0",
    });
    const url = `http://${peer.host}:${peer.port}${this.filePath}?${q.toString()}`;
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/octet-stream",
        Authorization: `Bearer ${this.config.workspaceSecret}`,
      },
      body: new Uint8Array(out.data),
    });
    const j = (await resp.json().catch(() => ({}))) as any;
    if (!resp.ok) throw new Error(j?.error || `HTTP ${resp.status}`);
    return { path: j.path, bytes: j.bytes, sha256: j.sha256 };
  }

  /** client: pull a file from a peer into the local sandbox. */
  async getFile(
    peerName: string,
    remoteRel: string,
    localRel: string,
    overwrite: boolean,
  ): Promise<{ path: string; bytes: number; sha256: string }> {
    const peer = this.getPeer(peerName);
    if (!peer) throw new Error(`peer not found: ${peerName}`);
    const q = new URLSearchParams({ path: remoteRel });
    const url = `http://${peer.host}:${peer.port}${this.filePath}?${q.toString()}`;
    const resp = await fetch(url, {
      headers: { Authorization: `Bearer ${this.config.workspaceSecret}` },
    });
    if (!resp.ok) {
      const j = (await resp.json().catch(() => ({}))) as any;
      throw new Error(j?.error || `HTTP ${resp.status}`);
    }
    const shaHeader = resp.headers.get("x-a2a-file-sha256") ?? undefined;
    const buf = Buffer.from(await resp.arrayBuffer());
    const got = writeReceivedFile(this.fileRoot(), localRel, buf, shaHeader, overwrite);
    this.hooks.log?.(`file fetched: ${got.rel} (${got.bytes} bytes)`);
    return { path: got.rel, bytes: got.bytes, sha256: got.sha256 };
  }

  private readBody(req: http.IncomingMessage): Promise<any> {
    return new Promise((resolve, reject) => {
      const len = parseInt(req.headers["content-length"] ?? "0", 10);
      if (Number.isFinite(len) && len > MAX_BODY_BYTES) return reject(new Error("too large"));
      const chunks: Buffer[] = [];
      req.on("data", (c) => {
        chunks.push(c as Buffer);
        if (Buffer.concat(chunks).length > MAX_BODY_BYTES) {
          reject(new Error("too large"));
          req.destroy();
        }
      });
      req.on("end", () => {
        try {
          resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"));
        } catch (e) {
          reject(e);
        }
      });
      req.on("error", reject);
    });
  }
  private json(res: http.ServerResponse, status: number, data: unknown, extraHeaders?: Record<string, string>): void {
    const headers: Record<string, string> = { "Content-Type": "application/json", ...(extraHeaders ?? {}) };
    res.writeHead(status, headers);
    res.end(JSON.stringify(data));
  }
  private rpcJson(res: http.ServerResponse, id: any, payload: unknown): void {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(payload));
  }
}

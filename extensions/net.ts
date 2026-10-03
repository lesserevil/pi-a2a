/**
 * pi-a2a — 网络层（A2A / 局域网 P2P）
 *
 * 实现 Google A2A Protocol v1.0 的 JSON-RPC binding（spec §9）。
 *
 * 每个 agent 同时是 A2A Server + Client：
 *   Server (node:http):
 *     GET  /.well-known/agent-card.json  — Agent Card
 *     GET  /health                        — mDNS 续命探测（非 spec，无害）
 *     POST /rpc                           — JSON-RPC: SendMessage / GetTask
 *     POST /a2a/notify                    — push-notification webhook 接收
 *     POST /file                          — 收文件（body=原始字节，元数据在 query）
 *     GET  /file?path=<rel>               — 发文件（响应体=原始字节）
 *   Client (fetch):
 *     send(peer, msg)     — SendMessage（request 时附 pushNotificationConfig）
 *     getTask(peer, id)   — GetTask（push 兜底）
 *     notifyPeer(url, t)  — 完成入站 task 时把结果 push 给请求方
 *     putFile(peer, ...)  — 推送本地沙箱文件到 peer
 *     getFile(peer, ...)  — 从 peer 拉取文件到本地沙箱
 *
 * 发现: mDNS/Bonjour 广告 + 浏览 _pi-a2a._tcp（TXT proto=a2a）+ 同机 presence 兜底。
 * 鉴权: 共享 workspaceSecret 作 Bearer token（每个 JSON-RPC 请求校验）。
 * 异步: 对方离线 → 本地 outbox 暂存，上线重投；出站 request 等 push 回结果，超时兜底 GetTask。
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
  delivered: string[]; // 成功送达的 peerName
  queued: string[]; // 进 outbox 的 peerName（定向、离线）
  failed: string[]; // 广播时未能送达的在线 peer
}

export interface NetHooks {
  store: Store;
  onMessageReceived: (m: Message) => void; // 收到新消息 → toast + (request/result)注入
  onPeersChanged: () => void; // peer 上下线 → 刷 widget
  log?: (msg: string) => void;
}

// ── 常量 ───────────────────────────────────────────────
const SERVICE_TYPE = "pi-a2a";
const PROTO = "a2a"; // A2A 协议标记（硬切：旧 proto=1 不再识别）
const A2AVER = "1.0";
const PEER_TTL_MS = 60_000;
const REFRESH_INTERVAL_MS = 10_000;
const POST_TIMEOUT_MS = 3_000;
const MAX_BODY_BYTES = 512 * 1024;
const PRESENCE_TTL_MS = 35_000;
const DEFAULT_PUSH_SWEEP_MS = 15_000;
const DEFAULT_PUSH_BACKSTOP_MS = 30_000;

/** presence 文件记录（本机 `~/.pi/agent/pi-a2a-presence/<agentId>-<pid>.json`）。 */
export interface PresenceRec {
  peerName: string;
  agentId: string;
  role?: string;
  workspace: string; // 明文，向后兼容旧版本（过渡期双发；后续废弃）
  wsh?: string; // workspace hash，隔离靠它（明文 ws 存在泄漏，已迁移到此）
  host: string;
  port: number;
  proto: string;
  ts: number;
  pid: number;
}

/**
 * workspace hash：sha256(len(ws):ws:len(secret):secret)[:16]（64bit）。
 * 用长度前缀而非纯分隔符——纯 `:` 分隔会被 `ws=a:b,secret=c` 与 `ws=a,secret=b:c` 撞出相同输入串（两者都拼成 "a:b:c"）。
 * 长度前缀是密码学拼接防撞的标准做法：`3:a:b:1:c` ≠ `1:a:3:b:c`。
 */
export function wsHash(workspace: string, secret: string): string {
  const payload = `${workspace.length}:${workspace}:${secret.length}:${secret}`;
  return crypto.createHash("sha256").update(payload).digest("hex").slice(0, 16);
}

/**
 * workspace 匹配：优先 wsh（hash），缺失才回退 ws 明文。
 * 兼容窗口：新版本双发 ws+wsh，旧版本只发 ws。新版本发现旧版本走 ws 回退，旧版本发现新版本靠双发的 ws 命中。
 */
export function wsMatch(
  their: { ws?: string; wsh?: string },
  mine: { workspace: string; workspaceSecret: string },
): boolean {
  if (their.wsh) return their.wsh === wsHash(mine.workspace, mine.workspaceSecret);
  // 对端是旧版本（无 wsh），回退明文 ws 匹配
  return !!their.ws && their.ws === mine.workspace;
}

/**
 * presence 去重：同 agentId 可能多个 pid 文件（同目录双进程），按 ts 最新保留。
 * Production 与 test 共用此函数，从根源消除「抽离 helper」漂移问题。
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

// JSON-RPC 错误码（spec §5.4 / §9.5）
const ERR_PARSE = -32700;
const ERR_INVALID_REQ = -32600;
const ERR_METHOD_NOT_FOUND = -32601;
const ERR_INVALID_PARAMS = -32602;
const ERR_INTERNAL = -32603;
const ERR_TASK_NOT_FOUND = -32001;

// ── JSON-RPC / A2A 小工具 ──────────────────────────────

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

/** 虚拟网卡名特征（Docker / WSL / Hyper-V / VMware / VirtualBox / VPN / tunnel 等）。
 *  这些网卡的地址只在宿主机内部可达，跨机不可达；若广告出去会导致 peer 连不上。 */
const VIRTUAL_IFACE_RE =
  /^(docker|br-|veth|vEthernet|WSL|Hyper-V|VMware|VMnet|VirtualBox|TAP|tun|utun|llw|awdl|bridge|tap|p2p|anpi)/i;

function isLikelyVirtualIface(name: string): boolean {
  return VIRTUAL_IFACE_RE.test(name);
}

/** 取本机 LAN IPv4（push webhook URL 用；跨机可达，同机也能自达）。
 *  优先返回真实物理/无线网卡；跳过虚拟网卡（Docker/WSL/Hyper-V 等），
 *  仅当没有任何真实网卡时才回退到虚拟网卡地址。 */
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

/** 从 A2A Message.parts 里抽出文本（拼所有 TextPart）。 */
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
  private snapshottedPeers = new Set<string>(); // 已拉过 mem snapshot 的 peer（防同进程重复；重启重拉，幂等）
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

  // ── 生命周期 ───────────────────────────────────────────

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

  // ── peer 表查询 ────────────────────────────────────────

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

  // ── HTTP server（A2A Server 侧）──────────────────────

  private startHttp(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => this.handle(req, res));
      this.server.on("error", (e) => this.hooks.log?.(`HTTP server 错误: ${e?.message ?? e}`));
      const port = this.config.listenPort ?? 0;
      this.server.listen(port, "0.0.0.0", () => {
        const addr = this.server!.address();
        this.listenPort = typeof addr === "object" && addr ? addr.port : port;
        this.hooks.log?.(
          `A2A HTTP 监听 0.0.0.0:${this.listenPort} (lanIp=${this.lanIp}${this.config.advertiseHost ? " [手动指定]" : ""})`,
        );
        resolve();
      });
      this.server.once("error", reject);
    });
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = req.url ?? "";
    try {
      // Agent Card（无需鉴权，spec 要求可公开发现）
      if (req.method === "GET" && url === this.agentCardPath) {
        return this.json(res, 200, this.agentCard(), { "Cache-Control": "max-age=60" });
      }
      // 健康探测（mDNS 续命用，非 spec）
      if (req.method === "GET" && url === "/health") {
        return this.json(res, 200, {
          status: "ok",
          peer: this.config.peerName,
          workspace: this.config.workspace, // 明文，向后兼容（过渡期双发）
          wsh: wsHash(this.config.workspace, this.config.workspaceSecret), // workspace hash
          proto: PROTO,
        });
      }
      // 运维统计（per-peer outbox 积压 + 累计丢弃数；需鉴权，防静默丢消息可查）
      if (req.method === "GET" && url === "/stats") {
        if (!this.checkAuth(req)) return this.json(res, 401, { error: "unauthorized" });
        return this.json(res, 200, this.hooks.store.getStats());
      }
      // JSON-RPC 端点
      if (req.method === "POST" && url === this.rpcPath) {
        return this.handleRpc(req, res);
      }
      // push-notification webhook 接收
      if (req.method === "POST" && url === this.notifyPath) {
        return this.handleNotify(req, res);
      }
      // 文件传输：按路径分发（带 query）
      if (url === this.filePath || url.startsWith(this.filePath + "?")) {
        if (req.method === "POST") return this.handleFileReceive(req, res);
        if (req.method === "GET") return this.handleFileSend(req, res);
      }
      this.json(res, 404, { error: "Not found" });
    } catch (e) {
      this.hooks.log?.(`handle 异常: ${e instanceof Error ? e.message : String(e)}`);
      try {
        this.json(res, 500, { error: "internal" });
      } catch {
        /* ignore */
      }
    }
  }

  /** 校验 Bearer 共享密钥。 */
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

  /** SendMessage：收到一条消息 → 建 task → 按 kind 决定注入/通知。 */
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
      thread_id: contextId, // A2A contextId = 本地线程
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

  /** GetTask：返回本地 task 状态（push 兜底用）。 */
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

  /** MemUpdate：收到远端单个 entry 增量 → LWW 合并。不 re-broadcast（发送方已广播）。 */
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

  /** MemSnapshot：返回本地全量 mem（含 tombstone），供新 peer bootstrap 对齐。 */
  private handleMemSnapshot(res: http.ServerResponse, id: any): void {
    this.rpcJson(res, id, rpcResult(id, { entries: this.hooks.store.memGetAll() }));
  }

  /** push-notification 接收：对方完成我委派的 task → 解析 → 注入。 */
  private async handleNotify(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    // 鉴权：Bearer 或 X-A2A-Notification-Token 任一 == 共享密钥
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
    // StreamResponse: {task} 或 {statusUpdate}（取其 task）
    const task = body?.task ?? body?.statusUpdate;
    if (task) this.handleIncomingTaskUpdate(task);
    this.json(res, 200, {}); // 幂等 ack
  }

  /** 处理一条出站 task 的状态更新（push 或 GetTask 兜底共用）。 */
  private handleIncomingTaskUpdate(task: any): void {
    const taskId = String(task?.id ?? "");
    const state = String(task?.status?.state ?? "");
    const ob = this.hooks.store.getOutboundTaskByTaskId(taskId);
    if (!ob) return; // 未知/重复 → 忽略（幂等）
    if (!Store.isTerminalState(state)) return; // 还没完成，继续等

    const arts = Array.isArray(task?.artifacts) ? task.artifacts : [];
    const text =
      extractText(arts.flatMap((a: any) => a?.parts ?? [])) ||
      extractText(task?.status?.message?.parts ?? []) ||
      "";
    this.hooks.store.resolveOutboundTask(taskId);

    const success = state === "TASK_STATE_COMPLETED";
    const body = success
      ? text || "(空结果)"
      : `[委派未完成: ${state.replace("TASK_STATE_", "")}]${text ? "\n" + text : ""}`;

    const resultMsg: Message = {
      id: genId("msg"),
      thread_id: ob.contextId, // 回填到原始 request 的线程
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
    this.hooks.onMessageReceived(resultMsg); // kind=result → 自动注入
  }

  // ── A2A 构造 ───────────────────────────────────────────

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

  // ── A2A Client 侧 ──────────────────────────────────────

  /** 发起 JSON-RPC 调用。返回 {result} 或 {error}（网络失败抛异常）。 */
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

  /** 投递一条「我发出的」消息（已写本地 store direction=sent）。 */
  async deliver(msg: Message): Promise<DeliverResult> {
    // 广播：推给当前所有在线 peer
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
    // 定向：在线直推，离线/失败进 outbox
    if (this.isOnline(msg.to_name)) {
      const peer = this.getPeer(msg.to_name)!;
      if (await this.sendToOne(peer, msg)) return { delivered: [peer.peerName], queued: [], failed: [] };
    }
    this.hooks.store.queueOutbox(msg.id, msg.to_name);
    this.hooks.store.persist();
    return { delivered: [], queued: [msg.to_name], failed: [] };
  }

  /** 向某 peer 发 A2A SendMessage。成功后若是 request 则登记待 push 的出站 task。 */
  private async sendToOne(peer: Peer, msg: Message): Promise<boolean> {
    const params: any = {
      message: {
        messageId: msg.id,
        role: "ROLE_USER",
        contextId: msg.thread_id, // 本地线程 = A2A contextId
        parts: [{ text: msg.body }],
        metadata: {
          kind: msg.kind,
          subject: msg.subject,
          from: this.config.peerName,
        },
      },
    };
    // request → 注册 push，结果即时回推
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
        this.hooks.log?.(`SendMessage 错误 ← ${peer.peerName}: ${j.error.message}`);
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
      this.hooks.log?.(`sendToOne 失败 → ${peer.peerName}: ${e instanceof Error ? e.message : String(e)}`);
      return false;
    }
  }

  /** 把完成的入站 task 结果 push 给请求方（a2a_reply 回 request 时调用）。 */
  async completeInboundTask(msgId: string, body: string, state = "TASK_STATE_COMPLETED"): Promise<boolean> {
    const t = this.hooks.store.getWorkingInboundTaskByMsgId(msgId);
    if (!t) return false;
    this.hooks.store.completeInboundTask(t.taskId, state, body);
    this.hooks.store.persist();
    if (t.pushConfig) {
      const task = this.buildTask(t.taskId, t.contextId, state, body);
      const ok = await this.notifyPeer(t.pushConfig.url, t.pushConfig.token, task);
      if (!ok) this.hooks.log?.(`push 结果失败 → ${t.pushConfig.url}（task=${t.taskId}）`);
    }
    return true;
  }

  /** POST push-notification（StreamResponse {task}）到对方 webhook。 */
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

  /** push 兜底：超时未收到 push 的出站 task 主动 GetTask 补救。 */
  private async pushSweep(): Promise<void> {
    const backstop = this.config.pushBackstopMs ?? DEFAULT_PUSH_BACKSTOP_MS;
    const now = Date.now();
    const pending = this.hooks.store.getPendingOutbound();
    for (const ob of pending) {
      if (now - ob.createdAt * 1000 < backstop) continue; // 还新鲜，继续等 push
      if (!this.isOnline(ob.peerName)) continue; // 对方离线，无法轮询
      const peer = this.getPeer(ob.peerName);
      if (!peer) continue;
      try {
        const j = await this.rpcCall(peer, "GetTask", { id: ob.taskId });
        const task = j?.result; // GetTask result = Task 对象
        if (task && Store.isTerminalState(String(task.status?.state ?? ""))) {
          this.handleIncomingTaskUpdate(task);
        }
      } catch {
        /* 忽略，下次再试 */
      }
    }
  }

  // ── mDNS 广告 + 浏览 ───────────────────────────────────
  // （与旧版基本一致；TXT proto 改 "a2a"，硬切忽略非 a2a peer）

  private startMdns(): void {
    let BonjourCtor: any = null;
    try {
      const mod: any = nativeRequire("bonjour-service");
      BonjourCtor = mod.Bonjour ?? mod.default ?? mod;
    } catch {
      this.hooks.log?.(
        "⚠️ mDNS 不可用：未安装 bonjour-service（无法自动发现同局域网 peer）。请在 pi-a2a 目录运行 npm install。",
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
          ws: this.config.workspace, // 明文，向后兼容（过渡期双发，后续废弃）
          wsh: wsHash(this.config.workspace, this.config.workspaceSecret), // workspace hash，隔离靠它
          proto: PROTO,
          a2aver: A2AVER,
        },
      });
      this.service?.on?.("error", (e: unknown) => {
        this.hooks.log?.(`⚠️ mDNS 发布失败（服务名冲突？）: ${e instanceof Error ? e.message : String(e)}`);
      });
      this.browser = this.bonjour.find({ type: SERVICE_TYPE });
      this.browser.on("up", (svc: any) => this.onPeerUp(svc));
      this.browser.on("down", (svc: any) => this.onPeerDown(svc));
      this.hooks.log?.(`mDNS 广告 + 浏览已启动 (ws=${this.config.workspace}, proto=a2a, 实例=${instanceName})`);
    } catch (e) {
      this.hooks.log?.(`⚠️ mDNS 启动失败: ${e instanceof Error ? e.message : String(e)}`);
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
    // 优先选和本机同网段的地址（跨机可达），避免命中虚拟网卡（Docker/WSL 的 172.x 等）
    if (!this.lanIp.startsWith("127.")) {
      const prefix = this.lanIp.split(".").slice(0, 3).join(".") + ".";
      const same = addrs.find((a) => a.startsWith(prefix));
      if (same) return same;
    }
    // 回退：跳过 link-local(169.254) 和 loopback(127)
    return addrs.find((a) => !a.startsWith("169.254.") && !a.startsWith("127.")) ?? addrs[0];
  }
  private betterHost(prev: string | undefined, next: string): string {
    if (this.isIpv4(next)) return next;
    if (this.isIpv4(prev)) return prev;
    return next || prev || "";
  }

  private onPeerUp(svc: any): void {
    const txt = svc?.txt ?? {};
    if (!wsMatch(txt, this.config)) return; // 工作区隔离（优先 wsh hash，回退 ws 明文）
    if (txt.proto !== PROTO) {
      // 硬切：非 A2A 协议（含旧 proto=1 或无 proto）一律忽略
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
      this.hooks.log?.(`peer 上线: ${peerName} (${host}:${port})`);
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
    // 不立即删除：mDNS "down" 在 WiFi / 多网卡 / 防火墙环境下频繁误报，
    // 真正离线由 refresh() 的 TTL + healthCheck 兜底判定（网络不通才删）。
    // 这样避免 mDNS 多播偶发丢包导致 peer 被误删且无法用 healthCheck 拉回。
    if (this.peers.has(peerName)) {
      this.hooks.log?.(`peer mDNS 信号丢失: ${peerName}（保留，转 healthCheck 兜底）`);
    }
  }

  // ── 同机 presence 兜底发现 ───────────────────────────
  private presenceDir(): string {
    return path.join(os.homedir(), ".pi", "agent", "pi-a2a-presence");
  }
  private presenceFile(): string {
    // 文件名带 pid：同目录双进程（同 agentId）不再互相覆盖 presence 文件。
    // scanPresence 按 agentId 去重、取最新 ts 的那条供发现，但不删较旧的 pid 文件（活进程还在写）。
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
        workspace: this.config.workspace, // 明文，向后兼容（过渡期双发，后续废弃）
        wsh: wsHash(this.config.workspace, this.config.workspaceSecret), // workspace hash，隔离靠它
        host: "127.0.0.1",
        port: this.listenPort,
        proto: PROTO,
        ts: Date.now(),
        pid: process.pid,
      };
      fs.writeFileSync(this.presenceFile(), JSON.stringify(rec));
    } catch {
      /* 写失败不致命 */
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
    // 阶段 1：读 + 过滤 + TTL 清理（删文件是副作用，留在 scanPresence；纯去重才用 dedupePresence）
    const valid: PresenceRec[] = [];
    for (const f of files) {
      if (!f.endsWith(".json")) continue;
      let rec: any;
      try {
        rec = JSON.parse(fs.readFileSync(path.join(this.presenceDir(), f), "utf8"));
      } catch {
        continue;
      }
      // workspace 隔离：优先 wsh hash，回退 ws 明文（兼容旧版本）
      if (!rec || !wsMatch(rec, this.config)) continue;
      if (rec.proto !== PROTO) continue; // 硬切：必须是 A2A 协议
      const peerName = String(rec.peerName ?? "");
      if (!peerName || peerName === this.config.peerName) continue;
      const port = Number(rec.port);
      if (!Number.isFinite(port) || port <= 0) continue;
      const ts = typeof rec.ts === "number" ? rec.ts : 0;
      // TTL 过期 → 进程已死，安全删除（这是唯一允许删文件的路径）
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
    // 阶段 2：同 agentId 多 pid 去重（取最新 ts）— 抽成 export dedupePresence，production/test 共用
    const deduped = dedupePresence(valid);
    // 阶段 3：加入 peers
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
        this.hooks.log?.(`peer 上线(presence): ${peerName} (127.0.0.1:${port})`);
        this.hooks.onPeersChanged();
        void this.flushOutbox(peerName);
      }
    }
  }

  // ── 健康探测续命 ──────────────────────────────────────
  private async healthCheck(peer: Peer): Promise<boolean> {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), POST_TIMEOUT_MS);
    try {
      const r = await fetch(`http://${peer.host}:${peer.port}/health`, { signal: ctrl.signal });
      if (!r.ok) return false;
      const j = await r.json().catch(() => null);
      // workspace 校验：优先 wsh hash，回退 ws 明文（兼容旧版本 peer）
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

  // ── 离线 outbox 重投 ──────────────────────────────────
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
        this.hooks.log?.(`重投成功: ${e.id} → ${peerName}`);
      }
    }
    this.hooks.store.persist();
  }

  // ── 共享记忆 mem 同步 ─────────────────────────────────
  /**
   * 向 peer 拉一次 mem 全量 snapshot，LWW 合并到本地。每个 peer 每进程只拉一次
   * （snapshottedPeers 防重）；重启后 Set 清空，重新拉，幂等。失败静默，下次重试。
   */
  private async pullMemSnapshot(peerName: string): Promise<void> {
    if (this.snapshottedPeers.has(peerName)) return;
    const peer = this.getPeer(peerName);
    if (!peer) return;
    this.snapshottedPeers.add(peerName); // 先标记：即使失败也不重试同 peer，避免抖动网络下反复打
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
        this.hooks.log?.(`mem snapshot 对齐: ${peerName} (${entries.length} 条)`);
      }
    } catch (e) {
      this.snapshottedPeers.delete(peerName); // 失败则允许重试（refresh 会定期重拉，直到成功）
      this.hooks.log?.(`mem snapshot 拉取失败: ${peerName} ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  /**
   * 广播单个 mem entry 增量给所有在线 peer（best-effort，失败静默）。
   * 离线 peer 不进 outbox——下次 onPeerUp 时 pullMemSnapshot 从对方对齐即可。
   */
  broadcastMemUpdate(key: string, entry: MemEntry): void {
    const peers = this.getOnlinePeers().filter((p) => p.peerName !== this.config.peerName);
    for (const p of peers) {
      this.rpcCall(p, "MemUpdate", { key, entry }).catch(() => {
        /* 单 peer 投递失败不影响其他 peer；对方上线后 snapshot 治愈 */
      });
    }
  }

  // ── 文件传输 ──────────────────────────────────────────
  /** POST /file?path=<rel>&sha256=<hex>&overwrite=1  + 原始字节 body */
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
      this.hooks.log?.(`file 收到: ${got.rel} (${got.bytes} bytes)`);
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

  /** GET /file?path=<rel>  → 响应体 = 原始字节，头部带 sha256/字节数 */
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
    this.hooks.log?.(`file 发出: ${out.rel} (${out.bytes} bytes)`);
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

  /** 读取原始 body（非 JSON），带上限。 */
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

  /** 客户端：把本地沙箱文件推送到 peer。 */
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

  /** 客户端：从 peer 拉取文件到本地沙箱。 */
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
    this.hooks.log?.(`file 拉取: ${got.rel} (${got.bytes} bytes)`);
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

/**
 * pi-a2a — 本地存储（JSON 文件，去中心化）
 *
 * 每个 agent 本地各自存一份。同时保留「我收到的」(inbox) 和「我发出的」(sent)
 * 副本，于是 a2a_read 能在本地用 thread_id 把整条线程拼出来，无需问对方。
 *
 * A2A 迁移后新增两本「task 账本」（与 messages/outbox 平级落盘）：
 *   - inboundTasks : 我作为 A2A server 创建的 task（别人发来的消息都建 task）
 *   - outboundTasks: 我作为 A2A client 在远端建的 task（我发出的委派，等 push 回结果）
 *
 * 数据文件: ~/.pi/agent/pi-a2a.db.json (全局) 或 <cwd>/.pi/pi-a2a.db.json (项目)。
 */
import * as fs from "node:fs";
import * as path from "node:path";

export type Direction = "inbox" | "sent";
export type MsgKind = "message" | "request" | "result";

export interface Message {
  id: string;
  thread_id: string; // == 线程首条消息 id
  reply_to: string | null; // 父消息 id（线程根为 null）
  from_id: string;
  from_name: string;
  to_name: string; // 收件人 peerName，或 "*" 广播
  subject: string;
  body: string;
  kind: MsgKind;
  direction: Direction; // 本地视角：inbox 收到的 / sent 我发出的
  created_at: number; // unix 秒
  // ── A2A 关联（可选）──────────────────────────────────
  taskId?: string; // 关联的 A2A task（若有）
  contextId?: string; // A2A contextId（= 线程在 A2A 下的对应物）
  taskState?: string; // 关联 task 的当前状态（TASK_STATE_*），便于展示
}

export interface OutboxEntry {
  id: string; // 待投递的消息 id（消息本体已在 messages 里，direction=sent）
  to: string; // 目标 peerName
  queued_at: number; // unix 秒
}

/** 共享记忆条目：跨 peer 复制的 KV 值。LWW 按 ts 合并；删除=写 tombstone。 */
export interface MemEntry {
  value: string; // tombstone 时为 ""
  ts: number; // unix ms，LWW 用（注意：ms，与 Message.created_at 的秒区分）
  author: string; // 最后写入者的 peerName
  deleted?: boolean; // tombstone 标记
}

/** 我作为 server 持有的 A2A task（对应一条收到的消息）。 */
export interface InboundTask {
  taskId: string;
  msgId: string; // 对应的本地 inbox 消息 id
  contextId: string;
  state: string; // TASK_STATE_*
  fromName: string;
  createdAt: number; // unix 秒
  pushConfig?: { url: string; token: string }; // 发送方注册的 push 回调（仅 request 有）
  artifactText?: string; // 完成时挂的结果正文
}

/** 我作为 client 在远端建的 task（等 push 回结果）。 */
export interface OutboundTask {
  taskId: string;
  msgId: string; // 对应的本地 sent 消息 id（= 线程根，用于回填 thread_id）
  contextId: string;
  peerName: string;
  peerEndpoint: string; // http://host:port
  createdAt: number; // unix 秒
}

/** 归档文件结构（`pi-a2a.archive.<YYYYMMDD>.json`，与 db.json 同目录，不自动加载）。 */
export interface ArchiveFile {
  archivedAt: number; // unix 秒，归档时刻
  messages: Message[]; // 被归档的消息本体
  reads: Record<string, string[]>; // 仅含被归档消息的 reads entry（随消息整体搬迁）
}

interface StoreData {
  messages: Message[];
  reads: Record<string, string[]>; // message_id -> [reader_name,...]
  outbox: OutboxEntry[];
  inboundTasks: InboundTask[];
  outboundTasks: OutboundTask[];
  // 共享记忆 KV：跨 peer 复制；maybeArchive 只扫 messages，不受 mem 体量影响
  mem?: Record<string, MemEntry>;
  // per-peer 累计丢弃计数（outbox 溢出时递增，可观测用，防静默丢消息）
  droppedCount?: Record<string, number>;
}

const KINDS: MsgKind[] = ["message", "request", "result"];

/** 每个 peer 的 outbox 上限（超出 FIFO 丢弃最旧，防离线 peer 导致积压无界增长）。 */
const MAX_OUTBOX_PER_PEER = 1000;

/** messages 归档阈值：主 db messages 超过此数触发归档（软上限）。 */
const ARCHIVE_THRESHOLD = 5000;
/** 每次归档移走的最旧消息条数。 */
const ARCHIVE_BATCH_SIZE = 1000;
/** active task 超时窗口（unix 秒）：超过后其关联消息也归档，防 task 永不终态导致变相泄漏。 */
const TASK_STALE_SEC = 7 * 24 * 3600;

/** Store 回调钩子：把 store 内部事件冒泡到 extension 层（如 outbox 溢出 → toast）。 */
export interface StoreHooks {
  onOutboxOverflow?: (peer: string, droppedId: string, droppedSubject: string) => void;
}

// A2A TaskState 终态集合（spec §4.1.3）
// 注意：TASK_STATE_STALE 不是 spec 终态，是本地扩展状态——active task 超时归档时标记，
// 让 isTerminalState 认它（语义=已结束），防 task 永不终态导致变相 db 泄漏。
// 勿删勿改语义。如需对齐 spec 请走独立项，不要在归档逻辑里隐式处理。
const TERMINAL_STATES = new Set([
  "TASK_STATE_COMPLETED",
  "TASK_STATE_FAILED",
  "TASK_STATE_CANCELED",
  "TASK_STATE_REJECTED",
  "TASK_STATE_STALE",
]);

export class Store {
  private data: StoreData = {
    messages: [],
    reads: {},
    outbox: [],
    inboundTasks: [],
    outboundTasks: [],
    droppedCount: {},
  };
  private dirty = false;
  private flushTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly filePath: string,
    private readonly hooks?: StoreHooks,
  ) {}

  /** 从磁盘加载；文件不存在或损坏则从空开始。 */
  load(): void {
    try {
      const raw = fs.readFileSync(this.filePath, "utf8");
      const parsed = JSON.parse(raw);
      const messages = Array.isArray(parsed?.messages)
        ? (parsed.messages as Message[]).filter((m) => m && typeof m.id === "string")
        : [];
      const reads =
        parsed?.reads && typeof parsed.reads === "object" && !Array.isArray(parsed.reads)
          ? (parsed.reads as Record<string, string[]>)
          : {};
      const outbox = Array.isArray(parsed?.outbox) ? (parsed.outbox as OutboxEntry[]) : [];
      const inboundTasks = Array.isArray(parsed?.inboundTasks)
        ? (parsed.inboundTasks as InboundTask[])
        : [];
      const outboundTasks = Array.isArray(parsed?.outboundTasks)
        ? (parsed.outboundTasks as OutboundTask[])
        : [];
      const droppedCount =
        parsed?.droppedCount && typeof parsed.droppedCount === "object" && !Array.isArray(parsed.droppedCount)
          ? (parsed.droppedCount as Record<string, number>)
          : {};
      const mem =
        parsed?.mem && typeof parsed.mem === "object" && !Array.isArray(parsed.mem)
          ? (parsed.mem as Record<string, MemEntry>)
          : {};
      this.data = { messages, reads, outbox, inboundTasks, outboundTasks, droppedCount, mem };
    } catch {
      this.data = {
        messages: [],
        reads: {},
        outbox: [],
        inboundTasks: [],
        outboundTasks: [],
        droppedCount: {},
        mem: {},
      };
    }
    this.dirty = false;
  }

  /** 启动周期性落盘（防丢 + 防写放大）。 */
  startAutoFlush(intervalMs = 2000): void {
    this.stopAutoFlush();
    this.flushTimer = setInterval(() => this.persist(), intervalMs);
  }
  stopAutoFlush(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  /** 落盘（幂等：无变更则跳过）。超阈值时触发归档（软上限，治 db.json 无界增长）。 */
  persist(): void {
    if (!this.dirty) return;
    this.dirty = false;
    try {
      fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
      fs.writeFileSync(this.filePath, JSON.stringify(this.data, null, 2));
    } catch {
      /* 落盘失败不致命：内存数据本会话仍可用 */
    }
    // 主 db 落盘后检查归档阈值（归档是新增路径，crash 安全顺序见 maybeArchive）
    try {
      this.maybeArchive();
    } catch {
      /* 归档失败不致命：主 db 已存，下双 persist 重试 */
    }
  }

  // ── 归档（软上限 + archive 文件 + 惰性加载）────────────

  /** 归档文件名 = pi-a2a.archive.<YYYYMMDD>.json，与 db.json 同目录。 */
  private archiveFilePath(): string {
    const d = new Date();
    const ymd = `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
    return path.join(path.dirname(this.filePath), `pi-a2a.archive.${ymd}.json`);
  }

  /** 主 db messages 超阈值 → 归档最旧的 ARCHIVE_BATCH_SIZE 条到当天 archive 文件。 */
  private maybeArchive(): void {
    if (this.data.messages.length < ARCHIVE_THRESHOLD) return;
    const archivable = this.computeArchivable();
    if (archivable.length === 0) return;

    // ── crash 安全顺序：先写 archive（持久化待归档数据），再删主 db ──
    // 最坏情况：archive 写成功但主 db 未更新前 crash → 重启后消息在 archive（不丢），
    //   addMessage 幂等去重处理潜在重复。绝不先删主 db。
    const archivedReads: Record<string, string[]> = {};
    for (const id of archivable) {
      const r = this.data.reads[id];
      if (r) archivedReads[id] = r;
    }
    const archivePayload: ArchiveFile = {
      archivedAt: Math.floor(Date.now() / 1000),
      messages: archivable.map((id) => this.data.messages.find((m) => m.id === id)!).filter(Boolean),
      reads: archivedReads,
    };
    if (!this.appendToArchive(this.archiveFilePath(), archivePayload)) return; // archive 写失败则放弃，不删主 db

    // archive 写成功 → 从主 db 移除 messages + reads entry（整体搬迁，非复制）
    const idSet = new Set(archivable);
    this.data.messages = this.data.messages.filter((m) => !idSet.has(m.id));
    for (const id of archivable) delete this.data.reads[id];
    this.dirty = true;
    // 立即再 persist 一次（删减后的主 db）。此时最坏 crash 也只是主 db 还是旧的（含已归档消息），
    // 不丢消息（archive 有了，主 db 也有），addMessage 幂等去重生效。
    try {
      fs.writeFileSync(this.filePath, JSON.stringify(this.data, null, 2));
      this.dirty = false;
    } catch {
      /* 保持 dirty，下双 persist 重试 */
    }
  }

  /**
   * 计算可归档的消息 id（按 created_at 最旧优先，上限 ARCHIVE_BATCH_SIZE）。
   * 不归档：① outbox 引用的（重投要本体） ② active task 关联且未超时的（task 状态完整性）。
   * active task 超时（>TASK_STALE_SEC）的归档，并标 TASK_STATE_STALE（本地扩展终态，防变相泄漏）。
   */
  private computeArchivable(): string[] {
    const now = Math.floor(Date.now() / 1000);
    const result: string[] = [];
    const sorted = [...this.data.messages].sort((a, b) => a.created_at - b.created_at);
    for (const m of sorted) {
      // ① outbox 引用的不归档（pinned 优先，连 task 检查都不用进）
      if (this.data.outbox.some((e) => e.id === m.id)) continue;
      // ② active task 关联且未超时的不归档
      const inbound = this.data.inboundTasks.find((t) => t.msgId === m.id);
      if (inbound && !Store.isTerminalState(inbound.state)) {
        if (inbound.createdAt && now - inbound.createdAt <= TASK_STALE_SEC) continue; // 还新鲜，跳过
        inbound.state = "TASK_STATE_STALE"; // 超时：归档并标 stale（防变相泄漏）
      }
      // ③ outbound task（我发出的 request，等远端 push 结果）的保护：和 inbound 对称用 TASK_STALE_SEC。
      // outbound 无本地 state（state 在远端），只看 createdAt 新鲜度。
      // 不用 PUSH_BACKSTOP_SEC（30s）——那是 push 通道超时，非 task 生命周期窗口，语义不对称。
      const outbound = this.data.outboundTasks.find((t) => t.msgId === m.id);
      if (outbound && outbound.createdAt && now - outbound.createdAt <= TASK_STALE_SEC) continue;
      result.push(m.id);
      if (result.length >= ARCHIVE_BATCH_SIZE) break;
    }
    return result;
  }

  /** 追加写入 archive 文件（O_APPEND 原子性 + fsync crash 安全）。返回是否成功。 */
  private appendToArchive(file: string, payload: ArchiveFile): boolean {
    try {
      fs.mkdirSync(path.dirname(file), { recursive: true });
      // archive 是新增路径，用 openSync('a') + fsyncSync 做 crash 安全（主 db 沿用 writeFileSync 不动）
      const fd = fs.openSync(file, "a");
      try {
        // 追加一个 JSON 对象 + 换行（每个 payload 独立一行，方便后续按行解析）
        fs.writeSync(fd, JSON.stringify(payload) + "\n");
        fs.fsyncSync(fd); // crash 安全：强制刷盘
      } finally {
        fs.closeSync(fd);
      }
      return true;
    } catch {
      return false;
    }
  }

  /** 扫描所有 archive 文件，返回 (ArchiveFile, 文件路径) 列表（按日期倒序）。 */
  private scanArchives(): { file: string; payloads: ArchiveFile[] }[] {
    const dir = path.dirname(this.filePath);
    let names: string[] = [];
    try {
      names = fs.readdirSync(dir).filter((n) => /^pi-a2a\.archive\.\d{8}\.json$/.test(n));
    } catch {
      return [];
    }
    names.sort((a, b) => b.localeCompare(a)); // 文件名含日期，倒序=最新在前
    const result: { file: string; payloads: ArchiveFile[] }[] = [];
    for (const n of names) {
      const fp = path.join(dir, n);
      try {
        const raw = fs.readFileSync(fp, "utf8");
        // archive 文件是多个 payload 逐行追加，按行解析
        const payloads = raw
          .split("\n")
          .filter((l) => l.trim())
          .map((l) => {
            try {
              return JSON.parse(l) as ArchiveFile;
            } catch {
              return null;
            }
          })
          .filter((p): p is ArchiveFile => !!p && Array.isArray(p.messages));
        if (payloads.length > 0) result.push({ file: fp, payloads });
      } catch {
        /* 单个 archive 损坏不阻断其他 */
      }
    }
    return result;
  }

  private touch(): void {
    this.dirty = true;
  }

  // ── 消息 ───────────────────────────────────────────────

  /** 新增消息。已存在同 id 则忽略（幂等，用于重投去重）。返回是否真正新增。 */
  addMessage(m: Message): boolean {
    if (this.data.messages.some((x) => x.id === m.id)) return false;
    this.data.messages.push(m);
    this.touch();
    return true;
  }

  getMessage(id: string): Message | undefined {
    return this.data.messages.find((m) => m.id === id);
  }

  /** 把任意 message_id 解析到所属线程，返回整条线程（按时间升序）。 */
  getThread(messageId: string): Message[] {
    const hit = this.getMessage(messageId);
    const tid = hit ? hit.thread_id : messageId;
    return this.data.messages
      .filter((m) => m.thread_id === tid)
      .sort((a, b) => a.created_at - b.created_at);
  }

  // ── Deep 读取（主 db miss → 扫 archive，低频路径用：a2a_read 历史阅读）──────────
  // 性能红线：getInbox 绝不调 deep（高频路径，TUI 列表渲染）。

  /** 深查消息：主 db 找不到时扫所有 archive 文件（按日期倒序，找到即止）。 */
  getMessageDeep(id: string): Message | undefined {
    const main = this.getMessage(id);
    if (main) return main;
    for (const { payloads } of this.scanArchives()) {
      for (const p of payloads) {
        const hit = p.messages.find((m) => m.id === id);
        if (hit) return hit;
      }
    }
    return undefined;
  }

  /** 深查线程：合并主 db + archive 里同 thread_id 的消息（历史阅读拼全整条线程）。 */
  getThreadDeep(messageId: string): Message[] {
    const all: Message[] = [...this.data.messages];
    for (const { payloads } of this.scanArchives()) {
      for (const p of payloads) all.push(...p.messages);
    }
    const hit = all.find((m) => m.id === messageId);
    const tid = hit ? hit.thread_id : messageId;
    // 同 thread_id 去重（archive 和主 db 可能有重复，crash 安全顺序的副作用）
    const seen = new Set<string>();
    return all
      .filter((m) => m.thread_id === tid)
      .filter((m) => (seen.has(m.id) ? false : (seen.add(m.id), true)))
      .sort((a, b) => a.created_at - b.created_at);
  }

  /** 深查已读状态：主 db reads miss 时查 archive 的 reads（历史消息已读状态正确）。 */
  isReadDeep(id: string, reader: string): boolean {
    if (this.isRead(id, reader)) return true;
    for (const { payloads } of this.scanArchives()) {
      for (const p of payloads) {
        if ((p.reads[id] ?? []).includes(reader)) return true;
      }
    }
    return false;
  }

  /** 收件箱：direction=inbox、发给 me 或广播、非自发。可仅未读、限制条数、按时间倒序。 */
  getInbox(me: string, opts: { unread?: boolean; limit?: number }): Message[] {
    let list = this.data.messages.filter(
      (m) => m.direction === "inbox" && (m.to_name === me || m.to_name === "*") && m.from_name !== me,
    );
    if (opts.unread) list = list.filter((m) => !this.isRead(m.id, me));
    list.sort((a, b) => b.created_at - a.created_at);
    return list.slice(0, opts.limit ?? 50);
  }

  unreadCount(me: string): number {
    return this.data.messages.filter(
      (m) => m.direction === "inbox" && (m.to_name === me || m.to_name === "*") && m.from_name !== me && !this.isRead(m.id, me),
    ).length;
  }

  /** 清空收件箱：删除所有 inbox 方向消息 + 对应 reads entry。返回删除条数。归档文件与发件历史不动。 */
  clearInbox(): number {
    const inboxIds = new Set(this.data.messages.filter((m) => m.direction === "inbox").map((m) => m.id));
    if (inboxIds.size === 0) return 0;
    this.data.messages = this.data.messages.filter((m) => !inboxIds.has(m.id));
    for (const id of inboxIds) delete this.data.reads[id];
    this.touch();
    return inboxIds.size;
  }

  // ── 共享记忆 mem（KV，跨 peer 复制，LWW + tombstone）─────────

  private memMap(): Record<string, MemEntry> {
    return (this.data.mem ??= {});
  }

  /** 读单条；tombstone 视为不存在。 */
  memGet(key: string): MemEntry | undefined {
    const e = this.memMap()[key];
    return e && !e.deleted ? e : undefined;
  }

  /** 全量（含 tombstone）：snapshot 出站用，远端按 LWW 合并。 */
  memGetAll(): { key: string; entry: MemEntry }[] {
    return Object.entries(this.memMap()).map(([key, entry]) => ({ key, entry }));
  }

  /** 活跃 key 列表（排除 tombstone）。 */
  memKeys(): string[] {
    return Object.entries(this.memMap())
      .filter(([, e]) => !e.deleted)
      .map(([k]) => k)
      .sort();
  }

  /** 本地写：写新 entry（ts=now），返回 entry 供调用方广播。 */
  memSetLocal(key: string, value: string, author: string): MemEntry {
    const entry: MemEntry = { value, ts: Date.now(), author };
    this.memMap()[key] = entry;
    this.touch();
    return entry;
  }

  /** 本地删除：写 tombstone，返回 entry 供广播。 */
  memDeleteLocal(key: string, author: string): MemEntry {
    const entry: MemEntry = { value: "", ts: Date.now(), author, deleted: true };
    this.memMap()[key] = entry;
    this.touch();
    return entry;
  }

  /**
   * 远端 entry 合并（LWW）。ts 大者胜；ts 相等按 author 字典序大者胜。
   * ts+author 都相等视为 echo（自己广播回来的），无变化。
   * 返回是否发生变化——调用方据此决定是否 persist。
   */
  memApplyRemote(key: string, incoming: MemEntry): boolean {
    const cur = this.memMap()[key];
    if (cur) {
      const newer = incoming.ts > cur.ts || (incoming.ts === cur.ts && incoming.author > cur.author);
      if (!newer) return false;
    }
    this.memMap()[key] = incoming;
    this.touch();
    return true;
  }

  // ── 已读（per-reader）───────────────────────────────────

  isRead(id: string, reader: string): boolean {
    return (this.data.reads[id] ?? []).includes(reader);
  }

  markRead(id: string, reader: string): void {
    const cur = this.data.reads[id] ?? [];
    if (!cur.includes(reader)) {
      this.data.reads[id] = [...cur, reader];
      this.touch();
    }
  }

  /** 把整条线程里所有消息标记为 reader 已读。 */
  markThreadRead(threadId: string, reader: string): void {
    const targets = this.data.messages.filter((m) => m.thread_id === threadId);
    let changed = false;
    for (const m of targets) {
      const cur = this.data.reads[m.id] ?? [];
      if (!cur.includes(reader)) {
        this.data.reads[m.id] = [...cur, reader];
        changed = true;
      }
    }
    if (changed) this.touch();
  }

  // ── 发件箱（离线暂存，peer 上线重投）─────────────────────

  /**
   * 入队一条待投递任务（同 id+to 去重）。
   * 超出该 peer 的 outbox 上限时 FIFO 丢弃最旧（按 queued_at 入队时间），
   * 并累加 droppedCount + 触发 onOutboxOverflow 回调，防止静默丢消息。
   */
  queueOutbox(id: string, to: string): void {
    if (this.data.outbox.some((e) => e.id === id && e.to === to)) return;
    const mine = this.data.outbox.filter((e) => e.to === to);
    if (mine.length >= MAX_OUTBOX_PER_PEER) {
      // 丢弃该 peer 最旧的一条（入队时间最早）；message 本体保留（线程历史不断）
      const oldest = mine.sort((a, b) => a.queued_at - b.queued_at)[0];
      this.data.outbox = this.data.outbox.filter((e) => !(e.id === oldest.id && e.to === to));
      this.data.droppedCount ??= {};
      this.data.droppedCount[to] = (this.data.droppedCount[to] ?? 0) + 1;
      const subj = this.getMessage(oldest.id)?.subject ?? "(无主题)";
      try {
        this.hooks?.onOutboxOverflow?.(to, oldest.id, subj);
      } catch {
        /* 回调失败不影响入队 */
      }
    }
    this.data.outbox.push({ id, to, queued_at: Math.floor(Date.now() / 1000) });
    this.touch();
  }

  getOutboxFor(to: string): OutboxEntry[] {
    return this.data.outbox.filter((e) => e.to === to);
  }

  removeOutbox(id: string, to: string): void {
    const before = this.data.outbox.length;
    this.data.outbox = this.data.outbox.filter((e) => !(e.id === id && e.to === to));
    if (this.data.outbox.length !== before) this.touch();
  }

  /** 运维统计：per-peer 的 outbox 当前积压数 + 累计丢弃数（溢出可观测）。 */
  getStats(): { outbox: Record<string, number>; dropped: Record<string, number> } {
    const outbox: Record<string, number> = {};
    for (const e of this.data.outbox) outbox[e.to] = (outbox[e.to] ?? 0) + 1;
    return { outbox, dropped: { ...(this.data.droppedCount ?? {}) } };
  }

  // ── A2A: inbound tasks（我作为 server）───────────────────

  addInboundTask(t: InboundTask): void {
    const i = this.data.inboundTasks.findIndex((x) => x.taskId === t.taskId);
    if (i >= 0) this.data.inboundTasks[i] = t;
    else this.data.inboundTasks.push(t);
    this.touch();
  }

  getInboundTaskByTaskId(taskId: string): InboundTask | undefined {
    return this.data.inboundTasks.find((t) => t.taskId === taskId);
  }

  /** 仍处于 WORKING（可被 reply 完结）的入站 task。 */
  getWorkingInboundTaskByMsgId(msgId: string): InboundTask | undefined {
    return this.data.inboundTasks.find((t) => t.msgId === msgId && t.state === "TASK_STATE_WORKING");
  }

  /** 把入站 task 推进到终态并挂结果。返回更新后的 task。 */
  completeInboundTask(taskId: string, state: string, artifactText: string): InboundTask | undefined {
    const t = this.data.inboundTasks.find((x) => x.taskId === taskId);
    if (!t) return undefined;
    t.state = state;
    t.artifactText = artifactText;
    this.touch();
    // 同步刷新关联 inbox 消息的 taskState
    const m = this.getMessage(t.msgId);
    if (m) m.taskState = state;
    return t;
  }

  // ── A2A: outbound tasks（我作为 client，等 push）─────────

  addOutboundTask(t: OutboundTask): void {
    const i = this.data.outboundTasks.findIndex((x) => x.taskId === t.taskId);
    if (i >= 0) this.data.outboundTasks[i] = t;
    else this.data.outboundTasks.push(t);
    this.touch();
  }

  getOutboundTaskByTaskId(taskId: string): OutboundTask | undefined {
    return this.data.outboundTasks.find((t) => t.taskId === taskId);
  }

  resolveOutboundTask(taskId: string): OutboundTask | undefined {
    const i = this.data.outboundTasks.findIndex((x) => x.taskId === taskId);
    if (i < 0) return undefined;
    const [removed] = this.data.outboundTasks.splice(i, 1);
    this.touch();
    return removed;
  }

  /** 所有待 push 的出站 task（兜底扫描用）。 */
  getPendingOutbound(): OutboundTask[] {
    return [...this.data.outboundTasks];
  }

  // ── 静态工具 ───────────────────────────────────────────

  /** 校验 kind 合法。 */
  static validKind(k: string): k is MsgKind {
    return (KINDS as string[]).includes(k);
  }

  /** 是否终态。 */
  static isTerminalState(state: string): boolean {
    return TERMINAL_STATES.has(state);
  }
}

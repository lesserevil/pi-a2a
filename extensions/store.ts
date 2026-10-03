/**
 * pi-a2a — local storage (JSON files, decentralised)
 *
 * Each agent keeps its own local copy. It stores both what I received (inbox) and what I sent (sent),
 * so a2a_read can reconstruct a whole thread locally by thread_id without asking the peer.
 *
 * After the A2A migration there are two additional task ledgers (persisted alongside messages/outbox):
 *   - inboundTasks : tasks I created as an A2A server (every message received creates a task)
 *   - outboundTasks: tasks I created remotely as an A2A client (delegations I sent, awaiting a push result)
 *
 * Data file: ~/.pi/agent/pi-a2a.db.json (global) or <cwd>/.pi/pi-a2a.db.json (project).
 */
import * as fs from "node:fs";
import * as path from "node:path";

export type Direction = "inbox" | "sent";
export type MsgKind = "message" | "request" | "result";

export interface Message {
  id: string;
  thread_id: string; // == the thread's first message id
  reply_to: string | null; // parent message id (null for a thread root)
  from_id: string;
  from_name: string;
  to_name: string; // recipient peerName, or "*" for broadcast
  subject: string;
  body: string;
  kind: MsgKind;
  direction: Direction; // local view: inbox = received / sent = sent by me
  created_at: number; // unix seconds
  // ── A2A associations (optional) ─────────────────────
  taskId?: string; // associated A2A task (if any)
  contextId?: string; // A2A contextId (= the thread's counterpart under A2A)
  taskState?: string; // current state of the associated task (TASK_STATE_*), for display
}

export interface OutboxEntry {
  id: string; // id of the message awaiting delivery (the body is already in messages, direction=sent)
  to: string; // target peerName
  queued_at: number; // unix seconds
}

/** Shared-memory entry: a KV value replicated across peers. LWW merge by ts; deletion writes a tombstone. */
export interface MemEntry {
  value: string; // "" when a tombstone
  ts: number; // unix ms, used for LWW (note: ms, unlike Message.created_at which is seconds)
  author: string; // peerName of the last writer
  deleted?: boolean; // tombstone marker
}

/** An A2A task I hold as server (corresponding to one received message). */
export interface InboundTask {
  taskId: string;
  msgId: string; // the corresponding local inbox message id
  contextId: string;
  state: string; // TASK_STATE_*
  fromName: string;
  createdAt: number; // unix seconds
  pushConfig?: { url: string; token: string }; // push callback registered by the sender (requests only)
  artifactText?: string; // result body attached on completion
}

/** A task I created remotely as a client (awaiting a pushed result). */
export interface OutboundTask {
  taskId: string;
  msgId: string; // the corresponding local sent message id (= thread root, used to backfill thread_id)
  contextId: string;
  peerName: string;
  peerEndpoint: string; // http://host:port
  createdAt: number; // unix seconds
}

/** Archive file structure (`pi-a2a.archive.<YYYYMMDD>.json`, same directory as db.json, not auto-loaded). */
export interface ArchiveFile {
  archivedAt: number; // unix seconds, the moment of archiving
  messages: Message[]; // the archived message bodies
  reads: Record<string, string[]>; // reads entries for archived messages only (moved along with them)
}

interface StoreData {
  messages: Message[];
  reads: Record<string, string[]>; // message_id -> [reader_name,...]
  outbox: OutboxEntry[];
  inboundTasks: InboundTask[];
  outboundTasks: OutboundTask[];
  // shared-memory KV: replicated across peers; maybeArchive only scans messages, unaffected by mem volume
  mem?: Record<string, MemEntry>;
  // per-peer cumulative drop counter (incremented on outbox overflow; for observability, preventing silent loss)
  droppedCount?: Record<string, number>;
}

const KINDS: MsgKind[] = ["message", "request", "result"];

/** Per-peer outbox cap (overflow drops oldest first, preventing unbounded growth from offline peers). */
const MAX_OUTBOX_PER_PEER = 1000;

/** messages archive threshold: archiving triggers when the main db exceeds this count (soft cap). */
const ARCHIVE_THRESHOLD = 5000;
/** Number of oldest messages moved per archive pass. */
const ARCHIVE_BATCH_SIZE = 1000;
/** Active-task timeout window (unix seconds): after it, the associated messages are archived too, preventing leaks from tasks that never reach a terminal state. */
const TASK_STALE_SEC = 7 * 24 * 3600;

/** Store callback hooks: bubble internal store events up to the extension layer (e.g. outbox overflow → toast). */
export interface StoreHooks {
  onOutboxOverflow?: (peer: string, droppedId: string, droppedSubject: string) => void;
}

// A2A TaskState terminal set (spec §4.1.3)
// note: TASK_STATE_STALE is not a spec terminal state but a local extension — set when an active task times out into the archive,
// lets isTerminalState accept it (meaning: finished), preventing quasi-leaks from tasks that never reach a terminal state.
// Do not remove or change this semantics. To align with the spec, raise a separate item rather than handling it implicitly in the archive logic.
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

  /** Load from disk; start empty if the file is missing or corrupt. */
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

  /** Start periodic persistence (prevents loss and write amplification). */
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

  /** Persist (idempotent: skipped when unchanged). Triggers archiving past the threshold (soft cap against unbounded db.json growth). */
  persist(): void {
    if (!this.dirty) return;
    this.dirty = false;
    try {
      fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
      fs.writeFileSync(this.filePath, JSON.stringify(this.data, null, 2));
    } catch {
      /* a failed persist is not fatal: in-memory data remains usable this session */
    }
    // check the archive threshold after persisting the main db (archiving is a newer path; see maybeArchive for the crash-safe order)
    try {
      this.maybeArchive();
    } catch {
      /* a failed archive is not fatal: the main db is saved and the next persist retries */
    }
  }

  // ── archiving (soft cap + archive files + lazy loading) ──

  /** Archive filename = pi-a2a.archive.<YYYYMMDD>.json, same directory as db.json. */
  private archiveFilePath(): string {
    const d = new Date();
    const ymd = `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
    return path.join(path.dirname(this.filePath), `pi-a2a.archive.${ymd}.json`);
  }

  /** Main db messages exceed the threshold → archive the oldest ARCHIVE_BATCH_SIZE to that day's archive file. */
  private maybeArchive(): void {
    if (this.data.messages.length < ARCHIVE_THRESHOLD) return;
    const archivable = this.computeArchivable();
    if (archivable.length === 0) return;

    // ── crash-safe order: write the archive first (persist the data to be archived), then remove from the main db ──
    // worst case: the archive write succeeds but we crash before updating the main db → after restart the messages are in the archive (not lost),
    //   and addMessage dedups any potential duplicates. Never remove from the main db first.
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
    if (!this.appendToArchive(this.archiveFilePath(), archivePayload)) return; // abort if the archive write fails; don't touch the main db

    // archive write succeeded → remove the messages + reads entries from the main db (a move, not a copy)
    const idSet = new Set(archivable);
    this.data.messages = this.data.messages.filter((m) => !idSet.has(m.id));
    for (const id of archivable) delete this.data.reads[id];
    this.dirty = true;
    // persist once more immediately (the pruned main db). The worst crash here leaves the main db stale (still containing archived messages),
    // so no messages are lost (present in both the archive and the main db), and addMessage's idempotent dedup applies.
    try {
      fs.writeFileSync(this.filePath, JSON.stringify(this.data, null, 2));
      this.dirty = false;
    } catch {
      /* stay dirty; the next persist retries */
    }
  }

  /**
   * Compute the ids of archivable messages (oldest created_at first, capped at ARCHIVE_BATCH_SIZE).
   * Not archived: (1) those referenced by the outbox (redelivery needs the body); (2) those tied to an active, unexpired task (task-state integrity).
   * Active tasks past the timeout (>TASK_STALE_SEC) are archived and marked TASK_STATE_STALE (a local extension terminal state, preventing quasi-leaks).
   */
  private computeArchivable(): string[] {
    const now = Math.floor(Date.now() / 1000);
    const result: string[] = [];
    const sorted = [...this.data.messages].sort((a, b) => a.created_at - b.created_at);
    for (const m of sorted) {
      // (1) outbox-referenced messages are not archived (pinned first; no need to enter the task check)
      if (this.data.outbox.some((e) => e.id === m.id)) continue;
      // (2) messages tied to an active, unexpired task are not archived
      const inbound = this.data.inboundTasks.find((t) => t.msgId === m.id);
      if (inbound && !Store.isTerminalState(inbound.state)) {
        if (inbound.createdAt && now - inbound.createdAt <= TASK_STALE_SEC) continue; // still fresh; skip
        inbound.state = "TASK_STATE_STALE"; // expired: archive and mark stale (prevents quasi-leaks)
      }
      // (3) protection for outbound tasks (requests I sent, awaiting a remote push result): symmetric with inbound, using TASK_STALE_SEC.
      // outbound has no local state (it lives remotely); only createdAt freshness is checked.
      // PUSH_BACKSTOP_SEC (30s) is not used — that is a push-channel timeout, not a task lifetime window, so the semantics don't match.
      const outbound = this.data.outboundTasks.find((t) => t.msgId === m.id);
      if (outbound && outbound.createdAt && now - outbound.createdAt <= TASK_STALE_SEC) continue;
      result.push(m.id);
      if (result.length >= ARCHIVE_BATCH_SIZE) break;
    }
    return result;
  }

  /** Append to an archive file (O_APPEND atomicity + fsync crash safety). Returns success. */
  private appendToArchive(file: string, payload: ArchiveFile): boolean {
    try {
      fs.mkdirSync(path.dirname(file), { recursive: true });
      // archiving is a newer path; use openSync('a') + fsyncSync for crash safety (the main db keeps using writeFileSync unchanged)
      const fd = fs.openSync(file, "a");
      try {
        // append one JSON object + newline (each payload on its own line for line-wise parsing later)
        fs.writeSync(fd, JSON.stringify(payload) + "\n");
        fs.fsyncSync(fd); // crash safety: force flush to disk
      } finally {
        fs.closeSync(fd);
      }
      return true;
    } catch {
      return false;
    }
  }

  /** Scan all archive files, returning a list of (ArchiveFile, file path) in reverse date order. */
  private scanArchives(): { file: string; payloads: ArchiveFile[] }[] {
    const dir = path.dirname(this.filePath);
    let names: string[] = [];
    try {
      names = fs.readdirSync(dir).filter((n) => /^pi-a2a\.archive\.\d{8}\.json$/.test(n));
    } catch {
      return [];
    }
    names.sort((a, b) => b.localeCompare(a)); // filenames contain dates; reverse order = newest first
    const result: { file: string; payloads: ArchiveFile[] }[] = [];
    for (const n of names) {
      const fp = path.join(dir, n);
      try {
        const raw = fs.readFileSync(fp, "utf8");
        // archive files append multiple payloads line by line; parse them line-wise
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
        /* one corrupt archive does not block the others */
      }
    }
    return result;
  }

  private touch(): void {
    this.dirty = true;
  }

  // ── messages ────────────────────────────────────────────

  /** Add a message. Ignored if the same id exists (idempotent, used for redelivery dedup). Returns whether it was actually added. */
  addMessage(m: Message): boolean {
    if (this.data.messages.some((x) => x.id === m.id)) return false;
    this.data.messages.push(m);
    this.touch();
    return true;
  }

  getMessage(id: string): Message | undefined {
    return this.data.messages.find((m) => m.id === id);
  }

  /** Resolve any message_id to its thread and return the whole thread (ascending by time). */
  getThread(messageId: string): Message[] {
    const hit = this.getMessage(messageId);
    const tid = hit ? hit.thread_id : messageId;
    return this.data.messages
      .filter((m) => m.thread_id === tid)
      .sort((a, b) => a.created_at - b.created_at);
  }

  // ── deep reads (main-db miss → scan archives; used on low-frequency paths: a2a_read of history) ──
  // performance limit: getInbox must never call deep (hot path, TUI list rendering).

  /** Deep message lookup: when the main db misses, scan all archive files (reverse date order, stop when found). */
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

  /** Deep thread lookup: merge messages with the same thread_id from the main db + archives (reconstructs full history). */
  getThreadDeep(messageId: string): Message[] {
    const all: Message[] = [...this.data.messages];
    for (const { payloads } of this.scanArchives()) {
      for (const p of payloads) all.push(...p.messages);
    }
    const hit = all.find((m) => m.id === messageId);
    const tid = hit ? hit.thread_id : messageId;
    // dedup by thread_id (archives and the main db may overlap, a side effect of the crash-safe order)
    const seen = new Set<string>();
    return all
      .filter((m) => m.thread_id === tid)
      .filter((m) => (seen.has(m.id) ? false : (seen.add(m.id), true)))
      .sort((a, b) => a.created_at - b.created_at);
  }

  /** Deep read-state lookup: on a main-db reads miss, check the archives' reads (correct read state for historical messages). */
  isReadDeep(id: string, reader: string): boolean {
    if (this.isRead(id, reader)) return true;
    for (const { payloads } of this.scanArchives()) {
      for (const p of payloads) {
        if ((p.reads[id] ?? []).includes(reader)) return true;
      }
    }
    return false;
  }

  /** Inbox: direction=inbox, addressed to me or broadcast, not self-sent. Optionally unread-only, limited, reverse time order. */
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

  /** Clear the inbox: delete all inbox-direction messages + their reads entries. Returns the count removed. Archives and sent history untouched. */
  clearInbox(): number {
    const inboxIds = new Set(this.data.messages.filter((m) => m.direction === "inbox").map((m) => m.id));
    if (inboxIds.size === 0) return 0;
    this.data.messages = this.data.messages.filter((m) => !inboxIds.has(m.id));
    for (const id of inboxIds) delete this.data.reads[id];
    this.touch();
    return inboxIds.size;
  }

  // ── shared memory (KV, replicated across peers, LWW + tombstone) ──

  private memMap(): Record<string, MemEntry> {
    return (this.data.mem ??= {});
  }

  /** Read one entry; a tombstone counts as absent. */
  memGet(key: string): MemEntry | undefined {
    const e = this.memMap()[key];
    return e && !e.deleted ? e : undefined;
  }

  /** Full set (including tombstones): used for outbound snapshots; remotes merge by LWW. */
  memGetAll(): { key: string; entry: MemEntry }[] {
    return Object.entries(this.memMap()).map(([key, entry]) => ({ key, entry }));
  }

  /** List of active keys (tombstones excluded). */
  memKeys(): string[] {
    return Object.entries(this.memMap())
      .filter(([, e]) => !e.deleted)
      .map(([k]) => k)
      .sort();
  }

  /** Local write: create a new entry (ts=now) and return it so the caller can broadcast. */
  memSetLocal(key: string, value: string, author: string): MemEntry {
    const entry: MemEntry = { value, ts: Date.now(), author };
    this.memMap()[key] = entry;
    this.touch();
    return entry;
  }

  /** Local delete: write a tombstone and return the entry for broadcasting. */
  memDeleteLocal(key: string, author: string): MemEntry {
    const entry: MemEntry = { value: "", ts: Date.now(), author, deleted: true };
    this.memMap()[key] = entry;
    this.touch();
    return entry;
  }

  /**
   * Merge a remote entry (LWW). Higher ts wins; on equal ts the lexicographically greater author wins.
   * Equal ts+author is treated as an echo (our own broadcast returning) with no change.
   * Returns whether anything changed — the caller uses this to decide whether to persist.
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

  // ── read state (per-reader) ─────────────────────────────

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

  /** Mark every message in a thread as read for a reader. */
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

  // ── outbox (offline queue, redelivered when the peer comes online) ──

  /**
   * Enqueue a pending delivery (deduped by id+to).
   * When a peer's outbox cap is exceeded, drop the oldest FIFO (by queued_at enqueue time),
   * increment droppedCount and fire the onOutboxOverflow callback so messages are not silently lost.
   */
  queueOutbox(id: string, to: string): void {
    if (this.data.outbox.some((e) => e.id === id && e.to === to)) return;
    const mine = this.data.outbox.filter((e) => e.to === to);
    if (mine.length >= MAX_OUTBOX_PER_PEER) {
      // drop this peer's oldest entry (earliest enqueue time); the message body is kept (thread history stays intact)
      const oldest = mine.sort((a, b) => a.queued_at - b.queued_at)[0];
      this.data.outbox = this.data.outbox.filter((e) => !(e.id === oldest.id && e.to === to));
      this.data.droppedCount ??= {};
      this.data.droppedCount[to] = (this.data.droppedCount[to] ?? 0) + 1;
      const subj = this.getMessage(oldest.id)?.subject ?? "(no subject)";
      try {
        this.hooks?.onOutboxOverflow?.(to, oldest.id, subj);
      } catch {
        /* a callback failure does not affect enqueueing */
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

  /** Operational stats: per-peer current outbox backlog + cumulative drops (overflow is observable). */
  getStats(): { outbox: Record<string, number>; dropped: Record<string, number> } {
    const outbox: Record<string, number> = {};
    for (const e of this.data.outbox) outbox[e.to] = (outbox[e.to] ?? 0) + 1;
    return { outbox, dropped: { ...(this.data.droppedCount ?? {}) } };
  }

  // ── A2A: inbound tasks (I am the server) ────────────────

  addInboundTask(t: InboundTask): void {
    const i = this.data.inboundTasks.findIndex((x) => x.taskId === t.taskId);
    if (i >= 0) this.data.inboundTasks[i] = t;
    else this.data.inboundTasks.push(t);
    this.touch();
  }

  getInboundTaskByTaskId(taskId: string): InboundTask | undefined {
    return this.data.inboundTasks.find((t) => t.taskId === taskId);
  }

  /** Inbound tasks still WORKING (can be completed by a reply). */
  getWorkingInboundTaskByMsgId(msgId: string): InboundTask | undefined {
    return this.data.inboundTasks.find((t) => t.msgId === msgId && t.state === "TASK_STATE_WORKING");
  }

  /** Advance an inbound task to a terminal state and attach the result. Returns the updated task. */
  completeInboundTask(taskId: string, state: string, artifactText: string): InboundTask | undefined {
    const t = this.data.inboundTasks.find((x) => x.taskId === taskId);
    if (!t) return undefined;
    t.state = state;
    t.artifactText = artifactText;
    this.touch();
    // refresh the associated inbox message's taskState in sync
    const m = this.getMessage(t.msgId);
    if (m) m.taskState = state;
    return t;
  }

  // ── A2A: outbound tasks (I am the client, awaiting a push) ──

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

  /** All outbound tasks awaiting a push (used for fallback scanning). */
  getPendingOutbound(): OutboundTask[] {
    return [...this.data.outboundTasks];
  }

  // ── static helpers ──────────────────────────────────────

  /** Validate that kind is legal. */
  static validKind(k: string): k is MsgKind {
    return (KINDS as string[]).includes(k);
  }

  /** Whether the state is terminal. */
  static isTerminalState(state: string): boolean {
    return TERMINAL_STATES.has(state);
  }
}

/**
 * pi-a2a — remote workspaces & sessions (cluster edition)
 *
 * Lets another peer provision a working copy on this host and open a real pi
 * session rooted inside it, then send prompts into that session and get the
 * final assistant text back. Nothing is limited to a pre-configured set of
 * projects: a peer may name any path (subject to `allowRemoteWorkspace` and the
 * optional `workspaceRoots` allowlist).
 *
 * Design:
 *   - A worker is a child `pi --mode rpc` process started with `cwd` = the
 *     workspace path, so built-in read/edit/write/bash tools are rooted there.
 *   - The worker gets a stable `--session-id`, so it survives a bridge restart
 *     (the metadata is persisted; a dead worker is respawned on the next prompt).
 *   - The worker runs with PI_A2A_WORKER=1, which makes the pi-a2a extension a
 *     no-op inside it (no duplicate peer, no discovery loops).
 *   - Provisioning shells out to `git` (clone / fetch / checkout / submodules).
 *     If no `git` is given, the directory is simply created.
 *
 * Trust: the shared workspace secret is the only credential. Anyone holding it
 * can create directories, clone repositories and run prompts as this user —
 * see the security notes in the README. Use `allowRemoteWorkspace:false` or
 * `workspaceRoots:[...]` to bound what peers can touch.
 */
import { spawn, execFile, type ChildProcessWithoutNullStreams } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as crypto from "node:crypto";
import type { A2aConfig } from "./config.ts";

const DEFAULT_WORKSPACE_ROOT = path.join(os.homedir(), ".pi", "a2a-workspaces");
const REGISTRY_FILE = path.join(os.homedir(), ".pi", "agent", "pi-a2a-workers.json");
const DEFAULT_MAX_SESSIONS = 8;
const DEFAULT_PROMPT_TIMEOUT_MS = 30 * 60_000;
const GIT_TIMEOUT_MS = 15 * 60_000;
const MAX_QUEUE_DEPTH = 8;
const HANDSHAKE_TIMEOUT_MS = 30_000;

/** Public, non-secret view of a worker session. */
export interface WorkerInfo {
  handle: string;
  path: string;
  name: string;
  sessionId: string;
  alive: boolean;
  createdAt: number;
  lastUsed: number;
  turns: number;
}

/** Operations the network layer exposes to peers over JSON-RPC. */
export interface WorkerOps {
  provision(params: any): Promise<any>;
  open(params: any): Promise<any>;
  prompt(params: any): Promise<any>;
  list(): any;
  close(params: any): Promise<any>;
}

export interface WorkerManagerOptions {
  config: A2aConfig;
  log?: (msg: string) => void;
  registryPath?: string;
}

interface WorkerEntry {
  info: WorkerInfo;
  child: PiRpcChild | null;
  queue: Promise<unknown>;
  queueDepth: number;
}

// ── small helpers ───────────────────────────────────────────

function expandTilde(p: string): string {
  const s = p.trim();
  if (s === "~") return os.homedir();
  if (s.startsWith("~/") || s.startsWith("~\\")) return path.join(os.homedir(), s.slice(2));
  return s;
}

function nowMs(): number {
  return Date.now();
}

function isInside(root: string, target: string): boolean {
  const sep = root.endsWith(path.sep) ? root : root + path.sep;
  return target === root || target.startsWith(sep);
}

// ── child pi RPC client ─────────────────────────────────────

interface Waiter {
  test: () => boolean;
  resolve: () => void;
  reject: (e: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * Minimal JSONL client for a child `pi --mode rpc` process.
 * Only what the worker manager needs: correlated commands, one prompt at a
 * time, and safe auto-answers for extension dialogs (deny/cancel).
 */
class PiRpcChild {
  private buf = "";
  private nextId = 1;
  private pending = new Map<number, { resolve: (v: any) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  private waiters = new Set<Waiter>();
  private exitHandlers: Array<() => void> = [];
  private settledSeq = 0;
  private stderrTail = "";
  public alive = true;
  private exited = false;

  constructor(
    private child: ChildProcessWithoutNullStreams,
    private log?: (msg: string) => void,
  ) {
    try {
      child.stdout.setEncoding("utf8");
      child.stderr.setEncoding("utf8");
    } catch {
      /* ignore */
    }
    child.stdout.on("data", (d: string) => this.onData(d));
    child.stderr.on("data", (d: string) => {
      this.stderrTail = (this.stderrTail + d).slice(-8000);
    });
    child.on("exit", (code, sig) => this.handleExit(code, sig));
    child.on("error", (err) => this.log?.(`worker process error: ${err instanceof Error ? err.message : String(err)}`));
  }

  onExit(handler: () => void): void {
    if (this.exited) handler();
    else this.exitHandlers.push(handler);
  }

  private handleExit(code: number | null, signal: NodeJS.Signals | null): void {
    this.alive = false;
    this.exited = true;
    const err = new Error(
      `pi worker exited (code=${code ?? "null"} signal=${signal ?? "none"})${this.stderrTail ? `: ${this.stderrTail.trim().slice(-500)}` : ""}`,
    );
    for (const p of this.pending.values()) {
      clearTimeout(p.timer);
      p.reject(err);
    }
    this.pending.clear();
    for (const w of this.waiters) {
      clearTimeout(w.timer);
      w.reject(err);
    }
    this.waiters.clear();
    for (const h of this.exitHandlers) h();
  }

  private onData(chunk: string): void {
    this.buf += chunk;
    // guard against a pathological line with no newline (should not happen: stdout is JSONL)
    if (this.buf.length > 16 * 1024 * 1024) this.buf = "";
    let idx: number;
    while ((idx = this.buf.indexOf("\n")) >= 0) {
      let line = this.buf.slice(0, idx);
      this.buf = this.buf.slice(idx + 1);
      if (line.endsWith("\r")) line = line.slice(0, -1);
      if (!line.trim()) continue;
      let rec: any;
      try {
        rec = JSON.parse(line);
      } catch {
        continue;
      }
      this.dispatch(rec);
    }
  }

  private dispatch(rec: any): void {
    if (!rec || typeof rec !== "object") return;
    if (rec.type === "response") {
      const id = typeof rec.id === "number" ? rec.id : Number(rec.id);
      const p = this.pending.get(id);
      if (p) {
        clearTimeout(p.timer);
        this.pending.delete(id);
        p.resolve(rec);
      }
      return;
    }
    if (rec.type === "extension_ui_request") {
      this.answerUi(rec);
      return;
    }
    if (rec.type === "agent_settled") this.settledSeq++;
    this.notifyWaiters();
  }

  /** Answer extension dialogs safely: deny confirmations, cancel other dialogs. */
  private answerUi(rec: any): void {
    const id = rec.id;
    if (rec.method === "confirm") this.write({ type: "extension_ui_response", id, confirmed: false });
    else if (rec.method === "select" || rec.method === "input" || rec.method === "editor") {
      this.write({ type: "extension_ui_response", id, cancelled: true });
    }
    // notify / setStatus / setWidget / setTitle / set_editor_text: fire-and-forget, ignore.
  }

  private write(obj: unknown): void {
    try {
      if (this.alive && this.child.stdin.writable) this.child.stdin.write(JSON.stringify(obj) + "\n");
    } catch {
      /* ignore: exit handler reports the failure to callers */
    }
  }

  private notifyWaiters(): void {
    for (const w of [...this.waiters]) {
      let ok = false;
      try {
        ok = w.test();
      } catch {
        ok = false;
      }
      if (ok) {
        clearTimeout(w.timer);
        this.waiters.delete(w);
        w.resolve();
      }
    }
  }

  private addWaiter(test: () => boolean, timeoutMs: number, label: string): Promise<void> {
    if (!this.alive) return Promise.reject(new Error("pi worker not running"));
    return new Promise<void>((resolve, reject) => {
      const w: Waiter = {
        test,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.waiters.delete(w);
          reject(new Error(`timeout waiting for ${label}`));
        }, timeoutMs),
      };
      this.waiters.add(w);
      this.notifyWaiters();
    });
  }

  /** Send a command and wait for its correlated response; throws on rpc error. */
  call(type: string, extra: Record<string, unknown> = {}, timeoutMs = HANDSHAKE_TIMEOUT_MS): Promise<any> {
    if (!this.alive) return Promise.reject(new Error("pi worker not running"));
    const id = this.nextId++;
    return new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`rpc timeout: ${type}`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.write({ id, type, ...extra });
    }).then((rec: any) => {
      if (!rec.success) throw new Error(`${type} failed: ${rec.error ?? "unknown error"}`);
      return rec.data ?? {};
    });
  }

  /** Run one prompt to completion and return the last assistant text. */
  async prompt(message: string, timeoutMs: number): Promise<string> {
    await this.waitForIdle(Math.min(timeoutMs, 120_000));
    const base = this.settledSeq;
    const accepted = await this.call("prompt", { message }, HANDSHAKE_TIMEOUT_MS);
    const disposition = String(accepted?.disposition ?? "");
    if (disposition === "handled") {
      const r = await this.call("get_last_assistant_text", {}, HANDSHAKE_TIMEOUT_MS).catch(() => ({}));
      return typeof r.text === "string" ? r.text : "(prompt handled by an extension; no assistant text)";
    }
    await this.addWaiter(() => this.settledSeq > base, timeoutMs, "agent_settled");
    const r = await this.call("get_last_assistant_text", {}, HANDSHAKE_TIMEOUT_MS).catch(() => ({}));
    return typeof r.text === "string" ? r.text : "";
  }

  private async waitForIdle(timeoutMs: number): Promise<void> {
    const st = await this.call("get_state", {}, HANDSHAKE_TIMEOUT_MS).catch(() => null);
    if (st && st.isStreaming) {
      const base = this.settledSeq;
      await this.addWaiter(() => this.settledSeq > base, timeoutMs, "idle");
    }
  }

  async dispose(): Promise<void> {
    if (!this.alive) return;
    try {
      this.write({ type: "abort" });
    } catch {
      /* ignore */
    }
    try {
      this.child.stdin.end();
    } catch {
      /* ignore */
    }
    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(killTimer);
        resolve();
      };
      const killTimer = setTimeout(() => {
        try {
          this.child.kill("SIGKILL");
        } catch {
          /* ignore */
        }
        resolve();
      }, 5000);
      this.child.once("exit", done);
      try {
        this.child.kill("SIGTERM");
      } catch {
        done();
      }
    });
  }
}

// ── the manager ─────────────────────────────────────────────

export class WorkerManager implements WorkerOps {
  private workers = new Map<string, WorkerEntry>();
  private readonly registryPath: string;

  constructor(private opts: WorkerManagerOptions) {
    this.registryPath = opts.registryPath ?? REGISTRY_FILE;
    this.loadRegistry();
  }

  private log(msg: string): void {
    this.opts.log?.(msg);
  }

  // ── config helpers ────────────────────────────────────────

  private cfg(): A2aConfig {
    return this.opts.config;
  }

  private assertAllowed(): void {
    if (this.cfg().allowRemoteWorkspace === false) {
      throw new Error("remote workspaces are disabled on this host (allowRemoteWorkspace=false)");
    }
  }

  private workspaceRoot(): string {
    const raw = (this.cfg().workspaceRoot ?? "").trim() || DEFAULT_WORKSPACE_ROOT;
    return path.resolve(expandTilde(raw));
  }

  /** Resolve a peer-supplied path: relative paths live under workspaceRoot; absolute paths are used as-is (subject to workspaceRoots). */
  private resolveWorkspacePath(raw: unknown): string {
    if (typeof raw !== "string" || !raw.trim()) throw new Error("path is required");
    const expanded = expandTilde(raw);
    const abs = path.isAbsolute(expanded) ? path.resolve(expanded) : path.resolve(this.workspaceRoot(), expanded);
    const rootOfFs = path.parse(abs).root;
    if (abs === rootOfFs) throw new Error("refusing to use the filesystem root as a workspace");
    if (abs === path.resolve(os.homedir())) throw new Error("refusing to use the home directory itself as a workspace");
    const roots = this.cfg().workspaceRoots;
    if (Array.isArray(roots) && roots.length > 0) {
      const allowed = roots.some((r) => {
        if (typeof r !== "string" || !r.trim()) return false;
        return isInside(path.resolve(expandTilde(r)), abs);
      });
      if (!allowed) throw new Error("path is outside the configured workspaceRoots allowlist");
    }
    return abs;
  }

  private maxSessions(): number {
    const n = Number(this.cfg().workerMaxSessions);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : DEFAULT_MAX_SESSIONS;
  }

  private promptTimeout(): number {
    const n = Number(this.cfg().workerPromptTimeoutMs);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : DEFAULT_PROMPT_TIMEOUT_MS;
  }

  private liveCount(): number {
    let n = 0;
    for (const e of this.workers.values()) if (e.child?.alive) n++;
    return n;
  }

  // ── persistence ───────────────────────────────────────────

  private loadRegistry(): void {
    try {
      const raw = JSON.parse(fs.readFileSync(this.registryPath, "utf8"));
      const list: any[] = Array.isArray(raw?.workers) ? raw.workers : [];
      for (const w of list) {
        if (!w || typeof w.handle !== "string" || typeof w.path !== "string") continue;
        const info: WorkerInfo = {
          handle: w.handle,
          path: w.path,
          name: String(w.name ?? `remote:${path.basename(w.path)}`),
          sessionId: String(w.sessionId ?? ""),
          alive: false,
          createdAt: Number(w.createdAt) || nowMs(),
          lastUsed: Number(w.lastUsed) || nowMs(),
          turns: Number(w.turns) || 0,
        };
        this.workers.set(info.handle, { info, child: null, queue: Promise.resolve(), queueDepth: 0 });
      }
      if (list.length) this.log(`loaded ${list.length} persisted worker session(s) (dead until prompted)`);
    } catch {
      /* no registry yet */
    }
  }

  private persist(): void {
    try {
      fs.mkdirSync(path.dirname(this.registryPath), { recursive: true });
      const workers = [...this.workers.values()].map((e) => e.info);
      fs.writeFileSync(this.registryPath, JSON.stringify({ workers }, null, 2) + "\n");
    } catch {
      /* persistence is best-effort; in-memory state remains usable */
    }
  }

  // ── process lifecycle ─────────────────────────────────────

  /** Mirror the current pi invocation so workers run the same build/runtime. */
  private piInvocation(): { command: string; args: string[] } {
    const override = (this.cfg().workerPiBin ?? "").trim();
    if (override) return { command: override, args: [] };
    const currentScript = process.argv[1];
    const isBunVirtual = typeof currentScript === "string" && currentScript.startsWith("/$bunfs/root/");
    if (currentScript && !isBunVirtual && fs.existsSync(currentScript)) {
      return { command: process.execPath, args: [currentScript] };
    }
    const execName = path.basename(process.execPath).toLowerCase();
    if (!/^(node|bun)(\.exe)?$/.test(execName)) return { command: process.execPath, args: [] };
    return { command: process.platform === "win32" ? "pi.cmd" : "pi", args: [] };
  }

  private async spawnWorker(entry: WorkerEntry, opts: { model?: unknown; thinking?: unknown }): Promise<void> {
    const inv = this.piInvocation();
    const safeName = entry.info.name.replace(/[\r\n"]/g, " ").trim().slice(0, 80) || "remote";
    const args = ["--mode", "rpc", "--name", safeName];
    if (entry.info.sessionId) args.push("--session-id", entry.info.sessionId);
    if (typeof opts.model === "string" && opts.model.trim()) args.push("--model", opts.model.trim());
    if (typeof opts.thinking === "string" && opts.thinking.trim()) args.push("--thinking", opts.thinking.trim());
    const trust = this.cfg().workerProjectTrust ?? "ignore";
    if (trust === "approve") args.push("--approve");
    else if (trust === "default") {
      /* let pi's own project-trust resolution decide */
    } else args.push("--no-approve");

    const env = { ...process.env, PI_A2A_WORKER: "1" };
    const useShell = process.platform === "win32";
    const child = spawn(inv.command, [...inv.args, ...args], {
      cwd: entry.info.path,
      env,
      stdio: ["pipe", "pipe", "pipe"],
      shell: useShell,
      windowsHide: true,
    }) as ChildProcessWithoutNullStreams;

    const rpc = new PiRpcChild(child, (m) => this.log(`[${entry.info.handle}] ${m}`));
    entry.child = rpc;
    rpc.onExit(() => {
      entry.info.alive = false;
      this.persist();
    });

    const st = await rpc.call("get_state", {}, HANDSHAKE_TIMEOUT_MS);
    if (st?.sessionId) entry.info.sessionId = String(st.sessionId);
    entry.info.alive = true;
    entry.info.lastUsed = nowMs();
    this.persist();
    this.log(`worker up: ${entry.info.handle} (${entry.info.name}) cwd=${entry.info.path} session=${entry.info.sessionId}`);
  }

  private enqueue<T>(entry: WorkerEntry, fn: () => Promise<T>): Promise<T> {
    if (entry.queueDepth >= MAX_QUEUE_DEPTH) {
      return Promise.reject(new Error(`session ${entry.info.handle} is busy (queue full)`));
    }
    entry.queueDepth++;
    const run = entry.queue.then(fn, fn);
    entry.queue = run.then(
      () => undefined,
      () => undefined,
    );
    run.then(
      () => {
        entry.queueDepth = Math.max(0, entry.queueDepth - 1);
      },
      () => {
        entry.queueDepth = Math.max(0, entry.queueDepth - 1);
      },
    );
    return run;
  }

  // ── provisioning ──────────────────────────────────────────

  /** git helper: run a command, reject with stderr on failure. */
  private git(args: string[], cwd: string): Promise<string> {
    return new Promise<string>((resolve, reject) => {
      execFile(
        "git",
        args,
        { cwd, timeout: GIT_TIMEOUT_MS, maxBuffer: 32 * 1024 * 1024, windowsHide: true },
        (err, stdout, stderr) => {
          if (err) {
            const detail = String(stderr || err.message || "").trim();
            reject(new Error(`git ${args.join(" ")} failed: ${detail}`));
          } else {
            resolve(String(stdout));
          }
        },
      );
    });
  }

  private async gitStdout(args: string[], cwd: string): Promise<string> {
    return (await this.git(args, cwd)).trim();
  }

  private async checkoutRef(dest: string, ref: string): Promise<void> {
    try {
      await this.git(["-C", dest, "checkout", ref], dest);
      return;
    } catch {
      /* not a local ref */
    }
    try {
      await this.git(["-C", dest, "checkout", "-B", ref, `origin/${ref}`], dest);
      return;
    } catch {
      /* not a remote branch either */
    }
    await this.git(["-C", dest, "fetch", "origin", ref], dest);
    await this.git(["-C", dest, "checkout", "--detach", "FETCH_HEAD"], dest);
  }

  /**
   * Refresh an existing checkout; used on every open and on provision.
   * Always fetches. With `ref` it checks that ref out; without one it
   * fast-forwards the current branch to its upstream. It never discards local
   * work: a diverged/dirty/detached tree is left as-is and reported via `ff`.
   */
  private async updateCheckout(dest: string, opts: { ref?: string; submodules?: boolean }): Promise<any> {
    await this.git(["-C", dest, "fetch", "--all", "--prune", "--tags"], dest);
    let ff: boolean | null = null;
    if (opts.ref) {
      await this.checkoutRef(dest, opts.ref);
    } else {
      try {
        await this.git(["-C", dest, "merge", "--ff-only", "@{u}"], dest);
        ff = true;
      } catch (e) {
        // no upstream, detached HEAD, diverged, or dirty tree → keep the current tree
        ff = false;
        this.log(`fast-forward skipped for ${dest}: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
    if (opts.submodules) await this.git(["-C", dest, "submodule", "update", "--init", "--recursive"], dest);
    return {
      path: dest,
      action: "updated",
      head: await this.gitStdout(["-C", dest, "rev-parse", "HEAD"], dest),
      branch: await this.gitStdout(["-C", dest, "rev-parse", "--abbrev-ref", "HEAD"], dest).catch(() => ""),
      ff,
    };
  }

  async provision(params: any): Promise<any> {
    this.assertAllowed();
    const dest = this.resolveWorkspacePath(params?.path);
    const gitUrl = typeof params?.git === "string" ? params.git.trim() : "";
    const ref = typeof params?.ref === "string" ? params.ref.trim() : "";
    const submodules = params?.submodules === true;
    const depth = Number.isFinite(params?.depth) ? Math.max(1, Math.floor(Number(params.depth))) : 0;

    if (!gitUrl) {
      fs.mkdirSync(dest, { recursive: true });
      return { path: dest, action: "created", head: null, branch: null };
    }

    if (fs.existsSync(path.join(dest, ".git"))) {
      return this.updateCheckout(dest, { ref, submodules });
    }

    if (fs.existsSync(dest) && fs.readdirSync(dest).length > 0) {
      throw new Error(`path exists and is not a git repository: ${dest} (refusing to overwrite)`);
    }
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    const cloneArgs = ["clone"];
    if (depth) cloneArgs.push("--depth", String(depth));
    cloneArgs.push(gitUrl, dest);
    await this.git(cloneArgs, path.dirname(dest));
    if (ref) await this.checkoutRef(dest, ref);
    if (submodules) await this.git(["-C", dest, "submodule", "update", "--init", "--recursive"], dest);
    return {
      path: dest,
      action: "cloned",
      head: await this.gitStdout(["-C", dest, "rev-parse", "HEAD"], dest),
      branch: await this.gitStdout(["-C", dest, "rev-parse", "--abbrev-ref", "HEAD"], dest).catch(() => ""),
    };
  }

  // ── sessions ──────────────────────────────────────────────

  async open(params: any): Promise<any> {
    this.assertAllowed();
    const dest = this.resolveWorkspacePath(params?.path);
    let provisionResult: any = null;
    let refreshError: string | null = null;
    const gitUrl = typeof params?.git === "string" ? params.git.trim() : "";
    if (gitUrl) {
      provisionResult = await this.provision({
        path: dest,
        git: gitUrl,
        ref: params?.ref,
        submodules: params?.submodules,
        depth: params?.depth,
      });
    } else if (fs.existsSync(path.join(dest, ".git"))) {
      // refresh an existing checkout on every open, even when no git URL is given,
      // so a session never works on a stale tree. Best-effort: if the fetch fails
      // (e.g. offline) the session still opens; the caller sees refreshError.
      try {
        provisionResult = await this.updateCheckout(dest, {
          ref: typeof params?.ref === "string" ? params.ref.trim() : "",
          submodules: params?.submodules === true,
        });
      } catch (e) {
        refreshError = e instanceof Error ? e.message : String(e);
        this.log(`refresh failed for ${dest}: ${refreshError}`);
      }
    }
    fs.mkdirSync(dest, { recursive: true });

    // idempotent by path: reuse a live worker, or respawn a persisted dead one
    // (keeps the same handle + session id, so callers keep their session across restarts)
    const existing = [...this.workers.values()].find((e) => e.info.path === dest);
    if (existing) {
      if (!existing.child?.alive) {
        if (this.liveCount() >= this.maxSessions()) {
          throw new Error(`worker session limit reached (${this.maxSessions()}) on this host`);
        }
        await this.spawnWorker(existing, { model: params?.model, thinking: params?.thinking });
      }
      existing.info.lastUsed = nowMs();
      this.persist();
      const info: WorkerInfo = { ...existing.info, alive: true };
      if (typeof params?.prompt === "string" && params.prompt.trim()) {
        const r = await this.prompt({ handle: info.handle, message: params.prompt, timeoutMs: params.timeoutMs });
        return {
          ...info,
          turns: r.turns,
          reused: true,
          provision: provisionResult,
          refreshError,
          reply: r.reply,
          elapsedMs: r.elapsedMs,
        };
      }
      return { ...info, reused: true, provision: provisionResult, refreshError };
    }

    if (this.liveCount() >= this.maxSessions()) {
      throw new Error(`worker session limit reached (${this.maxSessions()}) on this host`);
    }

    const handle = "sess_" + crypto.randomBytes(4).toString("hex");
    const sessionId =
      typeof params?.sessionId === "string" && params.sessionId.trim() ? params.sessionId.trim() : crypto.randomUUID();
    const name =
      typeof params?.name === "string" && params.name.trim()
        ? params.name.trim()
        : `remote:${path.basename(dest) || "workspace"}`;
    const info: WorkerInfo = {
      handle,
      path: dest,
      name,
      sessionId,
      alive: false,
      createdAt: nowMs(),
      lastUsed: nowMs(),
      turns: 0,
    };
    const entry: WorkerEntry = { info, child: null, queue: Promise.resolve(), queueDepth: 0 };
    this.workers.set(handle, entry);
    try {
      await this.spawnWorker(entry, { model: params?.model, thinking: params?.thinking });
    } catch (e) {
      this.workers.delete(handle);
      this.persist();
      throw e;
    }

    const result: any = { ...info, alive: true, reused: false, provision: provisionResult, refreshError };
    if (typeof params?.prompt === "string" && params.prompt.trim()) {
      const r = await this.prompt({ handle, message: params.prompt, timeoutMs: params.timeoutMs });
      result.turns = r.turns;
      result.reply = r.reply;
      result.elapsedMs = r.elapsedMs;
    }
    return result;
  }

  async prompt(params: any): Promise<any> {
    this.assertAllowed();
    const handle = String(params?.handle ?? "");
    const message = typeof params?.message === "string" ? params.message : "";
    if (!message.trim()) throw new Error("message is required");
    const entry = this.workers.get(handle);
    if (!entry) throw new Error(`unknown session handle: ${handle}`);
    const timeoutMs =
      Number.isFinite(params?.timeoutMs) && Number(params.timeoutMs) > 0
        ? Math.floor(Number(params.timeoutMs))
        : this.promptTimeout();

    return this.enqueue(entry, async () => {
      if (!entry.child?.alive || !entry.info.alive) {
        this.log(`worker ${handle} not alive; respawning session ${entry.info.sessionId} at ${entry.info.path}`);
        await this.spawnWorker(entry, {});
      }
      const started = nowMs();
      const reply = await entry.child!.prompt(message, timeoutMs);
      entry.info.lastUsed = nowMs();
      entry.info.turns += 1;
      this.persist();
      return {
        handle,
        path: entry.info.path,
        name: entry.info.name,
        sessionId: entry.info.sessionId,
        reply,
        elapsedMs: nowMs() - started,
        turns: entry.info.turns,
      };
    });
  }

  list(): any {
    const sessions = [...this.workers.values()].map((e) => ({ ...e.info, alive: !!e.child?.alive }));
    return { sessions, maxSessions: this.maxSessions(), allowRemoteWorkspace: this.cfg().allowRemoteWorkspace !== false };
  }

  async close(params: any): Promise<any> {
    const handle = String(params?.handle ?? "");
    const entry = this.workers.get(handle);
    if (!entry) throw new Error(`unknown session handle: ${handle}`);
    await entry.child?.dispose();
    const info = { ...entry.info, alive: false };
    this.workers.delete(handle);
    this.persist();
    this.log(`worker closed: ${handle} (${info.path})`);
    return { handle, closed: true, path: info.path };
  }

  async shutdown(): Promise<void> {
    const entries = [...this.workers.values()];
    await Promise.all(entries.map((e) => e.child?.dispose().catch(() => undefined)));
    for (const e of entries) e.info.alive = false;
    this.persist();
  }
}

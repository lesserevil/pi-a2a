/**
 * pi-a2a — file transfer (peer-to-peer blob transfer, bypassing LLM context)
 *
 * Design:
 *   - all file reads/writes are confined to a sandbox root (default ~/.pi/a2a-files);
 *     paths are resolved then verified to still be inside the root, preventing ../ escapes.
 *   - transfer uses two new endpoints on the existing A2A HTTP server:
 *       POST /file   receive a file (body = raw bytes, metadata in the query)
 *       GET  /file   send a file (?path=<sandbox-relative path>, response body = raw bytes)
 *   - integrity: sha256 travels with the file; the receiver recomputes and compares after writing.
 *   - limits: per-transfer cap is configurable (default 64 MiB) to avoid exhausting memory/disk.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as crypto from "node:crypto";

export const DEFAULT_FILE_ROOT = path.join(os.homedir(), ".pi", "a2a-files");
export const DEFAULT_FILE_MAX_BYTES = 64 * 1024 * 1024; // 64 MiB

/** Expand ~ and return an absolute path. */
export function resolveFileRoot(configured?: string): string {
  const raw = (configured ?? "").trim() || DEFAULT_FILE_ROOT;
  const expanded = raw.startsWith("~") ? path.join(os.homedir(), raw.slice(1)) : raw;
  return path.resolve(expanded);
}

/**
 * Resolve a sandbox-relative path to an absolute path inside the root.
 * Rejects absolute paths and any relative path escaping the root.
 */
export function sandboxPath(root: string, rel: string): string {
  if (!rel || typeof rel !== "string") throw new Error("path required");
  // normalise: strip the leading slash, unify separators
  const cleaned = rel.replace(/\\/g, "/").replace(/^\/+/, "");
  if (cleaned.split("/").includes("..")) throw new Error("path escapes file root");
  const abs = path.resolve(root, cleaned);
  const rootWithSep = root.endsWith(path.sep) ? root : root + path.sep;
  if (abs !== root && !abs.startsWith(rootWithSep)) {
    throw new Error("path escapes file root");
  }
  return abs;
}

/** Absolute path -> path relative to the sandbox root (for display in receipts). */
export function relativeToRoot(root: string, abs: string): string {
  const rel = path.relative(root, abs);
  return rel.split(path.sep).join("/");
}

export function sha256File(abs: string): string {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(abs));
  return hash.digest("hex");
}

export function sha256Buffer(buf: Buffer): string {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

/** Receive a file: write to disk and verify sha256; returns write info. */
export interface ReceivedFile {
  abs: string;
  rel: string;
  bytes: number;
  sha256: string;
}

export function writeReceivedFile(
  root: string,
  rel: string,
  data: Buffer,
  expectedSha256: string | undefined,
  overwrite: boolean,
): ReceivedFile {
  const abs = sandboxPath(root, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  if (!overwrite && fs.existsSync(abs)) {
    throw new Error(`destination exists (set overwrite=true): ${rel}`);
  }
  const actual = sha256Buffer(data);
  if (expectedSha256 && expectedSha256.toLowerCase() !== actual) {
    throw new Error(`sha256 mismatch: expected ${expectedSha256}, got ${actual}`);
  }
  fs.writeFileSync(abs, data);
  return { abs, rel: relativeToRoot(root, abs), bytes: data.length, sha256: actual };
}

/** Read a file to send: verify it exists in the sandbox, return bytes + metadata. */
export interface OutgoingFile {
  data: Buffer;
  rel: string;
  bytes: number;
  sha256: string;
}

export function readOutgoingFile(root: string, rel: string): OutgoingFile {
  const abs = sandboxPath(root, rel);
  if (!fs.statSync(abs).isFile()) throw new Error(`not a file: ${rel}`);
  const data = fs.readFileSync(abs);
  return { data, rel: relativeToRoot(root, abs), bytes: data.length, sha256: sha256Buffer(data) };
}

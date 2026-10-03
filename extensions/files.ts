/**
 * pi-a2a — 文件传输（peer-to-peer blob transfer，绕过 LLM 上下文）
 *
 * 设计：
 *   - 所有文件读写都限制在一个沙箱根目录（默认 ~/.pi/a2a-files）下，
 *     路径先 resolve 再校验仍在根目录内，杜绝 ../ 逃逸。
 *   - 传输走现有 A2A HTTP server 的两个新端点：
 *       POST /file   收文件（body = 原始字节，元数据在 query）
 *       GET  /file   发文件（?path=<沙箱相对路径>，响应体 = 原始字节）
 *   - 完整性：随文件带 sha256，接收端落盘后重新计算并比对。
 *   - 计数：单次传输上限可配（默认 64 MiB），避免打爆内存/磁盘。
 */
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as crypto from "node:crypto";

export const DEFAULT_FILE_ROOT = path.join(os.homedir(), ".pi", "a2a-files");
export const DEFAULT_FILE_MAX_BYTES = 64 * 1024 * 1024; // 64 MiB

/** 展开 ~ 并取绝对路径。 */
export function resolveFileRoot(configured?: string): string {
  const raw = (configured ?? "").trim() || DEFAULT_FILE_ROOT;
  const expanded = raw.startsWith("~") ? path.join(os.homedir(), raw.slice(1)) : raw;
  return path.resolve(expanded);
}

/**
 * 把一个「沙箱相对路径」解析为根目录内的绝对路径。
 * 拒绝绝对路径与任何逃逸出根的相对路径。
 */
export function sandboxPath(root: string, rel: string): string {
  if (!rel || typeof rel !== "string") throw new Error("path required");
  // 归一化：去掉前导斜杠，统一分隔符
  const cleaned = rel.replace(/\\/g, "/").replace(/^\/+/, "");
  if (cleaned.split("/").includes("..")) throw new Error("path escapes file root");
  const abs = path.resolve(root, cleaned);
  const rootWithSep = root.endsWith(path.sep) ? root : root + path.sep;
  if (abs !== root && !abs.startsWith(rootWithSep)) {
    throw new Error("path escapes file root");
  }
  return abs;
}

/** 绝对路径 → 相对沙箱根的路径（用于回执中展示）。 */
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

/** 接收文件：写盘并校验 sha256；返回落盘信息。 */
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

/** 读取待发送文件：校验存在于沙箱内，返回字节 + 元数据。 */
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

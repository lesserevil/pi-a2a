/**
 * pi-a2a — 配置读写（A2A / 局域网 P2P 版）
 *
 * 配置形状:
 *   {
 *     "workspace": "my-team",            // 工作区名，决定 mDNS ws 隔离
 *     "workspaceSecret": "shared-pass",   // 共享密钥（A2A Bearer 互验 + ws 校验）
 *     "agentId": "<auto>",               // 首次自动生成并持久化
 *     "peerName": "backend",
 *     "role": "writes the API",
 *     "listenPort": 0,                   // 可选，0 = OS 分配
 *     "advertiseHost": "192.168.1.50",   // 可选，手动指定对外广告 host（多网卡/Docker/WSL 自动检测错误时用）
 *     // ── A2A 端点/时序（均可选，有默认）─────────────
 *     "agentCardPath": "/.well-known/agent-card.json",
 *     "rpcPath": "/rpc",                  // JSON-RPC 单端点
 *     "notifyPath": "/a2a/notify",         // push-notification webhook 接收
 *     "pushSweepMs": 15000,               // push 兜底扫描间隔
 *     "pushBackstopMs": 30000,            // 超过此时长未收 push 则主动 GetTask
 *     "autoInjectMessage": false         // 普通消息是否也自动注入会话（默认 false=只通知）
 *   }
 *
 * 配置优先级：项目级 (.pi/pi-a2a.json) > 全局 (~/.pi/agent/pi-a2a.json)
 */
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as crypto from "node:crypto";

export interface A2aConfig {
  workspace: string; // 工作区名，决定 mDNS ws 隔离
  workspaceSecret: string; // 共享密钥（A2A Bearer 鉴权 + ws 校验）
  agentId: string; // 首次自动生成并持久化
  peerName: string;
  role?: string;
  listenPort?: number; // 可选，0 = OS 分配（默认 0）
  advertiseHost?: string; // 可选，手动指定对外广告 host（agent card / push webhook URL 用）；多网卡/Docker/WSL 环境自动检测到虚拟网卡时用此项覆盖
  // ── A2A 端点/时序（均可选）──────────────────────────
  agentCardPath?: string; // 默认 /.well-known/agent-card.json
  rpcPath?: string; // 默认 /rpc（JSON-RPC 单端点）
  notifyPath?: string; // 默认 /a2a/notify（push webhook 接收）
  pushSweepMs?: number; // 默认 15000
  pushBackstopMs?: number; // 默认 30000
  autoInjectMessage?: boolean; // 默认 false：普通 message 只通知；true 时也注入会话
}

// 占位符：表示 agentId 尚未真正生成（如 config.example.json 里的空串）
const AGENTID_PLACEHOLDERS = new Set(["", "auto-generated-on-first-use", "auto-generated"]);

/**
 * 配置/数据库都绑定到项目级 .pi/ 目录。
 * 设计：不做全局 fallback。这样不同目录各自有独立配置（独立 agentId），
 * 不同项目即使 peerName 重名（都叫 FE）也能通过 workspace + secret 经全局
 * presence 目录（~/.pi/agent/pi-a2a-presence/）互相发现并通信，
 * 而不会因共用全局配置导致 agentId 混淆/双实例。
 */
export function configPath(cwd: string): string {
  return path.join(cwd, ".pi", "pi-a2a.json");
}

/** 本地数据库（JSON 文件）路径：与 config 同目录。 */
export function dbPathFor(cwd: string): string {
  return path.join(cwd, ".pi", "pi-a2a.db.json");
}

/** 展开 $VAR / ${VAR} 形式的环境变量引用。 */
export function expandEnv(s: string): string {
  return s.replace(/\$\{?([A-Z_][A-Z0-9_]*)\}?/g, (_, v) => process.env[v] ?? `$${v}`);
}

/** 生成稳定的 agentId（16 位 hex）。 */
export function genAgentId(): string {
  return crypto
    .createHash("sha256")
    .update(
      `${os.hostname()}:${process.pid}:${Date.now()}:${crypto.randomBytes(8).toString("hex")}`,
    )
    .digest("hex")
    .slice(0, 16);
}

export interface LoadedConfig {
  config: A2aConfig;
  path: string;
}

/** 读取项目级配置（<cwd>/.pi/pi-a2a.json）。首次使用时自动补全 agentId 并回写。 */
export function loadConfig(cwd: string): LoadedConfig | null {
  const p = configPath(cwd);
  if (!fs.existsSync(p)) return null;
  try {
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    // 新形状校验：必须有 workspace + workspaceSecret + peerName
    if (raw && raw.workspace && raw.workspaceSecret && raw.peerName) {
      raw.workspace = expandEnv(String(raw.workspace));
      raw.workspaceSecret = expandEnv(String(raw.workspaceSecret));
      if (!raw.agentId || AGENTID_PLACEHOLDERS.has(raw.agentId)) {
        raw.agentId = genAgentId();
        try {
          fs.writeFileSync(p, JSON.stringify(raw, null, 2) + "\n");
        } catch {
          /* 写失败则用内存 id，本会话仍可用 */
        }
      }
      return { config: raw as A2aConfig, path: p };
    }
  } catch {
    /* 忽略损坏文件 */
  }
  return null;
}

/** 保存配置到项目级 .pi/ 目录。返回文件路径。 */
export function saveConfig(cwd: string, config: A2aConfig): string {
  const dir = path.join(cwd, ".pi");
  fs.mkdirSync(dir, { recursive: true });
  const fp = path.join(dir, "pi-a2a.json");
  fs.writeFileSync(fp, JSON.stringify(config, null, 2) + "\n");
  return fp;
}

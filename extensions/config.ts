/**
 * pi-a2a — configuration read/write (A2A / LAN P2P edition)
 *
 * Config shape:
 *   {
 *     "workspace": "my-team",            // workspace name; determines mDNS ws isolation
 *     "workspaceSecret": "shared-pass",   // shared secret (A2A Bearer auth + ws check)
 *     "agentId": "<auto>",               // generated and persisted automatically on first use
 *     "peerName": "backend",
 *     "role": "writes the API",
 *     "listenPort": 0,                   // optional; 0 = OS-assigned
 *     "advertiseHost": "192.168.1.50",   // optional; manually set the advertised host (use when auto-detection picks the wrong NIC under multi-NIC/Docker/WSL)
 *     "mdnsInterface": "192.168.1.50",    // optional; bind mDNS multicast to a specific NIC (when the multi-NIC default route is wrong; do NOT set on hosts with multiple addresses on the same subnet)
 *     // ── A2A endpoints / timings (all optional, defaults apply) ──
 *     "agentCardPath": "/.well-known/agent-card.json",
 *     "rpcPath": "/rpc",                  // JSON-RPC single endpoint
 *     "notifyPath": "/a2a/notify",         // push-notification webhook receiver
 *     "pushSweepMs": 15000,               // push fallback sweep interval
 *     "pushBackstopMs": 30000,            // if no push arrives within this window, actively call GetTask
 *     "autoInjectMessage": false         // whether plain messages are also auto-injected (default false = notify only)
 *     "fileRoot": "~/.pi/a2a-files",    // optional; sandbox root for file transfer (a2a_put/get can only read/write under it)
 *     "fileMaxBytes": 67108864,           // optional; per-transfer size limit (default 64 MiB)
 *     // ── remote workspaces / sessions (cluster) ──
 *     "allowRemoteWorkspace": true,       // default true; false rejects ProvisionWorkspace/OpenSession/PromptSession from peers
 *     "workspaceRoot": "~/.pi/a2a-workspaces", // default; base dir for relative workspace paths
 *     "workspaceRoots": ["~/src", "/opt/work"], // optional allowlist; when set, workspace paths must be inside one of these roots
 *     "workerProjectTrust": "ignore",     // default "ignore" (--no-approve); "approve" (--approve) or "default"
 *     "workerPiBin": "pi",                // optional override for the pi executable used to spawn worker sessions
 *     "workerMaxSessions": 8,             // default; max concurrent worker sessions on this host
 *     "workerPromptTimeoutMs": 1800000    // default 30 min; max time a remote prompt may run in one worker
 *   }
 *
 * Config precedence: project-level (.pi/pi-a2a.json) > global (~/.pi/agent/pi-a2a.json)
 */
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as crypto from "node:crypto";

export interface A2aConfig {
  workspace: string; // workspace name; determines mDNS ws isolation
  workspaceSecret: string; // shared secret (A2A Bearer auth + ws check)
  agentId: string; // generated and persisted automatically on first use
  peerName: string;
  role?: string;
  listenPort?: number; // optional; 0 = OS-assigned (default 0)
  advertiseHost?: string; // optional; manually set the advertised host (used in the agent card / push webhook URL); override when auto-detection finds a virtual NIC under multi-NIC/Docker/WSL
  mdnsInterface?: string; // optional; bind the mDNS/multicast socket to this NIC address. Only set when the host's default multicast egress uses the wrong NIC (e.g. Windows + Tailscale). Do NOT set on multi-NIC hosts with several addresses on the same LAN, or peer announcements will not be received.
  // ── A2A endpoints / timings (all optional) ──────────
  agentCardPath?: string; // default /.well-known/agent-card.json
  rpcPath?: string; // default /rpc (JSON-RPC single endpoint)
  notifyPath?: string; // default /a2a/notify (push webhook receiver)
  pushSweepMs?: number; // default 15000
  pushBackstopMs?: number; // default 30000
  autoInjectMessage?: boolean; // default false: plain messages notify only; when true they are also injected into the session
  fileRoot?: string; // optional; sandbox root for file transfer (default ~/.pi/a2a-files); a2a_put/get can only read/write under it
  fileMaxBytes?: number; // optional; per-transfer size limit (default 64 MiB)
  // ── remote workspaces / sessions (cluster) ──────────
  allowRemoteWorkspace?: boolean; // default true: peers may provision working copies and open/prompt sessions on this host. Set false to refuse all remote workspace/session operations.
  workspaceRoot?: string; // default ~/.pi/a2a-workspaces; base directory for relative workspace paths requested by peers
  workspaceRoots?: string[]; // optional allowlist of roots; when set, every remote workspace path must resolve inside one of them
  workerProjectTrust?: "approve" | "ignore" | "default"; // default "ignore": spawn workers with --no-approve (AGENTS.md context still loads). "approve" trusts project-local resources; "default" lets pi decide.
  workerPiBin?: string; // optional; explicit pi executable for worker sessions (defaults to the current pi invocation)
  workerMaxSessions?: number; // default 8; maximum concurrent remote worker sessions on this host
  workerPromptTimeoutMs?: number; // default 1800000 (30 min); maximum wall time for a single remote prompt
}

// Placeholder: indicates agentId has not really been generated yet (e.g. the empty string in config.example.json)
const AGENTID_PLACEHOLDERS = new Set(["", "auto-generated-on-first-use", "auto-generated"]);

/**
 * Both config and database are scoped to the project-level .pi/ directory.
 * Design: no global fallback. This way each directory keeps its own config (its own agentId),
 * so even if different projects reuse a peerName (both called FE) they can still find each other via workspace + secret through the global
 * presence directory (~/.pi/agent/pi-a2a-presence/) and communicate,
 * without a shared global config causing agentId confusion / double instances.
 */
export function configPath(cwd: string): string {
  return path.join(cwd, ".pi", "pi-a2a.json");
}

/** Local database (JSON file) path: same directory as config. */
export function dbPathFor(cwd: string): string {
  return path.join(cwd, ".pi", "pi-a2a.db.json");
}

/** Expand $VAR / ${VAR} environment variable references. */
export function expandEnv(s: string): string {
  return s.replace(/\$\{?([A-Z_][A-Z0-9_]*)\}?/g, (_, v) => process.env[v] ?? `$${v}`);
}

/** Generate a stable agentId (16 hex chars). */
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

/** Read the project-level config (<cwd>/.pi/pi-a2a.json). Fills in and writes back agentId on first use. */
export function loadConfig(cwd: string): LoadedConfig | null {
  const p = configPath(cwd);
  if (!fs.existsSync(p)) return null;
  try {
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    // new-shape validation: requires workspace + workspaceSecret + peerName
    if (raw && raw.workspace && raw.workspaceSecret && raw.peerName) {
      raw.workspace = expandEnv(String(raw.workspace));
      raw.workspaceSecret = expandEnv(String(raw.workspaceSecret));
      if (!raw.agentId || AGENTID_PLACEHOLDERS.has(raw.agentId)) {
        raw.agentId = genAgentId();
        try {
          fs.writeFileSync(p, JSON.stringify(raw, null, 2) + "\n");
        } catch {
          /* if the write fails, fall back to an in-memory id; still usable this session */
        }
      }
      return { config: raw as A2aConfig, path: p };
    }
  } catch {
    /* ignore a corrupt file */
  }
  return null;
}

/** Save config to the project-level .pi/ directory. Returns the file path. */
export function saveConfig(cwd: string, config: A2aConfig): string {
  const dir = path.join(cwd, ".pi");
  fs.mkdirSync(dir, { recursive: true });
  const fp = path.join(dir, "pi-a2a.json");
  fs.writeFileSync(fp, JSON.stringify(config, null, 2) + "\n");
  return fp;
}

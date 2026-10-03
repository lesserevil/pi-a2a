/**
 * test-remote-sessions.ts — network round-trip test for the cluster RPC methods.
 *
 * Starts a real pi-a2a Network (HTTP + WorkerManager) and drives it through the
 * client methods exactly as a peer would over the LAN:
 *   provisionWorkspace → openSession → listSessions → promptSession → closeSession
 *
 * Run:  node --experimental-transform-types scripts/test-remote-sessions.ts
 *       PI_A2A_TEST_PROMPT=1 node --experimental-transform-types scripts/test-remote-sessions.ts
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import { Store } from "../extensions/store.ts";
import { Network, type Peer } from "../extensions/net.ts";
import { WorkerManager } from "../extensions/workers.ts";
import type { A2aConfig } from "../extensions/config.ts";

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "pi-a2a-net-"));
const srcRepo = path.join(tmp, "src-repo");
let failures = 0;

function check(name: string, cond: boolean, detail = ""): void {
  if (cond) console.log(`  ✓ ${name}`);
  else {
    failures++;
    console.log(`  ✗ ${name}${detail ? ` — ${detail}` : ""}`);
  }
}

function git(cwd: string, args: string[]): void {
  execFileSync("git", args, {
    cwd,
    stdio: "pipe",
    env: { ...process.env, GIT_AUTHOR_NAME: "t", GIT_AUTHOR_EMAIL: "t@t", GIT_COMMITTER_NAME: "t", GIT_COMMITTER_EMAIL: "t@t" },
  });
}

async function main(): Promise<void> {
  fs.mkdirSync(srcRepo, { recursive: true });
  fs.writeFileSync(path.join(srcRepo, "README.md"), "# hi\n");
  git(srcRepo, ["init", "-q", "-b", "main"]);
  git(srcRepo, ["-c", "user.name=t", "-c", "user.email=t@t", "add", "-A"]);
  git(srcRepo, ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "init"]);

  const config: A2aConfig = {
    workspace: "net-test",
    workspaceSecret: "secret",
    agentId: "aaaaaaaaaaaaaaaa",
    peerName: "srv",
    workspaceRoot: tmp,
    allowRemoteWorkspace: true,
    workerPiBin: process.env.PI_A2A_PI_BIN || "pi",
    workerProjectTrust: "ignore",
    listenPort: 0,
  };

  const store = new Store(path.join(tmp, "db.json"));
  store.load();
  const workers = new WorkerManager({ config, registryPath: path.join(tmp, "workers.json") });
  const net = new Network(config, {
    store,
    workers,
    onMessageReceived: () => undefined,
    onPeersChanged: () => undefined,
    log: (m) => console.log(`    [net] ${m}`),
  });

  let handle = "";
  try {
    await net.start();
    const port = net.getListenPort();
    console.log(`server listening on 127.0.0.1:${port}`);

    // inject a synthetic peer that points back at this server (as a LAN peer would)
    const peer: Peer = {
      peerName: "peerA",
      agentId: "bbbbbbbbbbbbbbbb",
      role: "test caller",
      host: "127.0.0.1",
      port,
      lastSeen: Date.now(),
      caps: "sessions",
    };
    (net as any).peers.set("peerA", peer);

    console.log("\n1. provisionWorkspace over JSON-RPC");
    const p = await net.provisionWorkspace("peerA", { path: "ws-a", git: srcRepo });
    check("cloned", p.action === "cloned", JSON.stringify(p));
    check("file present", fs.existsSync(path.join(tmp, "ws-a", "README.md")));

    console.log("\n2. openSession over JSON-RPC");
    const o = await net.openSession("peerA", { path: "ws-a", name: "net-session" });
    handle = String(o.handle);
    check("handle", /^sess_[0-9a-f]{8}$/.test(handle), handle);
    check("alive", o.alive === true);
    check("sessionId", typeof o.sessionId === "string" && o.sessionId.length > 0);

    console.log("\n3. listSessions over JSON-RPC");
    const l = await net.listSessions("peerA");
    check("listed", Array.isArray(l.sessions) && l.sessions.length === 1, JSON.stringify(l));

    if (process.env.PI_A2A_TEST_PROMPT === "1") {
      console.log("\n4. promptSession over JSON-RPC");
      const r = await net.promptSession("peerA", { handle, message: "Reply with exactly: PONG" }, 240_000);
      check("reply received", typeof r.reply === "string" && r.reply.trim().length > 0, JSON.stringify(r.reply));
      console.log(`    reply: ${JSON.stringify(r.reply).slice(0, 120)}`);
    } else {
      console.log("\n4. promptSession skipped (set PI_A2A_TEST_PROMPT=1)");
    }

    console.log("\n5. closeSession over JSON-RPC");
    const c = await net.closeSession("peerA", { handle });
    check("closed", c.closed === true);
    handle = "";

    console.log("\n6. unknown peer is rejected");
    let threw = false;
    try {
      await net.listSessions("nope");
    } catch {
      threw = true;
    }
    check("throws for unknown peer", threw);
  } finally {
    if (handle) await net.closeSession("peerA", { handle }).catch(() => undefined);
    await workers.shutdown();
    await net.stop();
    fs.rmSync(tmp, { recursive: true, force: true });
  }

  console.log(failures === 0 ? "\n✅ all remote-session checks passed" : `\n❌ ${failures} check(s) failed`);
  process.exitCode = failures === 0 ? 0 : 1;
}

main().catch((e) => {
  console.error("test-remote-sessions crashed:", e);
  try {
    fs.rmSync(tmp, { recursive: true, force: true });
  } catch {
    /* ignore */
  }
  process.exitCode = 1;
});

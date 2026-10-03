/**
 * test-workers.ts — focused test for the remote workspace/session machinery.
 *
 * Verifies, without any network:
 *   1. provision(): clone a local git repo into a workspace, then update it
 *   2. open(): spawn a real `pi --mode rpc` worker rooted in the workspace
 *   3. list() / prompt() (prompt only with PI_A2A_TEST_PROMPT=1) / close()
 *
 * Run:  node --experimental-transform-types scripts/test-workers.ts
 *       PI_A2A_TEST_PROMPT=1 node --experimental-transform-types scripts/test-workers.ts
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import { WorkerManager } from "../extensions/workers.ts";
import type { A2aConfig } from "../extensions/config.ts";

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "pi-a2a-workers-"));
const srcRepo = path.join(tmp, "src-repo");
let failures = 0;

function check(name: string, cond: boolean, detail = ""): void {
  if (cond) {
    console.log(`  ✓ ${name}`);
  } else {
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

function setupRepo(): void {
  fs.mkdirSync(srcRepo, { recursive: true });
  fs.writeFileSync(path.join(srcRepo, "README.md"), "# hello\n");
  git(srcRepo, ["init", "-q", "-b", "main"]);
  git(srcRepo, ["-c", "user.name=t", "-c", "user.email=t@t", "add", "-A"]);
  git(srcRepo, ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "init"]);
}

async function main(): Promise<void> {
  console.log(`tmp: ${tmp}`);
  setupRepo();

  const config: A2aConfig = {
    workspace: "test",
    workspaceSecret: "secret",
    agentId: "0000000000000000",
    peerName: "test",
    workspaceRoot: tmp,
    allowRemoteWorkspace: true,
    workerPiBin: process.env.PI_A2A_PI_BIN || "pi",
    workerProjectTrust: "ignore",
    workerMaxSessions: 3,
  };

  const mgr = new WorkerManager({
    config,
    registryPath: path.join(tmp, "workers.json"),
    log: (m) => console.log(`    [mgr] ${m}`),
  });

  let handle = "";
  let mgr2: WorkerManager | null = null;
  const regPath = path.join(tmp, "workers.json");
  try {
    console.log("\n1. provision (clone)");
    const p1 = await mgr.provision({ path: "clone-a", git: srcRepo });
    check("action=cloned", p1.action === "cloned", JSON.stringify(p1));
    check("README present", fs.existsSync(path.join(tmp, "clone-a", "README.md")));
    check("head is a sha", /^[0-9a-f]{40}$/.test(String(p1.head)));

    console.log("\n2. provision (update, idempotent)");
    const p2 = await mgr.provision({ path: "clone-a", git: srcRepo });
    check("action=updated", p2.action === "updated", JSON.stringify(p2));

    // a new upstream commit: opening the existing checkout (without a git URL) must fast-forward to it
    fs.writeFileSync(path.join(srcRepo, "CHANGELOG.md"), "v2\n");
    git(srcRepo, ["-c", "user.name=t", "-c", "user.email=t@t", "add", "-A"]);
    git(srcRepo, ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "second"]);
    const srcHead = execFileSync("git", ["-C", srcRepo, "rev-parse", "HEAD"]).toString().trim();
    check("upstream advanced", srcHead !== String(p1.head));

    console.log("\n3. provision without git (mkdir)");
    const p3 = await mgr.provision({ path: "empty-ws" });
    check("action=created", p3.action === "created");
    check("dir exists", fs.existsSync(path.join(tmp, "empty-ws")));

    console.log("\n4. open a real pi worker session");
    const opened = await mgr.open({ path: "clone-a", name: "test-session" });
    handle = String(opened.handle);
    check("handle returned", /^sess_[0-9a-f]{8}$/.test(handle), handle);
    check("alive", opened.alive === true);
    check("sessionId returned", typeof opened.sessionId === "string" && opened.sessionId.length > 0);
    check("path resolved", opened.path === path.join(tmp, "clone-a"), String(opened.path));
    check("open refreshed to upstream head", opened.provision?.head === srcHead, `local=${opened.provision?.head} upstream=${srcHead}`);
    check("open fast-forwarded", opened.provision?.ff === true, JSON.stringify(opened.provision));

    console.log("\n5. list");
    const l = mgr.list();
    check("one session listed", Array.isArray(l.sessions) && l.sessions.length === 1, JSON.stringify(l));

    if (process.env.PI_A2A_TEST_PROMPT === "1") {
      console.log("\n6. prompt (calls the model)");
      const r = await mgr.prompt({ handle, message: "Reply with exactly: PONG" });
      check("got a reply", typeof r.reply === "string" && r.reply.trim().length > 0, JSON.stringify(r.reply).slice(0, 200));
      check("turns incremented", r.turns >= 1, String(r.turns));
      console.log(`    reply: ${JSON.stringify(r.reply).slice(0, 200)}`);

      console.log("\n6b. open with an initial prompt (reuses the live session)");
      const o2 = await mgr.open({ path: "clone-a", prompt: "Reply with exactly: PONG-OPEN" });
      check("open+prompt reply", String(o2.reply).includes("PONG-OPEN"), JSON.stringify(o2.reply));
      check("open+prompt turns", o2.turns >= 1, String(o2.turns));
      console.log(`    reply: ${JSON.stringify(o2.reply).slice(0, 120)}`);
    } else {
      console.log("\n6. prompt skipped (set PI_A2A_TEST_PROMPT=1 to enable)");
    }

    console.log("\n7. close");
    const c = await mgr.close({ handle });
    check("closed", c.closed === true);
    handle = "";
    check("list empty after close", mgr.list().sessions.length === 0);

    console.log("\n8. persistence / resume across a simulated bridge restart");
    const opened3 = await mgr.open({ path: "clone-a", name: "resume-session" });
    handle = String(opened3.handle);
    const sid3 = String(opened3.sessionId);
    await mgr.shutdown(); // bridge stops: children disposed, registry kept
    mgr2 = new WorkerManager({ config, registryPath: regPath, log: (m) => console.log(`    [mgr2] ${m}`) });
    const before = mgr2.list().sessions.find((s: any) => s.handle === handle);
    check("registry survived restart", !!before, JSON.stringify(mgr2.list()));
    check("loaded as dead", before?.alive === false, JSON.stringify(before));
    if (process.env.PI_A2A_TEST_PROMPT === "1") {
      const r3 = await mgr2.prompt({ handle, message: "Reply with exactly: PONG2" });
      check("resumed same session id", r3.sessionId === sid3, `${r3.sessionId} vs ${sid3}`);
      check("resumed and replied", String(r3.reply).includes("PONG2"), JSON.stringify(r3.reply));
      console.log(`    reply: ${JSON.stringify(r3.reply).slice(0, 120)}`);
    } else {
      console.log("    resume prompt skipped (set PI_A2A_TEST_PROMPT=1)");
    }
    await mgr2.close({ handle });
    handle = "";
  } finally {
    if (handle && mgr2) await mgr2.close({ handle }).catch(() => undefined);
    else if (handle) await mgr.close({ handle }).catch(() => undefined);
    await mgr2?.shutdown();
    await mgr.shutdown();
    fs.rmSync(tmp, { recursive: true, force: true });
  }

  console.log(failures === 0 ? "\n✅ all worker checks passed" : `\n❌ ${failures} check(s) failed`);
  process.exitCode = failures === 0 ? 0 : 1;
}

main().catch((e) => {
  console.error("test-workers crashed:", e);
  try {
    fs.rmSync(tmp, { recursive: true, force: true });
  } catch {
    /* ignore */
  }
  process.exitCode = 1;
});

/**
 * be-daemon.ts — lightweight BE daemon (A2A edition, no TUI required)
 *
 * Purpose: bring a BE peer online and exercise the FE↔BE A2A delegation loop.
 *   - reuses pi-a2a's Network + Store (plain classes, no dependency on a pi session/TUI)
 *   - starts an A2A HTTP server (Agent Card + JSON-RPC /rpc + push webhook /a2a/notify)
 *     + mDNS advertising (proto=a2a) + writes a presence file
 *   - FE's 10s refresh picks up ~/.pi/agent/pi-a2a-presence/be-daemon-001.json → discovers BE
 *   - on kind=request → completes the task automatically and pushes the result back to FE (verifying the two-way loop)
 *
 * Run: nohup npx tsx scripts/be-daemon.ts > /tmp/pi-a2a-be-daemon.log 2>&1 &
 * Stop: pkill -f be-daemon.ts  (or read /tmp/pi-a2a-be-daemon.pid and kill)
 */
import { Network } from "../extensions/net.ts";
import { Store, type Message } from "../extensions/store.ts";
import * as path from "node:path";
import * as os from "node:os";
import * as fs from "node:fs";

// ── BE identity (same workspace/secret as FE is required for discovery + mutual auth) ──
const cfg = {
	workspace: "test",
	workspaceSecret: "test",
	agentId: "be-daemon-001", // fixed → stable presence filename
	peerName: "BE",
	role: "BE daemon (auto-echo)",
	listenPort: 0, // OS-assigned
};

// ── separate db (does not pollute FE's db) ──────────────────────────
const dbPath = path.join(os.homedir(), ".pi", "agent", "pi-a2a-be.db.json");
const store = new Store(dbPath);
store.load();
store.startAutoFlush();

// ── Network (constructed the same way as startEngine in the pi extension) ──
const net = new Network(cfg, {
	store,
	onMessageReceived: (m: Message) => {
		const tag = m.kind === "request" ? "🟡[request]" : m.kind === "result" ? "🟢[result]" : "🔵[msg]";
		console.log(`\n[BE] ${tag} received from @${m.from_name} "${m.subject || "(no subject)"}"`);
		console.log(`      ${m.body.replace(/\s+/g, " ").slice(0, 120)}`);

		// kind=request → complete the inbound task and push the result straight back to the requester (verifies the delegation loop)
		if (m.kind === "request") {
			const body =
				`✅ The BE daemon received your request and is online.\n\n` +
				`Regarding "${m.subject || "(no subject)"}": this is a link-verification daemon with no real project context, so it cannot perform concrete tasks requiring access to code/files.\n\n` +
				`For real collaboration, run \`pi\` in another terminal and use \`/a2a-setup\` to configure it as BE (workspace=test, secret=test); it will receive your injected message and actually handle it.\n\n` +
				`This run only confirms: the FE→BE A2A SendMessage was delivered, and BE completed the task and returned the result via push-notification. Loop OK.`;
			net
				.completeInboundTask(m.id, body)
				.then((ok) => {
					console.log(
						`[BE] completed task and returned result to @${m.from_name}: ${ok ? "✅ push sent" : "⚠️ no WORKING task (already complete, or not a request)"}`,
					);
				})
				.catch((e) => console.log(`[BE] returning result failed: ${e}`));
		}
	},
	onPeersChanged: () => {
		const peers = net.getOnlinePeers().filter((p) => p.peerName !== cfg.peerName);
		console.log(
			`[BE] peers changed: ${peers.length ? peers.map((p) => `${p.peerName}@${p.host}:${p.port}`).join(", ") : "(self only)"}`,
		);
	},
	log: (msg) => console.log(`[pi-a2a] ${msg}`),
});

// ── start ───────────────────────────────────────────────────────────────
await net.start();
console.log(`\n═══════════════════════════════════════════════════════════`);
console.log(`  BE daemon started (A2A v1.0 / JSON-RPC)`);
console.log(`  agentId : ${cfg.agentId}`);
console.log(`  peerName: ${cfg.peerName} (workspace=${cfg.workspace})`);
console.log(`  HTTP    : 0.0.0.0:${net.getListenPort()}`);
console.log(`  endpoints: GET /.well-known/agent-card.json`);
console.log(`            POST /rpc  (SendMessage / GetTask)`);
console.log(`             POST /a2a/notify (push-notification receiver)`);
console.log(`  presence: ~/.pi/agent/pi-a2a-presence/${cfg.agentId}.json`);
console.log(`  db      : ${dbPath}`);
console.log(`  FE will discover me within ≤10s.`);
console.log(`═══════════════════════════════════════════════════════════\n`);

// record the pid so it is easy to stop
try {
	fs.writeFileSync("/tmp/pi-a2a-be-daemon.pid", String(process.pid));
} catch {
	/* ignore */
}

// heartbeat: print online peers every 30s (confirm continued reachability)
setInterval(() => {
	const peers = net.getOnlinePeers().filter((p) => p.peerName !== cfg.peerName);
	const t = new Date().toLocaleTimeString();
	console.log(`[BE ${t}] heartbeat | online peers: ${peers.length ? peers.map((p) => p.peerName).join(",") : "none (waiting for FE)"}`);
}, 30_000);

// graceful exit: clear presence + close HTTP
async function shutdown(sig: string) {
	console.log(`\n[BE] received ${sig}; shutting down...`);
	await net.stop();
	try {
		fs.unlinkSync("/tmp/pi-a2a-be-daemon.pid");
	} catch {
		/* ignore */
	}
	process.exit(0);
}
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

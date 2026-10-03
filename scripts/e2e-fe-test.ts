/**
 * e2e-fe-test.ts — headless FE verifying the A2A delegation loop (start be-daemon.ts first)
 *
 * Flow:
 *   1. start the FE Network (workspace=test, secret=test)
 *   2. discover BE (presence/mDNS)
 *   3. a2a_send(kind=request) → BE
 *   4. BE completes the task automatically + pushes back a result
 *   5. FE's /a2a/notify receives it → onMessageReceived(kind=result)
 *   6. assert the result arrived → PASS
 *
 * Run: first `npx tsx scripts/be-daemon.ts &`, then `npx tsx scripts/e2e-fe-test.ts`
 */
import { Network } from "../extensions/net.ts";
import { Store, type Message } from "../extensions/store.ts";
import * as path from "node:path";
import * as os from "node:os";
import * as crypto from "node:crypto";

const cfg = {
	workspace: "test",
	workspaceSecret: "test",
	agentId: "fe-test-" + crypto.randomBytes(3).toString("hex"),
	peerName: "FETEST",
	role: "FE e2e test",
	listenPort: 0,
};

const dbPath = path.join(os.homedir(), ".pi", "agent", "pi-a2a-fe-test.db.json");
const store = new Store(dbPath);
store.load();
store.startAutoFlush();

let gotResult = false;
let resultBody = "";

const net = new Network(cfg, {
	store,
	onMessageReceived: (m: Message) => {
		const tag = m.kind === "request" ? "🟡[request]" : m.kind === "result" ? "🟢[result]" : "🔵[msg]";
		console.log(`[FE] ${tag} ← @${m.from_name}: ${m.body.replace(/\s+/g, " ").slice(0, 100)}`);
		if (m.kind === "result") {
			gotResult = true;
			resultBody = m.body;
		}
	},
	onPeersChanged: () => {
		const peers = net.getOnlinePeers().filter((p) => p.peerName !== cfg.peerName);
		console.log(`[FE] peers: ${peers.length ? peers.map((p) => p.peerName).join(",") : "(none)"}`);
	},
	log: (msg) => console.log(`[pi-a2a] ${msg}`),
});

await net.start();
console.log(`[FE] started, port=${net.getListenPort()}, waiting to discover BE...`);

// 1) wait for BE to come online (max 20s)
const beOnline = await waitFor(() => net.isOnline("BE"), 20_000, 500);
if (!beOnline) {
	console.log(`\n❌ FAIL: BE not discovered within 20s. Make sure be-daemon.ts is running (workspace=test, secret=test).`);
	await shutdown(1);
}

// 2) send a request
const reqId = "msg_" + crypto.randomBytes(5).toString("hex");
const reqMsg: Message = {
	id: reqId,
	thread_id: reqId,
	reply_to: null,
	from_id: cfg.agentId,
	from_name: cfg.peerName,
	to_name: "BE",
	subject: "e2e delegation test",
	body: "Please confirm the A2A link: this is a delegation request from FE; return the result when done.",
	kind: "request",
	direction: "sent",
	created_at: Math.floor(Date.now() / 1000),
};
store.addMessage(reqMsg);
store.persist();
console.log(`\n[FE] → sending request to BE (msg=${reqId})...`);
const dr = await net.deliver(reqMsg);
console.log(`[FE] deliver: delivered=${dr.delivered.join(",")||"(none)"} queued=${dr.queued.join(",")||"(none)"}`);

if (!dr.delivered.includes("BE")) {
	console.log(`\n❌ FAIL: could not deliver to BE (deliver did not succeed).`);
	await shutdown(1);
}

// 3) wait for the result to come back via push (max 15s)
const ok = await waitFor(() => gotResult, 15_000, 300);
if (ok) {
	console.log(`\n✅ PASS: received BE's result receipt (delivered immediately via push-notification).`);
	console.log(`   result body: ${resultBody.replace(/\s+/g, " ").slice(0, 120)}`);
	await shutdown(0);
} else {
	// fallback: check whether any outboundTasks are still pending
	const pending = store.getPendingOutbound();
	console.log(`\n❌ FAIL: no result within 15s. pending outbound tasks: ${pending.length}`);
	await shutdown(1);
}

// ── helpers ──────────────────────────────────────────────
async function waitFor(fn: () => boolean, timeoutMs: number, intervalMs: number): Promise<boolean> {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (fn()) return true;
		await sleep(intervalMs);
	}
	return fn();
}
function sleep(ms: number) {
	return new Promise((r) => setTimeout(r, ms));
}
async function shutdown(code: number) {
	await net.stop();
	try {
		store.persist();
	} catch {
		/* ignore */
	}
	// clean up the test db (optional)
	// fs.unlinkSync(dbPath);
	process.exit(code);
}

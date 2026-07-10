/**
 * e2e-fe-test.ts — 无头 FE，验证 A2A 委派闭环（需先起 be-daemon.ts）
 *
 * 流程:
 *   1. 起 FE Network（workspace=test, secret=test）
 *   2. 发现 BE（presence/mDNS）
 *   3. a2a_send(kind=request) → BE
 *   4. BE 自动完结 task + 经 push 回 result
 *   5. FE 的 /a2a/notify 收到 → onMessageReceived(kind=result)
 *   6. 断言收到 result → PASS
 *
 * 运行: 先 `npx tsx scripts/be-daemon.ts &` 再 `npx tsx scripts/e2e-fe-test.ts`
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
		console.log(`[FE] peers: ${peers.length ? peers.map((p) => p.peerName).join(",") : "(无)"}`);
	},
	log: (msg) => console.log(`[pi-a2a] ${msg}`),
});

await net.start();
console.log(`[FE] 已启动，port=${net.getListenPort()}，等待发现 BE...`);

// 1) 等 BE 上线（最多 20s）
const beOnline = await waitFor(() => net.isOnline("BE"), 20_000, 500);
if (!beOnline) {
	console.log(`\n❌ FAIL: 20s 内未发现 BE。请确认 be-daemon.ts 已启动（workspace=test, secret=test）。`);
	await shutdown(1);
}

// 2) 发 request
const reqId = "msg_" + crypto.randomBytes(5).toString("hex");
const reqMsg: Message = {
	id: reqId,
	thread_id: reqId,
	reply_to: null,
	from_id: cfg.agentId,
	from_name: cfg.peerName,
	to_name: "BE",
	subject: "e2a 委派测试",
	body: "请确认 A2A 链路：这是 FE 发来的委派请求，处理完请把结果回我。",
	kind: "request",
	direction: "sent",
	created_at: Math.floor(Date.now() / 1000),
};
store.addMessage(reqMsg);
store.persist();
console.log(`\n[FE] → 发送 request 给 BE (msg=${reqId})...`);
const dr = await net.deliver(reqMsg);
console.log(`[FE] deliver: delivered=${dr.delivered.join(",")||"(无)"} queued=${dr.queued.join(",")||"(无)"}`);

if (!dr.delivered.includes("BE")) {
	console.log(`\n❌ FAIL: 未能投递给 BE（deliver 未成功）。`);
	await shutdown(1);
}

// 3) 等 result 经 push 回来（最多 15s）
const ok = await waitFor(() => gotResult, 15_000, 300);
if (ok) {
	console.log(`\n✅ PASS: 收到 BE 的 result 回执（经 push-notification 即时送达）。`);
	console.log(`   结果正文: ${resultBody.replace(/\s+/g, " ").slice(0, 120)}`);
	await shutdown(0);
} else {
	// 兜底：查 outboundTasks 是否还 pending
	const pending = store.getPendingOutbound();
	console.log(`\n❌ FAIL: 15s 内未收到 result。pending outbound tasks: ${pending.length}`);
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
	// 清理测试 db（可选）
	// fs.unlinkSync(dbPath);
	process.exit(code);
}

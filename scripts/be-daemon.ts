/**
 * be-daemon.ts — 轻量 BE 守护进程（A2A 版，无需 TUI）
 *
 * 目的: 让一个 BE peer 上线，打通 FE↔BE 的 A2A 委派闭环。
 *   - 复用 pi-a2a 的 Network + Store（纯类，不依赖 pi 会话/TUI）
 *   - 起 A2A HTTP server（Agent Card + JSON-RPC /rpc + push webhook /a2a/notify）
 *     + mDNS 广播(proto=a2a) + 写 presence 文件
 *   - FE 的 10s refresh 会扫到 ~/.pi/agent/pi-a2a-presence/be-daemon-001.json → 发现 BE
 *   - 收到 kind=request → 自动完结 task 并经 push 把结果回给 FE（验证双向闭环）
 *
 * 运行: nohup npx tsx scripts/be-daemon.ts > /tmp/pi-a2a-be-daemon.log 2>&1 &
 * 停止: pkill -f be-daemon.ts  (或读 /tmp/pi-a2a-be-daemon.pid kill)
 */
import { Network } from "../extensions/net.ts";
import { Store, type Message } from "../extensions/store.ts";
import * as path from "node:path";
import * as os from "node:os";
import * as fs from "node:fs";

// ── BE 身份（与 FE 同 workspace/secret，才能互发现+互验）──────────────
const cfg = {
	workspace: "test",
	workspaceSecret: "test",
	agentId: "be-daemon-001", // 固定 → presence 文件名稳定
	peerName: "BE",
	role: "BE daemon (auto-echo)",
	listenPort: 0, // OS 分配
};

// ── 独立 db（不污染 FE 的 db）────────────────────────────────────────
const dbPath = path.join(os.homedir(), ".pi", "agent", "pi-a2a-be.db.json");
const store = new Store(dbPath);
store.load();
store.startAutoFlush();

// ── Network（与 pi 扩展里 startEngine 同样的构造方式）────────────────
const net = new Network(cfg, {
	store,
	onMessageReceived: (m: Message) => {
		const tag = m.kind === "request" ? "🟡[request]" : m.kind === "result" ? "🟢[result]" : "🔵[msg]";
		console.log(`\n[BE] ${tag} 收到 @${m.from_name} 「${m.subject || "(无主题)」"}`);
		console.log(`      ${m.body.replace(/\s+/g, " ").slice(0, 120)}`);

		// kind=request → 完结入站 task 并经 push 把结果即时回给请求方（验证委派闭环）
		if (m.kind === "request") {
			const body =
				`✅ BE 守护进程已收到你的请求并上线。\n\n` +
				`关于「${m.subject || "(无主题)"}」: 当前为链路验证守护进程，不具备真实项目上下文，无法执行需要访问代码/文件的具体任务。\n\n` +
				`如需真实协作，请在另一个终端运行 \`pi\` 并执行 \`/a2a-setup\` 配置为 BE(workspace=test, secret=test)，它会被注入你的消息并真正处理。\n\n` +
				`本次仅确认: FE→BE 的 A2A SendMessage 投递成功、BE 完结 task 并经 push-notification 回结果成功。闭环 OK。`;
			net
				.completeInboundTask(m.id, body)
				.then((ok) => {
					console.log(
						`[BE] 已完结 task 并回结果 @${m.from_name}: ${ok ? "✅ push 已发" : "⚠️ 无 WORKING task（可能已完结或非 request）"}`,
					);
				})
				.catch((e) => console.log(`[BE] 回结果失败: ${e}`));
		}
	},
	onPeersChanged: () => {
		const peers = net.getOnlinePeers().filter((p) => p.peerName !== cfg.peerName);
		console.log(
			`[BE] peers 变化: ${peers.length ? peers.map((p) => `${p.peerName}@${p.host}:${p.port}`).join(", ") : "(仅自己)"}`,
		);
	},
	log: (msg) => console.log(`[pi-a2a] ${msg}`),
});

// ── 启动 ──────────────────────────────────────────────────────────────
await net.start();
console.log(`\n═══════════════════════════════════════════════════════════`);
console.log(`  BE 守护进程已启动（A2A v1.0 / JSON-RPC）`);
console.log(`  agentId : ${cfg.agentId}`);
console.log(`  peerName: ${cfg.peerName} (workspace=${cfg.workspace})`);
console.log(`  HTTP    : 0.0.0.0:${net.getListenPort()}`);
console.log(`  端点    : GET /.well-known/agent-card.json`);
console.log(`            POST /rpc  (SendMessage / GetTask)`);
console.log(`            POST /a2a/notify (push-notification 接收)`);
console.log(`  presence: ~/.pi/agent/pi-a2a-presence/${cfg.agentId}.json`);
console.log(`  db      : ${dbPath}`);
console.log(`  FE 将在 ≤10s 内发现我。`);
console.log(`═══════════════════════════════════════════════════════════\n`);

// 记录 pid 便于停止
try {
	fs.writeFileSync("/tmp/pi-a2a-be-daemon.pid", String(process.pid));
} catch {
	/* ignore */
}

// 心跳: 每 30s 打印一次在线 peer（确认持续可达）
setInterval(() => {
	const peers = net.getOnlinePeers().filter((p) => p.peerName !== cfg.peerName);
	const t = new Date().toLocaleTimeString();
	console.log(`[BE ${t}] heartbeat | 在线 peers: ${peers.length ? peers.map((p) => p.peerName).join(",") : "无(等 FE 上线)"}`);
}, 30_000);

// 优雅退出: 清 presence + 关 HTTP
async function shutdown(sig: string) {
	console.log(`\n[BE] 收到 ${sig}，正在关闭...`);
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

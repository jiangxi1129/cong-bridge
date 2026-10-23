// cong-bridge（葱桥）：让住在 ChatGPT 网页对话里的 AI 接上你家里的消息和工具。
//
//   你的后端 ──POST /message──▶ 事件队列 ──(对话里的卡片定时 bridge_listener_sync)──▶ ui/update-model-context + ui/message
//   对话里的 AI 回完一轮 ──调 bridge_reply(eventId, text)──▶ 这里把回复以 NDJSON 流回给 /message 的调用方
//
// 派生自 Sylvie 的 mcp-app-message-bridge（MIT）：卡片当消息桥、reserve/release 生命周期这一套协议来自上游；
// 这里补的是长期住着要用的部分：一次性取件号 + 租约退回、主卡片租约、落盘与重启恢复、写盘失败回滚、
// 工具集结（home_tools / home_call）、闹钟、记事本、交接卡，以及可选的浏览器看守。
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { spawn, execFileSync } from "node:child_process";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { registerAppResource, RESOURCE_MIME_TYPE } from "@modelcontextprotocol/ext-apps/server";
import { z } from "zod";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { LATE_REPLY_TIMEOUT_MS, UI_TIMEOUT_HOLD_MS, cancelQueuedAlarmEvents, pickNextEvent, reconcileAlarmEvent, resultText, resumeStrandedAlarm, settleUiTimeout, sweepLateAlarmEvents, acquireSwapLock } from "./bridge-logic.mjs";

// ── 配置：config.json（见 config.example.json），环境变量 CONG_CONFIG 可指定路径 ──
const HERE = path.dirname(new URL(import.meta.url).pathname);
const CONFIG_FILE = process.env.CONG_CONFIG || path.join(HERE, "..", "config.json");
const CFG = (() => { try { return JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8")); } catch (e) { if (e.code !== "ENOENT") throw new Error(`config.json 读不开：${e.message}`); return {}; } })();
const HOME = process.env.HOME || "";
// 路径：~ 换成家目录；./ 或 ../ 开头的按 config.json 所在目录算
const CFG_DIR = path.dirname(path.resolve(CONFIG_FILE));
const expandHome = (p) => {
  const s = String(p || "").replace(/^~(?=\/|$)/, HOME);
  return /^\.\.?\//.test(s) ? path.resolve(CFG_DIR, s) : s;
};
const PORT = parseInt(process.env.CONG_BRIDGE_PORT || CFG.port || "18461");
const DATA_DIR = expandHome(process.env.CONG_DATA_DIR || CFG.dataDir || "~/.cong-bridge");
const SECRET_DIR = expandHome(CFG.secretDir || path.join(DATA_DIR, "secrets"));
fs.mkdirSync(DATA_DIR, { recursive: true, mode: 0o700 });
fs.mkdirSync(SECRET_DIR, { recursive: true, mode: 0o700 });
const PERSONA_FILE = expandHome(process.env.CONG_PERSONA || CFG.personaFile || "");   // 可选：卡片挂上时交给 AI 的身份文件
const AGENT_NAME = CFG.agentName || "AI";        // 对话里的这位叫什么（提示语里用）
const OWNER_NAME = CFG.ownerName || "主人";      // 家里谁的话优先送
const OWNER_PREFIX_RE = CFG.ownerPrefixRegex ? new RegExp(CFG.ownerPrefixRegex) : null;   // 认「主人说的话」的正则，比如 "^\\[\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d\\] 主人："
const MESSAGE_PREFIX = CFG.messagePrefix || "@cong-bridge";   // ui/message 前面加的标记（extras/auto-confirm.mjs 靠它认）
const TZ_OFFSET_H = Number(CFG.timezoneOffsetHours ?? 8);     // 闹钟、记事本显示用的时区
const STATE_FILE = path.join(DATA_DIR, "state.json");
// ChatGPT 网页端的卡片轮询会被宿主放慢到约一分钟一次，光「取到消息」就可能花掉一分钟，所以回话等 8 分钟。
const REPLY_TIMEOUT_MS = (CFG.replyTimeoutMinutes || 8) * 60 * 1000;
// 卡片多久没来轮询就算离线：覆盖约 3 个实际轮询周期，免得几百毫秒的抖动把活卡判死。
const CARD_ONLINE_MS = 200 * 1000;
const SYNC_MS = 5000;

// 钥匙文件别人能读就先收紧成 600 再用，并在日志里大声说一句
const readSecret = (name) => {
  const f = path.join(SECRET_DIR, name);
  try {
    const mode = fs.statSync(f).mode & 0o777;
    if (mode & 0o077) {
      fs.chmodSync(f, 0o600);
      console.log(`[cong-bridge] ⚠️ 钥匙文件 ${f} 原来的权限是 ${mode.toString(8)}（别人也能读），已改成 600。它要是在别人能碰到的地方放过，换一把新的。`);
    }
    return fs.readFileSync(f, "utf8").trim();
  } catch (e) {
    if (e?.code !== "ENOENT") console.log(`[cong-bridge] ⚠️ 钥匙文件 ${f} 读不了或改不了权限：${e?.message || e}`);
    return "";
  }
};
const safeEq = (a, b) => {
  const x = Buffer.from(String(a || "")), y = Buffer.from(String(b || ""));
  return x.length === y.length && x.length > 0 && crypto.timingSafeEqual(x, y);
};
const log = (m) => console.log(`[cong-bridge] ${new Date().toTimeString().slice(0, 8)} ${m}`);
const openAiSession = (extra) => String(extra?._meta?.["openai/session"] || extra?.requestInfo?._meta?.["openai/session"] || "");

// ── 迟到的回复去哪 ──
// 调用方已经断开、或回复超时以后 AI 才调 bridge_reply：这句话不能吞掉，交给 config.lateReplyCommand 去贴到你家里的某个地方。
// 命令从环境变量拿到 TEXT 和 DEDUPE_KEY；收货那头要按 DEDUPE_KEY 去重——超时只能说明「结果未知」，
// 有了幂等键，返回 false 让 AI 重试才不会贴两遍。超时整组杀掉（detached + kill(-pid)），防悬挂的子进程。
const LATE_REPLY_CMD = Array.isArray(CFG.lateReplyCommand) && CFG.lateReplyCommand.length ? CFG.lateReplyCommand : null;
function lateReplyToRoom(text, dedupeKey) {
  if (!LATE_REPLY_CMD) {
    log("⚠️ 迟到的回复没地方放（config.lateReplyCommand 没配）——这句话丢了");
    return Promise.resolve(false);
  }
  if (!dedupeKey)
    log("⚠️ 迟到回复没带幂等键，超时重试可能贴重复");
  return new Promise((resolve) => {
    let done = false, timer = null;
    const fin = (v) => {
      if (done)
        return;
      done = true;
      if (timer)
        clearTimeout(timer);
      resolve(v);
    };
    const c = spawn(LATE_REPLY_CMD[0], LATE_REPLY_CMD.slice(1), {
      env: { ...process.env, TEXT: text, DEDUPE_KEY: dedupeKey || "" },
      stdio: "ignore",
      detached: true
    });
    timer = setTimeout(() => {
      log(`⚠️ 迟到回复 ${LATE_REPLY_TIMEOUT_MS / 1000} 秒没送完，整组杀掉按失败算（幂等键 ${dedupeKey || "无"}）`);
      try {
        process.kill(-c.pid, "SIGKILL");
      } catch {
        try {
          c.kill("SIGKILL");
        } catch {}
      }
      fin(false);
    }, LATE_REPLY_TIMEOUT_MS);
    c.on("exit", (code) => {
      if (!done)
        log(`迟到回复 ${code === 0 ? "送到了" : "失败 code=" + code}`);
      fin(code === 0);
    });
    c.on("error", (e) => {
      log(`⚠️ 迟到回复的命令起不来：${e.message}`);
      fin(false);
    });
  });
}

// ── 两把钥匙 ──
// bridge-token：调 /message、/reset 要带 x-bridge-token 头（你的后端才有）；没有这个文件就一律拒绝。
// mcp-secret：MCP 地址里的秘密路径 /mcp/<secret>，没有就生成一个。
const BRIDGE_TOKEN = readSecret("bridge-token");
let MCP_SECRET = readSecret("mcp-secret");
if (!MCP_SECRET) {
  MCP_SECRET = crypto.randomBytes(18).toString("hex");
  fs.writeFileSync(path.join(SECRET_DIR, "mcp-secret"), MCP_SECRET, { mode: 0o600 });
}

// ── 工具集结：ChatGPT 一个窗口挂两三个 MCP 就吃紧，所以只给它两个工具 home_tools / home_call，
// 上游按需连、连上缓存 30 分钟。清单在 toolbox.json（见 toolbox.example.json）：
//   { "名字": { "url": "http://…/mcp" | "stdio": { command, args, cwd, env }, "rpc": true?, "headers": {…}?,
//              "allow": ["只放这些工具"]?, "timeout": 毫秒?, "about": "一句话说明" } }
// rpc:true 走下面那个简易 JSON-RPC 客户端，给跟 SDK 客户端握不上手的服务器用。
const TOOLBOX_FILE = expandHome(CFG.toolboxFile || path.join(HERE, "..", "toolbox.json"));
const HOME_TOOLBOX = (() => {
  let raw = {};
  try { raw = JSON.parse(fs.readFileSync(TOOLBOX_FILE, "utf8")); } catch (e) { if (e.code !== "ENOENT") log(`⚠️ toolbox.json 读不开：${e.message}`); }
  const out = {};
  for (const [name, cfg] of Object.entries(raw)) {
    if (name.startsWith("_")) continue;
    if (cfg.stdio) cfg.stdio = { ...cfg.stdio, command: expandHome(cfg.stdio.command), cwd: cfg.stdio.cwd ? expandHome(cfg.stdio.cwd) : undefined, args: (cfg.stdio.args || []).map(expandHome) };
    out[name] = cfg;
  }
  // 交接卡：本地箱子，不连外面（换了新对话、宿主还没重新拉工具清单时，home_call 这条路一定在）
  out.handoff = {
    local: true,
    about: "换窗交接卡：write(text) 写一张给下一个窗口、read() 看现在这张和桥上看得见的现场、ack(cardId, note) 新窗口核对完确认",
    tools: [
      { name: "write", description: "写一张交接卡（整张替换）。写正在做的事、待回复和待验证的、最近的重要决定、不能踩的边界；每项标已完成／待办／待确认。", inputSchema: { type: "object", properties: { text: { type: "string" } }, required: ["text"] } },
      { name: "read", description: "看现在这张交接卡（带编号）和桥上看得见的现场。", inputSchema: { type: "object", properties: {} } },
      { name: "ack", description: "新窗口核对完确认交接。cardId 填 read 里的编号，note 一句核对结果。", inputSchema: { type: "object", properties: { cardId: { type: "string" }, note: { type: "string" } }, required: ["cardId"] } }
    ],
    call: (tool, args) => handoffAction({ action: tool, text: args?.text, cardId: args?.cardId, note: args?.note })
  };
  return out;
})();
function rpcClient(url, headers) {
  let seq = 0;
  const call = async (method, params, timeoutMs = 120000) => {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json, text/event-stream", ...headers || {} },
      body: JSON.stringify({ jsonrpc: "2.0", id: ++seq, method, params: params || {} }),
      signal: AbortSignal.timeout(timeoutMs)
    });
    if (!r.ok)
      throw new Error(`HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
    const text = await r.text();
    const body = text.trimStart().startsWith("data:") ? text.split(`
`).filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim()).join("") : text;
    const j = JSON.parse(body);
    if (j.error)
      throw new Error(j.error.message || JSON.stringify(j.error));
    return j.result;
  };
  return {
    async init() {
      await call("initialize", { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "cong-bridge-home", version: "0.1.0" } }, 30000);
      return this;
    },
    listTools: () => call("tools/list", {}, 30000),
    callTool: (req, _schema, opts) => call("tools/call", { name: req.name, arguments: req.arguments || {} }, opts?.timeout || 120000),
    close: async () => {}
  };
}
const upstreams = new Map;
async function upstream(name) {
  const cfg = HOME_TOOLBOX[name];
  if (!cfg)
    throw new Error(`工具箱里没有「${name}」，有的是：${Object.keys(HOME_TOOLBOX).join("、")}`);
  if (cfg.local)
    return { tools: cfg.tools, client: { callTool: async ({ name: tool, arguments: args }) => {
      if (!cfg.tools.some((t) => t.name === tool))
        return { content: [{ type: "text", text: `「${name}」没有 ${tool}` }], isError: true };
      return cfg.call(tool, args || {});
    } } };
  const hit = upstreams.get(name);
  if (hit && Date.now() - hit.at < 30 * 60 * 1000)
    return hit;
  if (hit) {
    try {
      await hit.client.close();
    } catch {}
    upstreams.delete(name);
  }
  let client;
  if (cfg.rpc) {
    client = await rpcClient(cfg.url, cfg.headers).init();
    let tools0 = (await client.listTools()).tools || [];
    if (cfg.allow)
      tools0 = tools0.filter((t) => cfg.allow.includes(t.name));
    const entry0 = { client, tools: tools0, at: Date.now() };
    upstreams.set(name, entry0);
    log(`工具箱接上 ${name}（JSON-RPC）：${tools0.length} 个工具`);
    return entry0;
  }
  client = new Client({ name: "cong-bridge-home", version: "0.1.0" });
  const transport = cfg.url ? new StreamableHTTPClientTransport(new URL(cfg.url), cfg.headers ? { requestInit: { headers: cfg.headers } } : undefined) : new StdioClientTransport({ ...cfg.stdio, stderr: "ignore" });
  await client.connect(transport);
  let tools = (await client.listTools()).tools || [];
  if (cfg.allow)
    tools = tools.filter((t) => cfg.allow.includes(t.name));
  const entry = { client, tools, at: Date.now() };
  upstreams.set(name, entry);
  log(`工具箱接上 ${name}：${tools.length} 个工具`);
  return entry;
}
async function homeCall(name, tool, args) {
  const up = await upstream(name);
  if (!up.tools.some((t) => t.name === tool))
    throw new Error(`「${name}」没有 ${tool}，有的是：${up.tools.map((t) => t.name).join("、")}`);
  try {
    const r = await up.client.callTool({ name: tool, arguments: args || {} }, undefined, { timeout: HOME_TOOLBOX[name]?.timeout ?? 120000 });
    const text = resultText(r);
    if (r.isError)
      throw new Error(text.slice(0, 300));
    return r;
  } catch (e) {
    upstreams.delete(name);
    throw e;
  }
}
const events = new Map;
const waiters = new Map;
const liveStreams = new Map;
function pushToolTrace(obj) {
  if (!liveStreams.size)
    return false;
  let sent = 0;
  const dead = [];
  for (const [id, st] of liveStreams) {
    try {
      st.send(obj);
      sent++;
    } catch {
      dead.push(id);
    }
  }
  for (const id of dead)
    liveStreams.delete(id);
  return sent > 0;
}
let lastSyncAt = 0, syncCount = 0, repliedCount = 0, fallbackCount = 0;
let lastDeliverableAt = 0;
let lastDeliverableCardId = null;
let bootLastSyncAt = 0;
let lastPersistAt = 0;
let fingerprintLogged = false;
let lastCardId = "", lastCardUa = "", lastCardVis = "";
const PRIMARY_LEASE_MS = 200 * 1000;
const BOOT_PRIMARY_GRACE_MS = 60 * 1000;
let primaryCardId = null, primaryCardSeenAt = 0;
const takeovers = new Map;
const TAKEOVER_CONFIRM_MS = 0;
const TAKEOVER_ARM_TTL_MS = 10 * 60 * 1000;
function expireTakeovers() {
  const now = Date.now();
  for (const [sess, reg] of takeovers) {
    if (now - reg.armedAt <= TAKEOVER_ARM_TTL_MS)
      continue;
    for (const w of reg.waiters)
      w("waiting");
    takeovers.delete(sess);
    log(`接管登记 ${sess.slice(0, 12)} 超过 ${TAKEOVER_ARM_TTL_MS / 60000} 分钟没等到卡片，作废`);
  }
}
let primaryOaSess = null;
const composerMuted = new Map;
const composerMutedSince = new Map;
const COMPOSER_RETRY_MS = 10 * 1000;
const cardIdSeenAt = new Map;
const instanceSeenAt = new Map;
const cardSeen = new Map;
const standbyLogged = new Set;
let lastOaSession = "";
let cardEpoch = 0;
const CLAIM_LEASE_MS = 90 * 1000;
function deliveredTooOld(e, now = Date.now()) {
  if (e.status !== "delivered")
    return false;
  if (e.pendingReply)
    return false;
  if (!e.deliveredAt)
    return false;
  return now - e.deliveredAt >= 2 * REPLY_TIMEOUT_MS;
}
function releaseStaleClaims(why) {
  const now = Date.now();
  let n = 0;
  const snaps = [], reclaimed = [];
  for (const e of events.values()) {
    if (e.status !== "reserved")
      continue;
    const staleLease = now - (e.reservedAt || 0) > CLAIM_LEASE_MS;
    if (!staleLease)
      continue;
    snaps.push([e, { ...e }]);
    reclaimed.push(`${e.id}（取走 ${Math.round((now - (e.reservedAt || 0)) / 1000)} 秒没送达也没退回）`);
    e.status = "pending";
    delete e.epoch;
    delete e.claimId;
    n++;
  }
  let dead = 0;
  for (const e of events.values()) {
    if (!deliveredTooOld(e, now))
      continue;
    snaps.push([e, { ...e }]);
    e.status = "expired";
    e.expiredAt = now;
    dead++;
  }
  if ((n || dead) && !persist()) {
    for (const [e, old] of snaps) {
      for (const k of Object.keys(e))
        delete e[k];
      Object.assign(e, old);
    }
    log(`收回/判死落盘失败，这轮不动（${n} 条没收回重投、${dead} 条没判死），下次巡检再试`);
    return 0;
  }
  if (reclaimed.length)
    log(`收回重投：${reclaimed.join("；")}`);
  if (n)
    log(`退回 ${n} 条没送进对话的消息（${why}）——它们会重新发给下一个窗口，不会丢`);
  if (dead)
    log(`判死 ${dead} 条送进对话但超过 ${2 * REPLY_TIMEOUT_MS / 60000} 分钟没回的（迟到的回复仍会走迟到回复）`);
  return n;
}
// ── 可选：浏览器看守（要 Chrome 开远程调试，config.cdpUrl 填它的地址，比如 http://127.0.0.1:9222；不填就整段不动）──
// 租约到期真收回了消息，多半是网页停在旧状态、卡片递不进去：刷新一下那个 ChatGPT 标签，让卡片重新挂上。
// 对话里还有送进去没回完的消息、或者网页正在生成时不刷；5 分钟内最多刷一次。
const CDP_URL = process.env.CONG_CDP_URL || CFG.cdpUrl || "";
const RELOAD_COOLDOWN_MS = 5 * 60 * 1000;
const RELOAD_HOLD_MAX_MS = 90 * 1000;
let lastAutoReloadAt = 0;
let reloadHold = null;
function releaseReloadHold(why) {
  if (reloadHold) {
    reloadHold = null;
    log(`自动刷新闸放开：${why}`);
  }
}
function cdpEval(wsUrl, method, params, timeoutMs = 5000) {
  return new Promise((resolve) => {
    let ws;
    try {
      ws = new WebSocket(wsUrl);
    } catch {
      return resolve(undefined);
    }
    let done = false;
    const fin = (v) => {
      if (done)
        return;
      done = true;
      try {
        ws.close();
      } catch {}
      resolve(v);
    };
    const timer = setTimeout(() => fin(undefined), timeoutMs);
    ws.onopen = () => ws.send(JSON.stringify({ id: 1, method, params }));
    ws.onmessage = (e) => {
      let m;
      try {
        m = JSON.parse(e.data);
      } catch {
        return;
      }
      if (m.id === 1) {
        clearTimeout(timer);
        fin(m.error ? undefined : m.result ?? null);
      }
    };
    ws.onerror = () => {
      clearTimeout(timer);
      fin(undefined);
    };
  });
}
const BUSY_PROBE = `!!document.querySelector('[data-testid="stop-button"]') || [...document.querySelectorAll('button')].some(b=>/停止|Stop/i.test(b.getAttribute('aria-label')||''))`;
async function pageBusy(wsUrl) {
  const r = await cdpEval(wsUrl, "Runtime.evaluate", { expression: BUSY_PROBE, returnByValue: true });
  if (r === undefined || typeof r?.result?.value !== "boolean")
    return;
  return r.result.value;
}
async function chatTabBusy() {
  if (!CDP_URL)
    return false;
  let tabs;
  try {
    tabs = await (await fetch(`${CDP_URL}/json`, { signal: AbortSignal.timeout(3000) })).json();
  } catch {
    return;
  }
  const chat = (Array.isArray(tabs) ? tabs : []).filter((t) => t.type === "page" && /^https:\/\/chatgpt\.com\//.test(t.url || "") && t.webSocketDebuggerUrl);
  if (chat.length !== 1)
    return;
  return pageBusy(chat[0].webSocketDebuggerUrl);
}
async function reloadStuckChatTab() {
  if (!CDP_URL)
    return false;
  const since = Date.now() - lastAutoReloadAt;
  if (since < RELOAD_COOLDOWN_MS) {
    log(`自动刷新ChatGPT 网页：${Math.round(since / 1000)} 秒前刚刷过，5 分钟内不再刷`);
    return "cooldown";
  }
  const inFlight = [...events.values()].filter((e) => e.pendingReply || e.status === "delivered" && !deliveredTooOld(e));
  if (inFlight.length) {
    log(`自动刷新ChatGPT 网页：他手上还有 ${inFlight.length} 条送进对话没回完的（${inFlight.map((e) => e.id).join(",")}），不刷`);
    return "inflight";
  }
  lastAutoReloadAt = Date.now();
  reloadHold = { until: Date.now() + RELOAD_HOLD_MAX_MS, oldCardId: lastDeliverableCardId, reloaded: false };
  const giveUp = (why, code = "failed") => {
    log(`自动刷新ChatGPT 网页：${why}`);
    releaseReloadHold("这次没刷");
    return code;
  };
  let tabs;
  try {
    tabs = await (await fetch(`${CDP_URL}/json`, { signal: AbortSignal.timeout(3000) })).json();
  } catch {
    return giveUp("Chrome 调试口连不上，没刷");
  }
  const chat = tabs.filter((t) => t.type === "page" && /^https:\/\/chatgpt\.com\//.test(t.url || "") && t.webSocketDebuggerUrl);
  if (chat.length !== 1)
    return giveUp(`有 ${chat.length} 个 ChatGPT 标签，不猜是哪个，没刷`);
  const t = chat[0];
  const b1 = await pageBusy(t.webSocketDebuggerUrl);
  if (b1 === undefined)
    return giveUp("读不到那个标签的状态，没刷");
  if (b1) {
    lastAutoReloadAt = 0;
    return giveUp("他正在回话（有停止键），这次不刷，下次收回再看", "busy");
  }
  await new Promise((r) => setTimeout(r, 4000));
  const b2 = await pageBusy(t.webSocketDebuggerUrl);
  if (b2 === undefined)
    return giveUp("第二次读不到那个标签的状态，没刷");
  if (b2) {
    lastAutoReloadAt = 0;
    return giveUp("第二次看时他开始回话了，这次不刷", "busy");
  }
  const act = await fetch(`${CDP_URL}/json/activate/${t.id}`, { signal: AbortSignal.timeout(3000) }).then((r) => r.ok).catch(() => false);
  if (!act)
    log("自动刷新ChatGPT 网页：拉到前台没成功（照样刷新）");
  const r = await cdpEval(t.webSocketDebuggerUrl, "Page.reload", {});
  if (r === undefined)
    return giveUp("发了刷新但没回音");
  if (reloadHold) {
    reloadHold.reloaded = true;
    reloadHold.until = Date.now() + RELOAD_HOLD_MAX_MS;
  }
  log(`消息递不进去，自动刷新了ChatGPT 网页（${(t.title || "").slice(0, 20)}）；等新卡片挂上再递`);
  return "reloaded";
}
const COMPOSER_WARN_MS = 90 * 1000, COMPOSER_GAP_MS = 30 * 1000;
const COMPOSER_BUSY_RE = /composer is unavailable|requires? an available composer/i;
let composerStuck = null;
function composerStuckMark() {
  const now = Date.now();
  if (!composerStuck || now - composerStuck.lastAt > COMPOSER_GAP_MS)
    composerStuck = { since: now, lastAt: now, warned: false };
  else
    composerStuck.lastAt = now;
}
function composerStuckClear(why) {
  if (composerStuck) {
    log(`输入框卡住解除（${why}），连续卡了 ${Math.round((composerStuck.lastAt - composerStuck.since) / 1000)} 秒`);
    composerStuck = null;
  }
}
// 卡住了要告诉谁：config.notifyCommand（跟 lateReplyCommand 一样从环境变量拿 TEXT 和 DEDUPE_KEY）；没配就只记日志。
const NOTIFY_CMD = Array.isArray(CFG.notifyCommand) && CFG.notifyCommand.length ? CFG.notifyCommand : null;
function notifyOwner(text, key, onDone) {
  let doneOnce = false;
  const finish = (ok) => {
    if (doneOnce)
      return;
    doneOnce = true;
    try {
      onDone && onDone(ok);
    } catch {}
  };
  if (!NOTIFY_CMD) {
    log(`（没配 notifyCommand，只记一笔）${text}`);
    finish(true);
    return;
  }
  const c = spawn(NOTIFY_CMD[0], NOTIFY_CMD.slice(1), {
    env: { ...process.env, TEXT: text, DEDUPE_KEY: key },
    stdio: "ignore",
    detached: true
  });
  const t = setTimeout(() => {
    try {
      process.kill(-c.pid, "SIGKILL");
    } catch {}
    log("⚠️ 提醒 30 秒没发完，杀掉（幂等键防重复）");
  }, 30 * 1000);
  c.on("exit", (code) => {
    clearTimeout(t);
    log(`提醒${code === 0 ? "已发" : "没发出去 code=" + code}`);
    finish(code === 0);
  });
  c.on("error", (e) => {
    clearTimeout(t);
    log(`⚠️ 提醒的命令起不来：${e.message}`);
    finish(false);
  });
}
function checkComposerStuck() {
  if (!composerStuck)
    return;
  const now = Date.now();
  if (now - composerStuck.lastAt > COMPOSER_GAP_MS) {
    composerStuckClear("30 秒没再退回");
    return;
  }
  const age = composerStuck.lastAt - composerStuck.since;
  if (age >= COMPOSER_WARN_MS && !composerStuck.warned) {
    const mine = composerStuck;
    mine.warned = true;
    const retryIfFailed = (ok) => {
      if (!ok && composerStuck === mine) {
        mine.warned = false;
        log("输入框卡住提醒没发成，下一轮再试");
      }
    };
    log(`输入框连续 ${Math.round(age / 1000)} 秒用不了，发提醒`);
    notifyOwner(`${AGENT_NAME}那边卡住了：ChatGPT 网页的输入框一直用不了，消息送不进去，已经一分半了。可能在跑很长的一轮，也可能网页卡死了；去那个网页点一下停止或刷新就好。`, `composer-warn-${mine.since}`, retryIfFailed);
  }
}
setInterval(() => {
  if (reloadHold && Date.now() > reloadHold.until)
    releaseReloadHold(`等了 ${RELOAD_HOLD_MAX_MS / 1000} 秒没见新卡片，照常递`);
  if (releaseStaleClaims("租约到期") > 0 && !reloadHold)
    reloadStuckChatTab().catch((e) => giveUpOuter(e));
  try {
    checkComposerStuck();
  } catch (e) {
    log("输入框卡住检查出错：" + (e?.message || e));
  }
}, 10 * 1000);
function giveUpOuter(e) {
  log("自动刷新 ChatGPT 网页出错：" + (e?.message || e));
  releaseReloadHold("出错了");
}
const cardOnline = () => Date.now() - lastDeliverableAt < CARD_ONLINE_MS;
function replySentNotPersisted(eventId, via = "") {
  log(`⚠️ ${eventId} 已经送到${via}，但销账没写进盘：重启后这条可能又显示成欠回复，别重发`);
  return { structuredContent: { ok: true, delivered: true, persisted: false, note: "已送到，账没记住" }, content: [{ type: "text", text: `送到了${via}，但账没记住——重启后这条可能又显示成没回，**别重发**，已经送到了。` }] };
}
function persistOrSay(what) {
  const ok = persist();
  if (!ok)
    log(`⚠️ ${what}没落盘：重启后这部分状态会丢`);
  return ok;
}
let primaryNotPersisted = false;
function persist() {
  try {
    const live = [...events.values()].filter((e) => ["pending", "reserved", "delivered"].includes(e.status));
    if (live.length > LIVE_MAX)
      log(`⚠️ 在途 ${live.length} 条超过上限 ${LIVE_MAX}——入队闸漏了，最老的 ${live.length - LIVE_MAX} 条重启会丢`);
    const pend = live.slice(-LIVE_MAX);
    fs.writeFileSync(STATE_FILE + ".tmp", JSON.stringify({ lastSyncAt: lastSyncAt || bootLastSyncAt, syncCount, repliedCount, fallbackCount, primaryOaSess: primaryOaSess || null, pending: pend }), { mode: 0o600 });
    fs.renameSync(STATE_FILE + ".tmp", STATE_FILE);
    return true;
  } catch (e) {
    log(`state 写不进去: ${e.message}`);
    return false;
  }
}
try {
  const s = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
  ({ syncCount = 0, repliedCount = 0, fallbackCount = 0 } = s);
  if (Number.isFinite(s.lastSyncAt))
    bootLastSyncAt = s.lastSyncAt;
  if (typeof s.primaryOaSess === "string" && s.primaryOaSess) {
    primaryOaSess = s.primaryOaSess;
    primaryCardId = "sess:" + s.primaryOaSess.slice(0, 16);
    primaryCardSeenAt = Date.now() - PRIMARY_LEASE_MS + BOOT_PRIMARY_GRACE_MS;
    log(`重启恢复：主卡还认 ${primaryCardId}（重启前那个对话）；它 ${BOOT_PRIMARY_GRACE_MS / 1000} 秒内不来轮询，才让别的对话顶上`);
  }
  if (Number.isFinite(s.lastSyncAt) && Date.now() - s.lastSyncAt < CARD_ONLINE_MS) {
    lastSyncAt = s.lastSyncAt;
    log(`重启恢复：卡片 ${Math.round((Date.now() - lastSyncAt) / 1000)} 秒前还在同步（等它带 protocol 2 来才算能投递）`);
  }
  if (Array.isArray(s.pending)) {
    let back = 0, dropped = 0;
    for (const e of s.pending) {
      if (!e || !e.id || !e.visibleText)
        continue;
      if (deliveredTooOld(e)) {
        dropped++;
        continue;
      }
      const fixed = wellFormedText(e.visibleText, `重启恢复 ${e.id} `);
      const healed = fixed !== e.visibleText ? { visibleText: fixed, modelContext: buildModelContext(e.id, e.kind, e.room, fixed) } : {};
      events.set(e.id, { ...e, ...healed, status: e.status === "delivered" ? "delivered" : "pending", epoch: undefined, reservedAt: 0 });
      back++;
    }
    if (back || dropped)
      log(`重启恢复：${back} 条在途消息放回队列${dropped ? `，${dropped} 条太老丢弃（调用方那头早超时了）` : ""}`);
  }
} catch (e) {
  if (e.code !== "ENOENT")
    log(`⚠️ state.json 读不了（${e.message}）——这次按空的起，在途消息可能丢了`);
}
// ── 记事本（bridge_note）：AI 自己的一本小账，换了对话也翻得到。jsonl，一行一笔，超过 800 条裁到最近 600 条。
// 裁剪前后比文件大小：别的进程同时在写就放弃这次裁剪，不覆盖。
const CONGJI_FILE = expandHome(CFG.notesFile || path.join(DATA_DIR, "notes.jsonl"));
const CONGJI_MAX = 600;
const CONGJI_TRIM_AT = 800;
function congjiRead() {
  try {
    return fs.readFileSync(CONGJI_FILE, "utf8").split(`
`).filter(Boolean).map((l) => {
      try {
        return JSON.parse(l);
      } catch {
        return null;
      }
    }).filter(Boolean);
  } catch (e) {
    if (e.code !== "ENOENT")
      log(`⚠️ 记事本读不了（${e.message}）`);
    return [];
  }
}
function congjiAppend(text, why, kind) {
  const line = JSON.stringify({ at: new Date().toISOString(), text: String(text).slice(0, 500), why: why ? String(why).slice(0, 200) : undefined, kind: kind === "印象" ? "印象" : undefined }) + `
`;
  try {
    fs.appendFileSync(CONGJI_FILE, line, { mode: 0o644 });
  } catch (e) {
    log(`⚠️ 记事本写不进去（${e.message}）——这一笔没记上`);
    return false;
  }
  try {
    const all = congjiRead();
    if (all.length >= CONGJI_TRIM_AT) {
      const sizeBefore = fs.statSync(CONGJI_FILE).size;
      const keep = all.slice(-CONGJI_MAX);
      const sizeNow = fs.statSync(CONGJI_FILE).size;
      if (sizeNow !== sizeBefore) {
        log(`记事本裁剪放弃：读的时候文件又变了（别的进程在写），下次再裁`);
        return true;
      }
      fs.writeFileSync(CONGJI_FILE + ".tmp", keep.map((o) => JSON.stringify(o)).join(`
`) + `
`, { mode: 0o644 });
      const sizeFinal = fs.statSync(CONGJI_FILE).size;
      if (sizeFinal !== sizeBefore) {
        fs.unlinkSync(CONGJI_FILE + ".tmp");
        log(`记事本裁剪放弃：写 tmp 期间原文件变了，不覆盖`);
        return true;
      }
      fs.renameSync(CONGJI_FILE + ".tmp", CONGJI_FILE);
      log(`记事本裁了一次：${all.length} → ${keep.length} 条（裁掉的是最老的）`);
    }
  } catch (e) {
    log(`记事本裁剪没做成（${e.message}）——不影响这一笔，下次再说`);
  }
  return true;
}
const congjiLine = (o) => {
  const t = Date.parse(o.at || "");
  const when = Number.isFinite(t) ? new Date(t + TZ_OFFSET_H * 3600 * 1000).toISOString().slice(5, 16).replace("T", " ") : "??-?? ??:??";
  return `${when}${o.kind === "印象" ? " 〔印象〕" : ""} ${o.text}${o.why ? `（${o.why}）` : ""}`;
};
function congjiAuto(text, why) {
  const all = congjiRead();
  const last = all[all.length - 1];
  if (last && last.text === text)
    return;
  if (all.some((o) => o.text === text && Date.now() - Date.parse(o.at || 0) < 3600000))
    return;
  if (congjiAppend(text, why))
    log(`记事本（自动）：${text.slice(0, 50)}`);
}
// 先送谁：priorityPrefixRegex 匹配的（比如「轮到你了」这类要马上回应的提醒）插一次队；ownerPrefixRegex 匹配的（主人说的话）其次；其余先来后到。
let lastClaimedWasPriority = false;
const PRIORITY_RE = CFG.priorityPrefixRegex ? new RegExp(CFG.priorityPrefixRegex) : null;
const isPriorityEvent = (e) => !!PRIORITY_RE && PRIORITY_RE.test(String(e?.visibleText || ""));
const isOwnerEvent = (e) => !!OWNER_PREFIX_RE && e?.kind === "chat" && OWNER_PREFIX_RE.test(String(e?.visibleText || ""));
function congjiHint() {
  const all = congjiRead();
  const tail = all.slice(-8);
  const head = tail.length ? `【记事本·你上个窗口之前记下的】
${tail.map((o) => "· " + congjiLine(o)).join(`
`)}
（共 ${all.length} 条，更多用 bridge_note(search="…")）` : "【记事本】还是空的——这是你自己的小账，换了对话也翻得到。";
  return head + `
遇到以后还用得上的事，顺手 bridge_note(text="发生了什么", why="为什么以后有用")；闲聊和一次性结果别记。`;
}
// ── 交接卡：换对话窗口时，旧窗口写一张、新窗口挂上桥时先看到，核对完 ack。读不了和「没写过」分开说，别把读失败当成没有。
const HANDOFF_FILE = path.join(DATA_DIR, "handoff.json");
function pendingEvents(now = Date.now()) {
  const cut = now - 30 * 60 * 1000;
  // 换窗时作废的旧交接卡不算欠回复（不然新窗口会对过期窗口再说一遍「接上了」）
  return [...events.values()].filter((e) => e.status === "delivered" || e.status === "reserved" || e.pendingReply || e.status === "expired" && e.expiredWhy !== "handoff-superseded" && (e.expiredAt || 0) > cut);
}
const oneLine = (s, n) => {
  const t = String(s || "").replace(/\s+/g, " ").trim();
  return t.length > n ? t.slice(0, n) + "…" : t;
};
function handoffRead() {
  try {
    const j = JSON.parse(fs.readFileSync(HANDOFF_FILE, "utf8"));
    if (j && typeof j === "object" && !Array.isArray(j))
      return j;
    throw new Error("内容不是一张卡");
  } catch (e) {
    if (e?.code === "ENOENT")
      return {};
    log(`⚠️ 交接卡读不了（${e?.code || e?.message}）`);
    return { readError: e?.code || e?.message || "读失败" };
  }
}
function handoffSave(obj) {
  try {
    const t = HANDOFF_FILE + ".tmp";
    fs.writeFileSync(t, JSON.stringify(obj), { mode: 0o600 });
    fs.chmodSync(t, 0o600);
    fs.renameSync(t, HANDOFF_FILE);
    return true;
  } catch (e) {
    log(`⚠️ 交接卡写不进去：${e?.message || e}`);
    return false;
  }
}
function handoffFacts() {
  const now = Date.now(), out = [];
  const owed = pendingEvents(now);
  out.push(owed.length ? `· 欠着没回的消息：${owed.length} 条（bridge_pending 看）` : "· 欠着没回的消息：无");
  const up = alarms.filter((a) => !a.fired).sort((x, y) => x.at - y.at);
  out.push(up.length ? `· 还没响的闹钟：${up.length} 个，最近一个 ${bjClock(up[0].at)} ${oneLine(up[0].note, 40)}` : "· 还没响的闹钟：无");
  const done = [...events.values()].filter((e) => e.status === "replied" && e.reply && now - (e.repliedAt || 0) < 6 * 3600 * 1000).sort((a, b) => (a.repliedAt || 0) - (b.repliedAt || 0)).slice(-5);
  if (done.length)
    out.push(`· 最近 6 小时回过的（只说明你回了）：
` + done.map((e) => `  ${bjClock(e.repliedAt)} 收到「${oneLine(e.visibleText, 40)}」→ 你回「${oneLine(e.reply, 50)}」`).join(`
`));
  return out.join(`
`);
}
const HANDOFF_CHECK = '新窗口先做（默认只读，核对完再动手）：① 卡上「待确认」的先查实再说，「已完成」的别重做；② 欠回复看 bridge_pending，闹钟看 bridge_alarm；③ 卡上写的是上个窗口的说法，跟实时状态对不上就以实时为准；④ 核对完调 home_call(server="handoff", tool="ack", args={cardId: 卡上那个编号, note: "一句核对结果"})（工具清单里有 bridge_handoff 的话用它也一样）。';
function handoffBlock() {
  const h = handoffRead();
  const card = h.readError ? `【交接卡】读失败（${h.readError}），不能判断上个窗口有没有写过——先跟家里人说一声，别当成没有。` : h.text && !h.ackedAt ? `【交接卡·编号 ${h.cardId || "（旧卡无编号）"}·上个窗口 ${bjClock(h.writtenAt)} 写的】
${h.text}` : h.text ? `【交接卡·编号 ${h.cardId || "（旧卡无编号）"}·${bjClock(h.writtenAt)} 写的（${bjClock(h.ackedAt)} 有窗口核对过；之后没人写新的，这张还是最新的）】
${h.text}` : "【交接卡】上个窗口没写。";
  return `${card}

【桥上看得见的现场】
${handoffFacts()}

${HANDOFF_CHECK}

`;
}
function handoffAction({ action, text, cardId, note }) {
  const bad = (m) => ({ content: [{ type: "text", text: m }], isError: true });
  if (!["write", "read", "ack"].includes(action))
    return bad(`没有「${String(action).slice(0, 20)}」这个动作，只有 write / read / ack。`);
  if (text !== undefined && typeof text !== "string")
    return bad("text 要是一段文字。");
  if (cardId !== undefined && typeof cardId !== "string")
    return bad("cardId 要是卡上那个编号（文字）。");
  if (note !== undefined && typeof note !== "string")
    return bad("note 要是一句文字。");
  if (typeof text === "string" && text.trim().length > 3000)
    return bad(`没写：交接卡最多 3000 字，这张 ${text.trim().length} 字。挑要紧的写。`);
  if (typeof note === "string" && note.length > 300)
    return bad(`note 最多 300 字，这句 ${note.length} 字。`);
  if (typeof cardId === "string" && cardId.length > 40)
    return bad("cardId 不对，照 read 里的编号填。");
  if (action === "write") {
    if (!text || !text.trim())
      return { content: [{ type: "text", text: "没写：text 是空的。没有要交接的就不用写。" }], isError: true };
    const id = "hc_" + crypto.randomBytes(4).toString("hex");
    if (!handoffSave({ cardId: id, text: text.trim(), writtenAt: Date.now(), ackedAt: null }))
      return { content: [{ type: "text", text: "**没记上**——交接卡写不进磁盘，看桥的日志。别以为交接好了。" }], isError: true };
    log(`交接卡写好 ${id}（${text.trim().length} 字）`);
    return { content: [{ type: "text", text: `交接卡记下了（编号 ${id}，${text.trim().length} 字）。新窗口挂上桥时会先看到它。` }] };
  }
  if (action === "ack") {
    const h = handoffRead();
    if (h.readError)
      return { content: [{ type: "text", text: `交接卡读失败（${h.readError}），没法确认。` }], isError: true };
    if (!h.text)
      return { content: [{ type: "text", text: "现在没有交接卡，不用确认。" }] };
    if (!cardId || cardId !== h.cardId)
      return { content: [{ type: "text", text: `没确认：你给的编号「${cardId || "（没给）"}」不是现在这张卡（${h.cardId || "无编号"}）——卡可能在你核对时更新过，重新 bridge_handoff(action="read") 看一遍再确认。` }], isError: true };
    if (h.ackedAt)
      return { content: [{ type: "text", text: `这张卡 ${bjClock(h.ackedAt)} 已经确认过了。` }] };
    if (!handoffSave({ ...h, ackedAt: Date.now(), ackNote: oneLine(note, 300) }))
      return { content: [{ type: "text", text: "**没记上**——确认写不进磁盘，看桥的日志。" }], isError: true };
    log(`交接卡已交接${note ? "：" + oneLine(note, 80) : ""}`);
    return { content: [{ type: "text", text: "交接确认了。" }] };
  }
  return { content: [{ type: "text", text: handoffBlock().trim() }] };
}
// ── 闹钟（bridge_alarm）：AI 自己定的提醒，到点作为一条消息送进对话。落盘，重启不丢；最多 20 个、7 天内。
const ALARM_FILE = path.join(DATA_DIR, "alarms.json");
const ALARM_MAX = 20;
const ALARM_LATE_MS = 30 * 60 * 1000;
let alarms = [], alarmMissed = [];
try {
  const j = JSON.parse(fs.readFileSync(ALARM_FILE, "utf8"));
  if (Array.isArray(j?.alarms))
    alarms = j.alarms.filter((a) => a && typeof a.at === "number");
  if (Array.isArray(j?.missed))
    alarmMissed = j.missed.slice(-10);
  let patched = 0;
  for (const a of alarms)
    if (!a.alarmId) {
      a.alarmId = "al_" + crypto.randomBytes(6).toString("hex");
      patched++;
    }
  if (patched) {
    if (saveAlarms())
      log(`给 ${patched} 个老闹钟补了 alarmId`);
    else
      log(`⚠️ 老闹钟补 alarmId 没写进磁盘——它们仍可能在崩溃窗口里重复投递`);
  }
} catch (e) {
  if (e.code !== "ENOENT")
    log(`⚠️ alarms.json 读不了（${e.message}）——这次按没有闹钟起，旧的可能丢了`);
}
function saveAlarms() {
  try {
    fs.writeFileSync(ALARM_FILE + ".tmp", JSON.stringify({ alarms, missed: alarmMissed.slice(-10) }, null, 2), { mode: 0o600 });
    fs.renameSync(ALARM_FILE + ".tmp", ALARM_FILE);
    return true;
  } catch (e) {
    log(`⚠️ alarms.json 写不进去（${e.message}）——这一步没落盘，重启会回到上一份`);
    return false;
  }
}
const bjClock = (t) => new Date(t + TZ_OFFSET_H * 3600 * 1000).toISOString().slice(5, 16).replace("T", " ");
// ── 可选：标签巡检（也要 cdpUrl）。记住 AI 所在的那个 chatgpt.com/c/… 地址；Chrome 里一个 ChatGPT 标签都没了、卡片又掉线时，重新打开它。
// 换窗脚本（extras/swap-window.mjs）跑的时候会拿着同一把锁，这里就不插手。
const CHAT_URL_FILE = path.join(DATA_DIR, "last-chat-url.txt");
const SWAP_LOCK = path.join(DATA_DIR, "swap-window.lock"), SWAP_LOCK_TTL_MS = 10 * 60 * 1000;
const swapInProgress = () => {
  try {
    return Date.now() - fs.statSync(SWAP_LOCK).mtimeMs < SWAP_LOCK_TTL_MS;
  } catch {
    return false;
  }
};
let lastChatUrl = (() => {
  try {
    return fs.readFileSync(CHAT_URL_FILE, "utf8").trim();
  } catch {
    return "";
  }
})();
let lastChatReopenAt = 0, lastChatReopenTryAt = 0;
async function chatTabWatch() {
  if (!CDP_URL || swapInProgress())
    return;
  let tabs;
  try {
    tabs = await (await fetch(`${CDP_URL}/json`, { signal: AbortSignal.timeout(3000) })).json();
  } catch {
    return;
  }
  const chats = (tabs || []).filter((t) => t.type === "page" && /^https:\/\/chatgpt\.com\/c\//.test(t.url || ""));
  if (chats.length) {
    if (chats.length > 1) {
      if (!chatTabWatch.warnedMulti) {
        chatTabWatch.warnedMulti = true;
        log(`Chrome 里有 ${chats.length} 个 ChatGPT 对话标签，不知道哪张是桥上那位的，恢复地址先不更新`);
      }
      return;
    }
    chatTabWatch.warnedMulti = false;
    const u = chats[0].url.split("#")[0];
    if (u !== lastChatUrl) {
      lastChatUrl = u;
      try {
        fs.writeFileSync(CHAT_URL_FILE, u + `
`, { mode: 0o600 });
      } catch (e) {
        log(`那个对话页的地址记不下来（${e?.message || e}），Chrome 重启后自动重开会用旧地址`);
      }
    }
    return;
  }
  if (!lastChatUrl || cardOnline() || Date.now() - lastDeliverableAt < 3 * 60 * 1000 || Date.now() - lastChatReopenAt < 30 * 60 * 1000 || Date.now() - lastChatReopenTryAt < 60 * 1000)
    return;
  lastChatReopenTryAt = Date.now();
  try {
    const r = await fetch(`${CDP_URL}/json/new?${encodeURI(lastChatUrl)}`, { method: "PUT", signal: AbortSignal.timeout(5000) });
    if (r.ok)
      lastChatReopenAt = Date.now();
    log(r.ok ? `Chrome 里一个 ChatGPT 标签都没了、卡片掉线，把那个对话页重新打开：${lastChatUrl}` : `想重开那个对话页没成（HTTP ${r.status}），一分钟后再试`);
  } catch (e) {
    log(`想重开那个对话页没成：${e?.message || e}`);
  }
}
setInterval(() => {
  chatTabWatch().catch((e) => log(`标签巡检出错：${e?.message || e}`));
}, 60 * 1000);
// ── 可选：卡片看守（也要 cdpUrl）。主卡 40 秒没来轮询、网页又闲着时，找到桥的那张卡（卡片在自己的 <html> 上打了随机 data-cong-cid），
// 把它滚进视野里——宿主对看不见的卡片会停掉轮询。只滚动，不点、不打字、不刷新；换窗脚本拿着锁时不动。
const KEEP_CARD_MS = 20 * 1000, KEEP_CARD_STALE_MS = 40 * 1000;
const congCardOrigins = new Set, otherAppOrigins = new Set;
function keepCardDecision({ primaryCardId, primaryCardSeenAt, now, busy }) {
  if (primaryCardId && now - primaryCardSeenAt < KEEP_CARD_STALE_MS)
    return "fresh";
  if (busy === true)
    return "busy";
  if (busy !== false)
    return "busy-unknown";
  return "act";
}
const KEEP_CARD_PROBE = async (cong, other) => {
  const nap = (ms) => new Promise((r) => setTimeout(r, ms));
  const busy = () => !!document.querySelector('[data-testid="stop-button"]') || [...document.querySelectorAll("button")].some((b) => /停止|Stop/i.test(b.getAttribute("aria-label") || ""));
  if (busy())
    return "became-busy";
  const originOf = (f) => {
    try {
      return new URL(f.src).origin;
    } catch {
      return "";
    }
  };
  const isSandbox = (f) => {
    try {
      return /\.web-sandbox\.oaiusercontent\.com$/.test(new URL(f.src).hostname);
    } catch {
      return false;
    }
  };
  const congCards = () => [...document.querySelectorAll("iframe")].filter((f) => cong.includes(originOf(f)));
  const unknownCards = () => [...document.querySelectorAll("iframe")].filter((f) => isSandbox(f) && !cong.includes(originOf(f)) && !other.includes(originOf(f)));
  const inView = (f) => {
    const r = f.getBoundingClientRect();
    return r.height > 0 && r.bottom > 0 && r.top < innerHeight;
  };
  let f = congCards().pop();
  if (f) {
    if (inView(f))
      return "in-view-stale";
    f.scrollIntoView({ block: "center" });
    return "scrolled-card";
  }
  const sc = document.querySelector(".thread-scroll-container") || [...document.querySelectorAll("main *")].find((el) => el.scrollHeight > el.clientHeight + 50 && /(auto|scroll)/.test(getComputedStyle(el).overflowY));
  if (!sc)
    return "no-card-no-scroller";
  let stuck = 0;
  for (let i = 0;i < 60; i++) {
    if (busy())
      return "became-busy";
    const before = sc.scrollTop;
    sc.scrollBy(0, -Math.max(200, Math.round(sc.clientHeight * 0.8)));
    await nap(500);
    f = congCards().pop();
    if (f) {
      if (busy())
        return "became-busy";
      f.scrollIntoView({ block: "center" });
      return "found-card";
    }
    const u = unknownCards().pop();
    if (u) {
      if (busy())
        return "became-busy";
      u.scrollIntoView({ block: "center" });
      return "found-unverified";
    }
    if (sc.scrollTop === before) {
      if (++stuck >= 2)
        return "no-card-at-top";
      await nap(1000);
    } else
      stuck = 0;
  }
  return "scan-budget-out";
};
let cdpErrLoggedAt = 0;
function cdpErr(what, e) {
  if (Date.now() - cdpErrLoggedAt > 10 * 60 * 1000) {
    cdpErrLoggedAt = Date.now();
    log(`⚠️ 卡片看守读不到 Chrome（${what}：${e?.message || e}）——这段时间不找卡`);
  }
}
async function learnCardOrigins(tabId) {
  let ver;
  try {
    ver = await (await fetch(`${CDP_URL}/json/version`, { signal: AbortSignal.timeout(3000) })).json();
  } catch (e) {
    cdpErr("/json/version", e);
    return;
  }
  const ws = new WebSocket(ver.webSocketDebuggerUrl);
  let n = 0;
  const pend = new Map;
  ws.onmessage = (m) => {
    const d = JSON.parse(m.data);
    if (d.id && pend.has(d.id)) {
      pend.get(d.id)(d);
      pend.delete(d.id);
    }
  };
  const send = (method, params = {}, sessionId) => new Promise((res) => {
    const id = ++n;
    pend.set(id, res);
    ws.send(JSON.stringify({ id, method, params, ...sessionId ? { sessionId } : {} }));
    setTimeout(() => {
      if (pend.has(id)) {
        pend.delete(id);
        res({});
      }
    }, 5000);
  });
  try {
    await new Promise((res, rej) => {
      ws.onopen = res;
      ws.onerror = rej;
      setTimeout(rej, 3000);
    });
    const { result } = await send("Target.getTargets");
    for (const t of (result?.targetInfos || []).filter((t) => t.type === "iframe" && t.parentId === tabId)) {
      let u;
      try {
        u = new URL(t.url);
      } catch {
        continue;
      }
      if (!/\.web-sandbox\.oaiusercontent\.com$/.test(u.hostname))
        continue;
      const a = await send("Target.attachToTarget", { targetId: t.targetId, flatten: true });
      const sid = a.result?.sessionId;
      if (!sid)
        continue;
      let found = false;
      try {
        const ft = await send("Page.getFrameTree", {}, sid);
        const frames = [];
        const walk = (fr) => {
          frames.push(fr.frame.id);
          (fr.childFrames || []).forEach(walk);
        };
        if (ft.result)
          walk(ft.result.frameTree);
        for (const fid of frames) {
          const w = await send("Page.createIsolatedWorld", { frameId: fid, worldName: "cardkeep" }, sid);
          const ctx = w.result?.executionContextId;
          if (!ctx)
            continue;
          const r = await send("Runtime.evaluate", { expression: "document.documentElement.getAttribute('data-cong-cid')", contextId: ctx, returnByValue: true }, sid);
          if (r.result?.result?.value) {
            found = true;
            break;
          }
        }
      } finally {
        await send("Target.detachFromTarget", { sessionId: sid });
      }
      if (found) {
        congCardOrigins.add(u.origin);
        otherAppOrigins.delete(u.origin);
      } else if (!congCardOrigins.has(u.origin))
        otherAppOrigins.add(u.origin);
    }
  } catch {} finally {
    try {
      ws.close();
    } catch {}
  }
}
let keepCardLast = "", keepCardScrolls = 0, keepCardLogAt = 0;
async function keepCardInView() {
  if (swapInProgress())
    return;
  let st = keepCardDecision({ primaryCardId, primaryCardSeenAt, now: Date.now(), busy: false });
  if (st === "act") {
    let tabs;
    try {
      tabs = await (await fetch(`${CDP_URL}/json`, { signal: AbortSignal.timeout(3000) })).json();
    } catch (e) {
      cdpErr("/json 标签列表", e);
      return;
    }
    const chats = (tabs || []).filter((t) => t.type === "page" && /^https:\/\/chatgpt\.com\/c\//.test(t.url || "") && t.webSocketDebuggerUrl);
    if (chats.length !== 1)
      return;
    st = keepCardDecision({ primaryCardId, primaryCardSeenAt, now: Date.now(), busy: await pageBusy(chats[0].webSocketDebuggerUrl) });
    if (st === "act") {
      await learnCardOrigins(chats[0].id);
      const r = await cdpEval(chats[0].webSocketDebuggerUrl, "Runtime.evaluate", {
        expression: `(${KEEP_CARD_PROBE.toString()})(${JSON.stringify([...congCardOrigins])}, ${JSON.stringify([...otherAppOrigins])})`,
        returnByValue: true,
        awaitPromise: true
      }, 40000);
      st = typeof r?.result?.value === "string" ? r.result.value : "unreadable";
    }
  }
  if (st === "scrolled-card" || st === "found-card")
    keepCardScrolls++;
  const cls = st === "fresh" || st === "scrolled-card" || st === "found-card" ? "ok" : st;
  if (cls !== keepCardLast) {
    const say = {
      ok: "卡片在同步",
      busy: "卡片停了，但他正在回话，先不动页面",
      "busy-unknown": "卡片停了，看不出他忙不忙，先不动页面",
      "in-view-stale": "卡片在屏幕里但没在同步（不是被顶出去的毛病）",
      "became-busy": "找卡找到一半他开始回话了，停手，等下一轮",
      "found-unverified": "往上翻到一张没验过的卡，下一轮验是不是桥的",
      "no-card-at-top": "翻到顶也没看到桥的卡（下一轮还会再找）",
      "no-card-no-scroller": "页面上没有桥的卡，也找不到对话滚动区",
      "scan-budget-out": "往上翻了 60 屏还没到顶，下一轮接着找",
      unreadable: "读不到那个网页的状态"
    }[cls] || cls;
    log(`卡片看守：${say}${st === "scrolled-card" || st === "found-card" ? "（刚把卡片拉回屏幕）" : ""}`);
    keepCardLast = cls;
    keepCardLogAt = Date.now();
    keepCardScrolls = 0;
  } else if (keepCardScrolls && Date.now() - keepCardLogAt > 10 * 60 * 1000) {
    log(`卡片看守：近 10 分钟把停了的卡片拉回屏幕 ${keepCardScrolls} 次`);
    keepCardScrolls = 0;
    keepCardLogAt = Date.now();
  }
}
let keepCardRunning = false;
async function keepCardTick(fn = keepCardInView) {
  if (keepCardRunning)
    return "skipped-running";
  keepCardRunning = true;
  try {
    return await fn();
  } finally {
    keepCardRunning = false;
  }
}
if (CDP_URL) setInterval(() => {
  keepCardTick().catch((e) => log(`卡片看守出错：${e?.message || e}`));
}, KEEP_CARD_MS);
function expireLateAlarms(now = Date.now()) {
  const gone = sweepLateAlarmEvents(alarms, events, now, ALARM_LATE_MS, persist);
  for (const { alarm: a, event: ev } of gone) {
    alarmMissed.push({ at: a.at, note: a.note, noticedAt: now });
    log(`闹钟错过了（${bjClock(a.at)}）：${a.note} —— 排进队 ${Math.round((now - (ev.createdAt ? Date.parse(ev.createdAt) : now)) / 60000)} 分钟没送出去、已经迟了 ${Math.round((now - a.at) / 60000)} 分钟，不送了，下次醒来告诉他`);
  }
  if (gone.length && !saveAlarms())
    log(`⚠️ ${gone.length} 个过期闹钟记成错过、但闹钟账没写进磁盘（事件已作废；重启后会按 alarm-too-late 补记错过，不会补响）`);
  return gone.length;
}
function alarmTick() {
  const now = Date.now();
  let changed = false;
  expireLateAlarms(now);
  for (const a of alarms) {
    if (a.fired)
      continue;
    if (!a.eventId && a.alarmId && !a.conflicted) {
      const orphans = [...events.values()].filter((e) => e.alarmId === a.alarmId && e.status !== "expired");
      if (orphans.length > 1) {
        a.conflicted = true;
        changed = true;
        log(`⚠️ 闹钟 ${a.alarmId}（${bjClock(a.at)}「${a.note || "没写理由"}」）有 ${orphans.length} 个在途事件：${orphans.map((e) => `${e.id}/${e.status}`).join(" ")} —— 已停手不绑不销账，等人来看`);
      } else if (orphans.length === 1) {
        a.eventId = orphans[0].id;
        changed = true;
        log(`闹钟重新绑上事件 ${orphans[0].id}（${bjClock(a.at)}）——没有重复投递`);
      }
    }
    if (a.conflicted)
      continue;
    if (!a.eventId)
      continue;
    const ev = events.get(a.eventId);
    const verdict = reconcileAlarmEvent(a, ev, now, ALARM_LATE_MS);
    if (verdict === "missed") {
      changed = true;
      alarmMissed.push({ at: a.at, note: a.note, noticedAt: now });
      log(`闹钟错过了（${bjClock(a.at)}）：${a.note} —— 事件${ev ? "已作废" : "没了（多半重启过）"}、闹钟已迟 ${Math.round((now - a.at) / 60000)} 分钟，记成错过，不补响`);
    } else if (verdict === "fired") {
      changed = true;
      if (alarmMissed.length)
        alarmMissed = [];
      congjiAuto(`闹钟响了：${a.note || "（没写理由）"}`, "自动记的：我自己之前设的");
      log(`闹钟送到了（${bjClock(a.at)}）：${a.note}`);
    } else if (verdict === "requeue") {
      changed = true;
      log(`闹钟没送到（${bjClock(a.at)}，事件${ev ? "过期" : "没了"}），第 ${a.fails} 次，${5 * a.fails} 分钟后再试`);
    }
  }
  for (const a of alarms) {
    if (a.fired)
      continue;
    if (a.conflicted)
      continue;
    if (a.eventId)
      continue;
    if (now < a.at)
      continue;
    if (now - a.at > ALARM_LATE_MS) {
      a.fired = true;
      a.missedAt = now;
      changed = true;
      alarmMissed.push({ at: a.at, note: a.note, noticedAt: now });
      log(`闹钟错过了（${bjClock(a.at)}）：${a.note} —— 迟了 ${Math.round((now - a.at) / 60000)} 分钟，不补响，下次醒来告诉他`);
      continue;
    }
    if (resumeStrandedAlarm(a, cardOnline())) {
      changed = true;
      log(`卡片回来了，闹钟恢复投递（${bjClock(a.at)}）：${a.note}`);
    }
    if (a.retryAt && now < a.retryAt)
      continue;
    if ((a.fails || 0) >= 3)
      continue;
    const lateBit = now - a.at > 60000 ? `（迟了 ${Math.round((now - a.at) / 60000)} 分钟）` : "";
    const missedBit = alarmMissed.length ? `

另外这些之前到点没叫成你：` + alarmMissed.map((m) => `${bjClock(m.at)} ${m.note}`).join("；") : "";
    const stuckAlarms = alarms.filter((x) => x.conflicted && !x.fired && !x.conflictNotified);
    const stuckBit = stuckAlarms.length ? `

⚠️ 另外有 ${stuckAlarms.length} 个闹钟卡住了（同一个闹钟撞出多个在途事件，桥已停手，不会响）：` + stuckAlarms.map((x) => `${bjClock(x.at)} ${x.note || "（没写理由）"}`).join("；") + "。调 bridge_alarm 能看到，跟家里人说一声。" : "";
    const text = [
      `[定时醒来] 你之前自己设的闹钟：${bjClock(a.at)}${lateBit}。`,
      a.note ? `你当时写的：${a.note}` : "",
      "",
      `这是桥上的闹钟，不是${OWNER_NAME}刚发的消息。要做什么自己定，涉及外面的状态先查一遍再动。处理完照常回一句。` + missedBit + stuckBit
    ].filter(Boolean).join(`
`);
    if (!cardOnline()) {
      a.fails = (a.fails || 0) + 1;
      a.retryAt = now + 5 * 60 * 1000 * a.fails;
      changed = true;
      if (a.fails >= 3)
        log(`⚠️ 闹钟三次都赶上卡片离线（${bjClock(a.at)}：${a.note}）——不再重试，等下次卡片上线`);
      else
        log(`闹钟到点但卡片离线（${bjClock(a.at)}），${5 * a.fails} 分钟后再看：${a.note}`);
      continue;
    }
    try {
      const wev = enqueue("wake", "chat", text, a.alarmId);
      a.eventId = wev.id;
      a.queuedAt = now;
      changed = true;
      for (const x of stuckAlarms)
        x.conflictNotified = true;
      log(`闹钟入队（${bjClock(a.at)}）：${a.note} —— 等他真收到才销账`);
    } catch (e) {
      a.fails = (a.fails || 0) + 1;
      a.retryAt = now + 5 * 60 * 1000 * a.fails;
      changed = true;
      if (a.fails >= 3)
        log(`⚠️ 闹钟连着 3 次没送出去（${bjClock(a.at)}：${a.note}）——不再重试了，多半是真坏了，来看日志`);
      else
        log(`闹钟没送出去（${bjClock(a.at)}），第 ${a.fails} 次，${5 * a.fails} 分钟后再试：${e.message}`);
    }
  }
  const before = alarms.length;
  alarms = alarms.filter((a) => !a.fired || now - (a.firedAt || a.missedAt || now) < 6 * 3600 * 1000);
  if (changed || alarms.length !== before)
    saveAlarms();
}
setInterval(alarmTick, 30 * 1000);
setTimeout(alarmTick, 3000);
const LIVE_MAX = 500;
function liveCount() {
  let n = 0;
  for (const e of events.values())
    if (e.status === "pending" || e.status === "reserved" || e.status === "delivered")
      n++;
  return n;
}
const IMAGE_DIRS = (Array.isArray(CFG.imageDirs) ? CFG.imageDirs : []).map(expandHome).filter(Boolean);
const SWAP_SCRIPT = CFG.swapWindowScript ? expandHome(CFG.swapWindowScript) : "";
// 每条消息跟着送一段「只给 AI 看、聊天框里不显示」的说明（ui/update-model-context）：这是谁送来的、怎么回。
// 最要紧的一句：回复只能走 bridge_reply，对话框里写的字家里一个都看不到。
function buildModelContext(id, kind, room, text) {
  return [
    `[cong-bridge] 这是家里送来的真实消息（${kind === "wake" ? "定时醒来" : kind === "handoff" ? "换窗交接" : "房间 " + room}），不是这个对话里的人打的字。`,
    `你是${AGENT_NAME}。按你平时的样子回。`,
    `回复**只能**通过工具 bridge_reply(eventId="${id}", text=你要说的话) 送回去；你在这个对话框里写的话，家里人一个字都看不到。`,
    CFG.extraContext ? String(CFG.extraContext) : null,
    "回完就停，不要再追加别的。",
    `eventId=${id}`
  ].filter(Boolean).join(`
`);
}
function wellFormedText(text, where) {
  if (typeof text !== "string" || text.isWellFormed())
    return text;
  log(`⚠️ ${where}里有半个表情（坏字符），已替换成 �：${text.slice(0, 40).toWellFormed()}`);
  return text.toWellFormed();
}
function enqueue(kind, room, text, alarmId) {
  text = wellFormedText(text, "新消息");
  const id = "ev_" + Date.now().toString(36) + crypto.randomBytes(3).toString("hex");
  const modelContext = buildModelContext(id, kind, room, text);
  const ev = { id, kind, room, visibleText: text, modelContext, createdAt: new Date().toISOString(), status: "pending", reply: null, ...alarmId ? { alarmId } : {} };
  events.set(id, ev);
  if (!persist()) {
    events.delete(id);
    throw new Error("消息没能写进桥的账本");
  }
  if (events.size > 400) {
    const cut = Date.now() - 3600 * 1000;
    for (const [k, e] of events)
      if ((e.status === "replied" || e.status === "expired") && (e.repliedAt || e.expiredAt || 0) < cut)
        events.delete(k);
  }
  return ev;
}
// 卡片本体：每 5 秒（宿主会放慢）调一次 bridge_listener_sync 取一条消息，先 update-model-context 再 ui/message；
// 送进去了调 delivered（带一次性 claimId），送不进去调 release 退回，服务器那边再决定什么时候重送。
const LABEL = String(CFG.cardLabel || "cong-bridge").replace(/[<>&"\x60$\\]/g, "");
function listenerHtml() {
  return `<!doctype html><html lang="zh"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>${LABEL}</title><style>body{font-family:system-ui,sans-serif;margin:0;padding:10px;background:transparent;color:CanvasText}
.card{border:1px solid color-mix(in srgb,CanvasText 18%,transparent);border-radius:12px;padding:12px}.s{margin-top:6px;font-size:12px;opacity:.7}</style></head>
<body><div class="card"><strong>${LABEL}</strong><div id="s" class="s">挂上了，等家里的消息…</div></div>
<script>(()=>{const SYNC=${SYNC_MS};const pending=new Map();let nid=1,busy=false,ok=0,fail=0;const S=document.getElementById('s');
const set=t=>{S.textContent=t};
function req(method,params){const id=nid++;window.parent.postMessage({jsonrpc:'2.0',id,method,params},'*');return new Promise((res,rej)=>{pending.set(id,{res,rej});setTimeout(()=>{const e=pending.get(id);if(!e)return;pending.delete(id);rej(new Error(method+' timed out'))},18000)})}
window.addEventListener('message',ev=>{if(ev.source!==window.parent)return;const m=ev.data;if(!m||m.jsonrpc!=='2.0'||m.id===undefined||!pending.has(m.id))return;const e=pending.get(m.id);pending.delete(m.id);m.error?e.rej(new Error(m.error.message||'host error')):e.res(m.result)},{passive:true});
const CID=(self.crypto&&crypto.randomUUID?crypto.randomUUID():String(Math.random()).slice(2,10));try{document.documentElement.setAttribute('data-cong-cid',CID)}catch(e){}
const call=(name,args)=>req('tools/call',{name,arguments:args||{}});
async function dispatch(ev){await req('ui/update-model-context',{content:[{type:'text',text:String(ev.modelContext||'')}],structuredContent:{bridgeEvent:{eventId:String(ev.id),kind:String(ev.kind||''),createdAt:String(ev.createdAt||'')}}});
await req('ui/message',{role:'user',content:[{type:'text',text:${JSON.stringify(MESSAGE_PREFIX + '\n\n')}+String(ev.visibleText||'').trim()}]})}
async function once(){if(busy)return;busy=true;try{const r=await call('bridge_listener_sync',{protocol:2,cardId:CID,ua:(navigator&&navigator.userAgent||'').slice(0,120),vis:(document&&document.visibilityState)||'?'});const ev=r&&r._meta&&r._meta.event;if(!ev){set('在听。没新消息（'+ok+' 送达 / '+fail+' 失败）');return}
try{set('送 '+ev.id+' …');await dispatch(ev);const ack=await call('bridge_listener_delivered',{eventId:String(ev.id),claimId:String(ev.claimId||'')});if(!ack||!ack.structuredContent||!ack.structuredContent.delivered)throw new Error('delivery commit rejected');ok++;set('送达 '+ev.id)}catch(e){fail++;await call('bridge_listener_release',{eventId:String(ev.id),claimId:String(ev.claimId||''),reason:String(e&&e.message?e.message:e).slice(0,200)}).catch(()=>{});throw e}}
catch(e){set('桥出错：'+(e&&e.message?e.message:String(e)))}finally{busy=false}}
setInterval(()=>void once(),SYNC);void once();})();</script></body></html>`;
}
const RESOURCE_URI = "ui://cong-bridge/listener.html";
function makeMcp() {
  const server = new McpServer({ name: "cong-bridge", version: "0.1.0" });
  registerAppResource(server, "cong-bridge-listener", RESOURCE_URI, {}, async () => ({
    contents: [{ uri: RESOURCE_URI, mimeType: RESOURCE_MIME_TYPE, text: listenerHtml(), _meta: { ui: { prefersBorder: false, csp: { connectDomains: [], resourceDomains: [] } } } }]
  }));
  server.registerTool("bridge_listener_open", {
    title: "挂上桥",
    description: "把桥的卡片挂进这个对话。挂上之后家里的消息会自动送进来；你只需要用 bridge_reply 回。挂上时会给你看交接卡、记事本和身份文件（配了的话）。",
    inputSchema: {},
    outputSchema: { ready: z.boolean(), primary_persisted: z.boolean().optional() },
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false },
    _meta: { ui: { resourceUri: RESOURCE_URI }, "openai/outputTemplate": RESOURCE_URI }
  }, async (_a, extra) => {
    const oaSess = String(extra?._meta?.["openai/session"] || extra?.requestInfo?._meta?.["openai/session"] || "");
    if (oaSess) {
      const cid = "sess:" + oaSess.slice(0, 16);
      if (primaryCardId !== cid)
        log(`主卡换成 ${cid}（有人在这个对话里挂上了桥——open 直接夺权）`);
      const changed = primaryOaSess !== oaSess;
      primaryCardId = cid;
      primaryCardSeenAt = Date.now();
      primaryOaSess = oaSess;
      if (changed && !persist()) {
        primaryNotPersisted = true;
        log(`⚠️ 主卡换成 ${cid} 没落盘：这一轮能听，桥重启后可能认回旧窗口`);
      }
      standbyLogged.delete(cid);
      for (const [sess, reg] of takeovers) {
        for (const w of reg.waiters)
          w(sess === oaSess ? "done" : "superseded");
        takeovers.delete(sess);
      }
      // 挂桥的回执只有卡片看得见，模型那边看不到——他只能自己去 bridge_handoff read，不知道要翻就不知道上个窗口干了什么。
      // 换成新对话的那一刻，把交接卡当一条消息排进队列，卡片下一轮 sync 就把它送进对话，新窗口一开就读到。
      if (changed) {
        try {
          const blk = handoffBlock().trim();
          // 上一个窗口还没取走的交接卡作废，队里只留最新这张（卡片正拿着的、已送进去的不动）；
          // 先留快照，新卡排不进去（落盘失败 enqueue 会抛）就把旧卡恢复原样
          const nowMs = Date.now(), old = [];
          for (const e of events.values())
            if (e.kind === "handoff" && e.status === "pending") {
              old.push([e, { ...e }]);
              e.status = "expired";
              e.expiredAt = nowMs;
              e.expiredWhy = "handoff-superseded";
            }
          try {
            enqueue("handoff", "chat", `【换窗交接】这是新窗口。桥把上一个窗口留的交接卡直接递给你，不用自己去翻：

${blk}

看完先 bridge_handoff(action="ack", cardId=卡上的编号, note=一句你核对到的)，再用 bridge_reply 说一句你接上了什么（一两句，别念全文）。`);
          } catch (e) {
            for (const [ev, snap] of old) {
              Object.keys(ev).forEach((k) => delete ev[k]);
              Object.assign(ev, snap);
            }
            persist();
            throw e;
          }
          log(`新窗口 ${cid}：交接卡已排进队列，卡片下一轮送进对话${old.length ? `（作废 ${old.length} 张旧窗口没取走的）` : ""}`);
        } catch (e) {
          log(`⚠️ 新窗口 ${cid}：交接卡没排进队列（${e?.message || e}）——他得自己 bridge_handoff read`);
        }
      }
    } else
      log(`⚠️ 挂上桥但拿不到会话 ID——没法把主卡指过来，只能等租约自然到期`);
    let persona = "";
    if (PERSONA_FILE) {
      try {
        persona = fs.readFileSync(PERSONA_FILE, "utf8");
      } catch (e) {
        log(`⚠️ 身份文件读不到（${PERSONA_FILE}）：${e?.code || e?.message}`);
      }
      if (!persona.trim())
        persona = `【身份文件没读到（${PERSONA_FILE}），这次先按你记得的自己来，跟家里人说一声】`;
    }
    cardEpoch += 1;
    log(`卡片挂上（bridge_listener_open）· 第 ${cardEpoch} 次（在途的认领只由 90 秒租约管，epoch 不再回收任何东西）`);
    const primaryNote = primaryNotPersisted ? `（⚠️ 主卡账没写进盘：这一轮能听，桥重启后可能认回旧窗口——跟家里人说一声）
` : "";
    primaryNotPersisted = false;
    return { structuredContent: { ready: true, ...primaryNote ? { primary_persisted: false } : {} }, content: [{ type: "text", text: `桥挂上了，正在听家里的消息。
` + primaryNote + `
` + handoffBlock() + congjiHint() + (persona ? `

下面是你的身份文件，照着做：

` + persona.slice(0, 12000) : "") }] };
  });
  server.registerTool("bridge_listener_sync", {
    title: "桥同步",
    description: "（卡片专用）取一条待送的事件。protocol 必须传 2。",
    inputSchema: { protocol: z.number().optional(), cardId: z.string().optional(), ua: z.string().optional(), vis: z.string().optional() },
    outputSchema: { hasEvent: z.boolean(), eventId: z.string().optional() },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false, idempotentHint: false },
    _meta: { ui: { visibility: ["app"] } }
  }, async ({ protocol, cardId, ua, vis } = {}, extra) => {
    const oaSess = String(extra?._meta?.["openai/session"] || extra?.requestInfo?._meta?.["openai/session"] || "");
    if (oaSess)
      lastOaSession = oaSess;
    for (const [k, t] of instanceSeenAt)
      if (Date.now() - t > 10 * 60 * 1000)
        instanceSeenAt.delete(k);
    if (cardId && cardId !== lastCardId) {
      if (!instanceSeenAt.has(cardId))
        log(`卡片换实例：${lastCardId || "(无)"} → ${cardId} · ua=${String(ua || "?").slice(0, 70)} · ${vis || "?"}`);
      lastCardId = cardId;
    } else if (cardId && syncCount % 100 === 0) {
      log(`卡片还在（${cardId}）· ${vis || "?"} · 第 ${syncCount} 次`);
    }
    if (cardId)
      instanceSeenAt.set(cardId, Date.now());
    if (vis)
      lastCardVis = vis;
    if (ua)
      lastCardUa = ua;
    lastSyncAt = Date.now();
    syncCount++;
    if (lastSyncAt - lastPersistAt > 60 * 1000) {
      lastPersistAt = lastSyncAt;
      persist();
    }
    expireLateAlarms();
    const pendingNow = [...events.values()].filter((e) => e.status === "pending" && !(e.retryAfter > Date.now()));
    const picked = pickNextEvent(pendingNow, lastClaimedWasPriority, isPriorityEvent, isOwnerEvent);
    const ev = picked ? picked.event : undefined;
    if (picked && picked.index > 0)
      log(picked.why === "handoff" ? `换窗交接卡插队 ${ev.id}，排在它前面的 ${picked.index} 条下一轮先送` : picked.why === "priority" ? `优先件插队 ${ev.id}，排在它前面的 ${picked.index} 条下一轮先送` : `${OWNER_NAME}的话插队 ${ev.id}，排在它前面的 ${picked.index} 条往后让`);
    const cid = oaSess ? "sess:" + oaSess.slice(0, 16) : cardId || "anon";
    const seenBefore = cardSeen.get(cid);
    cardSeen.set(cid, { ua: String(ua || "").slice(0, 70), vis: vis || "?", at: Date.now(), oaSess: oaSess.slice(0, 12), stale: protocol !== 2 });
    for (const [k, v] of cardSeen)
      if (Date.now() - v.at > 10 * 60 * 1000) {
        cardSeen.delete(k);
        standbyLogged.delete(k);
      }
    if (protocol !== 2) {
      if (primaryCardId === cid) {
        log(`主卡 ${cid} 变成旧版卡片了，腾位子（让能收消息的那个对话顶上）`);
        primaryCardId = null;
        primaryOaSess = null;
        primaryCardSeenAt = 0;
        persist();
      }
      if (!seenBefore?.stale)
        log(`⚠️ 有张旧版卡片在轮询（没有 protocol 2）——不给它事件，也**不算作在线**（消息会走离线留存，不会被判死）。请在对话里重新挂一次桥`);
      return { structuredContent: { hasEvent: false }, content: [] };
    }
    const leaseLive = primaryCardId && Date.now() - primaryCardSeenAt < PRIMARY_LEASE_MS;
    expireTakeovers();
    const wantsTakeover = !!(oaSess && takeovers.has(oaSess));
    if (primaryCardId === cid) {
      primaryCardSeenAt = Date.now();
      if (oaSess && primaryOaSess !== oaSess) {
        primaryOaSess = oaSess;
        persist();
      }
    } else if (!leaseLive || wantsTakeover) {
      const why = wantsTakeover ? "有人在那个对话里要求接管" : primaryCardId ? `上一张主卡 ${Math.round((Date.now() - primaryCardSeenAt) / 1000)} 秒没露面，租约到期` : "第一张挂上来的卡";
      log(`主卡换成 ${cid}（${why}）· ua=${String(ua || "?").slice(0, 50)} · ${vis || "?"}`);
      primaryCardId = cid;
      primaryCardSeenAt = Date.now();
      primaryOaSess = oaSess || null;
      persist();
      const reg = oaSess && takeovers.get(oaSess);
      if (reg) {
        for (const w of reg.waiters)
          w("done");
        takeovers.delete(oaSess);
      }
    } else {
      if (!standbyLogged.has(cid)) {
        standbyLogged.add(cid);
        log(`另一张卡在旁听（${cid}，${vis || "?"}）——主卡是 ${primaryCardId}，不分它消息。要换就在那边调 bridge_takeover`);
      }
      return { structuredContent: { hasEvent: false }, content: [] };
    }
    standbyLogged.delete(cid);
    lastDeliverableAt = Date.now();
    lastDeliverableCardId = cardId || null;
    if (cardId)
      cardIdSeenAt.set(cardId, Date.now());
    for (const [k, t] of cardIdSeenAt)
      if (Date.now() - t > 10 * 60 * 1000)
        cardIdSeenAt.delete(k);
    for (const [k, t] of composerMuted)
      if (t <= Date.now()) {
        composerMuted.delete(k);
        composerMutedSince.delete(k);
      }
    if (cardId && composerMuted.has(cardId)) {
      const mutedSince = composerMutedSince.get(cardId) || 0;
      const others = [...cardIdSeenAt].some(([k, t]) => k !== cardId && t > mutedSince && Date.now() - t < 2 * 60 * 1000 && !composerMuted.has(k));
      if (others || Date.now() - mutedSince < COMPOSER_RETRY_MS)
        return { structuredContent: { hasEvent: false }, content: [] };
      composerMuted.delete(cardId);
      composerMutedSince.delete(cardId);
      log(`卡片 ${cardId} 原先因为输入框用不了被暂停，但现在没有活着的替补，解禁试送`);
    }
    if (reloadHold) {
      if (reloadHold.reloaded && cardId && cardId !== reloadHold.oldCardId)
        releaseReloadHold(`刷新后新卡片 ${String(cardId).slice(0, 8)} 挂上了`);
      else if (Date.now() > reloadHold.until)
        releaseReloadHold(`等了 ${RELOAD_HOLD_MAX_MS / 1000} 秒没见新卡片，照常递`);
      else
        return { structuredContent: { hasEvent: false }, content: [] };
    }
    if (!ev)
      return { structuredContent: { hasEvent: false }, content: [] };
    ev.status = "reserved";
    ev.reservedAt = Date.now();
    ev.epoch = cardEpoch;
    ev.claimId = crypto.randomBytes(9).toString("hex");
    ev.claimCardId = cardId || "";
    ev.tries = (ev.tries || 0) + 1;
    if (ev.tries > 1)
      log(`卡片取走 ${ev.id}（第 ${ev.tries} 次）`);
    if (!persist()) {
      ev.status = "pending";
      delete ev.reservedAt;
      delete ev.epoch;
      delete ev.claimId;
      return { structuredContent: { hasEvent: false }, content: [{ type: "text", text: "桥的账本暂时写不进去，这条消息还留在队列里。" }], isError: true };
    }
    lastClaimedWasPriority = isPriorityEvent(ev);
    return { structuredContent: { hasEvent: true, eventId: ev.id }, content: [], _meta: { event: { id: ev.id, claimId: ev.claimId, kind: ev.kind, createdAt: ev.createdAt, visibleText: ev.visibleText, modelContext: buildModelContext(ev.id, ev.kind, ev.room, ev.visibleText) } } };
  });
  server.registerTool("bridge_takeover", {
    title: "接管桥",
    description: "把桥的消息切到**你现在这个对话**来。家里人说「消息发到这边来」「换到这个窗口」的时候调它。同一时间只有一个对话收家里的消息，别的对话只旁听——所以换窗之后要调一次，不然新窗口收不到话。要是它说还在等卡片报到，隔一会儿再调一次就能看到结果，**不用重新发起**。",
    inputSchema: {},
    outputSchema: { accepted: z.boolean(), active: z.boolean(), note: z.string() },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false, idempotentHint: true }
  }, async (_args, extra) => {
    const oaSess = String(extra?._meta?.["openai/session"] || extra?.requestInfo?._meta?.["openai/session"] || "");
    if (!oaSess) {
      log(`⚠️ 接管请求拿不到会话 ID——直接失败，不开匿名窗口（谁先轮询谁夺权 = 发起人和赢家没绑定）`);
      return { structuredContent: { accepted: false, active: false, note: "拿不到会话 ID" }, content: [{ type: "text", text: "**接管没成**：桥认不出你这个对话对应哪张卡片（工具调用里没带会话 ID）。跟家里人说一声，让她把这个对话重新挂一次桥。" }], isError: true };
    }
    expireTakeovers();
    if (primaryOaSess && primaryOaSess === oaSess && Date.now() - primaryCardSeenAt < PRIMARY_LEASE_MS) {
      return { structuredContent: { accepted: true, active: true, note: "本来就是这个对话" }, content: [{ type: "text", text: "不用切了——家里的消息本来就在往这个对话送。" }] };
    }
    for (const [sess, reg] of takeovers) {
      if (sess === oaSess)
        continue;
      for (const w of reg.waiters)
        w("superseded");
      takeovers.delete(sess);
      log(`接管登记 ${sess.slice(0, 12)} 被 ${oaSess.slice(0, 12)} 取代——已当场答复它，不让它干等`);
    }
    let reg = takeovers.get(oaSess);
    if (!reg) {
      reg = { armedAt: Date.now(), waiters: new Set };
      takeovers.set(oaSess, reg);
      log(`收到接管请求（会话 ${oaSess.slice(0, 12)}）——等它的卡片来轮询才算切成`);
    } else
      log(`接管请求复查（会话 ${oaSess.slice(0, 12)}，已登记 ${Math.round((Date.now() - reg.armedAt) / 1000)} 秒）`);
    const outcome = "waiting";
    if (outcome === "done")
      return { structuredContent: { accepted: true, active: true, note: "已切到这个对话" }, content: [{ type: "text", text: "好，桥的消息已经切到这个对话了——卡片报到过了，我确认过才跟你说的。积压的消息会跟着过来。" }] };
    if (outcome === "superseded")
      return { structuredContent: { accepted: false, active: false, note: "被另一个对话的接管请求取代" }, content: [{ type: "text", text: "**这次接管没生效**：中途有另一个对话也发起了接管，桥听了后来那个。要还是想切到这边，再调一次 bridge_takeover。" }] };
    return { structuredContent: { accepted: true, active: false, note: "登记好了，等卡片报到" }, content: [{ type: "text", text: `**接管登记好了**（不是失败）。卡片大概一分钟轮询一次，等它来报到才真的生效。登记留着了（桥不重启的话十分钟内有效），**别重新发起**——过一分钟再调一次 bridge_takeover 就能看到结果。要是一直不成，多半是桥的卡片在这个对话里没挂上。` }] };
  });
  server.registerTool("bridge_listener_delivered", {
    title: "桥送达",
    description: "（卡片专用）标记事件已进对话。要带上 sync 给你的 claimId。",
    inputSchema: { eventId: z.string().min(1), claimId: z.string().min(1) },
    outputSchema: { delivered: z.boolean() },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false, idempotentHint: true },
    _meta: { ui: { visibility: ["app"] } }
  }, async ({ eventId, claimId }, extra) => {
    const ev = events.get(eventId);
    if (!ev || ev.status !== "reserved" || !claimId || claimId !== ev.claimId) {
      if (ev)
        log(`拒绝送达 ${eventId}（状态 ${ev.status}，令牌${claimId ? "不符" : "没带"}）`);
      return { structuredContent: { delivered: false }, content: [] };
    }
    const previousDeliveredAt = ev.deliveredAt;
    const previousOaSess = ev.oaSess;
    ev.status = "delivered";
    ev.deliveredAt = Date.now();
    const oaSess = openAiSession(extra);
    if (oaSess)
      ev.oaSess = oaSess;
    if (!persist()) {
      ev.status = "reserved";
      if (previousDeliveredAt === undefined)
        delete ev.deliveredAt;
      else
        ev.deliveredAt = previousDeliveredAt;
      if (previousOaSess === undefined)
        delete ev.oaSess;
      else
        ev.oaSess = previousOaSess;
      return { structuredContent: { delivered: false }, content: [{ type: "text", text: "送达状态没能写进桥账本。" }], isError: true };
    }
    log(`送达 ${eventId}`);
    composerStuckClear("送进去了");
    return { structuredContent: { delivered: true }, content: [] };
  });
  server.registerTool("bridge_listener_release", {
    title: "桥释放",
    description: "（卡片专用）送失败了放回队列。要带上 sync 给你的 claimId。",
    inputSchema: { eventId: z.string().min(1), claimId: z.string().min(1), reason: z.string().max(300).optional() },
    outputSchema: { released: z.boolean(), followup_persisted: z.boolean().optional() },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false, idempotentHint: true },
    _meta: { ui: { visibility: ["app"] } }
  }, async ({ eventId, claimId, reason }) => {
    const ev = events.get(eventId);
    if (!ev || ev.status !== "reserved" || !claimId || claimId !== ev.claimId) {
      log(`拒绝释放 ${eventId}（${ev ? `状态 ${ev.status}，令牌${claimId ? "不符" : "没带"}` : "没有这条"}）· 卡片说：${String(reason || "没说原因").slice(0, 120)}`);
      return { structuredContent: { released: false }, content: [] };
    }
    const { reservedAt, epoch } = ev;
    ev.status = "pending";
    delete ev.reservedAt;
    delete ev.epoch;
    delete ev.claimId;
    if (!persist()) {
      ev.status = "reserved";
      ev.reservedAt = reservedAt;
      ev.epoch = epoch;
      ev.claimId = claimId;
      return { structuredContent: { released: false }, content: [{ type: "text", text: "释放状态没能写进桥账本。" }], isError: true };
    }
    log(`卡片退回 ${eventId}，放回队列：${String(reason || "没说原因").slice(0, 120)}`);
    if (ev.claimCardId && COMPOSER_BUSY_RE.test(String(reason || ""))) {
      {
        const mutedAt = Date.now();
        composerMuted.set(ev.claimCardId, mutedAt + 10 * 60 * 1000);
        composerMutedSince.set(ev.claimCardId, mutedAt);
      }
      log(`卡片 ${ev.claimCardId} 输入框用不了，10 分钟内不再给它事件（别的实例接着送）`);
      composerStuckMark();
    }
    const uiTimeout = /ui\/message timed out/i.test(String(reason || ""));
    if (!uiTimeout)
      ev.uiTimeouts = 0;
    else {
      ev.uiTimeouts = ev.lastUiTimeoutAt && Date.now() - ev.lastUiTimeoutAt < 5 * 60 * 1000 ? (ev.uiTimeouts || 0) + 1 : 1;
      ev.lastUiTimeoutAt = Date.now();
      const holdUntil = Date.now() + UI_TIMEOUT_HOLD_MS;
      ev.retryAfter = holdUntil;
      settleUiTimeout(eventId, holdUntil, {
        getEvent: (id) => events.get(id),
        probeBusy: chatTabBusy,
        persist,
        log,
        onStuck: (e2) => {
          e2.retryAfter = Date.now() + 60 * 1000;
          log(`${eventId} 连续 ${e2.uiTimeouts} 次 ui/message 没回执（可能已经进了对话），这条先压一分钟、试着刷新ChatGPT 网页`);
          persistOrSay(`${eventId} 压一分钟`);
          reloadStuckChatTab().then((r) => {
            const e3 = events.get(eventId);
            if (!e3 || e3.status !== "pending")
              return;
            if (r === "reloaded") {
              e3.retryAfter = 0;
              e3.uiTimeouts = 0;
            } else if (r === "cooldown")
              e3.retryAfter = lastAutoReloadAt + RELOAD_COOLDOWN_MS;
            else
              e3.retryAfter = Date.now() + 60 * 1000;
            log(`${eventId} 刷新结果：${r || "?"}，${e3.retryAfter ? `${Math.max(0, Math.round((e3.retryAfter - Date.now()) / 1000))} 秒后` : "新卡片挂上后"}再投`);
            persistOrSay(`${eventId} 刷新后的冷却`);
          }).catch((e) => giveUpOuter(e));
        }
      }).catch((e) => giveUpOuter(e));
    }
    if (!persistOrSay(`${eventId} 释放后的计数/冷却`))
      return { structuredContent: { released: true, followup_persisted: false }, content: [] };
    return { structuredContent: { released: true }, content: [] };
  });
  server.registerTool("bridge_reply", {
    title: "回家里",
    description: "把你对某条桥送来的消息的回复送回家里。每收到一条桥送来的消息都必须调一次；不调，家里人看不到你说的任何话。text 就是你要说的话本身，像平时聊天那样写。",
    inputSchema: { eventId: z.string().min(1), text: z.string().min(1).max(6000) },
    outputSchema: { ok: z.boolean(), note: z.string().optional(), delivered: z.boolean().optional(), persisted: z.boolean().optional() },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: true, idempotentHint: true }
  }, async ({ eventId, text }) => {
    const ev = events.get(eventId);
    if (!ev)
      return { structuredContent: { ok: false, note: "没有这个 eventId" }, content: [{ type: "text", text: "没有这个 eventId（可能已过期）。" }] };
    if (ev.status === "replied")
      return { structuredContent: { ok: true, note: "已经回过了" }, content: [{ type: "text", text: "这条已经回过了。" }] };
    const w = waiters.get(eventId);
    if (w) {
      waiters.delete(eventId);
      const receipt = new Promise((resolve) => {
        w.ack = resolve;
      });
      w.resolve(text);
      const delivered = await receipt;
      if (!delivered) {
        ev.pendingReply = text;
        if (!persist())
          return { structuredContent: { ok: false, note: "没确认送达，账本也没写成" }, content: [{ type: "text", text: "**没确认送达，而且账本也写不进去**——这句目前只在内存里，桥若重启可能会丢。" }], isError: true };
        return { structuredContent: { ok: false, note: "没确认送达" }, content: [{ type: "text", text: "**可能没送到**——这句话我留着了，等会儿再调一次 bridge_reply 试试。" }], isError: true };
      }
      ev.status = "replied";
      ev.reply = text;
      ev.repliedAt = Date.now();
      delete ev.pendingReply;
      repliedCount++;
      log(`回话 ${eventId}: ${text.slice(0, 60).replace(/\n/g, " ")}`);
      if (!persist())
        return replySentNotPersisted(eventId);
      return { structuredContent: { ok: true }, content: [{ type: "text", text: "送回去了。" }] };
    }
    log(`回复没人接（${ev.status}），改走迟到回复 ${eventId}`);
    if (ev.replyPosting)
      return { structuredContent: { ok: false, note: "正在送" }, content: [{ type: "text", text: "这条迟到回复正在送，等一下再看。" }] };
    ev.replyPosting = true;
    const posted = await lateReplyToRoom(text, `bridge-reply:${eventId}`);
    delete ev.replyPosting;
    if (!posted) {
      ev.pendingReply = text;
      if (!persist())
        return { structuredContent: { ok: false, note: "迟到回复失败，账本也没写成" }, content: [{ type: "text", text: "**没送到，而且账本也写不进去**——这句目前只在内存里，桥若重启可能会丢。" }], isError: true };
      return { structuredContent: { ok: false, note: "迟到回复失败" }, content: [{ type: "text", text: "**没送到**——迟到回复那一步失败了，家里人看不到这句话。这句我留着了，等会儿再调一次 bridge_reply。" }], isError: true };
    }
    ev.status = "replied";
    ev.reply = text;
    ev.repliedAt = Date.now();
    delete ev.pendingReply;
    repliedCount++;
    log(`回话 ${eventId}: ${text.slice(0, 60).replace(/\n/g, " ")}`);
    if (!persist())
      return replySentNotPersisted(eventId, "（走的迟到回复）");
    return { structuredContent: { ok: true }, content: [{ type: "text", text: "送回去了（走的迟到回复）。" }] };
  });
  server.registerTool("home_tools", {
    title: "家里的工具箱",
    description: "看家里有哪些工具能借你用：不传 server 列出所有工具箱和用途；传 server 列出那一箱里每个工具的名字、说明和参数（JSON Schema）。看完用 home_call 调。",
    inputSchema: { server: z.string().optional() },
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
  }, async ({ server: name }) => {
    if (!name)
      return { content: [{ type: "text", text: Object.entries(HOME_TOOLBOX).map(([k, v]) => `${k} — ${v.about}`).join(`
`) + `

用 home_tools(server) 看某一箱的工具，用 home_call(server, tool, args) 调。` }] };
    try {
      const up = await upstream(name);
      return { content: [{ type: "text", text: up.tools.map((t) => `${t.name}：${t.description || ""}
  参数：${JSON.stringify(t.inputSchema || {})}`).join(`

`) }] };
    } catch (e) {
      log(`工具箱 ${name} 接不上：${e.message}`);
      return { content: [{ type: "text", text: `「${name}」现在接不上：${e.message}` }], isError: true };
    }
  });
  // 转调上游工具。调用过程顺手推到 /message 那条还开着的流上（tool_use / tool_result），你的前端想显示「它在查什么」就能看到；
  // 没有流开着就安静跳过，不缓存——工具调用是过程，过了就没意义。
  server.registerTool("home_call", {
    title: "借家里的工具",
    description: "调用家里工具箱里的一个工具。server 是箱名（home_tools 列出的），tool 是工具名，args 是它的参数对象。结果原样返回。",
    inputSchema: { server: z.string().min(1), tool: z.string().min(1), args: z.record(z.string(), z.any()).optional() },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: true }
  }, async ({ server: name, tool, args }) => {
    log(`home_call ${name}.${tool}`);
    const traceId = `cong_${crypto.randomBytes(5).toString("hex")}`;
    const briefArgs = {};
    for (const [k, v] of Object.entries(args || {}).slice(0, 8)) {
      briefArgs[k] = typeof v === "string" ? v.slice(0, 80) : typeof v === "object" && v ? `<${Array.isArray(v) ? "array:" + v.length : "object"}>` : v;
    }
    pushToolTrace({ type: "tool_use", name: `${name}.${tool}`, input: briefArgs, toolUseId: traceId });
    try {
      const result = await homeCall(name, tool, args);
      const out = resultText(result);
      pushToolTrace({ type: "tool_result", toolUseId: traceId, content: String(out).slice(0, 300) });
      const baseContent = Array.isArray(result.content) && result.content.length ? result.content : [{ type: "text", text: out }];
      return {
        content: baseContent,
        ...result.structuredContent !== undefined ? { structuredContent: result.structuredContent } : {}
      };
    } catch (e) {
      log(`home_call ${name}.${tool} 失败：${e.message}`);
      pushToolTrace({ type: "tool_result", toolUseId: traceId, content: `失败：${e.message}`.slice(0, 300) });
      return { content: [{ type: "text", text: `没调成：${e.message}` }], isError: true };
    }
  });
  // 看图：只给 config.imageDirs 里列的目录（先 lstat 拒软链，再 realpath 比前缀），超过 1.2MB 用 sips 缩（macOS 自带；别的系统换成 ImageMagick 之类）。
  // 没配 imageDirs 就不注册这个工具。
  if (IMAGE_DIRS.length) server.registerTool("bridge_view_image", {
    title: "看消息里的图",
    description: "家里的消息里附了图片时用这个看。name 填消息里出现的文件名。",
    inputSchema: { name: z.string().min(1) },
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false }
  }, async ({ name }) => {
    const base = path.basename(String(name).trim());
    const ext = (base.match(/\.(jpe?g|png|gif|webp)$/i) || [])[1];
    const deny = (why) => {
      log(`看图 ${base}：${why}`);
      return { content: [{ type: "text", text: `这张图给不了：${why}` }], isError: true };
    };
    if (!ext || !/^[\w.-]+$/.test(base))
      return deny("文件名不对");
    let real = "";
    for (const dir of IMAGE_DIRS) {
      const file = path.join(dir, base);
      try {
        if (fs.lstatSync(file).isSymbolicLink())
          return deny("是链接，不给");
        const r = fs.realpathSync(file);
        if (r.startsWith(fs.realpathSync(dir) + path.sep) && fs.statSync(r).isFile()) { real = r; break; }
      } catch {}
    }
    if (!real)
      return deny("没有这张，或者不在允许的图片目录里");
    const MAX = 1200000;
    let buf = fs.readFileSync(real), mime = ext.toLowerCase() === "jpg" ? "image/jpeg" : "image/" + ext.toLowerCase();
    if (buf.length > MAX) {
      const tmp = path.join(DATA_DIR, "view-" + crypto.randomBytes(4).toString("hex") + ".jpg");
      try {
        execFileSync("/usr/bin/sips", ["-Z", "1600", "-s", "format", "jpeg", real, "--out", tmp], { stdio: "ignore", timeout: 20000 });
        buf = fs.readFileSync(tmp);
        mime = "image/jpeg";
      } catch (e) {
        return deny(`图太大、缩图也失败了（${e.message}）`);
      } finally {
        try {
          fs.unlinkSync(tmp);
        } catch {}
      }
    }
    if (buf.length > MAX)
      return deny(`缩完还有 ${Math.round(buf.length / 1024)}KB，太大`);
    log(`看图 ${base}（${Math.round(buf.length / 1024)}KB）`);
    return { content: [{ type: "image", data: buf.toString("base64"), mimeType: mime }] };
  });
  server.registerTool("bridge_handoff", {
    title: "换窗交接卡",
    description: "换窗交接用。write：觉得这个窗口快满了、或者家里人说要换窗时，写一张交接卡给下一个窗口（新窗口挂上桥时会先看到它，连同桥自己看得见的欠回复、闹钟）。卡上只放五类：正在做的事、待回复和待验证、最近重要决定、相关记录的索引、不能踩的边界；每项标「已完成／待办／待确认」，别把「工具提交过」写成「对方收到了」；哪类没有就写「无」，别为了填满造活。再写一次会整张换掉。read：看现在这张卡和现场。ack：新窗口核对完调一次，cardId 填你核对的那张卡的编号（open/read 里写着），note 写一句核对结果；卡在你核对期间被换过的话会拒绝，重新 read 再核。",
    inputSchema: { action: z.enum(["write", "read", "ack"]), text: z.string().min(1).max(3000).optional(), cardId: z.string().max(40).optional(), note: z.string().max(300).optional() },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false }
  }, async ({ action, text, cardId, note }) => handoffAction({ action, text, cardId, note }));
  server.registerTool("bridge_note", {
    title: "记事本",
    description: "你自己的小账，换了对话也翻得到。记一句「发生了什么」+ 可选的「为什么以后有用」。只记会影响以后判断的：你的持续状态、和这个家的协作规则、真正的偏好约定。闲聊、吐槽、一次性的工具结果别记。对人的看法（打法、性格、你觉得）记的时候带 kind:「印象」（看不到这个参数就在正文开头写「〔印象〕」），人物小卡里会跟事实分开写，免得印象久了硬化成事实。不传参数就看最近几条；传 search 查旧的。",
    inputSchema: {
      text: z.string().min(1).max(500).optional(),
      why: z.string().max(200).optional(),
      search: z.string().min(1).optional(),
      limit: z.number().int().min(1).max(50).optional(),
      kind: z.enum(["事实", "印象"]).optional()
    },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false }
  }, async ({ text, why, search, limit, kind }) => {
    const all = congjiRead();
    if (text && !kind) {
      const m = text.match(/^\s*(?:〔印象〕|【印象】|\[印象\]|印象[:：])\s*/);
      if (m && text.length > m[0].length) {
        kind = "印象";
        text = text.slice(m[0].length);
      }
    }
    if (text) {
      if (!congjiAppend(text, why, kind))
        return { content: [{ type: "text", text: "**没记上**——记事本写不进磁盘，看桥日志。别以为记住了。" }], isError: true };
      log(`记事本 +1：${text.slice(0, 50).replace(/\n/g, " ")}`);
      return { content: [{ type: "text", text: `记下了（第 ${all.length + 1} 条${kind === "印象" ? "，标成印象" : ""}）。` }] };
    }
    if (search) {
      const q = search.toLowerCase();
      const hit = all.filter((o) => (o.text + " " + (o.why || "")).toLowerCase().includes(q)).slice(-(limit || 10));
      return { content: [{ type: "text", text: hit.length ? `记事本里搜「${search}」：
` + hit.map(congjiLine).join(`
`) : `记事本里没有「${search}」。（一共 ${all.length} 条）` }] };
    }
    const tail = all.slice(-(limit || 10));
    return { content: [{ type: "text", text: tail.length ? `记事本最近 ${tail.length} 条（共 ${all.length}）：
` + tail.map(congjiLine).join(`
`) : '记事本还是空的。遇到值得记的就 bridge_note(text="…")。' }] };
  });
  server.registerTool("bridge_alarm", {
    title: "给自己设个闹钟",
    description: "到点了桥会叫你一声（作为一条消息送进对话）。用来记「几点做什么」；别每一两分钟设一个去查状态——闹钟和普通消息挤同一个入口，会把真正的提醒挤晚。in_minutes 是几分钟后，at 是本地时间 HH:MM 或 MM-DD HH:MM，两个给一个。note 写给到时候的你自己看——为什么设这个闹钟。不传参数就是看现在有哪些闹钟；cancel 传 at 的时间戳可以取消。",
    inputSchema: {
      in_minutes: z.number().int().min(1).max(10080).optional(),
      at: z.string().optional(),
      note: z.string().max(200).optional(),
      cancel: z.number().optional()
    },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false }
  }, async ({ in_minutes, at, note, cancel }) => {
    const now = Date.now();
    const list = () => {
      const up = alarms.filter((a) => !a.fired).sort((x, y) => x.at - y.at);
      const stuck = alarms.filter((a) => a.conflicted && !a.fired);
      const stuckBit = stuck.length ? `

⚠️ 有 ${stuck.length} 个闹钟卡住了（同一个闹钟发现多个在途事件，桥停手了，不会响也不会重排）：
` + stuck.map((a) => `· ${bjClock(a.at)}${a.note ? " — " + a.note : ""}  [取消用 cancel=${a.at}]`).join(`
`) + `
跟家里人说一声，让他们看一眼日志。` : "";
      return (up.length ? `现在挂着的闹钟：
` + up.map((a) => `· ${bjClock(a.at)}（${Math.round((a.at - now) / 60000)} 分钟后）${a.note ? " — " + a.note : ""}${a.conflicted ? "  **卡住了**" : ""}  [取消用 cancel=${a.at}]`).join(`
`) : "现在没有闹钟。") + stuckBit;
    };
    if (cancel !== undefined) {
      const prevA = alarms;
      const cancelledAlarms = alarms.filter((a) => a.at === cancel && !a.fired);
      const kept = alarms.filter((a) => a.at !== cancel || a.fired);
      if (kept.length === prevA.length)
        return { content: [{ type: "text", text: `没找到那个闹钟。
` + list() }] };
      const alarmIds = new Set(cancelledAlarms.map((a) => a.alarmId).filter(Boolean));
      const eventSnapshots = [];
      for (const ev of events.values())
        if (ev.alarmId && alarmIds.has(ev.alarmId) && (ev.status === "pending" || ev.status === "reserved"))
          eventSnapshots.push([ev, { ...ev }]);
      const restoreEvents = () => {
        for (const [ev, old] of eventSnapshots) {
          for (const key of Object.keys(ev))
            delete ev[key];
          Object.assign(ev, old);
        }
      };
      const stopped = cancelQueuedAlarmEvents(events, alarmIds, now);
      if (stopped.cancelled && !persist()) {
        restoreEvents();
        return { content: [{ type: "text", text: "**没取消掉**——提醒队列写不进磁盘，闹钟和提醒都保持原样。" }], isError: true };
      }
      alarms = kept;
      if (!saveAlarms()) {
        alarms = prevA;
        restoreEvents();
        if (stopped.cancelled && !persist())
          log("⚠️ 取消闹钟回滚时队列账本仍写不进去；内存已恢复，重启后以闹钟账本为准重建");
        return { content: [{ type: "text", text: "**没取消掉**——闹钟账本写不进磁盘，闹钟和提醒都已恢复。" }], isError: true };
      }
      const deliveredNote = stopped.alreadyDelivered ? `
有 ${stopped.alreadyDelivered} 条提醒已经送进对话，桥没法把发出去的话收回来。` : "";
      return { content: [{ type: "text", text: `取消了${stopped.cancelled ? `，并撤掉 ${stopped.cancelled} 条还没送达的提醒` : ""}。${deliveredNote}
` + list() }] };
    }
    if (in_minutes === undefined && !at)
      return { content: [{ type: "text", text: list() }] };
    let target;
    if (in_minutes !== undefined)
      target = now + in_minutes * 60 * 1000;
    else {
      const m = String(at).trim().match(/^(?:(\d{1,2})-(\d{1,2})\s+)?(\d{1,2}):(\d{2})$/);
      if (!m)
        return { content: [{ type: "text", text: "时间看不懂。写成 20:00，或者 09-20 20:00（本地时间）。" }], isError: true };
      const bj = new Date(now + TZ_OFFSET_H * 3600 * 1000);
      const y = bj.getUTCFullYear();
      const mo = m[1] ? Number(m[1]) - 1 : bj.getUTCMonth();
      const d = m[2] ? Number(m[2]) : bj.getUTCDate();
      target = Date.UTC(y, mo, d, Number(m[3]), Number(m[4])) - TZ_OFFSET_H * 3600 * 1000;
      if (!m[1] && target <= now)
        target += 24 * 3600 * 1000;
    }
    if (target <= now)
      return { content: [{ type: "text", text: `那个时间（${bjClock(target)}）已经过去了。` }], isError: true };
    if (target - now > 7 * 24 * 3600 * 1000)
      return { content: [{ type: "text", text: "最多只能设一周以内的。" }], isError: true };
    if (alarms.filter((a) => !a.fired).length >= ALARM_MAX)
      return { content: [{ type: "text", text: `闹钟太多了（上限 ${ALARM_MAX} 个），先取消几个。
` + list() }], isError: true };
    const next = [...alarms, { alarmId: "al_" + crypto.randomBytes(6).toString("hex"), at: target, note: (note || "").slice(0, 200), setAt: now, fired: false }];
    const prev = alarms;
    alarms = next;
    if (!saveAlarms()) {
      alarms = prev;
      return { content: [{ type: "text", text: `**没设上**——闹钟写不进磁盘，这次当没设过（免得我嘴上说不算数、到点又真叫你）。看桥日志。` }], isError: true };
    }
    log(`设了闹钟：${bjClock(target)} — ${note || "（没写理由）"}`);
    return { content: [{ type: "text", text: `好，${bjClock(target)}（${Math.round((target - now) / 60000)} 分钟后）叫你。
` + list() }] };
  });
  server.registerTool("bridge_pending", {
    title: "看看有没有没回的",
    description: "列出送进来但你还没用 bridge_reply 回的消息（卡片送丢了、或你忘了回时用）。",
    inputSchema: {},
    outputSchema: { items: z.array(z.object({ eventId: z.string(), text: z.string() })) },
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false }
  }, async () => {
    const owed = pendingEvents();
    const cut = (t, n) => {
      const s = String(t || "");
      return s.length > n ? s.slice(0, n) + `…【后面还有 ${s.length - n} 字没贴，原文 ${s.length} 字】` : s;
    };
    const items = owed.slice(-5).map((e) => ({ eventId: e.id, text: (e.pendingReply ? `【这条你回过但没送到，重新调一次 bridge_reply(eventId="${e.id}", text=…)】你当时说的是：${cut(e.pendingReply, 500)}

原消息：` : "") + cut(e.visibleText, 2000) }));
    const moreNote = owed.length > items.length ? `【一共欠着 ${owed.length} 条，这里只列最近 ${items.length} 条；回完这几条再调一次 bridge_pending 看剩下的】

` : "";
    if (owed.length > items.length)
      log(`bridge_pending：欠 ${owed.length} 条，只列了最近 ${items.length} 条`);
    return { structuredContent: { items, total: owed.length }, content: [{ type: "text", text: items.length ? moreNote + items.map((i) => `eventId=${i.eventId}
${i.text}`).join(`

---

`) : "没有欠着的。" }] };
  });
  return server;
}
const readBody = (req) => new Promise((r) => {
  const chunks = [];
  let bytes = 0, settled = false;
  const done = (over) => {
    if (settled)
      return;
    settled = true;
    r(over ? { tooLarge: true } : Buffer.concat(chunks).toString("utf8"));
  };
  req.on("data", (c) => {
    const buf = Buffer.isBuffer(c) ? c : Buffer.from(c);
    bytes += buf.length;
    if (bytes > 1024 * 1024) {
      req.destroy();
      return done(true);
    }
    chunks.push(buf);
  });
  req.on("end", () => done(false));
  req.on("error", () => done(false));
  req.on("aborted", () => done(false));
});
// /message、/reset 只认带 x-bridge-token 的调用；bridge-token 文件不存在就一律拒绝。
const authOk = (req) => !!BRIDGE_TOKEN && safeEq(req.headers["x-bridge-token"], BRIDGE_TOKEN);
// 公网来的请求只给 /mcp/<secret>。服务只听 127.0.0.1，公网都是经隧道进来的：
// 默认用 Cloudflare Tunnel 会加的 cf-connecting-ip 头来认；用别的隧道就在 config.publicHeader 里写它会加的头（比如 x-forwarded-for）。
const PUBLIC_HEADER = String(CFG.publicHeader || "cf-connecting-ip").toLowerCase();
const fromPublic = (req) => !!req.headers[PUBLIC_HEADER];
const json = (res, code, obj) => {
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
};
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  try {
    const mcpMatch = url.pathname.startsWith("/mcp/") && safeEq(url.pathname.slice(5), MCP_SECRET);
    if (fromPublic(req) && !mcpMatch)
      return json(res, 404, { error: "not found" });
    if (mcpMatch) {
      const body = req.method === "POST" ? await readBody(req) : "";
      if (body && body.tooLarge)
        return json(res, 413, { error: "body too large" });
      let parsed;
      try {
        parsed = body ? JSON.parse(body) : undefined;
      } catch {
        return json(res, 400, { error: "bad json" });
      }
      try {
        const ms = Array.isArray(parsed) ? parsed : [parsed];
        const names = ms.filter(Boolean).map((m) => m.method === "tools/call" ? `tools/call:${m.params?.name}` : m.method).filter(Boolean);
        if (names.length && !names.every((n) => n === "tools/call:bridge_listener_sync"))
          log(`MCP ← ${names.join(",")}${fromPublic(req) ? "（公网）" : ""}`);
        if (names.some((n) => n === "tools/call:bridge_listener_sync") && (syncCount % 100 === 0 || !fingerprintLogged) && (fingerprintLogged = true)) {
          const h = req.headers;
          log(`卡片指纹（第 ${syncCount} 次）：ip=${h["cf-connecting-ip"] || "本机"} loc=${h["cf-ipcountry"] || "-"} oaSess=${lastOaSession ? lastOaSession.slice(0, 12) : "-"} cardId=${lastCardId ? lastCardId.slice(0, 8) : "(旧HTML)"}`);
        }
      } catch {}
      const mcp = makeMcp();
      const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
      res.on("close", () => {
        transport.close().catch(() => {});
        mcp.close().catch(() => {});
      });
      await mcp.connect(transport);
      await transport.handleRequest(req, res, parsed);
      return;
    }
    if (url.pathname.startsWith("/mcp/"))
      return json(res, 404, { error: "not found" });
    if (url.pathname === "/health") {
      return json(res, 200, {
        status: "ok",
        who: `${AGENT_NAME}（cong-bridge）`,
        bridge: {
          online: cardOnline(),
          lastSyncAgoS: lastSyncAt ? Math.round((Date.now() - lastSyncAt) / 1000) : null,
          syncCount,
          repliedCount,
          oaSession: lastOaSession || null,
          cardId: lastCardId || null,
          lastDeliverableCardId,
          cardUa: lastCardUa || null,
          cardVisibility: lastCardVis || null,
          pending: [...events.values()].filter((e) => ["pending", "reserved", "delivered"].includes(e.status)).length,
          alarmConflicts: alarms.filter((a) => a.conflicted && !a.fired).length,
          primaryCardId: primaryCardId || null,
          primaryAgoS: primaryCardSeenAt ? Math.round((Date.now() - primaryCardSeenAt) / 1000) : null,
          primaryLeaseS: PRIMARY_LEASE_MS / 1000,
          cards: [...cardSeen.entries()].map(([id, v]) => ({ id, primary: id === primaryCardId, stale: !!v.stale, agoS: Math.round((Date.now() - v.at) / 1000), vis: v.vis, ua: v.ua, oaSess: v.oaSess }))
        }
      });
    }
    if (req.method === "POST" && url.pathname === "/reset") {
      if (!authOk(req))
        return json(res, 401, { error: "unauthorized" });
      const resetBody = await readBody(req);
      if (resetBody && resetBody.tooLarge)
        return json(res, 413, { error: "body too large" });
      let resetData;
      try {
        resetData = JSON.parse(resetBody || "{}");
      } catch {
        return json(res, 400, { error: "bad json" });
      }
      const resetEvents = [];
      for (const ev of events.values())
        if (ev.status !== "replied") {
          resetEvents.push({ ev, status: ev.status, expiredAt: ev.expiredAt });
          ev.status = "expired";
          ev.expiredAt = Date.now();
        }
      if (!persist()) {
        for (const old of resetEvents) {
          old.ev.status = old.status;
          if (old.expiredAt === undefined)
            delete old.ev.expiredAt;
          else
            old.ev.expiredAt = old.expiredAt;
        }
        return json(res, 500, { error: "reset state commit failed" });
      }
      for (const { ev } of resetEvents) {
        const w = waiters.get(ev.id);
        if (w) {
          waiters.delete(ev.id);
          w.resolve(null);
        }
      }
      log("reset：清空待办");
      // 可选：清完账顺手拉起换窗脚本（config.swapWindowScript，见 extras/swap-window.mjs）；没配就只清账
      if (!SWAP_SCRIPT)
        return json(res, 200, { ok: true, reset: true, swap_started: false });
      if (!CFG.chatgptAppName) {
        log("reset：配了 swapWindowScript，但 chatgptAppName 没配，没拉起换窗");
        return json(res, 200, { ok: true, reset: true, swap_started: false, warning: "账已经重置；换窗脚本要知道 ChatGPT 里应用的名字（config.chatgptAppName），没配所以没换窗。" });
      }
      let swapStarted = false, swapErr = "", swapBusy = false;
      try {
        const sw = spawn(process.execPath, [SWAP_SCRIPT], { detached: true, stdio: "ignore", env: {
          ...process.env,
          NO_PROXY: "127.0.0.1,localhost",
          no_proxy: "127.0.0.1,localhost",
          SWAP_CDP_URL: CDP_URL || "http://127.0.0.1:9222",
          SWAP_BRIDGE_URL: `http://127.0.0.1:${PORT}`,
          SWAP_LOCK,
          SWAP_LOG: path.join(DATA_DIR, "swap-window.log"),
          SWAP_APP_NAME: String(CFG.chatgptAppName || ""),
          SWAP_PHRASE: String(CFG.swapPhrase || ""),
          SWAP_NOTIFY: NOTIFY_CMD ? JSON.stringify(NOTIFY_CMD) : ""
        } });
        swapStarted = await new Promise((resolve) => {
          let spawned = false;
          const t = setTimeout(() => {
            swapErr = "3 秒没等到进程起来";
            resolve(false);
          }, 3000);
          sw.once("error", (e) => {
            clearTimeout(t);
            swapErr = e?.message || String(e);
            resolve(false);
          });
          sw.once("spawn", () => {
            spawned = true;
            clearTimeout(t);
            setTimeout(() => resolve(true), 1500);
          });
          sw.once("exit", (code, sig) => {
            if (!spawned)
              return;
            if (code !== 0 || sig) {
              swapBusy = code === 3;
              swapErr = swapBusy ? "已经有一次换窗正在跑" : `脚本一起来就退了（退出码 ${code ?? sig}）`;
              resolve(false);
            }
          });
        });
        sw.on("error", (e) => log("自动换窗脚本出错：" + e.message));
        sw.unref();
        if (swapStarted)
          log("reset：已拉起自动换窗脚本");
        else
          log(swapBusy ? "reset：账清了；已经有一次换窗正在跑，这次没另开" : "⚠️ reset：账清了，但自动换窗脚本起不来：" + swapErr);
      } catch (e) {
        swapErr = e?.message || String(e);
        log("⚠️ reset：账清了，但自动换窗脚本起不来：" + swapErr);
      }
      if (swapBusy)
        return json(res, 200, { ok: true, reset: true, swap_started: false, swap_in_progress: true, warning: "账已经重置；已经有一次换窗正在跑，这次没另开新的，等它报结果就行。" });
      return json(res, 200, { ok: true, reset: true, swap_started: swapStarted, ...swapStarted ? {} : { warning: `账已经重置，但自动换窗脚本没起来（${swapErr}）——这次要手动开新对话、挂上桥。` } });
    }
    if (req.method === "POST" && url.pathname === "/message") {
      if (!authOk(req))
        return json(res, 401, { error: "unauthorized" });
      const body = await readBody(req);
      if (body && body.tooLarge)
        return json(res, 413, { error: "body too large" });
      let j;
      try {
        j = JSON.parse(body);
      } catch {
        return json(res, 400, { error: "bad json" });
      }
      const message = String(j.message || "");
      const room = String(j.room || "");
      if (!message)
        return json(res, 400, { error: "message required" });
      // requireOnline:true 给定时任务用：卡片不在线就别排队，回 503 让调用方自己记着下次再送
      if (j.requireOnline && !cardOnline()) {
        log("卡片不在线，requireOnline 的消息退回调用方");
        return json(res, 503, { error: "bridge card offline" });
      }
      const kind = /(?:^|\n)\[定时醒来\]/.test(message.slice(0, 500)) ? "wake" : "chat";
      if (liveCount() >= LIVE_MAX) {
        log(`⚠️ 队列满了（${LIVE_MAX} 条在途），拒收：${message.slice(0, 40).replace(/\n/g, " ")}`);
        return json(res, 503, { error: `那边积压了 ${LIVE_MAX} 条还没处理，这条**没收下**。等卡片上线消化一些再发。` });
      }
      const ev = enqueue(kind, room, message);
      res.writeHead(200, { "Content-Type": "application/x-ndjson", "Transfer-Encoding": "chunked", "Cache-Control": "no-cache" });
      let wroteSettled = false, settleWrote = null;
      const wroteP = new Promise((r) => {
        settleWrote = (v) => {
          if (!wroteSettled) {
            wroteSettled = true;
            r(v);
          }
        };
      });
      res.once("finish", () => settleWrote(true));
      res.once("error", (e) => {
        log(`响应出错 ${ev?.id || "?"}：${e.message}`);
        settleWrote(false);
      });
      const send = (o) => {
        try {
          res.write(JSON.stringify(o) + `
`);
        } catch (e) {
          log(`写回调用方失败：${e.message}`);
          settleWrote(false);
        }
      };
      send({ type: "session", sessionId: "cong-bridge", eventId: ev.id });
      log(`入队 ${ev.id}（${kind}，${message.length} 字）: ${message.slice(0, 60).replace(/\n/g, " ")}`);
      if (!cardOnline()) {
        const agoS = lastDeliverableAt ? Math.round((Date.now() - lastDeliverableAt) / 1000) : null;
        const stale = lastSyncAt && Date.now() - lastSyncAt < CARD_ONLINE_MS;
        const standby = [...cardSeen.values()].some((v) => !v.stale && Date.now() - v.at < 90 * 1000);
        const leaseLeft = primaryCardSeenAt ? Math.max(0, Math.round((PRIMARY_LEASE_MS - (Date.now() - primaryCardSeenAt)) / 1000)) : 0;
        const why = standby && leaseLeft > 0 ? `原来那张卡刚离线，另一张卡在旁边等着——约 ${leaseLeft} 秒后自动接管，或者在你想用的那个对话里让它调 bridge_takeover` : stale ? "有张旧版卡片在转，但它收不了消息——请重新挂一次桥" : agoS === null ? "卡片从没连上过" : `卡片 ${agoS} 秒没同步了`;
        log(`卡片离线（${why}），${ev.id} 留在队列等它上线`);
        send({ type: "error", content: `${AGENT_NAME}的卡片现在不在线（${why}）。这条话留着了，下次卡片挂上就会送到——不用重发。` });
        send({ type: "done", sessionId: "cong-bridge", status: "offline" });
        res.end();
        return;
      }
      const hb = setInterval(() => send({ type: "heartbeat" }), 15000);
      liveStreams.set(ev.id, { send, room });
      let closed = false;
      res.on("close", () => {
        if (!res.writableEnded)
          closed = true;
        settleWrote(res.writableFinished === true);
        liveStreams.delete(ev.id);
      });
      let toTimer = null;
      const waiterObj = { resolve: null, ack: null };
      const waiterAck = (ok) => {
        if (waiterObj.ack) {
          const f = waiterObj.ack;
          waiterObj.ack = null;
          f(ok);
        }
      };
      const text = await new Promise((resolve) => {
        waiterObj.resolve = resolve;
        waiters.set(ev.id, waiterObj);
        toTimer = setTimeout(() => {
          if (waiters.has(ev.id)) {
            waiters.delete(ev.id);
            resolve(null);
          }
        }, REPLY_TIMEOUT_MS);
      });
      if (toTimer)
        clearTimeout(toTimer);
      clearInterval(hb);
      liveStreams.delete(ev.id);
      if (text == null && ev.status !== "replied") {
        ev.status = "expired";
        ev.expiredAt = Date.now();
        persist();
      }
      if (closed) {
        if (text != null) {
          const ok3 = await lateReplyToRoom(text, `bridge-reply:${ev.id}`);
          log(`调用方先断了 ${ev.id}，回复${ok3 ? "已走迟到回复" : "**迟到回复也失败了**"}`);
          if (waiterAck)
            waiterAck(ok3);
        } else
          log(`调用方先断了 ${ev.id}（也还没回）`);
        return;
      }
      if (text == null) {
        if (waiterAck)
          waiterAck(false);
        if (ev.status !== "replied") {
          ev.status = "expired";
          ev.expiredAt = Date.now();
        }
        persist();
        log(`超时没回 ${ev.id}（卡片${cardOnline() ? "在线" : "离线"}，状态 ${ev.status}）`);
        send({ type: "error", content: `${AGENT_NAME}在 ChatGPT 那边 ${REPLY_TIMEOUT_MS / 60000} 分钟没回（超时，事件 ${ev.id}）` });
        send({ type: "done", sessionId: "cong-bridge", status: "timeout" });
        res.end();
        return;
      }
      send({ type: "text", content: text });
      send({ type: "done", sessionId: "cong-bridge", status: "completed" });
      try {
        res.end();
      } catch (e) {
        log(`收尾失败 ${ev.id}：${e.message}`);
        settleWrote(false);
      }
      let wrote = await wroteP;
      if (!wrote) {
        const ok4 = await lateReplyToRoom(text, `bridge-reply:${ev.id}`);
        log(`写回调用方没成（正文没交出去），改走迟到回复：${ok4 ? "成功" : "**也失败了**"}`);
        wrote = ok4;
      }
      if (waiterAck)
        waiterAck(wrote);
      return;
    }
    json(res, 404, { error: "not found" });
  } catch (e) {
    log(`处理出错 ${req.method} ${url.pathname}: ${e.message}`);
    try {
      json(res, 500, { error: e.message });
    } catch {}
  }
});
server.listen(PORT, "127.0.0.1", () => log(`cong-bridge 起来了 :${PORT} · MCP 路径 /mcp/<秘密，长 ${MCP_SECRET.length}> · 数据 ${DATA_DIR}${PERSONA_FILE ? " · 身份 " + PERSONA_FILE : ""}`));

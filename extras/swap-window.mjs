#!/usr/bin/env node
// 可选附件：一键换窗。
// ChatGPT 的对话越聊越长会变慢、变贵，迟早要开新对话。这个脚本做的事等于你自己动手：
//   在开着远程调试口的 Chrome 里开一个新 chatgpt.com 标签 → 输入框里 @ 你的应用、打一句口令发出去
//   → 等新对话里的卡片真的挂上桥、拿到投递权 → 再关掉旧的桥标签。
// 由 server.mjs 的 /reset 拉起（config.swapWindowScript 指向本文件），配置全部从环境变量拿（server 会填好）：
//   SWAP_CDP_URL     Chrome 远程调试地址，例如 http://127.0.0.1:9222
//   SWAP_BRIDGE_URL  桥自己的地址（读 /health 判断新卡有没有接上）
//   SWAP_LOCK        锁文件（和 server 的标签巡检共用，一次只跑一个）
//   SWAP_LOG         日志文件
//   SWAP_APP_NAME    你在 ChatGPT 里给应用起的名字（输入框里 @ 它）
//   SWAP_PHRASE      发给新对话的口令，默认「挂上桥」
//   SWAP_NOTIFY      成败通知命令（JSON 数组，文字放在环境变量 TEXT 里）；不配就只写日志
// 脆弱的地方：完全依赖 ChatGPT 网页的 DOM（输入框、发送键、@ 候选），网页改版就可能失效。失效时只会留着新旧标签不动、退非零，不会丢消息。
import fs from "node:fs";
import { spawn } from "node:child_process";
import { acquireSwapLock } from "../src/bridge-logic.mjs";

const CDP = process.env.SWAP_CDP_URL || "http://127.0.0.1:9222";
const BRIDGE = process.env.SWAP_BRIDGE_URL || "http://127.0.0.1:18461";
const LOCK = process.env.SWAP_LOCK || "/tmp/cong-swap-window.lock";
const LOG = process.env.SWAP_LOG || "";
const APP_NAME = process.env.SWAP_APP_NAME || "";
const PHRASE = process.env.SWAP_PHRASE || "挂上桥";
const NOTIFY = (() => { try { const a = JSON.parse(process.env.SWAP_NOTIFY || "null"); return Array.isArray(a) && a.length ? a : null; } catch { return null; } })();
// 新旧版输入框、发送键都认；繁中 / 简中 / 英文界面都能找到
const INPUT_SEL = "#prompt-textarea, .ProseMirror[contenteditable=true]";
const SEND_SEL = "[data-testid=send-button], #composer-submit-button, button[aria-label='傳送'], button[aria-label='发送'], button[aria-label='Send prompt'], button[aria-label='Send']";
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (m) => { const line = `[swap] ${new Date().toISOString()} ${m}`; console.log(line); if (LOG) try { fs.appendFileSync(LOG, line + "\n"); } catch {} };

function say(text) {
  if (!NOTIFY) { log("（没配通知命令）" + text); return Promise.resolve(); }
  return new Promise((resolve) => {
    const c = spawn(NOTIFY[0], NOTIFY.slice(1), { env: { ...process.env, TEXT: text, DEDUPE_KEY: "swap-" + Date.now() }, stdio: "ignore" });
    const t = setTimeout(() => { try { c.kill("SIGKILL"); } catch {} resolve(); }, 30000);
    c.on("exit", () => { clearTimeout(t); resolve(); }); c.on("error", () => { clearTimeout(t); resolve(); });
  });
}
async function tabs() { return (await (await fetch(`${CDP}/json`)).json()).filter((t) => t.type === "page"); }
async function health() { return (await (await fetch(`${BRIDGE}/health`)).json()).bridge || {}; }

function cdpSession(wsUrl) {
  const ws = new WebSocket(wsUrl);
  let n = 0; const pend = new Map();
  ws.onmessage = (m) => { const d = JSON.parse(m.data); if (d.id && pend.has(d.id)) { pend.get(d.id)(d); pend.delete(d.id); } };
  const ready = new Promise((res, rej) => { ws.onopen = res; ws.onerror = () => rej(new Error("CDP websocket 连不上")); });
  const send = async (method, params = {}) => { await ready; const id = ++n; ws.send(JSON.stringify({ id, method, params })); return await new Promise((res, rej) => { pend.set(id, res); setTimeout(() => rej(new Error(method + " 超时")), 15000); }); };
  const evalJs = async (expression) => (await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true })).result?.result?.value;
  return { send, evalJs, close: () => ws.close() };
}
async function browserSend(method, params = {}) {
  const v = await (await fetch(`${CDP}/json/version`)).json();
  const ws = new WebSocket(v.webSocketDebuggerUrl);
  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = () => rej(new Error("浏览器 CDP 连不上")); });
  try { return await new Promise((res, rej) => { ws.onmessage = (m) => { const d = JSON.parse(m.data); if (d.id === 1) res(d.result); }; ws.send(JSON.stringify({ id: 1, method, params })); setTimeout(() => rej(new Error(method + " 超时")), 8000); }); }
  finally { ws.close(); }
}
// 某个标签里挂着哪些 iframe 源（应用卡片在 ChatGPT 的沙箱 iframe 里）
async function iframeOriginsInTab(tabId) {
  const { targetInfos } = (await browserSend("Target.getTargets")) || {};
  return new Set((targetInfos || []).filter((t) => t.type === "iframe" && t.parentId === tabId).map((t) => { try { return new URL(t.url).origin; } catch { return ""; } }));
}
// 卡片会把自己的随机编号写在 <html data-cong-cid> 上；从浏览器级 CDP 找到这个标签下的 iframe，逐层读出来
async function cardIdInTab(tabId) {
  const v = await (await fetch(`${CDP}/json/version`)).json();
  const ws = new WebSocket(v.webSocketDebuggerUrl); let n = 0; const pend = new Map();
  ws.onmessage = (m) => { const d = JSON.parse(m.data); if (d.id && pend.has(d.id)) { pend.get(d.id)(d); pend.delete(d.id); } };
  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = () => rej(new Error("浏览器 CDP 连不上")); });
  const send = (method, params = {}, sessionId) => new Promise((res, rej) => { const id = ++n; pend.set(id, res); ws.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) })); setTimeout(() => rej(new Error(method + " 超时")), 8000); });
  try {
    const { result } = await send("Target.getTargets");
    for (const t of (result?.targetInfos || []).filter((t) => t.type === "iframe" && t.parentId === tabId)) {
      const a = await send("Target.attachToTarget", { targetId: t.targetId, flatten: true }); const sid = a.result?.sessionId; if (!sid) continue;
      const ft = await send("Page.getFrameTree", {}, sid);
      const frames = []; const walk = (f) => { frames.push(f.frame.id); (f.childFrames || []).forEach(walk); }; if (ft.result) walk(ft.result.frameTree);
      for (const fid of frames) {
        const w = await send("Page.createIsolatedWorld", { frameId: fid, worldName: "swap" }, sid); const ctx = w.result?.executionContextId; if (!ctx) continue;
        const r = await send("Runtime.evaluate", { expression: "document.documentElement.getAttribute('data-cong-cid')", contextId: ctx, returnByValue: true }, sid);
        const cid = r.result?.result?.value; if (cid) return { cid, origin: new URL(t.url).origin };
      }
      await send("Target.detachFromTarget", { sessionId: sid }).catch(() => {});
    }
    return null;
  } finally { ws.close(); }
}

async function main() {
  // 一次只跑一个：独占创建锁文件，10 分钟后视为过期。没拿到锁退 3，失败退 1——/reset 看到非零退出会把原因带回去，不会假装换好了。
  if (!APP_NAME) { log("没配 SWAP_APP_NAME（config.chatgptAppName），不知道该 @ 谁"); process.exitCode = 1; return; }
  if (!acquireSwapLock(fs, LOCK, 10 * 60 * 1000, log)) { process.exitCode = 3; return; }
  try {
    let before;
    try { before = await tabs(); } catch { throw new Error(`Chrome 调试口 ${CDP} 连不上（Chrome 没开着？）`); }
    const oldIds = before.filter((t) => /^https:\/\/chatgpt\.com\//.test(t.url)).map((t) => t.id);
    log(`开始：旧 chatgpt 标签 ${oldIds.length} 个`);

    const created = await (await fetch(`${CDP}/json/new?https://chatgpt.com/`, { method: "PUT" })).json();
    const s = cdpSession(created.webSocketDebuggerUrl);
    await s.send("Page.bringToFront").catch(() => {});

    let ok = false;
    for (let i = 0; i < 60 && !ok; i++) { await pause(1000); ok = await s.evalJs(`!!document.querySelector(${JSON.stringify(INPUT_SEL)})`).catch(() => false); }
    if (!ok) throw new Error("新页面 60 秒没出现输入框（可能掉登录了）");
    await pause(1500);
    await s.evalJs(`(()=>{const e=document.querySelector(${JSON.stringify(INPUT_SEL)});e.focus();return true})()`);
    // 新对话里不先 @ 应用，ChatGPT 不会把应用的工具交给这个对话。先打 @名字、等候选出来、回车选中变成应用标签，再接着打口令。
    await s.send("Input.insertText", { text: "@" + APP_NAME });
    await pause(1800);
    await s.send("Input.dispatchKeyEvent", { type: "keyDown", key: "Enter", code: "Enter", windowsVirtualKeyCode: 13 });
    await s.send("Input.dispatchKeyEvent", { type: "keyUp", key: "Enter", code: "Enter", windowsVirtualKeyCode: 13 });
    await pause(800);
    const chip = await s.evalJs(`!!document.querySelector(${JSON.stringify(INPUT_SEL)}).querySelector("[app-mention-display-name]")`).catch(() => false);
    if (!chip) throw new Error(`打了「@${APP_NAME}」但没变成应用标签（应用没装或改名了？）——新标签留着，旧标签没关`);
    await s.send("Input.insertText", { text: " " + PHRASE });
    let sent = false;
    for (let i = 0; i < 20 && !sent; i++) {
      await pause(500);
      sent = await s.evalJs(`(()=>{const b=document.querySelector(${JSON.stringify(SEND_SEL)});if(b&&!b.disabled){b.click();return true}return false})()`).catch(() => false);
    }
    if (!sent) throw new Error("找不到发送按钮，「" + PHRASE + "」没发出去");
    log(`已在新标签发出「${PHRASE}」`);

    // 只认新标签里那张卡：读出它的编号，等桥的 /health 里 lastDeliverableCardId 真变成它（拿到投递权）才算接上。
    // 别的设备或旧对话碰巧挂上不算数；认不出来就不关旧标签。
    let found = null;
    for (let i = 0; i < 60 && !found; i++) { await pause(2000); found = await cardIdInTab(created.id).catch(() => null); }
    if (!found) throw new Error("新标签里 2 分钟没看到桥的卡片（AI 可能没调挂卡工具）——新标签留着，旧标签没关");
    const newCid = found.cid;
    log(`新标签里的卡：${newCid}（${found.origin}）`);
    let newPrimary = null;
    for (let i = 0; i < 120 && !newPrimary; i++) {
      await pause(2000);
      const h = await health().catch(() => ({}));
      if (h.lastDeliverableCardId === newCid) newPrimary = newCid;
    }
    if (!newPrimary) throw new Error("新标签的卡挂出来了，但 4 分钟没拿到投递权（旧卡租约还没让出来？）——新标签留着，旧标签没关");
    const url = await s.evalJs("location.href").catch(() => "?");
    s.close();

    // 只关确认挂着同一个应用 iframe 的旧标签；别的 ChatGPT 页（别的对话、草稿）不动。关没关掉只认返回码。
    const closed = [], kept = [], closeFailed = [];
    for (const id of oldIds) {
      const origins = await iframeOriginsInTab(id).catch(() => new Set());
      if (!origins.has(found.origin)) { kept.push(id); continue; }
      const ok = await fetch(`${CDP}/json/close/${id}`).then((r) => r.ok).catch(() => false);
      (ok ? closed : closeFailed).push(id);
    }
    if (closeFailed.length) {
      log(`部分成功：新卡已接上，但 ${closeFailed.length} 个旧标签没关掉：${closeFailed.join(",")}`);
      await say(`换窗只成了一半：新对话已经接上桥，但有 ${closeFailed.length} 个旧标签没关掉，旧卡可能还会抢消息，去手动关一下。`);
      return;
    }
    if (kept.length) log(`有 ${kept.length} 个 ChatGPT 标签里没认出桥的卡，留着没关`);
    log(`成功：新主卡 ${newPrimary}，新对话 ${url}，关掉旧标签 ${closed.length} 个、留着 ${kept.length} 个`);
    await say(`换窗好了：新对话已经挂上桥（${String(url).replace("https://chatgpt.com", "")}），关掉旧标签 ${closed.length} 个${kept.length ? `，另有 ${kept.length} 个 ChatGPT 页没认出桥的卡、留着` : ""}。`);
  } catch (e) {
    process.exitCode = 1;
    log("失败：" + (e?.message || e));
    await say(`自动换窗没成：${e?.message || e}。旧窗口还在，消息不会丢。`);
  } finally {
    try { fs.unlinkSync(LOCK); } catch {}
  }
}
main();

#!/usr/bin/env node
// 可选附件：自动点「传送后续提示？」。
// ChatGPT 规定：应用往对话里送后续消息时，要先弹一个确认框让人点「传送」。桥送进去的每一条家里消息都会卡在这个框上。
// 这个小帮手每 2 秒看一眼 Chrome（远程调试口）里的 chatgpt.com 标签，只点**内容以桥的消息标记开头**的那种框（config.messagePrefix，默认「@cong-bridge」，桥送进去的每条消息都带着它），
// 别的应用弹的框一概不碰。每点一次记一行日志。
// 另外：ChatGPT 标签进了后台，Chrome 会把它限速成大约一分钟才醒一次，确认框跟着一分钟才画出来一次、消息就慢了；
// 所以看到桥的标签在后台，就把它拉回前台。
// 配置（环境变量）：
//   CDP_URL   Chrome 远程调试地址，默认 http://127.0.0.1:9222
//   MESSAGE_PREFIX  和 config.messagePrefix 一样，默认 @cong-bridge
//   LOG_FILE  日志文件（不填就打到标准输出）
// 脆弱的地方：依赖确认框的 DOM 和按钮文字，网页改版就可能失效（失效时只是不点，不会误点别的框）。
import fs from "node:fs";
const CDP = process.env.CDP_URL || "http://127.0.0.1:9222";
const PREFIX = process.env.MESSAGE_PREFIX || "@cong-bridge";
const LOG = process.env.LOG_FILE || "";
const log = (s) => { const line = `[auto-confirm] ${new Date().toISOString()} ${s}`; if (!LOG) return console.log(line); try { fs.appendFileSync(LOG, line + "\n"); } catch {} };
const escRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const PROBE = `(()=>{
  const d = document.querySelector('[role=dialog]'); if (!d) return null;
  const t = d.innerText || '';
  if (!/後續提示|后续提示|follow-up/i.test(t)) return null;
  const box = [...d.querySelectorAll('div,p')].map(e => e.innerText.trim()).filter(x => x.startsWith('@') || x.startsWith(${JSON.stringify(PREFIX)}))[0] || '';
  if (!new RegExp(${JSON.stringify("^" + escRe(PREFIX) + "\\s")}).test(box + ' ')) return { skip: box.slice(0, 40) };
  const b = [...d.querySelectorAll('button')].find(b => /^(傳送|发送|Send)$/.test(b.innerText.trim()));
  if (!b) return { nobtn: true };
  b.click(); return { clicked: box.replace(/\\s+/g, ' ').slice(0, 80) };
})()`;
async function evalIn(wsUrl, expr) {
  return new Promise((resolve) => {
    const ws = new WebSocket(wsUrl); let done = false;
    const fin = (v) => { if (done) return; done = true; try { ws.close(); } catch {} resolve(v); };
    const timer = setTimeout(() => fin(undefined), 4000);
    ws.onopen = () => ws.send(JSON.stringify({ id: 1, method: "Runtime.evaluate", params: { expression: expr, returnByValue: true } }));
    ws.onmessage = (e) => { const m = JSON.parse(e.data); if (m.id === 1) { clearTimeout(timer); fin(m.result?.result?.value); } };
    ws.onerror = () => { clearTimeout(timer); fin(undefined); };
  });
}
let lastSkip = "", hiddenLogged = false;
async function tick() {
  let tabs;
  try { tabs = await (await fetch(`${CDP}/json`, { signal: AbortSignal.timeout(3000) })).json(); } catch { return; }
  for (const t of tabs) {
    if (t.type !== "page" || !/^https:\/\/chatgpt\.com\//.test(t.url || "") || !t.webSocketDebuggerUrl) continue;
    const hidden = await evalIn(t.webSocketDebuggerUrl, "document.visibilityState === 'hidden' && !!document.querySelector('iframe')");
    if (hidden) { await fetch(`${CDP}/json/activate/${t.id}`).catch(() => {}); if (!hiddenLogged) { hiddenLogged = true; log("ChatGPT 标签在后台/窗口被挡住，试着拉回前台（后台会被 Chrome 限速成一分钟一次）"); } }
    else if (hiddenLogged) { hiddenLogged = false; log("ChatGPT 标签回到前台了"); }
    const r = await evalIn(t.webSocketDebuggerUrl, PROBE);
    if (!r) continue;
    if (r.clicked) log(`点了「传送」：${r.clicked}`);
    else if (r.skip && r.skip !== lastSkip) { lastSkip = r.skip; log(`有个框不是桥的，没碰：${r.skip}`); }
    else if (r.nobtn) log("看到桥的确认框但找不到「传送」键（界面改版了？）");
  }
}
log("起来了");
setInterval(() => { tick().catch(e => log("出错：" + (e?.message || e))); }, 2000);

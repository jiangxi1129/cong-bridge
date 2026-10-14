import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { chmodSync, copyFileSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import http from 'node:http'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const srcDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src')
const token = 'replay-bridge-token'
const mcpSecret = 'replay-mcp-secret'

async function freePort() {
  const server = net.createServer()
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const port = server.address().port
  server.close()
  await once(server, 'close')
  return port
}

function decodeRpc(text) {
  const bodies = text.split(/\r?\n/).some((line) => line.startsWith('data:'))
    ? text.split(/\r?\n/).filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trim())
    : [text]
  for (const body of bodies) {
    if (!body || body === '[DONE]') continue
    const message = JSON.parse(body)
    if (message.error) throw new Error(message.error.message || JSON.stringify(message.error))
    if ('result' in message) return message.result
  }
  throw new Error(`no JSON-RPC result: ${text.slice(0, 300)}`)
}

async function waitForHealth(base, child, logs) {
  for (let i = 0; i < 100; i++) {
    if (child.exitCode !== null) throw new Error(`bridge exited ${child.exitCode}: ${logs.join('')}`)
    try {
      const response = await fetch(`${base}/health`)
      if (response.ok) return response.json()
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  throw new Error(`bridge did not start: ${logs.join('')}`)
}

// 在临时目录里起一个真的桥：自己的 config.json、自己的数据目录和钥匙；迟到回复和提醒都写进沙盒里的 said.txt
async function makeSandbox({ alarms, swapScript, noLateReply } = {}) {
  const root = mkdtempSync(path.join(os.tmpdir(), 'cong-bridge-replay-'))
  const dataDir = path.join(root, 'data'), secretDir = path.join(root, 'secrets')
  mkdirSync(dataDir, { recursive: true })
  mkdirSync(secretDir, { recursive: true })
  writeFileSync(path.join(secretDir, 'bridge-token'), token)
  writeFileSync(path.join(secretDir, 'mcp-secret'), mcpSecret)
  writeFileSync(path.join(root, 'persona.md'), '你是离线回放里的测试助手。')
  writeFileSync(path.join(root, 'say.sh'), '#!/bin/bash\nprintf "%s\\n" "$TEXT" >> "$(dirname "$0")/said.txt"\n')
  if (alarms) writeFileSync(path.join(dataDir, 'alarms.json'), JSON.stringify({ alarms, missed: [] }))
  let swapPath = ''
  if (swapScript !== undefined) {
    swapPath = path.join(root, 'swap-window.mjs')
    if (swapScript !== null) writeFileSync(swapPath, swapScript)
  }
  writeFileSync(path.join(root, 'config.json'), JSON.stringify({
    dataDir, secretDir, personaFile: path.join(root, 'persona.md'), agentName: '测试助手',
    ...(noLateReply ? {} : { lateReplyCommand: ['bash', path.join(root, 'say.sh')] }), notifyCommand: ['bash', path.join(root, 'say.sh')],
    toolboxFile: path.join(root, 'toolbox.json'),
    ...(swapPath ? { swapWindowScript: swapPath, chatgptAppName: 'test-app' } : {}),
  }))
  const port = await freePort()
  const base = `http://127.0.0.1:${port}`
  const logs = []
  let child

  async function start() {
    child = spawn(process.execPath, [path.join(srcDir, 'server.mjs')], {
      cwd: path.dirname(srcDir),
      env: { ...process.env, CONG_CONFIG: path.join(root, 'config.json'), CONG_BRIDGE_PORT: String(port) },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    child.stdout.on('data', (chunk) => logs.push(chunk.toString()))
    child.stderr.on('data', (chunk) => logs.push(chunk.toString()))
    await waitForHealth(base, child, logs)
  }

  async function stop() {
    if (!child || child.exitCode !== null) return
    child.kill('SIGTERM')
    await Promise.race([once(child, 'exit'), new Promise((resolve) => setTimeout(resolve, 1000))])
    if (child.exitCode === null) child.kill('SIGKILL')
  }

  async function rpc(method, params) {
    const response = await fetch(`${base}/mcp/${mcpSecret}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream' },
      body: JSON.stringify({ jsonrpc: '2.0', id: Math.floor(Math.random() * 1e9), method, params }),
    })
    const body = await response.text()
    assert.equal(response.status, 200, body)
    return decodeRpc(body)
  }

  const tool = (name, args = {}, session = '') => rpc('tools/call', {
    name,
    arguments: args,
    ...(session ? { _meta: { 'openai/session': session } } : {}),
  })
  const sync = (session, cardId = session) => tool('bridge_listener_sync', { protocol: 2, cardId, ua: 'replay', vis: 'visible' }, session)
  const message = (text) => fetch(`${base}/message`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-bridge-token': token },
    body: JSON.stringify({ room: 'chat', message: text }),
  }).then(async (response) => ({ status: response.status, text: await response.text() }))
  const reset = (room) => fetch(`${base}/reset`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-bridge-token': token },
    body: JSON.stringify({ room }),
  }).then(async (response) => ({ status: response.status, body: await response.text() }))

  await start()
  return {
    root, base, logs, tool, sync, message, reset, start, stop,
    setStateWritable(writable) { chmodSync(dataDir, writable ? 0o700 : 0o500) },
    blockStateCommit(blocked) {
      const tmp = path.join(dataDir, 'state.json.tmp')
      if (blocked) mkdirSync(tmp)
      else rmSync(tmp, { recursive: true, force: true })
    },
    async close() {
      chmodSync(dataDir, 0o700)
      await stop()
      rmSync(root, { recursive: true, force: true })
    },
  }
}

function eventFrom(result) {
  return result?._meta?.event
}

function ndjson(text) {
  return text.split('\n').filter(Boolean).map((line) => JSON.parse(line))
}

test('完整回话只交付一次，重复 bridge_reply 幂等', async () => {
  const box = await makeSandbox()
  try {
    await box.sync('session-a')
    const responsePromise = box.message('回放：只回一次')
    await new Promise((resolve) => setTimeout(resolve, 40))
    const event = eventFrom(await box.sync('session-a'))
    assert.ok(event?.id && event?.claimId)
    assert.equal((await box.tool('bridge_listener_delivered', { eventId: event.id, claimId: event.claimId }, 'session-a')).structuredContent.delivered, true)
    const replyPromise = box.tool('bridge_reply', { eventId: event.id, text: '收到一次' }, 'session-a')
    const response = await responsePromise
    const reply = await replyPromise
    assert.equal(reply.structuredContent.ok, true)
    assert.deepEqual(ndjson(response.text).filter((item) => item.type === 'text').map((item) => item.content), ['收到一次'])
    const duplicate = await box.tool('bridge_reply', { eventId: event.id, text: '不该再送' }, 'session-a')
    assert.equal(duplicate.structuredContent.ok, true)
    assert.match(duplicate.structuredContent.note, /已经回过/)
  } finally { await box.close() }
})

test('旧窗口令牌不能释放或提交新窗口的认领', async () => {
  const box = await makeSandbox()
  try {
    await box.sync('session-a')
    const responsePromise = box.message('回放：换窗口')
    await new Promise((resolve) => setTimeout(resolve, 40))
    const oldEvent = eventFrom(await box.sync('session-a'))
    assert.ok(oldEvent)
    assert.equal((await box.tool('bridge_listener_release', { eventId: oldEvent.id, claimId: oldEvent.claimId }, 'session-a')).structuredContent.released, true)
    const takeoverPromise = box.tool('bridge_takeover', {}, 'session-b')
    await new Promise((resolve) => setTimeout(resolve, 40))
    const newEvent = eventFrom(await box.sync('session-b'))
    assert.ok(newEvent)
    assert.notEqual(newEvent.claimId, oldEvent.claimId)
    assert.equal((await takeoverPromise).structuredContent.accepted, true)
    assert.equal((await box.tool('bridge_listener_release', { eventId: oldEvent.id, claimId: oldEvent.claimId }, 'session-a')).structuredContent.released, false)
    assert.equal((await box.tool('bridge_listener_delivered', { eventId: oldEvent.id, claimId: oldEvent.claimId }, 'session-a')).structuredContent.delivered, false)
    assert.equal((await box.tool('bridge_listener_delivered', { eventId: newEvent.id, claimId: newEvent.claimId }, 'session-b')).structuredContent.delivered, true)
    const replyPromise = box.tool('bridge_reply', { eventId: newEvent.id, text: '新窗口收到' }, 'session-b')
    assert.equal((await responsePromise).status, 200)
    assert.equal((await replyPromise).structuredContent.ok, true)
  } finally { await box.close() }
})

test('离线欠账跨重启保留，恢复后还是同一个 eventId', async () => {
  const box = await makeSandbox()
  try {
    const offline = await box.message('回放：重启后别丢')
    const originalId = ndjson(offline.text).find((item) => item.type === 'session').eventId
    assert.match(offline.text, /"status":"offline"/)
    await box.stop()
    await box.start()
    const restored = eventFrom(await box.sync('session-a'))
    assert.equal(restored.id, originalId)
    assert.equal((await box.tool('bridge_listener_release', { eventId: restored.id, claimId: restored.claimId }, 'session-a')).structuredContent.released, true)
  } finally { await box.close() }
})

test('已认领但未送达的消息重启后退回队列，不会卡死', async () => {
  const box = await makeSandbox()
  try {
    await box.sync('session-a')
    const responsePromise = box.message('回放：认领中重启').catch(() => null)
    await new Promise((resolve) => setTimeout(resolve, 40))
    const claimed = eventFrom(await box.sync('session-a'))
    assert.ok(claimed)
    await box.stop()
    await responsePromise
    await box.start()
    const restored = eventFrom(await box.sync('session-a'))
    assert.equal(restored.id, claimed.id)
    assert.notEqual(restored.claimId, claimed.claimId)
  } finally { await box.close() }
})

test('已经送进对话的消息重启后不重复投递，并留在欠账里', async () => {
  const box = await makeSandbox()
  try {
    await box.sync('session-a')
    const responsePromise = box.message('回放：送达后重启').catch(() => null)
    await new Promise((resolve) => setTimeout(resolve, 40))
    const delivered = eventFrom(await box.sync('session-a'))
    assert.ok(delivered)
    assert.equal((await box.tool('bridge_listener_delivered', { eventId: delivered.id, claimId: delivered.claimId }, 'session-a')).structuredContent.delivered, true)
    await box.stop()
    await responsePromise
    await box.start()
    assert.equal(eventFrom(await box.sync('session-a')), undefined)
    const pending = await box.tool('bridge_pending', {}, 'session-a')
    assert.equal(pending.structuredContent.items[0].eventId, delivered.id)
  } finally { await box.close() }
})

test('三次离线的闹钟随卡片恢复，并可在送达前真正取消', async () => {
  const at = Date.now() - 1000
  const box = await makeSandbox({ alarms: [{ alarmId: 'alarm-replay', at, note: '回放闹钟', fired: false, fails: 3, retryAt: Date.now() + 3600_000 }] })
  try {
    await box.sync('session-a')
    await new Promise((resolve) => setTimeout(resolve, 3300))
    box.blockStateCommit(true)
    const rejected = await box.tool('bridge_alarm', { cancel: at }, 'session-a')
    assert.equal(rejected.isError, true)
    assert.match(rejected.content[0].text, /闹钟和提醒都保持原样/)
    box.blockStateCommit(false)
    const cancelled = await box.tool('bridge_alarm', { cancel: at }, 'session-a')
    assert.match(cancelled.content[0].text, /撤掉 1 条还没送达的提醒/)
    assert.equal(eventFrom(await box.sync('session-a')), undefined)
    const saved = JSON.parse(readFileSync(path.join(box.root, 'data', 'alarms.json'), 'utf8'))
    assert.equal(saved.alarms.length, 0)
  } finally { await box.close() }
})

test('账本不可写时入队失败会明报，不留假成功事件', async () => {
  const box = await makeSandbox()
  try {
    box.setStateWritable(false)
    const response = await box.message('回放：这条不能假入队')
    assert.equal(response.status, 500, response.text)
    box.setStateWritable(true)
    assert.equal(eventFrom(await box.sync('session-a')), undefined)
  } finally { await box.close() }
})

test('账本不可写时不会假认领或假送达', async () => {
  const box = await makeSandbox()
  try {
    const offline = await box.message('回放：认领和送达都要落盘')
    const eventId = ndjson(offline.text).find((item) => item.type === 'session').eventId
    box.setStateWritable(false)
    const rejectedClaim = await box.sync('session-a')
    assert.equal(rejectedClaim.structuredContent.hasEvent, false)
    assert.equal(rejectedClaim.isError, true)
    box.setStateWritable(true)
    const event = eventFrom(await box.sync('session-a'))
    assert.equal(event.id, eventId)
    box.setStateWritable(false)
    const rejectedDelivery = await box.tool('bridge_listener_delivered', { eventId: event.id, claimId: event.claimId }, 'session-a')
    assert.equal(rejectedDelivery.structuredContent.delivered, false)
    assert.equal(rejectedDelivery.isError, true)
    box.setStateWritable(true)
    const acceptedDelivery = await box.tool('bridge_listener_delivered', { eventId: event.id, claimId: event.claimId }, 'session-a')
    assert.equal(acceptedDelivery.structuredContent.delivered, true)
  } finally { await box.close() }
})

test('账本不可写时不会假释放，原认领仍能提交', async () => {
  const box = await makeSandbox()
  try {
    const offline = await box.message('回放：释放也要落盘')
    const eventId = ndjson(offline.text).find((item) => item.type === 'session').eventId
    const event = eventFrom(await box.sync('session-a'))
    assert.equal(event.id, eventId)
    box.setStateWritable(false)
    const rejectedRelease = await box.tool('bridge_listener_release', { eventId: event.id, claimId: event.claimId }, 'session-a')
    assert.equal(rejectedRelease.structuredContent.released, false)
    assert.equal(rejectedRelease.isError, true)
    box.setStateWritable(true)
    const acceptedDelivery = await box.tool('bridge_listener_delivered', { eventId: event.id, claimId: event.claimId }, 'session-a')
    assert.equal(acceptedDelivery.structuredContent.delivered, true)
  } finally { await box.close() }
})

test('账本不可写时 reset 明报失败，欠账仍在', async () => {
  const box = await makeSandbox()
  try {
    const offline = await box.message('回放：重置不能假清空')
    const eventId = ndjson(offline.text).find((item) => item.type === 'session').eventId
    box.setStateWritable(false)
    const reset = await box.reset('chat')
    assert.equal(reset.status, 500, reset.body)
    box.setStateWritable(true)
    const event = eventFrom(await box.sync('session-a'))
    assert.equal(event.id, eventId)
  } finally { await box.close() }
})

test('回复既没送到也没落盘时不假称已留存', async () => {
  const box = await makeSandbox({ noLateReply: true })
  try {
    const offline = await box.message('回放：失败回复要说实话')
    const eventId = ndjson(offline.text).find((item) => item.type === 'session').eventId
    const event = eventFrom(await box.sync('session-a'))
    assert.equal(event.id, eventId)
    assert.equal((await box.tool('bridge_listener_delivered', { eventId, claimId: event.claimId }, 'session-a')).structuredContent.delivered, true)
    box.setStateWritable(false)
    const reply = await box.tool('bridge_reply', { eventId, text: '这句不能假留存' }, 'session-a')
    assert.equal(reply.structuredContent.ok, false)
    assert.match(reply.structuredContent.note, /账本也没写成/)
    assert.equal(reply.isError, true)
  } finally { await box.close() }
})

test('交接卡：写了新窗口挂上就先看到，确认后不再当新卡念，重启后还在', async () => {
  const box = await makeSandbox()
  const textOf = (r) => (r.content || []).map((c) => c.text || '').join('\n')
  try {
    let open = textOf(await box.tool('bridge_listener_open', {}, 'win-1'))
    assert.match(open, /【交接卡】上个窗口没写。/)
    assert.match(open, /【桥上看得见的现场】/)
    assert.match(open, /欠着没回的消息：无/)
    const empty = await box.tool('bridge_handoff', { action: 'ack' }, 'win-1')
    assert.match(textOf(empty), /没有交接卡/)
    const w = await box.tool('bridge_handoff', { action: 'write', text: '正在做：给朋友回第一封信（待办）\n待确认：UNO 那局结没结' }, 'win-1')
    assert.match(textOf(w), /交接卡记下了/)
    const { statSync } = await import('node:fs')
    assert.equal(statSync(path.join(box.root, 'data', 'handoff.json')).mode & 0o777, 0o600)
    await box.stop(); await box.start()
    open = textOf(await box.tool('bridge_listener_open', {}, 'win-2'))
    assert.match(open, /【交接卡·编号 hc_[0-9a-f]+·上个窗口 .* 写的】/)
    assert.match(open, /给朋友回第一封信（待办）/)
    assert.match(open, /home_call\(server="handoff", tool="ack"/)
    assert.ok(open.indexOf('【交接卡') < open.indexOf('【记事本'), '交接卡要在记事本前面')
    const id1 = open.match(/编号 (hc_[0-9a-f]+)/)?.[1]
    assert.ok(id1, '卡上要有编号')
    const noId = await box.tool('bridge_handoff', { action: 'ack' }, 'win-2')
    assert.equal(noId.isError, true)
    const ack = await box.tool('bridge_handoff', { action: 'ack', cardId: id1, note: '核对过，UNO 已结' }, 'win-2')
    assert.match(textOf(ack), /交接确认了/)
    open = textOf(await box.tool('bridge_listener_open', {}, 'win-3'))
    assert.match(open, /上个窗口没写新的/)
    assert.doesNotMatch(open, /给朋友回第一封信/)
    const read = textOf(await box.tool('bridge_handoff', { action: 'read' }, 'win-3'))
    assert.match(read, /【桥上看得见的现场】/)
  } finally { await box.close() }
})

test('交接卡：核对卡 1 期间别人写了卡 2，拿卡 1 的编号确认会被拒，卡 2 仍算没交接；读失败不说成没写', async () => {
  const box = await makeSandbox()
  const textOf = (r) => (r.content || []).map((c) => c.text || '').join('\n')
  try {
    await box.tool('bridge_handoff', { action: 'write', text: '卡一：待办 A' }, 'win-a')
    const id1 = textOf(await box.tool('bridge_handoff', { action: 'read' }, 'win-b')).match(/编号 (hc_[0-9a-f]+)/)?.[1]
    assert.ok(id1)
    await box.tool('bridge_handoff', { action: 'write', text: '卡二：待办 B' }, 'win-a')
    const stale = await box.tool('bridge_handoff', { action: 'ack', cardId: id1 }, 'win-b')
    assert.equal(stale.isError, true)
    assert.match(textOf(stale), /重新 bridge_handoff/)
    const open = textOf(await box.tool('bridge_listener_open', {}, 'win-c'))
    assert.match(open, /卡二：待办 B/)
    assert.doesNotMatch(open, /已交接过/)
    const { writeFileSync } = await import('node:fs')
    writeFileSync(path.join(box.root, 'data', 'handoff.json'), '{坏了')
    const broken = textOf(await box.tool('bridge_listener_open', {}, 'win-d'))
    assert.match(broken, /交接卡】读失败/)
    assert.doesNotMatch(broken, /上个窗口没写。/)
  } finally { await box.close() }
})

test('交接卡：工具清单没刷新也能用——home_call(server=handoff) 跟 bridge_handoff 是同一张卡', async () => {
  const box = await makeSandbox()
  const textOf = (r) => (r.content || []).map((c) => c.text || '').join('\n')
  try {
    assert.match(textOf(await box.tool('home_tools', {}, 'w1')), /handoff — 换窗交接卡/)
    const list = textOf(await box.tool('home_tools', { server: 'handoff' }, 'w1'))
    assert.match(list, /^write：/m); assert.match(list, /^read：/m); assert.match(list, /^ack：/m)
    assert.match(textOf(await box.tool('home_call', { server: 'handoff', tool: 'write', args: { text: '正在做：经 home_call 写的卡（待办）' } }, 'w1')), /交接卡记下了/)
    const read = textOf(await box.tool('home_call', { server: 'handoff', tool: 'read', args: {} }, 'w2'))
    assert.match(read, /经 home_call 写的卡/)
    const id = read.match(/编号 (hc_[0-9a-f]+)/)?.[1]
    assert.ok(id)
    assert.match(textOf(await box.tool('bridge_handoff', { action: 'read' }, 'w2')), new RegExp(id))
    const bad = await box.tool('home_call', { server: 'handoff', tool: 'ack', args: { cardId: 'hc_00000000' } }, 'w2')
    assert.equal(bad.isError, true)
    assert.match(textOf(await box.tool('home_call', { server: 'handoff', tool: 'ack', args: { cardId: id, note: '核对过' } }, 'w2')), /交接确认了/)
    assert.match(textOf(await box.tool('bridge_listener_open', {}, 'w3')), /上个窗口没写新的/)
    const nope = await box.tool('home_call', { server: 'handoff', tool: 'delete', args: {} }, 'w3')
    assert.equal(nope.isError, true)
    // home_call 这条路也受同一份合同管
    await box.tool('home_call', { server: 'handoff', tool: 'write', args: { text: '原卡：别被冲掉' } }, 'w3')
    const before = textOf(await box.tool('home_call', { server: 'handoff', tool: 'read', args: {} }, 'w3'))
    const big = await box.tool('home_call', { server: 'handoff', tool: 'write', args: { text: '字'.repeat(3001) } }, 'w3')
    assert.equal(big.isError, true)
    assert.match(textOf(big), /最多 3000 字/)
    for (const args of [{ text: { a: 1 } }, { text: ['x'] }]) assert.equal((await box.tool('home_call', { server: 'handoff', tool: 'write', args }, 'w3')).isError, true)
    for (const args of [{ cardId: { a: 1 } }, { cardId: 'hc_x', note: { a: 1 } }, { cardId: 'hc_x', note: 'n'.repeat(301) }]) assert.equal((await box.tool('home_call', { server: 'handoff', tool: 'ack', args }, 'w3')).isError, true)
    assert.equal(textOf(await box.tool('home_call', { server: 'handoff', tool: 'read', args: {} }, 'w3')), before, '拒掉的写入不能动原卡')
  } finally { await box.close() }
})

test('bridge_pending：超过 2000 字的原消息截断时写明还剩几字', async () => {
  const box = await makeSandbox()
  try {
    await box.sync('session-a')
    const long = '回放长消息' + '字'.repeat(2600)
    const responsePromise = box.message(long).catch(() => null)
    await new Promise((resolve) => setTimeout(resolve, 40))
    const delivered = eventFrom(await box.sync('session-a'))
    assert.ok(delivered)
    assert.equal((await box.tool('bridge_listener_delivered', { eventId: delivered.id, claimId: delivered.claimId }, 'session-a')).structuredContent.delivered, true)
    const pending = await box.tool('bridge_pending', {}, 'session-a')
    const item = pending.structuredContent.items.find((i) => i.eventId === delivered.id)
    assert.ok(item, '欠账里要有这条')
    assert.match(item.text, /…【后面还有 \d+ 字没贴，原文 \d+ 字】$/)
    await box.stop(); await responsePromise
  } finally { await box.close() }
})

test('chat reset：换窗脚本一起来就退 → 账照清、回包 swap_started:false 带 warning', async () => {
  const box = await makeSandbox({ swapScript: 'process.exit(1)\n' })
  try {
    const r = await box.reset('chat')
    assert.equal(r.status, 200, r.body)
    const j = JSON.parse(r.body)
    assert.equal(j.swap_started, false)
    assert.match(j.warning, /自动换窗脚本没起来/)
  } finally { await box.close() }
})
test('chat reset：换窗脚本文件没了 → 同样 swap_started:false', async () => {
  const box = await makeSandbox({ swapScript: null })
  try {
    const j = JSON.parse((await box.reset('chat')).body)
    assert.equal(j.swap_started, false)
    assert.match(j.warning, /退出码/)
  } finally { await box.close() }
})
test('chat reset：换窗脚本正常跑着 → swap_started:true、没有 warning', async () => {
  const box = await makeSandbox({ swapScript: 'setTimeout(() => {}, 4000)\n' })
  try {
    const j = JSON.parse((await box.reset('chat')).body)
    assert.equal(j.swap_started, true)
    assert.equal(j.warning, undefined)
  } finally { await box.close() }
})
test('回复送到了但销账没落盘：ok 但 persisted:false，明说别重发', async () => {
  const box = await makeSandbox()
  try {
    await box.sync('session-a')
    const responsePromise = box.message('回放：送到了但账没记住')
    await new Promise((resolve) => setTimeout(resolve, 40))
    const event = eventFrom(await box.sync('session-a'))
    assert.equal((await box.tool('bridge_listener_delivered', { eventId: event.id, claimId: event.claimId }, 'session-a')).structuredContent.delivered, true)
    box.setStateWritable(false)
    const reply = await box.tool('bridge_reply', { eventId: event.id, text: '送到了' }, 'session-a')
    const response = await responsePromise
    box.setStateWritable(true)
    assert.deepEqual(ndjson(response.text).filter((item) => item.type === 'text').map((item) => item.content), ['送到了'])
    assert.equal(reply.structuredContent.ok, true)
    assert.equal(reply.structuredContent.persisted, false)
    assert.match(reply.content[0].text, /别重发/)
    assert.notEqual(reply.isError, true)
  } finally { await box.close() }
})


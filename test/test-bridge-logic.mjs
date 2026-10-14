import test from 'node:test'
import assert from 'node:assert/strict'
import {
  LATE_REPLY_TIMEOUT_MS,
  cancelQueuedAlarmEvents,
  resultText,
  resumeStrandedAlarm,
  UI_TIMEOUT_HOLD_MS,
  alarmEventSettled,
  reconcileAlarmEvent,
  settleUiTimeout,
  expireLateAlarmEvent,
  pickNextEvent,
  sweepLateAlarmEvents,
  acquireSwapLock,
} from '../src/bridge-logic.mjs'

test('卡片恢复后解开三次离线搁浅', () => {
  const alarm = { fails: 3, retryAt: 123 }
  assert.equal(resumeStrandedAlarm(alarm, false), false)
  assert.deepEqual(alarm, { fails: 3, retryAt: 123 })
  assert.equal(resumeStrandedAlarm(alarm, true), true)
  assert.deepEqual(alarm, { fails: 0 })
})

test('取消闹钟同时作废未送达事件，不假装撤回已送达事件', () => {
  const events = new Map([
    ['p', { alarmId: 'a1', status: 'pending' }],
    ['r', { alarmId: 'a1', status: 'reserved', claimId: 'claim' }],
    ['d', { alarmId: 'a1', status: 'delivered' }],
    ['x', { alarmId: 'a2', status: 'pending' }],
  ])
  assert.deepEqual(cancelQueuedAlarmEvents(events, new Set(['a1']), 99), { cancelled: 2, alreadyDelivered: 1 })
  assert.deepEqual(events.get('p'), { alarmId: 'a1', status: 'expired', expiredAt: 99 })
  assert.deepEqual(events.get('r'), { alarmId: 'a1', status: 'expired', expiredAt: 99 })
  assert.equal(events.get('d').status, 'delivered')
  assert.equal(events.get('x').status, 'pending')
})

test('工具结果文本只用于日志判断，原内容可另行原样转发', () => {
  assert.equal(resultText({ content: [{ type: 'image', data: 'x' }], structuredContent: { count: 1 } }), '{"count":1}')
  assert.equal(resultText({ content: [{ type: 'text', text: '一' }, { type: 'image', data: 'x' }, { type: 'text', text: '二' }] }), '一\n二')
})

function uiDeps(events, { busy, persistOk = true, onProbe } = {}) {
  const logs = [], stuck = []
  let persists = 0
  return {
    logs, stuck, get persists() { return persists },
    getEvent: (id) => events.get(id),
    probeBusy: async () => { if (onProbe) onProbe(); return busy },
    persist: () => { persists++; return persistOk },
    log: (t) => logs.push(t),
    onStuck: (e) => stuck.push(e.id),
    now: () => 1_000_000,
  }
}
const held = (extra = {}) => ({ id: 'ev1', status: 'pending', uiTimeouts: 1, retryAfter: 1_000_000 + UI_TIMEOUT_HOLD_MS, ...extra })

test('送不进去① 网页正在回话：按可能已送达暂存，不再重投', async () => {
  const events = new Map([['ev1', held()]])
  const d = uiDeps(events, { busy: true })
  assert.equal(await settleUiTimeout('ev1', 1_000_000 + UI_TIMEOUT_HOLD_MS, d), 'unconfirmed')
  const e = events.get('ev1')
  assert.equal(e.status, 'delivered')
  assert.equal(e.deliveredUnconfirmed, true)
  assert.equal(e.deliveredAt, 1_000_000)
  assert.equal(e.retryAfter, undefined)
  assert.equal(d.persists, 1)
  assert.match(d.logs[0], /可能已送达/)
  assert.doesNotMatch(d.logs[0], /排在他这轮后面了/)
})

test('送不进去② 落盘失败：退回 pending、一分钟后再投，不留半截状态', async () => {
  const events = new Map([['ev1', held()]])
  const d = uiDeps(events, { busy: true, persistOk: false })
  assert.equal(await settleUiTimeout('ev1', 1_000_000 + UI_TIMEOUT_HOLD_MS, d), 'persist-failed')
  const e = events.get('ev1')
  assert.equal(e.status, 'pending')
  assert.equal(e.deliveredAt, undefined)
  assert.equal(e.deliveredUnconfirmed, undefined)
  assert.equal(e.retryAfter, 1_000_000 + 60_000)
})

test('送不进去③ 看网页那几秒里已经被领走：不改状态', async () => {
  const events = new Map([['ev1', held()]])
  const d = uiDeps(events, { busy: true, onProbe: () => { const e = events.get('ev1'); e.status = 'reserved'; e.claimId = 'c2' } })
  assert.equal(await settleUiTimeout('ev1', 1_000_000 + UI_TIMEOUT_HOLD_MS, d), 'gone')
  const e = events.get('ev1')
  assert.equal(e.status, 'reserved')
  assert.equal(e.claimId, 'c2')
  assert.equal(e.deliveredUnconfirmed, undefined)
  assert.equal(d.persists, 0)
})

test('送不进去④ 他空着、第一次没回执：照旧马上再投；看不了网页也一样', async () => {
  for (const busy of [false, undefined]) {
    const events = new Map([['ev1', held()]])
    const d = uiDeps(events, { busy })
    assert.equal(await settleUiTimeout('ev1', 1_000_000 + UI_TIMEOUT_HOLD_MS, d), 'retry')
    assert.equal(events.get('ev1').status, 'pending')
    assert.equal(events.get('ev1').retryAfter, undefined)
  }
  // retryAfter 已经被别处改过（不是我压的那个值）就不去清
  const events = new Map([['ev1', held({ retryAfter: 5 })]])
  await settleUiTimeout('ev1', 1_000_000 + UI_TIMEOUT_HOLD_MS, uiDeps(events, { busy: false }))
  assert.equal(events.get('ev1').retryAfter, 5)
})

test('送不进去④b 他空着、连着两次没回执：交给卡住那条路（刷新）', async () => {
  const events = new Map([['ev1', held({ uiTimeouts: 2 })]])
  const d = uiDeps(events, { busy: false })
  assert.equal(await settleUiTimeout('ev1', 1_000_000 + UI_TIMEOUT_HOLD_MS, d), 'stuck')
  assert.deepEqual(d.stuck, ['ev1'])
})

test('送不进去⑤ 只是可能已送达的闹钟不销账；判死后重排；真送到或回了才销账', () => {
  const unconfirmed = { status: 'delivered', deliveredUnconfirmed: true }
  assert.equal(alarmEventSettled(unconfirmed), false)
  const a = { alarmId: 'a1', eventId: 'ev1' }
  assert.equal(reconcileAlarmEvent(a, unconfirmed, 100), 'wait')
  assert.equal(a.fired, undefined)
  assert.equal(a.eventId, 'ev1')
  // deliveredTooOld 判死后事件变 expired → 走现成的重排
  assert.equal(reconcileAlarmEvent(a, { status: 'expired' }, 100), 'requeue')
  assert.equal(a.eventId, undefined)
  assert.equal(a.fails, 1)
  assert.equal(a.retryAt, 100 + 5 * 60 * 1000)
  // 他回了 → 销账；卡片真回执的 delivered → 销账
  const b = { alarmId: 'b', eventId: 'x' }
  assert.equal(reconcileAlarmEvent(b, { status: 'delivered', deliveredUnconfirmed: true, }, 1), 'wait')
  assert.equal(reconcileAlarmEvent(b, { status: 'replied', deliveredUnconfirmed: true }, 2), 'fired')
  assert.equal(b.fired, true)
  const c = { alarmId: 'c', eventId: 'y' }
  assert.equal(reconcileAlarmEvent(c, { status: 'delivered' }, 3), 'fired')
})

test('先送谁：优先件只连插一次 > 主人的话 > 先来先送', () => {
  const evt = (id, kind, visibleText) => ({ id, kind, visibleText, status: 'pending' })
  const isPriority = (e) => /^\[轮到你了\]/.test(String(e?.visibleText || ''))
  const isOwner = (e) => e?.kind === 'chat' && /^\[\d{4}-\d\d-\d\d \d\d:\d\d\] 主人：/.test(String(e?.visibleText || ''))
  const wake1 = evt('w1', 'wake', '[定时醒来] 你之前自己设的闹钟：10-05 20:46。')
  const wake2 = evt('w2', 'wake', '[定时醒来] 你之前自己设的闹钟：10-05 20:48。')
  const group = evt('g', 'chat', '[群聊] 有新消息')
  const own1 = evt('o1', 'chat', '[2026-10-05 21:35] 主人：来打牌！')
  const own2 = evt('o2', 'chat', '[2026-10-05 22:07] 主人：刚刚咋没进去')
  const bell = evt('b', 'chat', '[轮到你了] 牌桌上轮到你')
  let q = [wake1, wake2, group, own1, own2]
  assert.deepEqual(pickNextEvent(q, false, isPriority, isOwner), { event: own1, index: 3, why: 'owner' })
  q = [wake1, own2, own1]
  assert.equal(pickNextEvent(q, false, isPriority, isOwner).event.id, 'o2')
  q = [wake1, own1, bell]
  assert.deepEqual(pickNextEvent(q, false, isPriority, isOwner), { event: bell, index: 2, why: 'priority' })
  assert.deepEqual(pickNextEvent(q, true, isPriority, isOwner), { event: own1, index: 1, why: 'owner' })
  assert.deepEqual(pickNextEvent([wake1, group], false, isPriority, isOwner), { event: wake1, index: 0, why: 'fifo' })
  assert.deepEqual(pickNextEvent([wake1, own1]), { event: wake1, index: 0, why: 'fifo' })
  assert.equal(pickNextEvent([], false, isPriority, isOwner), null)
})

test('闹钟：排进队迟了半小时还没送出去的闹钟：不送，记成错过；正在送的、送进去的不动', () => {
  const LATE = 30 * 60 * 1000
  const now = 10_000_000
  const mk = (status, atAgo) => [{ alarmId: 'a', at: now - atAgo, eventId: 'e' }, { id: 'e', status, retryAfter: 5 }]
  let [a, e] = mk('pending', LATE + 1)
  assert.equal(expireLateAlarmEvent(a, e, now, LATE), true)
  assert.equal(e.status, 'expired'); assert.equal(e.expiredAt, now); assert.equal(e.retryAfter, undefined)
  assert.equal(a.fired, true); assert.equal(a.missedAt, now); assert.equal(a.eventId, undefined)
  ;[a, e] = mk('pending', LATE)          // 正好半小时：还送
  assert.equal(expireLateAlarmEvent(a, e, now, LATE), false); assert.equal(e.status, 'pending')
  for (const status of ['reserved', 'delivered', 'replied']) {
    ;[a, e] = mk(status, LATE + 60_000)
    assert.equal(expireLateAlarmEvent(a, e, now, LATE), false); assert.equal(e.status, status); assert.equal(a.fired, undefined)
  }
  ;[a, e] = mk('pending', LATE + 1); a.fired = true
  assert.equal(expireLateAlarmEvent(a, e, now, LATE), false)
  assert.equal(expireLateAlarmEvent({ alarmId: 'a', at: 0 }, undefined, now, LATE), false)
})

test('闹钟① 作废时 state 落盘失败：事件和闹钟都完整回滚', () => {
  const now = 10_000_000, LATE = 30 * 60 * 1000
  const alarm = { alarmId: 'a', at: now - LATE - 1, eventId: 'e', note: 'n', fails: 1 }
  const event = { id: 'e', status: 'pending', retryAfter: 7, alarmId: 'a', visibleText: 'x' }
  const a0 = structuredClone(alarm), e0 = structuredClone(event)
  assert.equal(expireLateAlarmEvent(alarm, event, now, LATE, () => false), false)
  assert.deepEqual(alarm, a0)
  assert.deepEqual(event, e0)
  // sweep 也一样：persist 失败什么都不报、什么都不改
  const events = new Map([['e', event]])
  assert.deepEqual(sweepLateAlarmEvents([alarm], events, now, LATE, () => false), [])
  assert.deepEqual(alarm, a0); assert.deepEqual(event, e0)
})

test('闹钟② 半截重启状态只能收成错过，不重排', () => {
  const now = 10_000_000, LATE = 30 * 60 * 1000
  // 内存里：事件已因迟到作废，闹钟还没来得及存
  let a = { alarmId: 'a', at: now - LATE - 1, eventId: 'e' }
  assert.equal(reconcileAlarmEvent(a, { status: 'expired', expiredWhy: 'alarm-too-late' }, now, LATE), 'missed')
  assert.equal(a.fired, true); assert.equal(a.missedAt, now); assert.equal(a.eventId, undefined); assert.equal(a.fails, undefined); assert.equal(a.retryAt, undefined)
  // 重启后：作废的事件不进 state.json，闹钟指着一个没了的事件——闹钟已迟过半小时，也只收成错过
  a = { alarmId: 'a', at: now - LATE - 1, eventId: 'e' }
  assert.equal(reconcileAlarmEvent(a, undefined, now, LATE), 'missed')
  assert.equal(a.fired, true); assert.equal(a.fails, undefined)
  // 没迟到的照旧重排（原来那条路不变）
  a = { alarmId: 'a', at: now - 60_000, eventId: 'e' }
  assert.equal(reconcileAlarmEvent(a, undefined, now, LATE), 'requeue')
  assert.equal(a.fails, 1)
})

test('闹钟③ 卡片领事件前先扫：迟了半小时的闹钟卡片拿不到', () => {
  const now = 10_000_000, LATE = 30 * 60 * 1000
  const late = { id: 'w', kind: 'wake', status: 'pending', alarmId: 'a', visibleText: '[定时醒来] 旧闹钟' }
  const fresh = { id: 'w2', kind: 'wake', status: 'pending', alarmId: 'b', visibleText: '[定时醒来] 新闹钟' }
  const alarms = [{ alarmId: 'a', at: now - LATE - 1, eventId: 'w' }, { alarmId: 'b', at: now - 1000, eventId: 'w2' }]
  const events = new Map([['w', late], ['w2', fresh]])
  let persisted = 0
  const gone = sweepLateAlarmEvents(alarms, events, now, LATE, () => { persisted++; return true })
  assert.deepEqual(gone.map((g) => g.event.id), ['w'])
  assert.equal(persisted, 1)
  const pendingNow = [...events.values()].filter((e) => e.status === 'pending')
  assert.equal(pickNextEvent(pendingNow, false).event.id, 'w2')
  // 卡片已经拿着的（reserved）不动——它在路上了
  const held = { id: 'r', status: 'reserved', alarmId: 'c' }
  assert.deepEqual(sweepLateAlarmEvents([{ alarmId: 'c', at: 0, eventId: 'r' }], new Map([['r', held]]), now, LATE, () => true), [])
  assert.equal(held.status, 'reserved')
})

test('换窗锁：旧锁过期后 8 个进程同时抢，每轮只有 1 个拿到（跑 15 轮）', async () => {
  const { mkdtempSync, writeFileSync, utimesSync, rmSync } = await import('node:fs')
  const { join } = await import('node:path')
  const { tmpdir } = await import('node:os')
  const { spawn } = await import('node:child_process')
  const logicUrl = new URL('../src/bridge-logic.mjs', import.meta.url).href
  for (let round = 0; round < 15; round++) {
    const dir = mkdtempSync(join(tmpdir(), 'swaplock-'))
    const lock = join(dir, 'lock')
    writeFileSync(lock, 'old'); const old = new Date(Date.now() - 20 * 60 * 1000); utimesSync(lock, old, old)
    const code = `import fs from 'node:fs'; import { acquireSwapLock } from ${JSON.stringify(logicUrl)}; const t = ${Date.now() + 300}; while (Date.now() < t) {} ; process.stdout.write(acquireSwapLock(fs, ${JSON.stringify(lock)}, 600000) ? 'GOT' : 'NO')`
    const outs = await Promise.all(Array.from({ length: 8 }, () => new Promise((res) => {
      const c = spawn(process.execPath, ['--input-type=module', '-e', code]); let o = ''
      c.stdout.on('data', (d) => { o += d }); c.on('exit', () => res(o))
    })))
    assert.equal(outs.filter((o) => o === 'GOT').length, 1, `第 ${round} 轮：${outs.join(',')}`)
    rmSync(dir, { recursive: true, force: true })
  }
})
test('换窗锁：没过期的锁不抢；锁不存在直接拿到', async () => {
  const fs = await import('node:fs'); const { join } = await import('node:path'); const { tmpdir } = await import('node:os')
  const dir = fs.mkdtempSync(join(tmpdir(), 'swaplock-')); const lock = join(dir, 'lock')
  assert.equal(acquireSwapLock(fs, lock, 600000), true)
  assert.equal(acquireSwapLock(fs, lock, 600000), false)
  fs.rmSync(dir, { recursive: true, force: true })
})

// cong-bridge · 纯逻辑（不碰网络、不碰磁盘，方便单测）
// 事件状态：pending → reserved（卡片取走，带一次性 claimId）→ delivered（已经进了对话）→ replied / expired
export const LATE_REPLY_TIMEOUT_MS = 90000;
export function resultText(result) {
  const text = (result?.content || []).filter((part) => part?.type === "text" && typeof part.text === "string").map((part) => part.text).join(`
`);
  if (text)
    return text;
  if (result?.structuredContent !== undefined)
    return JSON.stringify(result.structuredContent);
  return "（空）";
}
export function resumeStrandedAlarm(alarm, online) {
  if ((alarm.fails || 0) < 3 || !online)
    return false;
  alarm.fails = 0;
  delete alarm.retryAt;
  return true;
}
export function cancelQueuedAlarmEvents(events, alarmIds, now = Date.now()) {
  let cancelled = 0;
  let alreadyDelivered = 0;
  for (const event of events.values()) {
    if (!event.alarmId || !alarmIds.has(event.alarmId))
      continue;
    if (event.status === "pending" || event.status === "reserved") {
      event.status = "expired";
      event.expiredAt = now;
      delete event.claimId;
      cancelled++;
    } else if (event.status === "delivered")
      alreadyDelivered++;
  }
  return { cancelled, alreadyDelivered };
}
export const UI_TIMEOUT_HOLD_MS = 15000;
export function alarmEventSettled(event) {
  if (!event)
    return false;
  if (event.status === "replied")
    return true;
  return event.status === "delivered" && !event.deliveredUnconfirmed;
}
export function reconcileAlarmEvent(alarm, event, now = Date.now(), lateMs = 1 / 0) {
  const gone = !event || event.status === "expired";
  if (gone && (event && event.expiredWhy === "alarm-too-late" || now - alarm.at > lateMs)) {
    alarm.fired = true;
    alarm.missedAt = alarm.missedAt || now;
    delete alarm.eventId;
    return "missed";
  }
  if (alarmEventSettled(event)) {
    alarm.fired = true;
    alarm.firedAt = now;
    delete alarm.eventId;
    return "fired";
  }
  if (!event || event.status === "expired") {
    delete alarm.eventId;
    alarm.fails = (alarm.fails || 0) + 1;
    alarm.retryAt = now + 5 * 60 * 1000 * alarm.fails;
    return "requeue";
  }
  return "wait";
}
export async function settleUiTimeout(eventId, holdUntil, deps) {
  const now = deps.now || Date.now;
  let busy;
  try {
    busy = await deps.probeBusy();
  } catch (e) {
    busy = undefined;
    try {
      deps.log?.(`settleUiTimeout：看网页忙不忙的探针出错（${e?.message || e}），按「不知道」走普通重试 · ${eventId}`);
    } catch {}
  }
  const event = deps.getEvent(eventId);
  if (!event || event.status !== "pending")
    return "gone";
  if (busy === true) {
    event.status = "delivered";
    event.deliveredAt = now();
    event.deliveredUnconfirmed = true;
    delete event.retryAfter;
    if (!deps.persist()) {
      event.status = "pending";
      delete event.deliveredAt;
      delete event.deliveredUnconfirmed;
      event.retryAfter = now() + 60000;
      deps.log(`${eventId} 网页正在回话，想按可能已送达暂存，可账本写不进去，一分钟后再投`);
      return "persist-failed";
    }
    deps.log(`${eventId} ui/message 没回执，网页正在回话，按可能已送达暂存，不再重投（第 ${event.uiTimeouts || 1} 次没回执）；他回了才算数，闹钟等他回或判死后重排`);
    return "unconfirmed";
  }
  if ((event.uiTimeouts || 0) >= 2) {
    deps.onStuck(event);
    return "stuck";
  }
  if (event.retryAfter === holdUntil) {
    delete event.retryAfter;
    deps.persist();
  }
  return "retry";
}
// 下一条该送哪条：「优先件」（比如需要马上回应的提醒）只插一次队，不连着插；
// 然后是主人亲口说的话；其余按先来后到。isPriority / isOwner 由调用方给。
export function pickNextEvent(pendingNow, lastClaimedWasPriority, isPriority = () => false, isOwner = () => false) {
  if (!pendingNow.length)
    return null;
  // 换窗口时桥递过来的交接卡排在最前（连优先件都让它先）：新窗口先接上上一个窗口在做什么，再处理别的
  const handoffIdx = pendingNow.findIndex((e) => e && e.kind === "handoff");
  if (handoffIdx >= 0)
    return { event: pendingNow[handoffIdx], index: handoffIdx, why: "handoff" };
  const priIdx = lastClaimedWasPriority ? -1 : pendingNow.findIndex((e) => isPriority(e));
  if (priIdx >= 0)
    return { event: pendingNow[priIdx], index: priIdx, why: "priority" };
  const ownerIdx = pendingNow.findIndex((e) => isOwner(e));
  if (ownerIdx >= 0)
    return { event: pendingNow[ownerIdx], index: ownerIdx, why: "owner" };
  return { event: pendingNow[0], index: 0, why: "fifo" };
}
export const ALARM_TOO_LATE = "alarm-too-late";
export function expireLateAlarmEvent(alarm, event, now, lateMs, persist = () => true) {
  if (!alarm || alarm.fired || !event || event.status !== "pending")
    return false;
  if (now - alarm.at <= lateMs)
    return false;
  const evBefore = { ...event }, alarmBefore = { ...alarm };
  event.status = "expired";
  event.expiredAt = now;
  event.expiredWhy = ALARM_TOO_LATE;
  delete event.retryAfter;
  alarm.fired = true;
  alarm.missedAt = now;
  delete alarm.eventId;
  if (!persist()) {
    for (const k of Object.keys(event))
      delete event[k];
    Object.assign(event, evBefore);
    for (const k of Object.keys(alarm))
      delete alarm[k];
    Object.assign(alarm, alarmBefore);
    return false;
  }
  return true;
}
export function sweepLateAlarmEvents(alarms, events, now, lateMs, persist) {
  const expired = [];
  for (const alarm of alarms) {
    if (alarm.fired || alarm.conflicted || !alarm.eventId)
      continue;
    const event = events.get(alarm.eventId);
    if (expireLateAlarmEvent(alarm, event, now, lateMs, persist))
      expired.push({ alarm, event });
  }
  return expired;
}
export function acquireSwapLock(fs, lock, ttlMs, log = () => {}) {
  const grab = () => {
    try {
      fs.writeFileSync(lock, String(process.pid), { flag: "wx" });
      return true;
    } catch (e) {
      if (e?.code === "EEXIST")
        return false;
      throw e;
    }
  };
  const age = () => {
    try {
      return Date.now() - fs.statSync(lock).mtimeMs;
    } catch {
      return 1 / 0;
    }
  };
  if (grab())
    return true;
  if (age() < ttlMs) {
    log("已经有一个换窗在跑，这次不跑");
    return false;
  }
  const reap = lock + ".reap";
  try {
    fs.mkdirSync(reap);
  } catch {
    try {
      if (Date.now() - fs.statSync(reap).mtimeMs > 60 * 1000)
        fs.rmdirSync(reap);
    } catch {}
    log("已经有一个换窗在跑，这次不跑");
    return false;
  }
  try {
    if (age() < ttlMs) {
      log("已经有一个换窗在跑，这次不跑");
      return false;
    }
    try {
      fs.unlinkSync(lock);
    } catch {}
    if (!grab()) {
      log("已经有一个换窗在跑，这次不跑");
      return false;
    }
    return true;
  } finally {
    try {
      fs.rmdirSync(reap);
    } catch {}
  }
}

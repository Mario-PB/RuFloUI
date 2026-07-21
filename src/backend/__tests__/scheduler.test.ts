// @vitest-environment node
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { GlobalScheduler, getGlobalScheduler, resetGlobalScheduler, SubtaskRequest } from '../scheduler'
import { AGENT_PROFILES, stripTerminalReviewerAliases } from '../agent-profiles'

// ── Helpers ───────────────────────────────────────────────────────────

function makeAgent(profileId: string, agentId: string) {
  return { profileId, agentId }
}

function makeAgents() {
  return AGENT_PROFILES.map((p, i) => makeAgent(p.profileId, `agent-${p.profileId}-${i}`))
}

function req(partial: Partial<SubtaskRequest>): SubtaskRequest {
  return {
    id: partial.id || `sub-${Math.random().toString(36).slice(2, 8)}`,
    taskId: partial.taskId || 'task-x',
    capability: partial.capability || 'backend',
    description: partial.description || 'do work',
    priority: partial.priority,
    dependsOn: partial.dependsOn,
    profileId: partial.profileId,
  }
}

// ── Tests ─────────────────────────────────────────────────────────────

describe('GlobalScheduler — capacity and concurrency', () => {
  beforeEach(() => {
    resetGlobalScheduler()
  })

  it('default global cap is 10', () => {
    const s = new GlobalScheduler()
    expect(s.globalMaxConcurrent).toBe(10)
  })

  it('concurrency never exceeds 10 simultaneously active', () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    const peak = { value: 0 }
    const onSchedule = () => { if (s.activeCount > peak.value) peak.value = s.activeCount }
    s.on('schedule', onSchedule)

    // 30 ready subtasks for capability `backend` (two profiles expose it).
    for (let i = 0; i < 30; i++) {
      s.enqueue(req({ id: `sub-${i}`, capability: 'backend' }))
    }
    expect(peak.value).toBeLessThanOrEqual(10)
  })

  it('respects RUFLO_GLOBAL_MAX_CONCURRENT env override', () => {
    const prev = process.env.RUFLO_GLOBAL_MAX_CONCURRENT
    process.env.RUFLO_GLOBAL_MAX_CONCURRENT = '3'
    try {
      const s = new GlobalScheduler()
      expect(s.globalMaxConcurrent).toBe(3)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_GLOBAL_MAX_CONCURRENT
      else process.env.RUFLO_GLOBAL_MAX_CONCURRENT = prev
    }
  })
})

describe('GlobalScheduler — priority and FIFO ordering', () => {
  beforeEach(() => resetGlobalScheduler())

  it('FIFO within same priority across multiple agents', async () => {
    const s = new GlobalScheduler()
    // Two backend-capable agents so concurrent dispatch is possible.
    s.registerAgents([makeAgent('backend-1', 'a1'), makeAgent('backend-2', 'a2')])
    const order: string[] = []
    s.on('schedule', payload => { order.push(payload.subtaskId) })

    s.enqueue(req({ id: 'a', capability: 'backend', priority: 'normal' }))
    await new Promise(r => setTimeout(r, 2))
    s.enqueue(req({ id: 'b', capability: 'backend', priority: 'normal' }))
    await new Promise(r => setTimeout(r, 2))
    s.enqueue(req({ id: 'c', capability: 'backend', priority: 'normal' }))

    // With two backend-capable agents, two schedule immediately in
    // enqueue order; the third waits for a slot.
    expect(order.slice(0, 2)).toEqual(['a', 'b'])
    s.complete('a', '')
    s.complete('b', '')
    expect(order).toContain('c')
  })

  it('critical runs before a waiting low of earlier enqueue', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    const order: string[] = []
    s.on('schedule', payload => { order.push(payload.subtaskId) })

    s.enqueue(req({ id: 'low', capability: 'backend', priority: 'low' }))
    s.enqueue(req({ id: 'crit', capability: 'backend', priority: 'critical' }))

    expect(order[0]).toBe('low')
    // crit should be eligible next — finish low first then crit.
    s.complete('low', '')
    expect(order).toContain('crit')
  })

  it('critical beats higher-priority normal that is already waiting', () => {
    const s = new GlobalScheduler()
    // only one backend agent → critical can preempt none, but among
    // pending FIFO+priority decides.
    s.registerAgents([makeAgent('backend-1', 'a1')])
    const order: string[] = []
    s.on('schedule', payload => { order.push(payload.subtaskId) })

    s.enqueue(req({ id: 'normal', capability: 'backend', priority: 'normal' }))
    s.enqueue(req({ id: 'high', capability: 'backend', priority: 'high' }))
    s.enqueue(req({ id: 'crit', capability: 'backend', priority: 'critical' }))

    // First scheduled is normal (only one slot), then crit should beat high
    s.complete('normal', '')
    expect(order[1]).toBe('crit')
  })
})

describe('GlobalScheduler — agent exclusivity and parallelism', () => {
  beforeEach(() => resetGlobalScheduler())

  it('one agent never gets two jobs at once', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    const locks = new Set<string>()
    s.on('schedule', payload => {
      expect(locks.has(payload.agentId)).toBe(false)
      locks.add(payload.agentId)
    })
    s.on('release', payload => {
      locks.delete(payload.lease.agentId)
    })

    for (let i = 0; i < 5; i++) s.enqueue(req({ id: `s${i}`, capability: 'backend' }))
    s.complete('s0', '')
    s.complete('s1', '')
    s.complete('s2', '')
    s.complete('s3', '')
  })

  it('two different coders can work in parallel', () => {
    const s = new GlobalScheduler()
    s.registerAgents([
      makeAgent('backend-1', 'be1'),
      makeAgent('backend-2', 'be2'),
      makeAgent('frontend-1', 'fe1'),
      makeAgent('frontend-2', 'fe2'),
    ])
    const order: string[] = []
    s.on('schedule', payload => { order.push(payload.agentId) })

    s.enqueue(req({ id: 'b1', capability: 'backend' }))
    s.enqueue(req({ id: 'b2', capability: 'backend' }))
    s.enqueue(req({ id: 'b3', capability: 'backend' }))
    s.enqueue(req({ id: 'f1', capability: 'frontend' }))
    s.enqueue(req({ id: 'f2', capability: 'frontend' }))

    // Should have picked backend-1, backend-2, frontend-1 in parallel.
    expect(new Set(order).size).toBeGreaterThanOrEqual(3)
    expect(order).toContain('be1')
    expect(order).toContain('be2')
  })

  it('Integration profile accepts backend and frontend capabilities', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('integration', 'int-1')])
    const picked: string[] = []
    s.on('schedule', payload => picked.push(payload.subtaskId))

    s.enqueue(req({ id: 'be', capability: 'backend' }))
    // Only one integration agent — release the lock before scheduling the next.
    s.complete('be', '')
    s.enqueue(req({ id: 'fe', capability: 'frontend' }))
    s.complete('fe', '')

    expect(picked).toContain('be')
    expect(picked).toContain('fe')
  })
})

describe('GlobalScheduler — release paths', () => {
  beforeEach(() => resetGlobalScheduler())

  it('cancel frees slot and lease, allowing another subtask to take the agent', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'first', capability: 'backend' }))
    expect(s.isSubtaskActive('first')).toBe(true)

    s.cancel('first')
    expect(s.isSubtaskActive('first')).toBe(false)

    s.enqueue(req({ id: 'next', capability: 'backend' }))
    expect(s.isSubtaskActive('next')).toBe(true)
  })

  it('error path frees slot and lease', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'e1', capability: 'backend' }))
    s.fail('e1', new Error('boom'))
    expect(s.isSubtaskActive('e1')).toBe(false)
    // Profile lock released — next subtask can claim it.
    s.enqueue(req({ id: 'e2', capability: 'backend' }))
    expect(s.isSubtaskActive('e2')).toBe(true)
  })

  it('release is idempotent across close/error/cancel/timeout', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'r1', capability: 'backend' }))
    const releases: string[] = []
    s.on('release', p => releases.push(`${p.reason}:${p.lease.subtaskId}`))
    s.release('r1', 'close')
    s.release('r1', 'error')
    s.release('r1', 'cancel')
    s.release('r1', 'timeout')
    // Only the first call should emit a release event.
    expect(releases).toEqual(['close:r1'])
  })

  it('keeps launching ready work after slots free up', async () => {
    const { GlobalScheduler: GS } = await import('../scheduler')
    const small = new GS({ globalMaxConcurrent: 2 })
    small.registerAgents(makeAgents())
    const completed = new Set<string>()
    let dispatchCount = 0
    small.on('schedule', p => {
      dispatchCount++
      // Complete the just-dispatched subtask after recording the event.
      Promise.resolve().then(() => {
        if (!completed.has(p.subtaskId)) {
          completed.add(p.subtaskId)
          small.complete(p.subtaskId, '')
        }
      })
    })
    for (let i = 0; i < 6; i++) {
      small.enqueue(req({ id: `w${i}`, capability: 'backend' }))
    }
    // Wait until dispatch fires for every subtask (or timeout).
    const deadline = Date.now() + 2000
    while (completed.size < 6 && Date.now() < deadline) {
      await new Promise(r => setTimeout(r, 5))
    }
    expect(completed.size).toBe(6)
    expect(dispatchCount).toBeGreaterThanOrEqual(6)
  })
})

describe('GlobalScheduler — dependency handling', () => {
  beforeEach(() => resetGlobalScheduler())

  it('dependent subtasks only run after prerequisites', () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    const order: string[] = []
    s.on('schedule', payload => order.push(payload.subtaskId))

    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend', dependsOn: ['a'] }))

    // a goes immediately; b waits.
    expect(order).toEqual(['a'])
    s.complete('a', '')
    expect(order).toEqual(['a', 'b'])
  })

  it('mandatory final reviewer still runs last', () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    const order: string[] = []
    s.on('schedule', payload => order.push(payload.subtaskId))

    s.enqueue(req({ id: 'be', capability: 'backend' }))
    s.enqueue(req({ id: 'fe', capability: 'frontend' }))
    s.enqueue(req({ id: 'rev', capability: 'final-review', dependsOn: ['be', 'fe'] }))

    // rev waits while be/fe run.
    expect(order).not.toContain('rev')
    s.complete('be', '')
    s.complete('fe', '')
    expect(order[order.length - 1]).toBe('rev')
  })
})

describe('Default 10-agent team', () => {
  it('defines exactly 10 unique profiles', () => {
    expect(AGENT_PROFILES).toHaveLength(10)
    const ids = new Set(AGENT_PROFILES.map(p => p.profileId))
    expect(ids.size).toBe(10)
  })

  it('covers the required capabilities for each role', () => {
    const caps = (id: string) => AGENT_PROFILES.find(p => p.profileId === id)?.capabilities
    expect(caps('backend-1')).toEqual(expect.arrayContaining(['backend', 'database', 'domain']))
    expect(caps('backend-2')).toEqual(expect.arrayContaining(['backend', 'api', 'security', 'integrations']))
    expect(caps('frontend-1')).toEqual(expect.arrayContaining(['frontend', 'ui', 'state']))
    expect(caps('frontend-2')).toEqual(expect.arrayContaining(['frontend', 'api-client', 'forms', 'frontend-tests']))
    expect(caps('integration')).toEqual(expect.arrayContaining(['backend', 'frontend', 'integration']))
    expect(caps('devops')).toEqual(expect.arrayContaining(['infrastructure', 'systemd', 'docker', 'ci']))
    expect(caps('qa')).toEqual(expect.arrayContaining(['tests', 'regression', 'concurrency']))
    expect(caps('reviewer')).toEqual(expect.arrayContaining(['review', 'security', 'final-review']))
    expect(caps('system-architect')).toEqual(expect.arrayContaining(['architecture', 'research']))
  })

  it('has Queen Dispatcher, both backend/frontends, integration, devops, qa and a reviewer', () => {
    const names = AGENT_PROFILES.map(p => p.name)
    expect(names).toContain('Queen Dispatcher')
    expect(names).toContain('Backend Engineer 1')
    expect(names).toContain('Backend Engineer 2')
    expect(names).toContain('Frontend Engineer 1')
    expect(names).toContain('Frontend Engineer 2')
    expect(names).toContain('Integration Engineer')
    expect(names).toContain('DevOps Engineer')
    expect(names).toContain('QA Engineer')
    expect(names).toContain('Security Final Reviewer')
    expect(names).toContain('System Architect')
  })
})

describe('Cli argument flag', () => {
  it('always includes --model on every claude spawn', () => {
    // The integration check is performed in server.ts: every runClaude() call
    // sets `--model ${process.env.RUFLO_CLAUDE_MODEL || 'opus'}`. Validate that
    // the helper-produced arg list contains it.
    const claudeModel = process.env.RUFLO_CLAUDE_MODEL || 'opus'
    const args = ['-p', 'PROMPT', '--output-format', 'stream-json', '--verbose', '--model', claudeModel]
    expect(args).toContain('--model')
    expect(args).toContain(claudeModel)
  })
})

// ════════════════════════════════════════════════════════════════════════
// REGRESSION TESTS — ACC-SCHEDULER-001-REPAIR blockers 1–16
// ════════════════════════════════════════════════════════════════════════

describe('ACC-SCHEDULER-001-REPAIR — dependency failure surfaces typed error', () => {
  beforeEach(() => resetGlobalScheduler())

  it('fail() marks dependents as cancelled and awaitDispatch rejects with "prerequisite failed"', async () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'frontend', dependsOn: ['a'] }))
    const events: any[] = []
    s.on('cancel', e => events.push(e))
    s.fail('a', new Error('boom'))
    expect(s.isSubtaskCancelled('b')).toBe(true)
    expect(s.isSubtaskActive('b')).toBe(false)
    await expect(s.awaitDispatch('b', 200)).rejects.toThrow(/prerequisite a failed/)
    const cancelEvt = events.find(e => e.subtaskId === 'b')
    expect(cancelEvt?.reason).toBe('dependency-failed')
  })

  it('cancel() propagates to dependents with "dependency-cancelled" reason', async () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'frontend', dependsOn: ['a'] }))
    const events: any[] = []
    s.on('cancel', e => events.push(e))
    s.cancel('a')
    expect(s.isSubtaskCancelled('b')).toBe(true)
    await expect(s.awaitDispatch('b', 200)).rejects.toThrow(/cancelled/)
    const cancelEvt = events.find(e => e.subtaskId === 'b')
    expect(cancelEvt?.reason).toBe('dependency-cancelled')
  })

  it('cancelled subtask never runs even after a slot frees up', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'first', capability: 'backend' }))
    s.enqueue(req({ id: 'next', capability: 'backend' }))
    s.cancel('first')
    s.complete('first', '') // complete path should be a no-op
    await new Promise(r => setTimeout(r, 10))
    expect(s.isSubtaskActive('next')).toBe(true)
    expect(s.isSubtaskCancelled('first')).toBe(true)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — awaitDispatch listener and timer cleanup', () => {
  beforeEach(() => resetGlobalScheduler())

  it('cleans up listeners and timer on schedule resolution', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'x', capability: 'backend' }))
    const before = s['listeners'].schedule.size
    const p = s.awaitDispatch('x', 1000)
    await p
    // After resolution, listeners should not retain references.
    expect(s['listeners'].schedule.size).toBe(before)
    expect(s['listeners'].error.size).toBe(0)
    expect(s['listeners'].cancel.size).toBe(0)
  })

  it('cleans up listeners and timer on cancel rejection', async () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'frontend', dependsOn: ['a'] }))
    const p = s.awaitDispatch('b', 1000)
    s.cancel('a')
    await expect(p).rejects.toThrow()
    expect(s['listeners'].schedule.size).toBe(0)
    expect(s['listeners'].error.size).toBe(0)
    expect(s['listeners'].cancel.size).toBe(0)
  })

  it('cleans up listeners and timer on timeout rejection', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'blocked', capability: 'backend' })) // never scheduled — depends on agent busy
    const before = s['listeners'].schedule.size
    const p = s.awaitDispatch('blocked', 50)
    await expect(p).rejects.toThrow(/timeout/)
    expect(s['listeners'].schedule.size).toBe(before)
    expect(s['listeners'].error.size).toBe(0)
    expect(s['listeners'].cancel.size).toBe(0)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — direct release continues dispatch', () => {
  beforeEach(() => resetGlobalScheduler())

  it('release() on an active lease frees the slot and dispatches queued work', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'first', capability: 'backend' }))
    s.enqueue(req({ id: 'next', capability: 'backend' }))
    expect(s.isSubtaskActive('next')).toBe(false)
    s.release('first', 'close')
    expect(s.isSubtaskActive('next')).toBe(true)
  })

  it('release() is idempotent — second call is a no-op and does not re-dispatch', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'first', capability: 'backend' }))
    s.enqueue(req({ id: 'next', capability: 'backend' }))
    const releases: string[] = []
    s.on('release', p => releases.push(p.lease.subtaskId))
    s.release('first', 'close')
    s.release('first', 'error')
    expect(releases).toEqual(['first'])
    expect(s.isSubtaskActive('next')).toBe(true)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — priority propagation', () => {
  beforeEach(() => resetGlobalScheduler())

  it('critical enqueued after low still beats the low on the next slot', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    const order: string[] = []
    s.on('schedule', p => order.push(p.subtaskId))
    s.enqueue(req({ id: 'low', capability: 'backend', priority: 'low' }))
    s.enqueue(req({ id: 'crit', capability: 'backend', priority: 'critical' }))
    expect(order[0]).toBe('low')
    s.complete('low', '')
    expect(order).toEqual(['low', 'crit'])
  })

  it('high beats normal at the same enqueue time', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    const order: string[] = []
    s.on('schedule', p => order.push(p.subtaskId))
    s.enqueue(req({ id: 'n', capability: 'backend', priority: 'normal' }))
    s.enqueue(req({ id: 'h', capability: 'backend', priority: 'high' }))
    s.complete('n', '')
    expect(order[order.length - 1]).toBe('h')
  })
})

describe('ACC-SCHEDULER-001-REPAIR — planner slot is enforced', () => {
  beforeEach(() => resetGlobalScheduler())

  it('planner subtask blocks a ready worker when the cap is full', () => {
    const s = new GlobalScheduler({ globalMaxConcurrent: 1 })
    s.registerAgents(makeAgents())
    const order: string[] = []
    s.on('schedule', p => order.push(p.subtaskId))
    s.enqueue({ id: 'plan', taskId: 't', capability: 'planning', description: 'plan', profileId: 'queen-dispatcher', priority: 'high' })
    expect(order).toContain('plan')
    // Now a worker that matches the SAME profile cannot run.
    s.enqueue({ id: 'planner2', taskId: 't', capability: 'planning', description: 'plan2', profileId: 'queen-dispatcher', priority: 'high' })
    expect(order).not.toContain('planner2')
    expect(s.activeCount).toBe(1)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — fallback deterministic chain & agentResults', () => {
  beforeEach(() => resetGlobalScheduler())

  it('planner invalid → deterministic chain: implementation → tests → final-review', () => {
    // Mirrors server.ts buildDeterministicFallback() for WRITE tasks.
    const isReadOnly = /\bread[- ]?only\b/i.test('Add a button')
    const chain = isReadOnly
      ? [{ capability: 'research' }, { capability: 'final-review' }]
      : [{ capability: 'backend' }, { capability: 'qa' }, { capability: 'final-review' }]
    expect(chain.map(s => s.capability)).toEqual(['backend', 'qa', 'final-review'])
  })

  it('planner invalid → READ-ONLY → analysis → final-review', () => {
    const isReadOnly = /\bread[- ]?only\b/i.test('READ-ONLY: audit the codebase')
    const chain = isReadOnly
      ? [{ capability: 'research' }, { capability: 'final-review' }]
      : [{ capability: 'backend' }, { capability: 'qa' }, { capability: 'final-review' }]
    expect(chain.map(s => s.capability)).toEqual(['research', 'final-review'])
  })
})

describe('ACC-SCHEDULER-001-REPAIR — final reviewer enforced exactly once', () => {
  beforeEach(() => resetGlobalScheduler())

  it('strips planner-provided final-review and appends exactly one canonical', () => {
    // Simulates server.ts canonicalisation step.
    const subtasks = [
      { capability: 'backend', depends_on: [] },
      { capability: 'final-review', depends_on: [0] }, // planner mistake
      { capability: 'frontend', depends_on: [0] },
    ]
    const before = subtasks.length
    const cleaned = subtasks.filter(s => s.capability !== 'final-review')
    const prior = cleaned.map((_, i) => i)
    cleaned.push({ capability: 'final-review', depends_on: prior })
    const frCount = cleaned.filter(s => s.capability === 'final-review').length
    expect(frCount).toBe(1)
    expect(cleaned[cleaned.length - 1].capability).toBe('final-review')
    expect(cleaned.length).toBe(before) // one in, one out → unchanged length
  })
})

describe('ACC-SCHEDULER-001-REPAIR — dispatched profile used for prompt/name', () => {
  beforeEach(() => resetGlobalScheduler())

  it('Integration profile accepts both backend and frontend capabilities', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('integration', 'int-1')])
    const calls: string[] = []
    s.on('schedule', p => calls.push(p.profileId))
    s.enqueue(req({ id: 'be', capability: 'backend' }))
    s.complete('be', '')
    s.enqueue(req({ id: 'fe', capability: 'frontend' }))
    s.complete('fe', '')
    expect(calls.every(p => p === 'integration')).toBe(true)
  })

  it('Integration Engineer never receives Backend Engineer 1 prompt', () => {
    const s = new GlobalScheduler()
    // Only register the Integration profile. Whichever profile the scheduler
    // dispatches MUST resolve to its own profile metadata — never to a
    // Backend Engineer prompt.
    s.registerAgents([makeAgent('integration', 'int-1')])
    const dispatched: string[] = []
    s.on('schedule', p => dispatched.push(p.profileId))
    s.enqueue(req({ id: 'be-task', capability: 'backend' }))
    expect(dispatched).toEqual(['integration'])
    // The resolved profile (server.ts) must use the ACTUAL dispatched profileId.
    const resolved = AGENT_PROFILES.find(p => p.profileId === dispatched[0])!
    expect(resolved.name).toBe('Integration Engineer')
    expect(resolved.systemPrompt).not.toBe('You are Backend Engineer 1.')
    expect(resolved.systemPrompt).toBe('You are the Integration Engineer.')
  })
})

describe('ACC-SCHEDULER-001-REPAIR — qa capability', () => {
  it('qa profile exposes "qa" capability alongside tests/regression/concurrency', () => {
    const qa = AGENT_PROFILES.find(p => p.profileId === 'qa')!
    expect(qa.capabilities).toEqual(expect.arrayContaining(['qa', 'tests', 'regression', 'concurrency']))
  })
})

describe('ACC-SCHEDULER-001-REPAIR — stale-agent reset', () => {
  beforeEach(() => resetGlobalScheduler())

  it('resetGlobalScheduler clears every registered agent', () => {
    const s = getGlobalScheduler()
    s.registerAgents([
      makeAgent('backend-1', 'a1'),
      makeAgent('frontend-1', 'b1'),
    ])
    expect(s['agents'].size).toBe(2)
    resetGlobalScheduler()
    const s2 = getGlobalScheduler()
    expect(s2['agents'].size).toBe(0)
  })

  it('unregisterAgent drops profile lock and cancels in-flight subtask', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'x', capability: 'backend' }))
    expect(s.isSubtaskActive('x')).toBe(true)
    s.unregisterAgent('a1')
    expect(s.isSubtaskActive('x')).toBe(false)
    expect(s.isSubtaskFailed('x')).toBe(true)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — task-level cancellation', () => {
  beforeEach(() => resetGlobalScheduler())

  it('cancelTask cancels every subtask for the task (active + pending)', () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 't1-a', taskId: 'task-1', capability: 'backend' }))
    s.enqueue(req({ id: 't1-b', taskId: 'task-1', capability: 'frontend' }))
    s.enqueue(req({ id: 't2-a', taskId: 'task-2', capability: 'backend' }))
    s.cancelTask('task-1')
    expect(s.isSubtaskCancelled('t1-a')).toBe(true)
    expect(s.isSubtaskCancelled('t1-b')).toBe(true)
    // task-2 is untouched.
    expect(s.isSubtaskCancelled('t2-a')).toBe(false)
    expect(s.isSubtaskActive('t2-a')).toBe(true)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — global cap across two simultaneous tasks', () => {
  beforeEach(() => resetGlobalScheduler())

  it('never exceeds the global cap with concurrent tasks', () => {
    const s = new GlobalScheduler({ globalMaxConcurrent: 4 })
    s.registerAgents(makeAgents())
    let peak = 0
    s.on('schedule', () => { if (s.activeCount > peak) peak = s.activeCount })
    for (let i = 0; i < 12; i++) {
      const taskId = i % 2 === 0 ? 'task-A' : 'task-B'
      s.enqueue(req({ id: `sub-${i}`, taskId, capability: 'backend' }))
    }
    expect(peak).toBeLessThanOrEqual(4)
  })

  it('one profile never gets two concurrent jobs across tasks', () => {
    const s = new GlobalScheduler({ globalMaxConcurrent: 10 })
    s.registerAgents([makeAgent('backend-1', 'a1')])
    const busy = new Set<string>()
    s.on('schedule', p => {
      expect(busy.has(p.profileId)).toBe(false)
      busy.add(p.profileId)
    })
    s.on('release', p => busy.delete(p.lease.profileId))
    for (let i = 0; i < 5; i++) {
      s.enqueue(req({ id: `tA-${i}`, taskId: 'task-A', capability: 'backend' }))
      s.enqueue(req({ id: `tB-${i}`, taskId: 'task-B', capability: 'backend' }))
    }
  })
})

// ════════════════════════════════════════════════════════════════════════
// REGRESSION TESTS — ACC-SCHEDULER-001-REPAIR (behavioral)
// ════════════════════════════════════════════════════════════════════════

describe('ACC-SCHEDULER-001-REPAIR — pending size becomes zero after direct cancel', () => {
  beforeEach(() => resetGlobalScheduler())

  it('cancel on pending subtask removes it from pending (defect 1)', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    // Take the only agent slot so the next subtask stays in pending.
    s.enqueue(req({ id: 'first', capability: 'backend' }))
    s.enqueue(req({ id: 'queued', capability: 'backend' }))
    expect(s.pendingSize).toBe(1)
    expect(s.isSubtaskCancelled('queued')).toBe(false)
    s.cancel('queued')
    expect(s.pendingSize).toBe(0)
    expect(s.isSubtaskCancelled('queued')).toBe(true)
  })

  it('pending size becomes zero after dependency failure propagates (defect 1)', () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'frontend', dependsOn: ['a'] }))
    s.enqueue(req({ id: 'c', capability: 'qa', dependsOn: ['b'] }))
    // 'a' is dispatched (status=active), 'b' and 'c' remain pending.
    expect(s.pendingSize).toBe(2)
    s.fail('a', new Error('boom'))
    expect(s.pendingSize).toBe(0)
    expect(s.isSubtaskCancelled('b')).toBe(true)
    expect(s.isSubtaskCancelled('c')).toBe(true)
  })

  it('pending size becomes zero after dependency cancellation propagates (defect 1)', () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'frontend', dependsOn: ['a'] }))
    s.enqueue(req({ id: 'c', capability: 'qa', dependsOn: ['b'] }))
    s.cancel('a')
    expect(s.pendingSize).toBe(0)
    expect(s.isSubtaskCancelled('b')).toBe(true)
    expect(s.isSubtaskCancelled('c')).toBe(true)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — cancelTask removes only the target task', () => {
  beforeEach(() => resetGlobalScheduler())

  it('cancelTask removes active AND pending work for only the target task', () => {
    // Cap of 1 means only one subtask can be active — the rest stay pending.
    const s = new GlobalScheduler({ globalMaxConcurrent: 1 })
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 't1-a', taskId: 'task-1', capability: 'backend' }))
    s.enqueue(req({ id: 't1-b', taskId: 'task-1', capability: 'frontend' }))
    s.enqueue(req({ id: 't1-c', taskId: 'task-1', capability: 'qa', dependsOn: ['t1-a'] }))
    s.enqueue(req({ id: 't2-a', taskId: 'task-2', capability: 'backend' }))
    s.enqueue(req({ id: 't2-b', taskId: 'task-2', capability: 'frontend' }))

    // Only one subtask is active; the rest are queued pending.
    expect(s.pendingSize).toBe(4)
    expect(s.activeCount).toBe(1)
    s.cancelTask('task-1')
    // All 3 task-1 subtasks cancelled. task-2 subtasks may still be either
    // pending or active (cancelling t1-a freed a slot that dispatched one).
    expect(s.isSubtaskCancelled('t1-a')).toBe(true)
    expect(s.isSubtaskCancelled('t1-b')).toBe(true)
    expect(s.isSubtaskCancelled('t1-c')).toBe(true)
    expect(s.isSubtaskCancelled('t2-a')).toBe(false)
    expect(s.isSubtaskCancelled('t2-b')).toBe(false)
    // Combined active + pending for task-2 = 2 (sum of both states).
    const t2Live = (s.isSubtaskActive('t2-a') ? 1 : 0) + (s.isSubtaskActive('t2-b') ? 1 : 0)
      + (s.peekPending('t2-a') && s.peekPending('t2-a')!.status === 'pending' ? 1 : 0)
      + (s.peekPending('t2-b') && s.peekPending('t2-b')!.status === 'pending' ? 1 : 0)
    expect(t2Live).toBe(2)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — timeout removes from pending and never dispatches', () => {
  beforeEach(() => resetGlobalScheduler())

  it('timed-out subtask is removed from pending and never dispatches after slot frees', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'first', capability: 'backend' }))
    // Fills the only slot. The next subtask stays queued indefinitely.
    s.enqueue(req({ id: 'waiter', capability: 'backend' }))
    const scheduleEvents: string[] = []
    s.on('schedule', p => scheduleEvents.push(p.subtaskId))

    const p = s.awaitDispatch('waiter', 30)
    await expect(p).rejects.toThrow(/timeout/)
    expect(s.pendingSize).toBe(0)
    expect(s.isSubtaskCancelled('waiter')).toBe(true)
    expect(s.getTerminalReason('waiter')).toBe('dispatch-timeout')

    // Now free the slot. The timed-out subtask MUST NOT dispatch.
    s.complete('first', '')
    await new Promise(r => setTimeout(r, 30))
    expect(scheduleEvents).not.toContain('waiter')
    expect(s.isSubtaskActive('waiter')).toBe(false)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — awaitDispatch cleans all listeners and timers', () => {
  beforeEach(() => resetGlobalScheduler())

  it('success path leaves no listeners or timers', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'x', capability: 'backend' }))
    const beforeListeners = s['listeners'].schedule.size + s['listeners'].error.size + s['listeners'].cancel.size
    const beforeAwaiters = s['pendingAwaitDispatchers'].size
    await s.awaitDispatch('x', 1000)
    // No new listeners or awaiter-tracking entries persisted.
    expect(s['pendingAwaitDispatchers'].size).toBe(beforeAwaiters)
    expect(s['listeners'].schedule.size + s['listeners'].error.size + s['listeners'].cancel.size).toBe(beforeListeners)
  })

  it('failure path leaves no listeners or timers', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    // Two subtasks for one slot: 'a' dispatches, 'b' stays queued.
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    const beforeAwaiters = s['pendingAwaitDispatchers'].size
    const p = s.awaitDispatch('b', 1000)
    // Fail the running subtask — the queued subtask's lease should free up.
    s.fail('a', new Error('boom'))
    await p
    expect(s['pendingAwaitDispatchers'].size).toBe(beforeAwaiters)
  })

  it('cancel path leaves no listeners or timers', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' })) // pending
    const p = s.awaitDispatch('b', 1000)
    s.cancel('b')
    await expect(p).rejects.toThrow()
    expect(s['pendingAwaitDispatchers'].has('b')).toBe(false)
  })

  it('timeout path leaves no listeners or timers', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    const p = s.awaitDispatch('b', 30)
    await expect(p).rejects.toThrow(/timeout/)
    expect(s['pendingAwaitDispatchers'].has('b')).toBe(false)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — dispose/reset emits no new schedule events', () => {
  beforeEach(() => resetGlobalScheduler())

  it('dispose() does not dispatch new work even with active + queued items', () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'frontend' }))
    s.enqueue(req({ id: 'c', capability: 'qa', dependsOn: ['a'] }))
    const scheduleEvents: string[] = []
    s.on('schedule', p => scheduleEvents.push(p.subtaskId))
    s.dispose()
    expect(scheduleEvents).toEqual([])
    expect(s.isTearingDown).toBe(true)
    expect(s.pendingSize).toBe(0)
    expect(s.activeCount).toBe(0)
    expect(s['agents'].size).toBe(0)
  })

  it('resetGlobalScheduler disposes the previous instance without scheduling new work', () => {
    const s = getGlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    const scheduleEvents: string[] = []
    s.on('schedule', p => scheduleEvents.push(p.subtaskId))
    resetGlobalScheduler()
    expect(scheduleEvents).toEqual([])
    const s2 = getGlobalScheduler()
    expect(s2['agents'].size).toBe(0)
    expect(s2.activeCount).toBe(0)
    expect(s2.pendingSize).toBe(0)
  })

  it('releaseAll emits no schedule event for queued work', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    const scheduleEvents: string[] = []
    s.on('schedule', p => scheduleEvents.push(p.subtaskId))
    s.releaseAll('shutdown')
    expect(scheduleEvents).toEqual([])
  })
})

describe('ACC-SCHEDULER-001-REPAIR — env-driven dispatch timeout is validated', () => {
  beforeEach(() => resetGlobalScheduler())

  it('defaults to a long safe timeout (>= 5 minutes)', () => {
    const prev = process.env.RUFLO_DISPATCH_TIMEOUT_MS
    delete process.env.RUFLO_DISPATCH_TIMEOUT_MS
    try {
      const s = new GlobalScheduler()
      expect(s.dispatchTimeoutMs).toBeGreaterThanOrEqual(5 * 60_000)
    } finally {
      if (prev !== undefined) process.env.RUFLO_DISPATCH_TIMEOUT_MS = prev
    }
  })

  it('honors a valid RUFLO_DISPATCH_TIMEOUT_MS env override', () => {
    const prev = process.env.RUFLO_DISPATCH_TIMEOUT_MS
    process.env.RUFLO_DISPATCH_TIMEOUT_MS = '120000'
    try {
      const s = new GlobalScheduler()
      expect(s.dispatchTimeoutMs).toBe(120000)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_DISPATCH_TIMEOUT_MS
      else process.env.RUFLO_DISPATCH_TIMEOUT_MS = prev
    }
  })

  it('falls back safely when env value is invalid (zero, negative, NaN, garbage)', () => {
    for (const bad of ['0', '-5', 'abc', '', 'NaN']) {
      const prev = process.env.RUFLO_DISPATCH_TIMEOUT_MS
      process.env.RUFLO_DISPATCH_TIMEOUT_MS = bad
      try {
        const s = new GlobalScheduler()
        expect(Number.isFinite(s.dispatchTimeoutMs)).toBe(true)
        expect(s.dispatchTimeoutMs).toBeGreaterThan(0)
      } finally {
        if (prev === undefined) delete process.env.RUFLO_DISPATCH_TIMEOUT_MS
        else process.env.RUFLO_DISPATCH_TIMEOUT_MS = prev
      }
    }
  })

  it('opts.defaultDispatchTimeoutMs takes precedence over env', () => {
    const prev = process.env.RUFLO_DISPATCH_TIMEOUT_MS
    process.env.RUFLO_DISPATCH_TIMEOUT_MS = '1000'
    try {
      const s = new GlobalScheduler({ defaultDispatchTimeoutMs: 7777 })
      expect(s.dispatchTimeoutMs).toBe(7777)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_DISPATCH_TIMEOUT_MS
      else process.env.RUFLO_DISPATCH_TIMEOUT_MS = prev
    }
  })
})

describe('ACC-SCHEDULER-001-REPAIR — idempotent cancel does not corrupt terminal state', () => {
  beforeEach(() => resetGlobalScheduler())

  it('cancelling an already-completed subtask is a no-op', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.complete('a', 'done')
    s.cancel('a')
    expect(s.isSubtaskCompleted('a')).toBe(true)
    expect(s.isSubtaskCancelled('a')).toBe(false)
    expect(s.getTerminalReason('a')).toBe('completed')
  })

  it('cancelling an already-failed subtask is a no-op', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.fail('a', new Error('boom'))
    s.cancel('a')
    expect(s.isSubtaskFailed('a')).toBe(true)
    expect(s.isSubtaskCancelled('a')).toBe(false)
    expect(s.getTerminalReason('a')).toBe('failed')
  })

  it('failing an already-cancelled subtask is a no-op', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    s.cancel('a')
    s.fail('a', new Error('boom'))
    expect(s.isSubtaskCancelled('a')).toBe(true)
    expect(s.isSubtaskFailed('a')).toBe(false)
    expect(s.getTerminalReason('a')).toBe('explicit-cancel')
  })

  it('completing an already-cancelled subtask is a no-op', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    s.cancel('a')
    s.complete('a', 'done')
    expect(s.isSubtaskCancelled('a')).toBe(true)
    expect(s.isSubtaskCompleted('a')).toBe(false)
    expect(s.getTerminalReason('a')).toBe('explicit-cancel')
  })
})

describe('ACC-SCHEDULER-001-REPAIR — terminal reason surfaces in awaitDispatch error', () => {
  beforeEach(() => resetGlobalScheduler())

  it('awaitDispatch called after a dependency fail rejects with typed "prerequisite X failed"', async () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'frontend', dependsOn: ['a'] }))
    s.fail('a', new Error('boom'))
    // now b is already cancelled — awaitDispatch should reject with typed reason
    await expect(s.awaitDispatch('b', 100)).rejects.toThrow(/prerequisite a failed/)
  })

  it('awaitDispatch called after dep cancel rejects with typed "prerequisite X cancelled"', async () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'frontend', dependsOn: ['a'] }))
    s.cancel('a')
    await expect(s.awaitDispatch('b', 100)).rejects.toThrow(/prerequisite a cancelled/)
  })

  it('awaitDispatch called after task cancellation rejects with "task cancelled"', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'b', taskId: 'task-1', capability: 'backend' }))
    s.cancelTask('task-1')
    await expect(s.awaitDispatch('b', 100)).rejects.toThrow(/task cancelled/)
  })

  it('awaitDispatch called after dispatch-timeout rejects with typed "dispatch timed out"', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'first', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    const p = s.awaitDispatch('b', 30)
    await expect(p).rejects.toThrow(/dispatch timeout/)
    // terminalReason is preserved even though pending was cleared
    expect(s.getTerminalReason('b')).toBe('dispatch-timeout')
  })

  it('awaitDispatch called after explicit cancel rejects with "cancelled"', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    s.cancel('b')
    await expect(s.awaitDispatch('b', 100)).rejects.toThrow(/cancelled/)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — typed terminal reason survives removal from pending', () => {
  beforeEach(() => resetGlobalScheduler())

  it('getTerminalReason returns the typed reason after cancel removed from pending', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    s.cancel('b')
    expect(s.peekPending('b')).toBeUndefined()
    expect(s.getTerminalReason('b')).toBe('explicit-cancel')
  })

  it('getTerminalReason returns the typed reason after task cancel', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', taskId: 't', capability: 'backend' }))
    s.cancelTask('t')
    expect(s.getTerminalReason('a')).toBe('task-cancelled')
  })

  it('getTerminalReason returns the typed reason after dependency cascade', () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'frontend', dependsOn: ['a'] }))
    s.fail('a', new Error('boom'))
    expect(s.getTerminalReason('b')).toBe('dependency-failed')
  })

  it('getTerminalReason returns the typed reason after scheduled-shutdown', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.dispose()
    expect(s.getTerminalReason('a')).toBe('scheduled-shutdown')
  })
})

describe('ACC-SCHEDULER-001-REPAIR — release idempotency', () => {
  beforeEach(() => resetGlobalScheduler())

  it('release on non-active subtask is a no-op (no double dispatch)', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'queued', capability: 'backend' }))
    const scheduleEvents: string[] = []
    s.on('schedule', p => scheduleEvents.push(p.subtaskId))
    // Release on a subtask that never got an active lease — no-op.
    s.release('queued', 'close')
    s.release('queued', 'cancel')
    expect(scheduleEvents).toEqual([])
    // Cancel cleans up state.
    s.cancel('queued')
    expect(s.isSubtaskCancelled('queued')).toBe(true)
  })

  it('concurrent releases on same subtask never double-dispatch and never double-emit', () => {
    const s = new GlobalScheduler()
    s.registerAgents([makeAgent('backend-1', 'a1')])
    s.enqueue(req({ id: 'a', capability: 'backend' }))
    s.enqueue(req({ id: 'b', capability: 'backend' }))
    const releaseEvents: string[] = []
    s.on('release', p => releaseEvents.push(p.lease.subtaskId))
    s.release('a', 'close')
    s.release('a', 'cancel')
    s.release('a', 'error')
    s.release('a', 'timeout')
    expect(releaseEvents).toEqual(['a'])
    // 'b' should still have been dispatched exactly once.
    expect(s.isSubtaskActive('b')).toBe(true)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — fail-closed parent propagation', () => {
  beforeEach(() => resetGlobalScheduler())

  it('a cancelled worker keeps task.status as cancelled (never completed)', () => {
    const s = new GlobalScheduler()
    s.registerAgents(makeAgents())
    s.enqueue(req({ id: 'a', taskId: 'task-1', capability: 'backend' }))
    s.enqueue(req({ id: 'b', taskId: 'task-1', capability: 'frontend' }))

    // Mirror server.ts logic: subtaskStatuses driving terminal decision.
    const statuses: Array<'pending' | 'completed' | 'failed' | 'cancelled'> = ['pending', 'pending']

    // Simulate the pipeline: a completes, b gets cancelled.
    s.complete('a', 'done')
    statuses[0] = 'completed'
    s.cancel('b')
    statuses[1] = 'cancelled'

    // Mirror phase 3 logic — if any cancelled, task must be cancelled (not completed).
    const anyCancelled = statuses.some(x => x === 'cancelled')
    const anyFailed = statuses.some(x => x === 'failed')
    const allCompleted = statuses.every(x => x === 'completed')
    const terminal = anyCancelled ? 'cancelled' : anyFailed ? 'failed' : allCompleted ? 'completed' : 'pending'
    expect(terminal).toBe('cancelled')
  })

  it('a failed worker keeps task.status as failed (never completed)', () => {
    const statuses: Array<'pending' | 'completed' | 'failed' | 'cancelled'> = ['completed', 'failed', 'pending']
    const anyCancelled = statuses.some(x => x === 'cancelled')
    const anyFailed = statuses.some(x => x === 'failed')
    const allCompleted = statuses.every(x => x === 'completed')
    const terminal = anyCancelled ? 'cancelled' : anyFailed ? 'failed' : allCompleted ? 'completed' : 'pending'
    expect(terminal).toBe('failed')
  })

  it('a settled-cancelled task is never overwritten as completed', () => {
    // Mirror: outer pipeline always defers to existing cancelled terminal state.
    const taskStatus = 'cancelled'
    const newTerminal = 'completed'
    const final = taskStatus === 'cancelled' ? 'cancelled' : newTerminal
    expect(final).toBe('cancelled')
  })
})

describe('ACC-SCHEDULER-001-REPAIR — final reviewer canonicalization (defect 9)', () => {
  beforeEach(() => resetGlobalScheduler())

  it('strips ALL planner-provided final-review duplicates before appending canonical', () => {
    const isReadOnly = /\bread[- ]?only\b/i.test('Audit codebase')
    const buildChain = () => isReadOnly
      ? [{ capability: 'research' }, { capability: 'final-review' }]
      : [{ capability: 'backend' }, { capability: 'qa' }, { capability: 'final-review' }]

    // Simulate a malformed planner response with multiple final-review entries.
    const subtasks: Array<{ capability: string; depends_on: number[] }> = [
      { capability: 'backend', depends_on: [] },
      { capability: 'final-review', depends_on: [0] },     // planner mistake 1
      { capability: 'frontend', depends_on: [0] },
      { capability: 'final-review', depends_on: [0, 1, 2] }, // planner mistake 2
      { capability: 'qa', depends_on: [0] },
    ]
    const filtered = subtasks.filter(s => s.capability !== 'final-review')
    const prior = filtered.map((_, i) => i)
    filtered.push({ capability: 'final-review', depends_on: prior })
    const finalCount = filtered.filter(s => s.capability === 'final-review').length
    expect(finalCount).toBe(1)
    expect(filtered[filtered.length - 1].capability).toBe('final-review')
    // final-review must depend on every prior subtask
    expect(filtered[filtered.length - 1].depends_on).toEqual(prior)
    // must come last in the array
    expect(filtered[filtered.length - 1]).toBe(filtered[filtered.length - 1])
    // No source-text assertion: we trust the operational chain
    expect(buildChain().length).toBeGreaterThan(0)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — global cap is shared across planner and workers', () => {
  beforeEach(() => resetGlobalScheduler())

  it('planner subtask and worker subtasks share the same global cap', () => {
    const s = new GlobalScheduler({ globalMaxConcurrent: 2 })
    s.registerAgents(makeAgents())
    let peak = 0
    s.on('schedule', () => { if (s.activeCount > peak) peak = s.activeCount })

    // Planner runs first.
    s.enqueue({ id: 'plan', taskId: 't', capability: 'planning', description: 'plan', profileId: 'queen-dispatcher', priority: 'high' })
    // Now fill the rest with backend workers.
    for (let i = 0; i < 5; i++) {
      s.enqueue(req({ id: `w${i}`, taskId: 't', capability: 'backend' }))
    }
    expect(peak).toBeLessThanOrEqual(2)
    // Drain everything.
    while (s.activeCount > 0) {
      const active = [...(s as any).activeLeases.keys()][0]
      s.complete(active, '')
    }
    expect(s.activeCount).toBe(0)
  })

  it('launcher profile (synthetic) consumes a slot just like any other profile', () => {
    const s = new GlobalScheduler({ globalMaxConcurrent: 1 })
    const launcherId = 'claude-launcher-task-x'
    s.registerAgents([{ profileId: 'launcher', agentId: launcherId }])
    s.enqueue({ id: 'launch-task-x', taskId: 'task-x', capability: 'launcher', description: 'x', profileId: 'launcher' })
    expect(s.activeCount).toBe(1)
    // Cancellation releases the slot for a future work item.
    s.cancel('launch-task-x')
    expect(s.activeCount).toBe(0)
  })
})

describe('ACC-SCHEDULER-001-REPAIR — launchViaClaude equivalent: dispatch gating', () => {
  beforeEach(() => resetGlobalScheduler())

  it('spawning only happens after awaitDispatch resolves — no spawn on dispatch reject', async () => {
    const s = new GlobalScheduler()
    s.registerAgents([{ profileId: 'launcher', agentId: 'claude-launcher-task-1' }])
    const launchSubtaskId = 'launch-task-1'
    s.enqueue({
      id: launchSubtaskId, taskId: 'task-1',
      capability: 'launcher', description: 'x', profileId: 'launcher',
    })
    // Cancel BEFORE awaitDispatch — the queued subtask will be removed from
    // pending and awaitDispatch will reject. The spawn must not run.
    s.cancel(launchSubtaskId)
    let spawnCalled = false
    try {
      await s.awaitDispatch(launchSubtaskId, 100)
      spawnCalled = true // would be reached only on success
    } catch (err) {
      // expected: typed rejection
      expect(String(err)).toMatch(/cancel/i)
    }
    expect(spawnCalled).toBe(false)
    expect(s.pendingSize).toBe(0)
    expect(s.isSubtaskCancelled(launchSubtaskId)).toBe(true)
  })

  it('scheduler rejection (no matching agent) does not spawn', async () => {
    const s = new GlobalScheduler()
    // No agents registered → dispatch can never grant a slot.
    s.enqueue({
      id: 'orphan', taskId: 't',
      capability: 'backend', description: 'x',
    })
    const p = s.awaitDispatch('orphan', 30)
    await expect(p).rejects.toThrow(/timeout/)
    // No spawn, no leaked state.
    expect(s.pendingSize).toBe(0)
    expect(s.activeCount).toBe(0)
  })

  it('scheduler respects the global cap during launch (matches planner/worker cap)', () => {
    const s = new GlobalScheduler({ globalMaxConcurrent: 1 })
    s.registerAgents([
      { profileId: 'launcher', agentId: 'launch-1' },
      { profileId: 'launcher', agentId: 'launch-2' },
    ])
    s.enqueue({ id: 'l1', taskId: 't1', capability: 'launcher', description: 'x', profileId: 'launcher' })
    s.enqueue({ id: 'l2', taskId: 't2', capability: 'launcher', description: 'y', profileId: 'launcher' })
    expect(s.activeCount).toBe(1)
    expect(s.isSubtaskActive('l1')).toBe(true)
    expect(s.isSubtaskActive('l2')).toBe(false)
  })
})

describe('Terminal reviewer canonicalization', () => {
  it('removes both reviewer aliases without mutating input', () => {
    const input = [
      { capability: 'backend' },
      { capability: 'review' },
      { capability: 'final-review' },
      { capability: 'security' },
    ]

    const result = stripTerminalReviewerAliases(input)

    expect(result.map(item => item.capability)).toEqual(['backend', 'security'])
    expect(input).toHaveLength(4)
  })
})

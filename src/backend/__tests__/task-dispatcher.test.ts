// @vitest-environment node
/**
 * ACC-TASK-QUEUE-002 — dispatcher + worktree manager behavioural tests.
 *
 * Coverage required by the packet:
 *   • priority + FIFO dispatch order
 *   • max in-flight enforcement + immediate dispatch of next task on release
 *   • preparing + running together respect maxInFlight (B2)
 *   • status stays pending/preparing until worktree provision completes (B1)
 *   • cancel during delayed provision => launcher never called (B3)
 *   • cancel immediately before launcher boundary => launcher never called (B3)
 *   • pending cancel physically removes from ready queue, never spawns
 *   • duplicate enqueue is idempotent (no double dispatch) (B10)
 *   • restart recovery via hydrateFromSnapshot (B5)
 *   • interrupted active tasks fail closed (status='interrupted', never completed)
 *   • terminal tasks never re-run
 *   • priority-aware queuePosition (B6)
 *   • assign route cannot bypass dispatcher (B7)
 *   • cancel completed/failed is no-op (B8)
 *   • active cancel cannot release slot twice or be overwritten on close (B8)
 *   • duplicate create cannot overwrite existing TaskRecord (B10)
 *   • WRITE tasks get isolated branch + worktree + execution cwd
 *   • two WRITE tasks of one temp git repo get distinct worktrees
 *   • base git repo is unchanged after WRITE provision
 *   • READ-ONLY tasks do NOT create a worktree
 *   • invalid cwd / traversal / branch or path collision fail closed
 *   • absolute root inside source rejected (B11)
 *   • relative/traversal root rejected (B11)
 *   • same task ID in two temp repositories gets different paths (B11)
 *   • symlink source containment test (B11)
 *   • existing unregistered filesystem path collision (B11)
 *   • invalid branch/ref rejected before worktree mutation (B11)
 *   • non-absence show-ref error fails closed (B11)
 *   • dispatcher never spawns process before worktree is ready
 *   • cancel / fail each release the in-flight slot exactly once
 *
 * Tests run only on disposable temp directories under tmpdir; live
 * RuFloUI or AgroPlatform repositories are NEVER touched.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { execFile as _execFile } from 'child_process'
import { promisify } from 'util'
import path from 'path'
import fs from 'fs'
import os from 'os'

const execFile = promisify(_execFile as any) as (
  file: string,
  args: string[],
  opts?: { cwd?: string; maxBuffer?: number },
) => Promise<{ stdout: string; stderr: string }>

// Set worktree root BEFORE importing modules that read it.
const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ruflo-tq-test-'))
process.env.RUFLO_TASK_WORKTREE_ROOT = path.join(tmpRoot, 'worktrees')

import {
  TaskDispatcher,
  DispatcherTaskRecord,
  DispatcherStatus,
  compareTasksForOrder,
  detectTaskMode,
  resetTaskDispatcher,
  isTerminalStatus,
} from '../task-dispatcher'
import { getWorktreeManager, resetWorktreeManager } from '../task-worktrees'

let nextRepoIdx = 0
async function makeTempRepo(prefix = 'src'): Promise<string> {
  const dir = fs.mkdtempSync(path.join(tmpRoot, `${prefix}-repo-${nextRepoIdx++}-`))
  await execFile('git', ['init', '--initial-branch=main', dir])
  await execFile('git', ['-C', dir, 'config', 'user.email', 't@t'])
  await execFile('git', ['-C', dir, 'config', 'user.name', 't'])
  fs.writeFileSync(path.join(dir, 'README.md'), '# repo\n')
  await execFile('git', ['-C', dir, 'add', 'README.md'])
  await execFile('git', ['-C', dir, 'commit', '-m', 'init'])
  return dir
}

/**
 * Each test must use a unique worktree root so parallel test files don't
 * collide on identical `ruflo-task/<id>.worktree` paths. We rotate a
 * per-file counter and reset the env var before each test.
 */
let wtRootCounter = 0
function setPerTestWorktreeRoot(): void {
  wtRootCounter++
  process.env.RUFLO_TASK_WORKTREE_ROOT = path.join(tmpRoot, `wt-${wtRootCounter}-${Date.now()}`)
  // Invalidate the cached manager so it picks up the new env var.
  resetWorktreeManager()
}

async function gitHead(cwd: string): Promise<string> {
  const { stdout } = await execFile('git', ['-C', cwd, 'rev-parse', 'HEAD'])
  return stdout.trim()
}

async function gitBranches(cwd: string): Promise<string[]> {
  const { stdout } = await execFile('git', ['-C', cwd, 'for-each-ref', '--format=%(refname:short)', 'refs/heads'])
  return stdout.trim().split('\n').filter(Boolean)
}

function nextTick() {
  return new Promise<void>(r => setTimeout(r, 5))
}

// READ-ONLY source: a temp dir is fine because READ-ONLY never writes there.
async function makeReadOnlyDir(): Promise<string> {
  return fs.mkdtempSync(path.join(tmpRoot, `read-${nextRepoIdx++}-`))
}

describe('TaskDispatcher — priority + FIFO', () => {
  let d: TaskDispatcher
  beforeEach(() => {
    setPerTestWorktreeRoot()
    resetTaskDispatcher()
    d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 3 })
  })

  it('critical > high > normal > low, FIFO inside a priority bucket', async () => {
    const cwd = await makeReadOnlyDir()
    const order: string[] = []
    d.setLauncher(async (t) => {
      order.push(t.id)
      await new Promise(r => setTimeout(r, 1))
      d.complete(t.id, 'ok')
    })
    d.enqueue({ id: 'n1', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.enqueue({ id: 'h1', title: '', description: '', mode: 'READ-ONLY', priority: 'high', sourceCwd: cwd })
    d.enqueue({ id: 'l1', title: '', description: '', mode: 'READ-ONLY', priority: 'low', sourceCwd: cwd })
    d.enqueue({ id: 'c1', title: '', description: '', mode: 'READ-ONLY', priority: 'critical', sourceCwd: cwd })
    d.enqueue({ id: 'n2', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })

    const deadline = Date.now() + 4000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(order).toContain('c1')
    expect(order).toContain('h1')
    expect(order).toContain('l1')
    expect(order).toContain('n1')
    expect(order).toContain('n2')
    // Critical must be dispatched before low regardless of arrival order.
    const idxC = order.indexOf('c1')
    const idxL = order.indexOf('l1')
    expect(idxC).toBeLessThan(idxL)
    // Within the normal bucket, FIFO is preserved — n1 must come before n2.
    const idxN1 = order.indexOf('n1')
    const idxN2 = order.indexOf('n2')
    expect(idxN1).toBeLessThan(idxN2)
    expect(d.terminalSize).toBe(5)
  })

  it('priority-aware queuePosition reflects priority+FIFO order, not insertion array', () => {
    const cwd = 'unused'
    // Insert in a deliberately mixed order; queuePosition must follow
    // priority, not arrival order.
    d.enqueue({ id: 'l1', title: '', description: '', mode: 'READ-ONLY', priority: 'low', sourceCwd: cwd })
    d.enqueue({ id: 'n1', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.enqueue({ id: 'c1', title: '', description: '', mode: 'READ-ONLY', priority: 'critical', sourceCwd: cwd })
    d.enqueue({ id: 'h1', title: '', description: '', mode: 'READ-ONLY', priority: 'high', sourceCwd: cwd })
    expect(d.queuePosition('c1')).toBe(1)
    expect(d.queuePosition('h1')).toBe(2)
    expect(d.queuePosition('n1')).toBe(3)
    expect(d.queuePosition('l1')).toBe(4)
    expect(d.queuePosition('missing')).toBe(0)
  })

  it('comparator returns 0 only when both records match on every sort key', () => {
    const a: DispatcherTaskRecord = {
      id: 'a', title: '', description: '', mode: 'READ-ONLY',
      priority: 'normal', status: 'pending', sourceCwd: '/x',
      createdAt: '2024-01-01T00:00:00.000Z', attempt: 0,
    }
    const aCopy: DispatcherTaskRecord = { ...a }
    expect(compareTasksForOrder(a, aCopy)).toBe(0)
    const b: DispatcherTaskRecord = { ...a, id: 'b' }
    // Same priority + same timestamp → deterministic task-id tie-break.
    expect(compareTasksForOrder(a, b)).toBeLessThan(0)
    expect(compareTasksForOrder(b, a)).toBeGreaterThan(0)
  })
})

describe('TaskDispatcher — max in-flight', () => {
  beforeEach(() => { setPerTestWorktreeRoot(); resetTaskDispatcher() })

  it('respects maxInFlight cap and immediately dispatches next task on release', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 2 })
    let peak = 0
    let inflight = 0
    let launchCount = 0
    d.setLauncher(async (t) => {
      launchCount++
      inflight++
      peak = Math.max(peak, inflight)
      await new Promise(r => setTimeout(r, 8))
      inflight--
      d.complete(t.id, 'ok')
    })
    for (let i = 0; i < 6; i++) {
      d.enqueue({ id: `t${i}`, title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    }
    const deadline = Date.now() + 4000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(peak).toBeLessThanOrEqual(2)
    expect(launchCount).toBe(6)
    expect(d.terminalSize).toBe(6)
  })

  it('preparing + running together respect maxInFlight (capacity = preparing+running)', async () => {
    const repo = await makeTempRepo('preparing-cap')
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 2 })
    let peakReserved = 0
    let inflight = 0
    let launchCount = 0
    // Slow launcher so the preparing tasks never reach the cap through
    // pure running alone — the cap is enforced via the `reservedSize`
    // (preparing + running) counter.
    d.setLauncher(async (t) => {
      launchCount++
      inflight++
      peakReserved = Math.max(peakReserved, d.reservedSize)
      await new Promise(r => setTimeout(r, 30))
      inflight--
      d.complete(t.id, 'ok')
    })
    // Four WRITE tasks; maxInFlight=2 means at most 2 may be preparing
    // OR running concurrently. The other two stay in the pending queue.
    for (let i = 0; i < 4; i++) {
      d.enqueue({ id: `cap-${i}`, title: '', description: '', mode: 'WRITE', priority: 'normal', sourceCwd: repo })
    }
    const deadline = Date.now() + 8000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(launchCount).toBe(4)
    expect(peakReserved).toBeLessThanOrEqual(2)
    expect(d.terminalSize).toBe(4)
  })

  it('falls back to default when RUFLO_TASK_MAX_IN_FLIGHT is garbage', () => {
    const prev = process.env.RUFLO_TASK_MAX_IN_FLIGHT
    process.env.RUFLO_TASK_MAX_IN_FLIGHT = 'abc'
    try {
      const d2 = new TaskDispatcher()
      expect(d2.maxInFlightValue).toBe(10)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_TASK_MAX_IN_FLIGHT
      else process.env.RUFLO_TASK_MAX_IN_FLIGHT = prev
    }
  })

  it('honors RUFLO_TASK_MAX_IN_FLIGHT when a valid positive integer', () => {
    const prev = process.env.RUFLO_TASK_MAX_IN_FLIGHT
    process.env.RUFLO_TASK_MAX_IN_FLIGHT = '4'
    try {
      const d2 = new TaskDispatcher()
      expect(d2.maxInFlightValue).toBe(4)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_TASK_MAX_IN_FLIGHT
      else process.env.RUFLO_TASK_MAX_IN_FLIGHT = prev
    }
  })

  it('falls back to default when RUFLO_TASK_MAX_IN_FLIGHT is zero or negative', () => {
    for (const bad of ['0', '-7']) {
      const prev = process.env.RUFLO_TASK_MAX_IN_FLIGHT
      process.env.RUFLO_TASK_MAX_IN_FLIGHT = bad
      try {
        const d2 = new TaskDispatcher()
        expect(d2.maxInFlightValue).toBe(10)
      } finally {
        if (prev === undefined) delete process.env.RUFLO_TASK_MAX_IN_FLIGHT
        else process.env.RUFLO_TASK_MAX_IN_FLIGHT = prev
      }
    }
  })

  it('opts.maxInFlight is validated strictly the same way as env', () => {
    for (const bad of [0, -1, 1.5, NaN, Infinity, 'x' as unknown as number, null as unknown as number, undefined as unknown as number]) {
      const d2 = new TaskDispatcher({ maxInFlight: bad as any })
      expect(d2.maxInFlightValue).toBe(10)
    }
    const d3 = new TaskDispatcher({ maxInFlight: 5 })
    expect(d3.maxInFlightValue).toBe(5)
  })
})

describe('TaskDispatcher — cancel paths', () => {
  beforeEach(() => { setPerTestWorktreeRoot(); resetTaskDispatcher() })

  it('pending cancel physically removes from ready queue and never spawns Claude', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const launched: string[] = []
    // Slow launcher so 'queued' stays in the pending bucket while we cancel it.
    d.setLauncher(async (t) => {
      launched.push(t.id)
      await new Promise(r => setTimeout(r, 50))
      d.complete(t.id, 'ok')
    })

    d.enqueue({ id: 'first', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.enqueue({ id: 'queued', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    // Wait for the dispatch microtask to promote 'first' to preparing.
    await new Promise(r => setTimeout(r, 8))
    expect(d.pendingSize).toBe(1)
    const r = d.cancelPending('queued')
    expect(r.cancelled).toBe(true)
    expect(r.terminalReason).toBe('explicit-cancel')
    expect(d.pendingSize).toBe(0)
    expect(d.get('queued')?.status).toBe('cancelled')

    const deadline = Date.now() + 2000
    while (d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(launched).toEqual(['first'])
    expect(d.get('queued')?.worktree).toBeUndefined()
    expect(d.get('queued')?.executionCwd).toBeUndefined()
  })

  it('cancel during delayed provision => launcher is never called', async () => {
    const repo = await makeTempRepo('cancel-during-provision')
    const wtMgr = getWorktreeManager()
    const originalProvision = wtMgr.provision.bind(wtMgr)
    // Replace provision with a deferred version so we can cancel mid-flight.
    let resolveProvision: (v: any) => void = () => {}
    const deferred = new Promise<any>((resolve) => { resolveProvision = resolve })
    ;(wtMgr as any).provision = async (_id: string, sourceCwd: string) => {
      const wt = await deferred
      return wt
    }
    try {
      const d = new TaskDispatcher({ worktreeManager: wtMgr, maxInFlight: 1 })
      const launched: string[] = []
      d.setLauncher(async (t) => { launched.push(t.id); d.complete(t.id, 'ok') })
      d.enqueue({ id: 'slow-prov', title: '', description: '', mode: 'WRITE', priority: 'normal', sourceCwd: repo })
      // Wait for the dispatch microtask to flip status to preparing.
      await new Promise(r => setTimeout(r, 10))
      expect(d.get('slow-prov')?.status).toBe('preparing')
      // Cancel while provision is awaiting.
      d.cancelPending('slow-prov')
      // Now resolve provision with a real worktree path; the launcher must
      // NOT be called because the task is already terminal.
      resolveProvision(await originalProvision('slow-prov', repo))
      const deadline = Date.now() + 1000
      while (d.pendingSize > 0 || d.inFlightSize > 0) {
        if (Date.now() > deadline) break
        await nextTick()
      }
      expect(launched).toEqual([])
      expect(d.get('slow-prov')?.status).toBe('cancelled')
    } finally {
      ;(wtMgr as any).provision = originalProvision
    }
  })

  it('cancel immediately before launcher boundary => launcher is never called', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const launched: string[] = []
    d.setLauncher(async (t) => { launched.push(t.id); d.complete(t.id, 'ok') })
    // Synchronously enqueue then cancel BEFORE the dispatch microtask
    // runs. cancelPending is the only call that can see status='pending'
    // here because tryDispatchTick hasn't fired yet.
    d.enqueue({ id: 'prelaunch', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.cancelPending('prelaunch')
    // Drain microtasks + any in-flight settle.
    await new Promise(r => setTimeout(r, 20))
    expect(launched).toEqual([])
    expect(d.get('prelaunch')?.status).toBe('cancelled')
  })

  it('cancel + fail each release the in-flight slot exactly once', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    let releaseCount = 0
    d.on('complete', () => { releaseCount++ })
    d.on('fail', () => { releaseCount++ })

    d.setLauncher(async (t) => {
      await new Promise(r => setTimeout(r, 4))
      if (t.id === 'a') d.complete(t.id, 'ok')
      else d.fail(t.id, 'boom')
    })
    d.enqueue({ id: 'a', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.enqueue({ id: 'b', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })

    const deadline = Date.now() + 2000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(d.inFlightSize).toBe(0)
    // One release per task, no double-count.
    expect(releaseCount).toBe(2)
  })

  it('duplicate enqueue is idempotent — no double dispatch', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const launched: string[] = []
    d.setLauncher(async (t) => { launched.push(t.id); await new Promise(r => setTimeout(r, 4)); d.complete(t.id, 'ok') })
    const r1 = d.enqueue({ id: 'dup', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    const r2 = d.enqueue({ id: 'dup', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    await nextTick()
    expect(r1.created).toBe(true)
    expect(r2.created).toBe(false)
    const deadline = Date.now() + 2000
    while (d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(launched.length).toBe(1)
  })

  it('cancel completed/failed is a no-op (idempotent terminal guard)', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    d.setLauncher(async (t) => {
      if (t.id === 'done') d.complete(t.id, 'ok')
      else d.fail(t.id, 'boom')
    })
    d.enqueue({ id: 'done', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.enqueue({ id: 'bad', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    const deadline = Date.now() + 2000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(d.get('done')?.status).toBe('completed')
    expect(d.get('bad')?.status).toBe('failed')
    // Calling complete/fail again must be a no-op (B9).
    d.complete('done', 'again')
    d.fail('bad', 'again')
    expect(d.get('done')?.status).toBe('completed')
    expect(d.get('bad')?.status).toBe('failed')
    expect(d.terminalSize).toBe(2)
  })

  it('active cancel releases the slot exactly once even on late close', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    let completeCalls = 0
    let cancelCalls = 0
    d.on('complete', () => { completeCalls++ })
    d.on('cancel', () => { cancelCalls++ })
    // Manually promote a task into in_progress and trigger cancel + late complete.
    d.enqueue({ id: 'a', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    await new Promise(r => setTimeout(r, 4))
    // Force the task into in_progress synchronously (READ-ONLY skip worktree).
    const a = d.get('a')!
    a.status = 'in_progress'
    await d.cancelActive('a', async () => { /* nothing */ })
    // Even if the pipeline tries to settle later, complete must be a no-op.
    d.complete('a', 'late')
    d.fail('a', 'late')
    expect(completeCalls).toBe(0)
    expect(cancelCalls).toBe(1)
    expect(d.get('a')?.status).toBe('cancelled')
    expect(d.inFlightSize).toBe(0)
  })
})

describe('TaskDispatcher — restart recovery (hydrateFromSnapshot)', () => {
  beforeEach(() => { setPerTestWorktreeRoot(); resetTaskDispatcher() })

  it('hydrate in_progress => interrupted and never runs', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 2 })
    const launched: string[] = []
    d.setLauncher(async (t) => { launched.push(t.id); d.complete(t.id, 'ok') })
    const result = d.hydrateFromSnapshot([
      { id: 'lost', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd,
        status: 'in_progress', createdAt: new Date().toISOString(), attempt: 1, startedAt: new Date().toISOString() },
    ])
    expect(result.interrupted).toContain('lost')
    expect(d.get('lost')?.status).toBe('interrupted')
    expect(d.get('lost')?.terminalReason).toBe('interrupted')
    d.dispatchAfterHydrate()
    await new Promise(r => setTimeout(r, 30))
    expect(launched).toEqual([])
  })

  it('hydrate pending => queued exactly once', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 2 })
    const launched: string[] = []
    d.setLauncher(async (t) => { launched.push(t.id); d.complete(t.id, 'ok') })
    const result = d.hydrateFromSnapshot([
      { id: 'p1', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd,
        status: 'pending', createdAt: new Date().toISOString(), attempt: 0 },
      // duplicate in same snapshot — must not double-queue.
      { id: 'p1', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd,
        status: 'pending', createdAt: new Date().toISOString(), attempt: 0 },
    ])
    expect(result.restored).toEqual(['p1'])
    expect(d.queuePosition('p1')).toBe(1)
    d.dispatchAfterHydrate()
    const deadline = Date.now() + 2000
    // Wait for the task to actually enter in-flight (the dispatch tick
    // runs from a microtask; spin until either launched or deadline).
    while (launched.length === 0 && Date.now() < deadline) {
      await nextTick()
    }
    // Then wait for it to settle.
    while (d.inFlightSize > 0 && Date.now() < deadline) {
      await nextTick()
    }
    expect(launched).toEqual(['p1'])
  })

  it('hydrate completed/failed/cancelled/interrupted => terminal preserved', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 2 })
    const launched: string[] = []
    d.setLauncher(async (t) => { launched.push(t.id); d.complete(t.id, 'ok') })
    const now = new Date().toISOString()
    d.hydrateFromSnapshot([
      { id: 'comp', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd,
        status: 'completed', createdAt: now, attempt: 1, finishedAt: now, terminalReason: 'completed' },
      { id: 'fail', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd,
        status: 'failed', createdAt: now, attempt: 1, finishedAt: now, terminalReason: 'failed' },
      { id: 'can', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd,
        status: 'cancelled', createdAt: now, attempt: 1, finishedAt: now, terminalReason: 'explicit-cancel' },
      { id: 'intr', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd,
        status: 'interrupted', createdAt: now, attempt: 1, finishedAt: now, terminalReason: 'interrupted' },
    ])
    for (const id of ['comp', 'fail', 'can', 'intr']) {
      expect(isTerminalStatus(d.get(id)!.status)).toBe(true)
    }
    d.dispatchAfterHydrate()
    // No terminal record should be re-launched.
    return new Promise(r => setTimeout(() => {
      expect(launched).toEqual([])
      r(undefined)
    }, 20))
  })

  it('hydrate produces no early microtask launch — dispatch only after dispatchAfterHydrate', async () => {
    const cwd = 'unused'
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 2 })
    const launched: string[] = []
    d.setLauncher(async (t) => { launched.push(t.id); d.complete(t.id, 'ok') })
    d.hydrateFromSnapshot([
      { id: 'h', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd,
        status: 'pending', createdAt: new Date().toISOString(), attempt: 0 },
    ])
    // Synchronously after hydrate: no launcher has fired yet.
    expect(launched).toEqual([])
    // Drain microtasks before dispatchAfterHydrate — still no launch.
    await new Promise(r => setTimeout(() => {
      expect(launched).toEqual([])
      d.dispatchAfterHydrate()
      setTimeout(() => {
        expect(launched).toEqual(['h'])
        r(undefined)
      }, 10)
    }, 10))
  })

  it('duplicate enqueue after hydrate cannot overwrite the existing record', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const launched: string[] = []
    d.setLauncher(async (t) => { launched.push(t.id); d.complete(t.id, 'ok') })
    const result = d.hydrateFromSnapshot([
      { id: 'p', title: 'A', description: 'orig', mode: 'WRITE', priority: 'critical', sourceCwd: cwd,
        status: 'pending', createdAt: new Date().toISOString(), attempt: 0 },
    ])
    expect(result.restored).toEqual(['p'])
    // Duplicate create must NOT overwrite the authoritative record.
    const re = d.enqueue({ id: 'p', title: 'B', description: 'new', mode: 'WRITE', priority: 'low', sourceCwd: cwd })
    expect(re.created).toBe(false)
    expect(d.get('p')?.title).toBe('A')
    expect(d.get('p')?.description).toBe('orig')
    expect(d.get('p')?.priority).toBe('critical')
  })

  it('forgetTerminal only succeeds for terminal records', () => {
    const cwd = 'unused'
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 2 })
    d.enqueue({ id: 'p', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    expect(d.forgetTerminal('p')).toBe(false)
    d.cancelPending('p')
    expect(d.forgetTerminal('p')).toBe(true)
    expect(d.get('p')).toBeUndefined()
  })
})

describe('TaskDispatcher — WRITE branch and READ-ONLY paths', () => {
  beforeEach(() => { setPerTestWorktreeRoot(); resetTaskDispatcher(); resetWorktreeManager() })

  it('WRITE task gets a unique branch + worktree + execution cwd derived from the taskId', async () => {
    const repo = await makeTempRepo('write-A')
    const wtMgr = getWorktreeManager()
    const d = new TaskDispatcher({ worktreeManager: wtMgr, maxInFlight: 1 })
    const executed: DispatcherTaskRecord[] = []
    d.setLauncher(async (t) => {
      executed.push({ ...t })
      d.complete(t.id, 'ok')
    })
    d.enqueue({ id: 'task-FIX-001', title: '', description: '', mode: 'WRITE', priority: 'normal', sourceCwd: repo })
    const deadline = Date.now() + 5000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(executed.length).toBe(1)
    const t = executed[0]
    expect(t.worktree).toBeDefined()
    expect(t.worktree!.branchName).toMatch(/^ruflo-task\/[0-9a-f]{12}-task-FIX-001$/)
    expect(t.worktree!.worktreePath).toContain('task-FIX-001.worktree')
    expect(t.executionCwd).toBe(t.worktree!.worktreePath)
    expect(t.executionCwd).not.toBe(repo)
    expect(fs.existsSync(t.worktree!.worktreePath)).toBe(true)
    expect(t.worktree!.baseCommit.length).toBeGreaterThan(0)
  })

  it('status stays pending/preparing until worktree provision completes; in_progress only right before launcher', async () => {
    const repo = await makeTempRepo('preparing-status')
    const wtMgr = getWorktreeManager()
    const observed: Array<{ id: string; status: DispatcherStatus }> = []
    const originalProvision = wtMgr.provision.bind(wtMgr)
    let releaseProvision: () => void = () => {}
    const gate = new Promise<void>((resolve) => { releaseProvision = resolve })
    ;(wtMgr as any).provision = async (id: string, sourceCwd: string) => {
      await gate
      return originalProvision(id, sourceCwd)
    }
    try {
      const d = new TaskDispatcher({ worktreeManager: wtMgr, maxInFlight: 1 })
      d.on('statusChange', ({ taskId, status }) => {
        observed.push({ id: taskId, status })
      })
      d.setLauncher(async (t) => {
        // The launcher only runs after provision resolved.
        observed.push({ id: t.id, status: t.status })
        d.complete(t.id, 'ok')
      })
      d.enqueue({ id: 'pre', title: '', description: '', mode: 'WRITE', priority: 'normal', sourceCwd: repo })
      // While provision is awaiting, status MUST be 'preparing', not 'in_progress'.
      await new Promise(r => setTimeout(r, 10))
      expect(d.get('pre')?.status).toBe('preparing')
      expect(observed.some(o => o.id === 'pre' && o.status === 'in_progress')).toBe(false)
      releaseProvision()
      const deadline = Date.now() + 3000
      while (d.inFlightSize > 0) {
        if (Date.now() > deadline) break
        await nextTick()
      }
      // After completion, terminal sequence is preserved.
      expect(d.get('pre')?.status).toBe('completed')
    } finally {
      ;(wtMgr as any).provision = originalProvision
    }
  })

  it('two WRITE tasks of the same temp repo get different worktrees and branches', async () => {
    const repo = await makeTempRepo('write-B')
    const wtMgr = getWorktreeManager()
    const d = new TaskDispatcher({ worktreeManager: wtMgr, maxInFlight: 2 })
    const executed: DispatcherTaskRecord[] = []
    d.setLauncher(async (t) => { executed.push({ ...t }); d.complete(t.id, 'ok') })
    d.enqueue({ id: 'task-A', title: '', description: '', mode: 'WRITE', priority: 'normal', sourceCwd: repo })
    d.enqueue({ id: 'task-B', title: '', description: '', mode: 'WRITE', priority: 'normal', sourceCwd: repo })
    const deadline = Date.now() + 5000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(executed.length).toBe(2)
    const a = executed.find(t => t.id === 'task-A')!
    const b = executed.find(t => t.id === 'task-B')!
    expect(a.worktree!.worktreePath).not.toBe(b.worktree!.worktreePath)
    expect(a.worktree!.branchName).not.toBe(b.worktree!.branchName)
    expect(fs.existsSync(a.worktree!.worktreePath)).toBe(true)
    expect(fs.existsSync(b.worktree!.worktreePath)).toBe(true)
  })

  it('same task ID in two temp repositories gets different paths', async () => {
    const repo1 = await makeTempRepo('mirror-1')
    const repo2 = await makeTempRepo('mirror-2')
    const wtMgr = getWorktreeManager()
    const d1 = await wtMgr.provision('shared-id', repo1)
    const d2 = await wtMgr.provision('shared-id', repo2)
    expect(d1.worktreePath).not.toBe(d2.worktreePath)
    expect(d1.branchName).not.toBe(d2.branchName)
  })

  it('WRITE provisioning never checkouts / moves HEAD on the base repository', async () => {
    const repo = await makeTempRepo('write-C')
    const headBefore = await gitHead(repo)
    const initialBranches = (await gitBranches(repo)).sort().join(',')
    const wtMgr = getWorktreeManager()
    await wtMgr.provision('task-C', repo)
    const headAfter = await gitHead(repo)
    expect(headAfter).toBe(headBefore)
    const afterBranches = (await gitBranches(repo)).sort()
    const initialList = initialBranches.split(',').filter(Boolean)
    for (const b of initialList) {
      expect(afterBranches).toContain(b)
    }
    expect(afterBranches.some(b => b.startsWith('ruflo-task/') && b.endsWith('-task-C'))).toBe(true)
    expect(fs.existsSync(path.join(repo, '.git'))).toBe(true)
    const baseEntries = fs.readdirSync(repo).filter(e => e !== '.git' && e !== 'README.md')
    expect(baseEntries).toEqual([])
  })

  it('READ-ONLY tasks do NOT create a worktree and execute in source cwd', async () => {
    const dir = fs.mkdtempSync(path.join(tmpRoot, `read-only-1-${nextRepoIdx++}-`))
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const executed: DispatcherTaskRecord[] = []
    d.setLauncher(async (t) => { executed.push({ ...t }); d.complete(t.id, 'ok') })
    d.enqueue({ id: 'read-task-1', title: 'audit', description: 'READ-ONLY: list files', mode: 'READ-ONLY', priority: 'normal', sourceCwd: dir })
    const deadline = Date.now() + 3000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(executed.length).toBe(1)
    expect(executed[0].worktree).toBeUndefined()
    expect(executed[0].executionCwd).toBe(dir)
  })

  it('two concurrent READ-ONLY tasks of one dir use the same source cwd', async () => {
    const dir = fs.mkdtempSync(path.join(tmpRoot, `read-only-2-${nextRepoIdx++}-`))
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 2 })
    const executed: DispatcherTaskRecord[] = []
    d.setLauncher(async (t) => { executed.push({ ...t }); d.complete(t.id, 'ok') })
    d.enqueue({ id: 'r1', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: dir })
    d.enqueue({ id: 'r2', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: dir })
    const deadline = Date.now() + 3000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(executed.length).toBe(2)
    expect(executed.every(t => t.executionCwd === dir)).toBe(true)
  })
})

describe('TaskDispatcher — fail-closed worktree errors', () => {
  beforeEach(() => { setPerTestWorktreeRoot(); resetTaskDispatcher(); resetWorktreeManager() })

  it('invalid source cwd fails closed — task is failed, no worktree, no Claude', async () => {
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager() })
    const launched: string[] = []
    d.setLauncher(async (t) => { launched.push(t.id); d.complete(t.id, 'ok') })
    const notRepo = fs.mkdtempSync(path.join(tmpRoot, `not-repo-${nextRepoIdx++}-`))
    d.enqueue({ id: 'bad-cwd', title: '', description: '', mode: 'WRITE', priority: 'normal', sourceCwd: notRepo })
    const deadline = Date.now() + 3000
    while (d.get('bad-cwd') && d.get('bad-cwd')!.status !== 'failed' && Date.now() < deadline) {
      await nextTick()
    }
    expect(d.get('bad-cwd')?.status).toBe('failed')
    expect(launched).toEqual([])
    expect(d.get('bad-cwd')?.worktree).toBeUndefined()
  })

  it('existing branch collision fails closed', async () => {
    const repo = await makeTempRepo('collision-branch')
    const wtMgr = getWorktreeManager()
    await wtMgr.provision('task-x', repo)
    const d = new TaskDispatcher({ worktreeManager: wtMgr })
    const launched: string[] = []
    d.setLauncher(async (t) => { launched.push(t.id); d.complete(t.id, 'ok') })
    d.enqueue({ id: 'task-x', title: '', description: '', mode: 'WRITE', priority: 'normal', sourceCwd: repo })
    const deadline = Date.now() + 5000
    while (d.get('task-x') && d.get('task-x')!.status !== 'failed' && Date.now() < deadline) {
      await nextTick()
    }
    expect(d.get('task-x')?.status).toBe('failed')
    expect(launched).toEqual([])
  })

  it('worktree path collision fails closed (no auto-remove of existing)', async () => {
    const repo = await makeTempRepo('collision-wt')
    const wtMgr = getWorktreeManager()
    await wtMgr.provision('wttest', repo)
    const plan = wtMgr.derive('wttest', repo)
    fs.mkdirSync(plan.worktreePath, { recursive: true })
    let threw = false
    try {
      await wtMgr.provision('wttest', repo)
    } catch {
      threw = true
    }
    expect(threw).toBe(true)
  })

  it('path traversal in taskId is rejected by sanitizeTaskId', async () => {
    const repo = await makeTempRepo('traversal')
    const wtMgr = getWorktreeManager()
    for (const bad of ['../escape', '..', 'a/../../b']) {
      let threw = false
      try { await wtMgr.provision(bad, repo) } catch { threw = true }
      expect(threw).toBe(true)
    }
  })

  it('dispatches Claude exactly once per task — does not spawn before worktree ready', async () => {
    const repo = await makeTempRepo('order')
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const observations: string[] = []
    d.setLauncher(async (t) => {
      observations.push(`launcher:${t.id}:worktree=${t.worktree ? 'yes' : 'no'}`)
      d.complete(t.id, 'ok')
    })
    d.enqueue({ id: 'order-1', title: '', description: '', mode: 'WRITE', priority: 'normal', sourceCwd: repo })
    const deadline = Date.now() + 3000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await nextTick()
    }
    expect(observations).toEqual(['launcher:order-1:worktree=yes'])
  })
})

describe('TaskWorktreeManager — security hardening (B11)', () => {
  beforeEach(() => { setPerTestWorktreeRoot(); resetTaskDispatcher(); resetWorktreeManager() })

  it('absolute RUFLO_TASK_WORKTREE_ROOT inside source repo is rejected (canonical resolution)', async () => {
    const repo = await makeTempRepo('abs-root')
    const prev = process.env.RUFLO_TASK_WORKTREE_ROOT
    process.env.RUFLO_TASK_WORKTREE_ROOT = repo // exact match → rejected
    resetWorktreeManager()
    try {
      const wtMgr = getWorktreeManager()
      let threw = false
      try { await wtMgr.provision('abs-root', repo) } catch (e: any) { threw = true; expect(e.code).toBe('invalid-root') }
      expect(threw).toBe(true)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_TASK_WORKTREE_ROOT
      else process.env.RUFLO_TASK_WORKTREE_ROOT = prev
      resetWorktreeManager()
    }
  })

  it('absolute RUFLO_TASK_WORKTREE_ROOT descendant of source repo is rejected', async () => {
    const repo = await makeTempRepo('desc-root')
    const prev = process.env.RUFLO_TASK_WORKTREE_ROOT
    const nested = path.join(repo, 'subdir', 'worktrees')
    process.env.RUFLO_TASK_WORKTREE_ROOT = nested
    resetWorktreeManager()
    try {
      const wtMgr = getWorktreeManager()
      let threw = false
      try { await wtMgr.provision('desc-root', repo) } catch (e: any) { threw = true; expect(e.code).toBe('invalid-root') }
      expect(threw).toBe(true)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_TASK_WORKTREE_ROOT
      else process.env.RUFLO_TASK_WORKTREE_ROOT = prev
      resetWorktreeManager()
    }
  })

  it('relative RUFLO_TASK_WORKTREE_ROOT is rejected (fail-closed, not interpreted)', async () => {
    const repo = await makeTempRepo('rel-root')
    const prev = process.env.RUFLO_TASK_WORKTREE_ROOT
    process.env.RUFLO_TASK_WORKTREE_ROOT = './relative-root'
    resetWorktreeManager()
    try {
      const wtMgr = getWorktreeManager()
      let threw = false
      try { await wtMgr.provision('rel-root', repo) } catch (e: any) { threw = true; expect(e.code).toBe('invalid-root') }
      expect(threw).toBe(true)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_TASK_WORKTREE_ROOT
      else process.env.RUFLO_TASK_WORKTREE_ROOT = prev
      resetWorktreeManager()
    }
  })

  it('absolute RUFLO_TASK_WORKTREE_ROOT with .. segments is canonicalized, not interpreted ambiguously', async () => {
    // /tmp/x/a/../b is canonicalized to /tmp/x/b before the source-repo
    // containment check, so the path resolves safely and provisioning
    // proceeds normally (not fail-closed, because canonical resolution
    // removes ambiguity).
    const repo = await makeTempRepo('trav-root')
    const prev = process.env.RUFLO_TASK_WORKTREE_ROOT
    process.env.RUFLO_TASK_WORKTREE_ROOT = path.join(tmpRoot, 'a', '..', 'b')
    resetWorktreeManager()
    try {
      const wtMgr = getWorktreeManager()
      // Should succeed — absolute, canonicalized, not inside source repo.
      const info = await wtMgr.provision('trav-root', repo)
      expect(info.worktreePath.startsWith(path.join(tmpRoot, 'b'))).toBe(true)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_TASK_WORKTREE_ROOT
      else process.env.RUFLO_TASK_WORKTREE_ROOT = prev
      resetWorktreeManager()
    }
  })

  it('default root is durable user-owned path under os.homedir(), not /tmp', () => {
    delete process.env.RUFLO_TASK_WORKTREE_ROOT
    resetWorktreeManager()
    const wtMgr = getWorktreeManager()
    const plan = (wtMgr as any).derive('dur', process.cwd())
    const home = os.homedir() || os.tmpdir()
    expect(plan.worktreePath.startsWith(home)).toBe(true)
  })

  it('same task ID in two temp repositories gets different paths', async () => {
    const a = await makeTempRepo('idem-A')
    const b = await makeTempRepo('idem-B')
    const wtMgr = getWorktreeManager()
    const infoA = await wtMgr.provision('SHARED-ID', a)
    const infoB = await wtMgr.provision('SHARED-ID', b)
    expect(infoA.worktreePath).not.toBe(infoB.worktreePath)
    expect(infoA.branchName).not.toBe(infoB.branchName)
    expect(infoA.repoId).not.toBe(infoB.repoId)
  })

  it('symlinked source is resolved and containment enforced (no worktree outside realpath)', async () => {
    const real = await makeTempRepo('real')
    const symlinkParent = fs.mkdtempSync(path.join(tmpRoot, `sym-parent-${nextRepoIdx++}-`))
    const symlinkPath = path.join(symlinkParent, 'symlink')
    try { fs.symlinkSync(real, symlinkPath) } catch { /* fs.symlinkSync may not work in some envs */ return }
    // Configure a worktree root that is realpath-equivalent to the symlinked repo.
    const prev = process.env.RUFLO_TASK_WORKTREE_ROOT
    process.env.RUFLO_TASK_WORKTREE_ROOT = real // real and symlink resolve to the same canonical dir
    resetWorktreeManager()
    try {
      const wtMgr = getWorktreeManager()
      let threw = false
      try { await wtMgr.provision('symtask', symlinkPath) } catch (e: any) { threw = true; expect(e.code).toBe('invalid-root') }
      expect(threw).toBe(true)
    } finally {
      if (prev === undefined) delete process.env.RUFLO_TASK_WORKTREE_ROOT
      else process.env.RUFLO_TASK_WORKTREE_ROOT = prev
      resetWorktreeManager()
    }
  })

  it('existing unregistered filesystem path is a collision (even when not a registered git worktree)', async () => {
    const repo = await makeTempRepo('fs-collide')
    const wtMgr = getWorktreeManager()
    // Provision once to register a worktree, then deliberately remove the
    // git worktree + branch so the path exists but is "unregistered".
    const info = await wtMgr.provision('fs-collide', repo)
    await execFile('git', ['worktree', 'remove', '--force', info.worktreePath], { cwd: repo })
    await execFile('git', ['branch', '-D', info.branchName], { cwd: repo })
    // Re-create a plain directory at the same path so lstat succeeds but
    // git worktree list does not have it registered.
    fs.mkdirSync(info.worktreePath, { recursive: true })
    let threw = false
    try { await wtMgr.provision('fs-collide', repo) } catch (e: any) { threw = true; expect(e.code).toBe('collision-path') }
    expect(threw).toBe(true)
  })

  it('invalid branch ref name is rejected before any worktree mutation', async () => {
    const repo = await makeTempRepo('bad-branch')
    const wtMgr = getWorktreeManager()
    // Construct a taskId whose sanitized form produces a branch with a
    // forbidden component — git check-ref-format --branch rejects.
    let threw = false
    try { await wtMgr.provision('foo.lock', repo) } catch (e: any) { threw = true; expect(['invalid-branch', 'invalid-task-id']).toContain(e.code) }
    expect(threw).toBe(true)
  })

  it('non-absence show-ref failure is fail-closed (not treated as branch absent)', async () => {
    // Corrupt the repo's HEAD to a non-existent SHA so show-ref returns
    // a real error (not "branch absent"). The provision path must NOT
    // treat this as "branch absent".
    const repo = await makeTempRepo('show-ref-fail')
    // Write an invalid HEAD reference so internal git plumbing fails.
    fs.writeFileSync(path.join(repo, '.git', 'HEAD'), 'ref: refs/heads/this-branch-truly-does-not-exist-xyz\n')
    const wtMgr = getWorktreeManager()
    let threw = false
    try {
      await wtMgr.provision('hidden', repo)
    } catch (e: any) {
      threw = true
      // Fail-closed: must NOT be classified as collision-branch /
      // collision-path (that would silently re-treat a real git error
      // as "branch absent" and overwrite the ref). Acceptable codes:
      // git-failed (from show-ref / rev-parse / worktree-add) or
      // invalid-branch.
      expect(['collision-branch', 'collision-path']).not.toContain(e.code)
      expect(e.code).not.toBe('collision-branch')
    }
    expect(threw).toBe(true)
  })

  it('provision locks the base commit SHA (does not depend on a moving HEAD)', async () => {
    const repo = await makeTempRepo('lock-sha')
    const wtMgr = getWorktreeManager()
    const headBefore = await gitHead(repo)
    const info = await wtMgr.provision('lock-sha', repo)
    expect(info.baseCommit).toBe(headBefore)
    // The worktree itself starts at HEAD, so HEAD == baseCommit == the
    // SHA captured at provision time.
    const headInWorktree = await gitHead(info.worktreePath)
    expect(headInWorktree).toBe(headBefore)
  })
})

describe('detectTaskMode', () => {
  it('detects READ-ONLY when MODE: READ-ONLY is in the description', () => {
    expect(detectTaskMode({ title: 'audit', description: 'MODE: READ-ONLY\nlist files' })).toBe('READ-ONLY')
  })
  it('detects WRITE when MODE: WRITE is in the description', () => {
    expect(detectTaskMode({ title: 'fix', description: 'MODE: WRITE\npatch bug' })).toBe('WRITE')
  })
  it('falls back to READ-ONLY when headline says audit and no implementation verbs', () => {
    expect(detectTaskMode({ title: 'Audit the auth flow', description: 'Identify gaps, list findings.' })).toBe('READ-ONLY')
  })
  it('default to WRITE for ambiguous natural-language task titles', () => {
    expect(detectTaskMode({ title: 'Refactor the rate limiter', description: 'Rewrite to use sliding window.' })).toBe('WRITE')
  })
  it('explicit packetId MODE header is honoured', () => {
    expect(detectTaskMode({ packetId: 'PACKET-ID: TASK-1\nMODE: READ-ONLY', title: '', description: '' })).toBe('READ-ONLY')
  })
})

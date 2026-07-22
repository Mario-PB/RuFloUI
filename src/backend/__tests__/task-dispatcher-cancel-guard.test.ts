// @vitest-environment node
/**
 * ACC-TASK-QUEUE-002-RACE-REPAIR — dispatcher cancellation guard tests.
 *
 * Covers:
 *   C. cancelActive capacity — slot not released until async cleanup
 *      settles; queued task does NOT launch.
 *   D. concurrent cancelActive — cleanup callback fires exactly once;
 *      terminal transition + slot release happen exactly once.
 *   E. worker-cancelled parent slot release — dispatcher slot released
 *      once, next task launches, parent remains cancelled.
 *   F. (dispatcher-level counterpart) terminal result preservation —
 *      subsequent complete/fail does not overwrite a cancelled task.
 *
 * All assertions are BEHAVIORAL against the production dispatcher code.
 */
import { describe, it, expect } from 'vitest'
import path from 'path'
import fs from 'fs'
import os from 'os'

process.env.RUFLO_TASK_WORKTREE_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), 'ruflo-cancel-'))

import {
  TaskDispatcher,
  DispatcherTaskRecord,
} from '../task-dispatcher'
import { getWorktreeManager } from '../task-worktrees'

function tick(): Promise<void> { return new Promise(r => setTimeout(r, 5)) }

async function makeReadOnlyDir(): Promise<string> {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ruflo-readonly-'))
}

// ── (C) cancelActive CAPACITY — slot held until cleanup resolves ───

describe('C — cancelActive holds the slot until async cleanup settles', () => {
  it('B is not launched while the cleanup promise for A is still pending', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const launched: string[] = []
    let releaseCleanup: (() => void) | null = null
    let cleanupStartedAt: number | null = null
    let cleanupFinishedAt: number | null = null
    let bLaunchedAt: number | null = null

    d.setLauncher(async (t) => {
      launched.push(t.id)
      if (t.id === 'A') {
        // Begin cancellation but do not let cleanup finish until we say so.
        const cleanupDone = d.cancelActive('A', () => new Promise<void>((res) => {
          cleanupStartedAt = Date.now()
          releaseCleanup = () => { cleanupFinishedAt = Date.now(); res() }
        }))
        // Late attempts to overwrite cancelled are no-ops.
        d.complete('A', 'late')
        d.fail('A', 'late')
        await cleanupDone
      } else if (t.id === 'B') {
        bLaunchedAt = Date.now()
        d.complete('B', 'ok')
      } else {
        d.complete(t.id, 'ok')
      }
    })
    d.enqueue({ id: 'A', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.enqueue({ id: 'B', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    // Wait until A is in flight and B is pending.
    let deadline = Date.now() + 3000
    while ((launched.length === 0) && Date.now() < deadline) {
      await tick()
    }
    expect(launched).toContain('A')
    // A must be cancelled while B is still pending — give it a chance.
    deadline = Date.now() + 1000
    while (!cleanupStartedAt && Date.now() < deadline) await tick()
    expect(cleanupStartedAt).not.toBeNull()
    // B must NOT have launched yet — the slot is leased.
    expect(bLaunchedAt).toBeNull()
    expect(d.get('B')?.status).toBe('pending')
    // Release the cleanup.
    releaseCleanup!()
    deadline = Date.now() + 3000
    while ((!bLaunchedAt || d.pendingSize > 0 || d.inFlightSize > 0) && Date.now() < deadline) {
      await tick()
    }
    expect(bLaunchedAt).not.toBeNull()
    expect(cleanupFinishedAt).not.toBeNull()
    expect(bLaunchedAt!).toBeGreaterThanOrEqual(cleanupFinishedAt!)
    expect(d.get('A')?.status).toBe('cancelled')
    expect(d.get('B')?.status).toBe('completed')
  })
})

// ── (D) TWO CONCURRENT cancelActive — exactly once each ───────────

describe('D — two simultaneous cancelActive calls share one cleanup chain', () => {
  it('cleanup callback runs exactly once; slot released once; one cancel emit', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    let cleanupCount = 0
    let cancelEvents = 0
    d.on('cancel', () => { cancelEvents++ })
    let resolveCleanup: (() => void) | null = null
    let cleanupStarted = false
    d.setLauncher(async (t) => {
      // Two simultaneous cancelActive calls.
      const cleanup1 = d.cancelActive('Z', () => new Promise<void>((res) => {
        cleanupStarted = true
        // First cleanup registers, increment count once. The second
        // call observes the same promise.
        cleanupCount++
        resolveCleanup = () => res()
      }))
      // Immediately call cancelActive AGAIN for the same task. This
      // must be idempotent — share the same chain.
      const cleanup2 = d.cancelActive('Z', () => new Promise<void>((res) => {
        cleanupCount++
        res()
      }))
      await Promise.all([cleanup1, cleanup2])
      d.complete('Z', 'late')
    })
    d.enqueue({ id: 'Z', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    const deadline = Date.now() + 3000
    while (!cleanupStarted && Date.now() < deadline) await tick()
    resolveCleanup!()
    // Wait for settling.
    let waited = 0
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (waited++ > 300) break
      await tick()
    }
    expect(cleanupCount).toBe(1) // exactly one cleanup callback fired
    expect(cancelEvents).toBe(1)
    expect(d.get('Z')?.status).toBe('cancelled')
    expect(d.inFlightSize).toBe(0)
    expect(d.terminalSize).toBe(1)
  })
})

// ── (E) WORKER-CANCELLED PARENT — slot released, next task launches ─

describe('E — worker-cancelled parent: slot released and next task launches', () => {
  it('after parent is cancelled, next queued task launches', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const launched: string[] = []
    d.setLauncher(async (t) => {
      launched.push(t.id)
      if (t.id === 'parent') {
        await d.cancelActive('parent', async () => { /* no extra kill */ })
      } else {
        d.complete(t.id, 'ok')
      }
    })
    d.enqueue({ id: 'parent', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.enqueue({ id: 'child', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    const deadline = Date.now() + 3000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await tick()
    }
    expect(launched).toContain('parent')
    expect(launched).toContain('child')
    expect(d.get('parent')?.status).toBe('cancelled')
    expect(d.get('child')?.status).toBe('completed')
    expect(d.inFlightSize).toBe(0)
    expect(d.terminalSize).toBe(2)
  })
})

// ── (F) complete/fail are no-op for a cancelled task ──────────────

describe('F — complete/fail are no-op for a task in cancelling guard', () => {
  it('complete and fail cannot overwrite a cancelled task', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    let completeEvents = 0
    let cancelEvents = 0
    d.on('complete', () => { completeEvents++ })
    d.on('cancel', () => { cancelEvents++ })
    d.setLauncher(async (t) => {
      const cleanupDone = d.cancelActive(t.id, async () => {
        // cleanup will resolve on its own after settle
      })
      // Try to overwrite cancelled with complete/fail — both no-op.
      d.complete(t.id, 'late-success')
      d.fail(t.id, 'late-fail')
      await cleanupDone
      // Try again after the guard clears (terminal.has is now true).
      d.complete(t.id, 'after-cleanup')
      d.fail(t.id, 'after-cleanup')
    })
    d.enqueue({ id: 'race', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    const deadline = Date.now() + 3000
    while (d.pendingSize > 0 || d.inFlightSize > 0) {
      if (Date.now() > deadline) break
      await tick()
    }
    expect(d.get('race')?.status).toBe('cancelled')
    expect(cancelEvents).toBe(1)
    expect(completeEvents).toBe(0)
    expect(d.terminalSize).toBe(1)
  })
})

// ── cancelViaScheduler THAT NEVER RESOLVES — fail-closed ───────────

describe('cancelActive — cleanup callback that never resolves leaves the slot leased', () => {
  it('cleanup that never settles keeps the slot leased; B never launches; complete/fail no-op', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const launched: string[] = []
    d.setLauncher(async (t) => {
      launched.push(t.id)
      if (t.id === 'A') {
        // Cancellation that never settles. The dispatcher's contract
        // is fail-closed: the guard stays active, the slot stays
        // leased, complete/fail no-op, B stays pending. There is no
        // wall-clock timeout inside cancelActive anymore.
        let cleanupPending = false
        const cancelPromise = d.cancelActive('A', () => new Promise(() => { /* never */ }))
          .catch(() => { /* expected */ })
        cleanupPending = true
        // Wait until the guard is in place.
        let waited = 0
        while ((d as any).cancelling?.has('A') !== true && waited++ < 200) {
          await tick()
        }
        expect((d as any).cancelling?.has('A')).toBe(true)
        // try complete/fail — both must be no-op.
        d.complete('A', 'late-success')
        d.fail('A', 'late-fail')
        // Wait a bit and verify state hasn't changed.
        await new Promise(r => setTimeout(r, 50))
        expect(d.get('A')?.status).toBe('in_progress')
        expect((d as any).cancelling?.has('A')).toBe(true)
        // cancelActive must reject (or never resolve) — we don't await
        // it; we just verify the slot is still leased and B never launches.
        void cancelPromise
      } else {
        d.complete(t.id, 'ok')
      }
    })
    d.enqueue({ id: 'A', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.enqueue({ id: 'B', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    // Wait until A is in flight.
    let waited = 0
    while (launched.length === 0 && waited++ < 200) await tick()
    expect(launched).toContain('A')
    // B must still be pending — A's slot is leased.
    expect(d.get('B')?.status).toBe('pending')
    // Wait an additional moment to confirm B is NOT promoted.
    await new Promise(r => setTimeout(r, 100))
    expect(d.get('B')?.status).toBe('pending')
    expect(launched).not.toContain('B')
    // Cleanup.
    d.dispose()
  }, 15_000)
})

// ── cancelViaScheduler that REJECTS — fail-closed ───────────────────

describe('cancelActive — cleanup callback rejection fails closed', () => {
  it('cleanup reject keeps the slot leased; B never launches; complete/fail no-op; throw surfaces', async () => {
    const cwd = await makeReadOnlyDir()
    const d = new TaskDispatcher({ worktreeManager: getWorktreeManager(), maxInFlight: 1 })
    const launched: string[] = []
    let cancelRejected: unknown = null
    let cleanupStarted = false
    let cleanupStartedAt = 0
    let cleanupCount = 0
    // Inner cleanup promise is referenced here so we can attach a
    // catch handler to it before its setTimeout fires — prevents an
    // unhandled rejection when the rejection wins the race against
    // the test's .catch on the outer cancelActive promise.
    let inner: Promise<void> | null = null
    d.setLauncher(async (t) => {
      launched.push(t.id)
      if (t.id === 'A') {
        const cleanupPromise = d.cancelActive('A', () => {
          cleanupStarted = true
          cleanupStartedAt = Date.now()
          cleanupCount++
          inner = new Promise<void>((_, reject) => {
            setTimeout(() => reject(new Error('cleanup-failed-deliberately')), 30)
          })
          inner.catch(() => { /* swallow unhandled race */ })
          return inner
        })
        await cleanupPromise.catch((err) => { cancelRejected = err })
        // After the rejection, the guard MUST still be active.
        expect((d as any).cancelling?.has('A')).toBe(true)
      } else {
        d.complete(t.id, 'ok')
      }
    })
    d.enqueue({ id: 'A', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    d.enqueue({ id: 'B', title: '', description: '', mode: 'READ-ONLY', priority: 'normal', sourceCwd: cwd })
    let waited = 0
    while (launched.length === 0 && waited++ < 200) await tick()
    expect(launched).toContain('A')
    // Wait until the cleanup has actually started.
    waited = 0
    while (!cleanupStarted && waited++ < 200) await tick()
    expect(cleanupStarted).toBe(true)
    // Wait until at least 50ms after cleanup start to give the
    // rejection time to settle.
    while (Date.now() - cleanupStartedAt < 50) await tick()
    // The rejected cleanup must have surfaced.
    expect(cancelRejected).toBeTruthy()
    expect((cancelRejected as Error).message).toBe('cleanup-failed-deliberately')
    // B must NOT be launched — A's slot is still leased.
    expect(d.get('B')?.status).toBe('pending')
    expect(launched).not.toContain('B')
    // A is still in cancelling guard; its status is in_progress.
    expect(d.get('A')?.status).toBe('in_progress')
    expect((d as any).cancelling?.has('A')).toBe(true)
    // cleanupCount must be exactly 1 — concurrent cancel does not
    // re-trigger cleanup.
    expect(cleanupCount).toBe(1)
    // complete/fail must be no-op.
    d.complete('A', 'late-success')
    d.fail('A', 'late-fail')
    expect(d.get('A')?.status).toBe('in_progress')
    // A second cancelActive call shares the SAME rejected cleanup
    // chain — it does NOT start a new cleanup chain. The await on
    // the existing chain surfaces the original rejection. cleanupCount
    // stays at 1.
    let secondRejected: unknown = null
    await d.cancelActive('A', () => new Promise<void>((_, reject) => {
      cleanupCount++
      reject(new Error('second-cleanup-must-not-fire'))
    })).catch((err) => { secondRejected = err })
    expect(cleanupCount).toBe(1)
    expect(secondRejected).toBeTruthy()
    expect((secondRejected as Error).message).toBe('cleanup-failed-deliberately')
    // Cleanup.
    d.dispose()
  })
})

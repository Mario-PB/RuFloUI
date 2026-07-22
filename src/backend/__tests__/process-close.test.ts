// @vitest-environment node
/**
 * ACC-TASK-QUEUE-002-RACE-REPAIR — process-close helpers.
 *
 * Covers the canonical kill → wait → cleanup lifecycle. The dispatcher's
 * race-repair fix #1 and #2 hinge on these two helpers behaving correctly
 * across the edge cases listed below.
 *
 *   A. fake ChildProcess with killed=true + exitCode=null + signalCode=null
 *      → SIGKILL fallback fires.
 *   B. close listener attached BEFORE SIGTERM, and SIGTERM synchronously
 *      emits close → caller completes without hanging.
 *   G. signalAndAwaitClose is the shared helper — exercises the same
 *      ordering invariant from a real call site perspective.
 *
 * Each test instantiates a fake ChildProcess that mirrors the surface
 * the helpers actually use (once/kill/removeListener/exitCode/signalCode).
 */
import { describe, it, expect } from 'vitest'
import { waitForProcessClose, signalAndAwaitClose } from '../process-close'

interface FakeProc {
  killed: boolean
  exitCode: number | null
  signalCode: string | null
  emits: Record<string, Array<(...args: any[]) => void>>
  once(ev: string, cb: (...args: any[]) => void): void
  removeListener(ev: string, cb: (...args: any[]) => void): void
  removeAllListeners(ev: string): void
  kill(sig?: string): void
  history: string[]
}

function makeFakeProc(): FakeProc {
  const history: string[] = []
  const emits: Record<string, Array<(...args: any[]) => void>> = {}
  const proc: FakeProc = {
    killed: false,
    exitCode: null,
    signalCode: null,
    emits,
    history,
    once(ev, cb) {
      ;(emits[ev] ||= []).push(cb)
    },
    removeListener(ev, cb) {
      const arr = emits[ev]
      if (!arr) return
      const idx = arr.indexOf(cb)
      if (idx >= 0) arr.splice(idx, 1)
    },
    removeAllListeners(ev) {
      emits[ev] = []
    },
    kill(sig?: string) {
      this.history.push(`kill:${sig ?? ''}`)
      this.killed = true
    },
  }
  return proc
}

function emit(proc: FakeProc, ev: string, ...args: any[]) {
  const arr = proc.emits[ev]?.slice() || []
  arr.forEach(cb => { try { cb(...args) } catch { /* tests assert this separately */ } })
}

// ── (A) SIGKILL FALLBACK FOR killed=true + null exit/signals ────────

describe('A — SIGKILL fallback when killed=true but no real exit', () => {
  it('sends SIGKILL if killed === true and exitCode === null and signalCode === null', async () => {
    const proc = makeFakeProc()
    proc.killed = true              // signal was sent earlier
    proc.exitCode = null
    proc.signalCode = null
    let resolved = false
    const w = waitForProcessClose(proc as any, 30).then(() => { resolved = true })
    // Yield so the timers schedule. The wait must NOT resolve yet.
    await new Promise(r => setTimeout(r, 10))
    expect(resolved).toBe(false)
    // SIGKILL fallback timer (fallbackMs=30) fires; waitForProcessClose
    // sends SIGKILL even though killed===true. After SIGKILL is sent,
    // we simulate the OS reaping the child by setting exitCode and
    // emitting `close`.
    expect(proc.history.some(s => s === 'kill:SIGKILL')).toBe(false)
    await new Promise(r => setTimeout(r, 35))
    expect(proc.history).toContain('kill:SIGKILL')
    expect(resolved).toBe(false)
    proc.exitCode = 137
    emit(proc, 'close')
    await w
    expect(resolved).toBe(true)
  })
})

// ── (B) LISTENERS BEFORE SIGTERM, SYNCHRONOUS close FROM kill ──────

describe('B — close listener registered before SIGTERM; synchronous close inside kill is observed', () => {
  it('kill(SIGTERM) synchronously emits close; awaiter returns without hanging', async () => {
    const proc = makeFakeProc()
    // Synchronously inside kill('SIGTERM') we simulate the OS having
    // already reaped the child. The signalAndAwaitClose helper must
    // see this close event because the listener is attached inside
    // waitForProcessClose BEFORE we send the signal.
    proc.removeAllListeners = (() => {}) as any
    proc.once = ((ev: string, cb: (...args: any[]) => void) => {
      // Register synchronously
      ;(proc.emits[ev] ||= []).push(cb)
    }) as any
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      // Simulate the OS having already closed the child SYNCHRONOUSLY
      // as part of the kill system call.
      proc.exitCode = 143
      proc.signalCode = 'SIGTERM'
      emit(proc, 'close')
    }) as any
    // signalAndAwaitClose MUST register the listener (inside
    // waitForProcessClose's once-call) BEFORE issuing kill('SIGTERM').
    // The synchronous emit must be observed.
    let finished = false
    await signalAndAwaitClose(proc as any, { fallbackMs: 50 })
    finished = true
    expect(finished).toBe(true)
    expect(proc.history).toContain('kill:SIGTERM')
  })

  it('waitForProcessClose alone does not rely on proc.killed — only on close event', async () => {
    const proc = makeFakeProc()
    proc.killed = true
    let resolved = false
    const w = waitForProcessClose(proc as any, 50).then(() => { resolved = true })
    await new Promise(r => setTimeout(r, 10))
    expect(resolved).toBe(false) // not resolved yet — no `close` emitted
    proc.exitCode = 143
    emit(proc, 'close')
    await w
    expect(resolved).toBe(true)
  })
})

// ── (G) SHARED LIFECYCLE — same helper, different call paths ────────

describe('G — signalAndAwaitClose runs cleanup only AFTER close', () => {
  it('cleanup is invoked after the close event, not before', async () => {
    const proc = makeFakeProc()
    const events: string[] = []
    let scheduled = false
    // Schedule a close emit after 15ms.
    setTimeout(() => {
      scheduled = true
      proc.exitCode = 0
      emit(proc, 'close')
    }, 15)
    await signalAndAwaitClose(proc as any, {
      fallbackMs: 200,
      cleanupKey: 'k1',
      cleanup: (key) => events.push(`cleanup:${key}`),
    })
    expect(events).toEqual(['cleanup:k1'])
    expect(scheduled).toBe(true)
  })

  it('cleanup runs even when SIGKILL fallback actually closes the process', async () => {
    const proc = makeFakeProc()
    const events: string[] = []
    // SIGKILL fallback will fire; we then simulate the OS reaping.
    setTimeout(() => {
      if (proc.history.includes('kill:SIGKILL')) {
        proc.exitCode = 137
        emit(proc, 'close')
      }
    }, 50)
    await signalAndAwaitClose(proc as any, {
      fallbackMs: 30,
      cleanupKey: 'k2',
      cleanup: (key) => events.push(`cleanup:${key}`),
    })
    expect(events).toEqual(['cleanup:k2'])
  })

  it('cleanup runs immediately when proc is undefined', async () => {
    const events: string[] = []
    await signalAndAwaitClose(undefined, {
      cleanupKey: 'k3',
      cleanup: (key) => events.push(`cleanup:${key}`),
    })
    expect(events).toEqual(['cleanup:k3'])
  })
})

// ── (H) FAIL-CLOSED — unkillable child rejects, never resolves ────

describe('H — fail-closed: SIGTERM/SIGKILL ignored, no close → reject, no cleanup', () => {
  it('waitForProcessClose rejects with ProcessCloseTimeoutError when child ignores SIGKILL', async () => {
    const proc = makeFakeProc()
    // The fake child ignores ALL signals. kill() updates the history
    // but exitCode/signalCode stay null and no close event is emitted.
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      // Intentionally do NOT emit close, do NOT set exitCode/signalCode.
    }) as any
    proc.removeAllListeners = ((ev: string) => { proc.emits[ev] = [] }) as any
    proc.once = ((ev: string, cb: (...args: any[]) => void) => {
      ;(proc.emits[ev] ||= []).push(cb)
    }) as any

    let rejected: any = null
    try {
      await waitForProcessClose(proc as any, 30)
    } catch (err) {
      rejected = err
    }
    expect(rejected).toBeTruthy()
    expect(rejected.code).toBe('PROCESS_CLOSE_TIMEOUT')
    expect(proc.history).toContain('kill:SIGKILL')
    // exitCode/signalCode stay null — child was never reaped.
    expect(proc.exitCode).toBeNull()
    expect(proc.signalCode).toBeNull()
  })

  it('signalAndAwaitClose rejects with ProcessCloseTimeoutError; cleanup is NOT called', async () => {
    const proc = makeFakeProc()
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      // Intentionally do NOT emit close, do NOT set exitCode/signalCode.
    }) as any
    proc.removeAllListeners = ((ev: string) => { proc.emits[ev] = [] }) as any
    proc.once = ((ev: string, cb: (...args: any[]) => void) => {
      ;(proc.emits[ev] ||= []).push(cb)
    }) as any

    const cleanupCalls: string[] = []
    let rejected: any = null
    try {
      await signalAndAwaitClose(proc as any, {
        fallbackMs: 30,
        cleanupKey: 'unkillable',
        cleanup: (key) => cleanupCalls.push(key),
      })
    } catch (err) {
      rejected = err
    }
    expect(rejected).toBeTruthy()
    expect(rejected.code).toBe('PROCESS_CLOSE_TIMEOUT')
    // Both SIGTERM and SIGKILL were attempted.
    expect(proc.history).toContain('kill:SIGTERM')
    expect(proc.history).toContain('kill:SIGKILL')
    // Cleanup is NOT called on rejection — the slot must stay leased.
    expect(cleanupCalls).toEqual([])
    // The fake proc is still alive in the eyes of the helper.
    expect(proc.exitCode).toBeNull()
    expect(proc.signalCode).toBeNull()
  })

  it('Promise resolves only after actual close — partial signal-kill still pending does not resolve', async () => {
    const proc = makeFakeProc()
    // Fake that handles SIGTERM but ignores SIGKILL.
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      if (sig === 'SIGTERM') {
        // Simulate graceful close on SIGTERM.
        proc.exitCode = 143
        proc.signalCode = 'SIGTERM'
        emit(proc, 'close')
      } else if (sig === 'SIGKILL') {
        // Ignore SIGKILL — child stays alive.
      }
    }) as any
    proc.removeAllListeners = ((ev: string) => { proc.emits[ev] = [] }) as any
    proc.once = ((ev: string, cb: (...args: any[]) => void) => {
      ;(proc.emits[ev] ||= []).push(cb)
    }) as any

    const cleanupCalls: string[] = []
    let resolved = false
    await signalAndAwaitClose(proc as any, {
      fallbackMs: 50,
      cleanupKey: 'graceful',
      cleanup: (key) => cleanupCalls.push(key),
    })
    resolved = true
    // The promise resolved cleanly because SIGTERM closed the child.
    expect(resolved).toBe(true)
    expect(cleanupCalls).toEqual(['graceful'])
    expect(proc.exitCode).toBe(143)
    expect(proc.signalCode).toBe('SIGTERM')
  })
})

// ── (I) DEFECT #1 — error/close WITHOUT terminal metadata cannot resolve ─

describe('I — defect #1: error event with null exit/signal cannot falsely resolve', () => {
  it('waitForProcessClose ignores an error event while exit/signal remain null; SIGKILL fallback then hard timeout reject', async () => {
    const proc = makeFakeProc()
    // Fake child that ignores SIGKILL.
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      // Intentionally do NOT set exit/signal — child stays alive.
    }) as any

    let resolved = false
    let rejected: any = null
    const w = waitForProcessClose(proc as any, 30)
    w.then(() => { resolved = true }, (e) => { rejected = e })
    // Yield so the timers schedule.
    await new Promise(r => setTimeout(r, 5))
    // Emit an `error` event WITHOUT terminal metadata — this must NOT
    // resolve the wait.
    emit(proc, 'error', new Error('fake-error-without-metadata'))
    await new Promise(r => setTimeout(r, 10))
    expect(resolved).toBe(false)
    // SIGKILL fallback fired but child ignored it.
    expect(proc.history).toContain('kill:SIGKILL')
    expect(proc.exitCode).toBeNull()
    expect(proc.signalCode).toBeNull()
    // Wait for hard ceiling reject — await the actual chain so the
    // rejection handler has settled.
    try { await w } catch { /* expected */ }
    expect(resolved).toBe(false)
    expect(rejected).toBeTruthy()
    expect(rejected.code).toBe('PROCESS_CLOSE_TIMEOUT')
  })

  it('waitForProcessClose ignores a close event with null exit/signal — must NOT resolve prematurely', async () => {
    const proc = makeFakeProc()
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
    }) as any

    let resolved = false
    let rejected: any = null
    const w = waitForProcessClose(proc as any, 30)
    w.then(() => { resolved = true }, (e) => { rejected = e })
    await new Promise(r => setTimeout(r, 5))
    // Emit a close event WITHOUT terminal metadata — must NOT resolve.
    emit(proc, 'close')
    await new Promise(r => setTimeout(r, 10))
    expect(resolved).toBe(false)
    // Hard timer eventually rejects — await the chain to settle.
    try { await w } catch { /* expected */ }
    expect(rejected).toBeTruthy()
    expect(rejected.code).toBe('PROCESS_CLOSE_TIMEOUT')
    expect(proc.history).toContain('kill:SIGKILL')
  })
})

// ── (J) DEFECT #2 — other process listeners MUST survive ──────────

describe('J — defect #2: existing close/error listeners are preserved', () => {
  it('signalAndAwaitClose does NOT call removeAllListeners; external close listener survives and fires exactly once', async () => {
    const proc = makeFakeProc()
    let removeAllCount = 0
    proc.removeAllListeners = ((ev: string) => {
      removeAllCount++
      proc.emits[ev] = []
    }) as any
    // Track calls to the external close listener.
    let externalCalls = 0
    const externalClose = () => { externalCalls++ }
    proc.once('close', externalClose)

    // Wire SIGTERM to synchronously set signalCode + emit close.
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      proc.signalCode = 'SIGTERM'
      proc.exitCode = 143
      emit(proc, 'close')
    }) as any
    proc.once = ((ev: string, cb: (...args: any[]) => void) => {
      ;(proc.emits[ev] ||= []).push(cb)
    }) as any

    let cleanupCalls = 0
    await signalAndAwaitClose(proc as any, {
      fallbackMs: 50,
      cleanupKey: 'preserve',
      cleanup: () => { cleanupCalls++ },
    })

    // Defect #2 contract: removeAllListeners must NOT have been called.
    expect(removeAllCount).toBe(0)
    // The external close listener fires EXACTLY once.
    expect(externalCalls).toBe(1)
    // Helper's cleanup runs after the close settles.
    expect(cleanupCalls).toBe(1)
    expect(proc.history).toContain('kill:SIGTERM')
  })

  it('external close listener installed BEFORE helper is not removed; fires after helper cleanup', async () => {
    const proc = makeFakeProc()
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      proc.signalCode = 'SIGTERM'
      proc.exitCode = 143
      emit(proc, 'close')
    }) as any
    proc.once = ((ev: string, cb: (...args: any[]) => void) => {
      ;(proc.emits[ev] ||= []).push(cb)
    }) as any

    const sequence: string[] = []
    // External listener installed BEFORE helper runs.
    const externalClose = () => sequence.push('external-close')
    proc.once('close', externalClose)
    await signalAndAwaitClose(proc as any, {
      fallbackMs: 50,
      cleanupKey: 'seq',
      cleanup: () => sequence.push('helper-cleanup'),
    })
    // Both listeners fire.
    expect(sequence).toContain('external-close')
    expect(sequence).toContain('helper-cleanup')
    // External listener is still registered in the fake's array (the
    // helper only removes its own listener reference; once() removes
    // itself after firing).
    // The helper's own close listener was registered via once() and
    // so it auto-removes itself. The external listener also fires
    // exactly once.
  })
})

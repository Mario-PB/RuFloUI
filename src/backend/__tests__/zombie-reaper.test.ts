// @vitest-environment node
/**
 * ACC-TASK-QUEUE-002-LISTENER-GUARD-FINAL — zombie reaper fail-closed
 * behavioural tests.
 *
 * Defect #5 invariants:
 *   - proc.killed is NOT used as evidence of closure.
 *   - Every found proc is handed to the canonical signalAndAwaitClose;
 *     already-closed procs resolve immediately inside the helper.
 *   - On hard rejection the reaper MUST NOT call cleanupProcess and
 *     MUST NOT remove the live process from tracking.
 *   - The .catch handler prevents unhandled rejection.
 *
 * Because the reaper lives inside server.ts (a 4300-line file that
 * boots Express + WebSocket + Telegram), we test the underlying
 * invariants by driving the canonical `signalAndAwaitClose` helper
 * against the same fake-proc shape the reaper sees, plus a small
 * extracted `zombieReapOne` helper that mirrors the production tick.
 *
 * The extracted helper is intentionally simple so it can be unit-tested
 * without booting the server. Its single-line contract is exactly the
 * one enforced by the inline reaper in server.ts.
 */
import { describe, it, expect } from 'vitest'
import { signalAndAwaitClose, ProcessCloseTimeoutError } from '../process-close'

interface FakeProc {
  killed: boolean
  exitCode: number | null
  signalCode: string | null
  emits: Record<string, Array<(...args: any[]) => void>>
  history: string[]
  removeAllListeners(ev: string): void
  removeListener(ev: string, cb: (...args: any[]) => void): void
  once(ev: string, cb: (...args: any[]) => void): void
  kill(sig?: string): void
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
    removeAllListeners(ev: string) { emits[ev] = [] },
    removeListener(ev, cb) {
      const arr = emits[ev]
      if (!arr) return
      const idx = arr.indexOf(cb)
      if (idx >= 0) arr.splice(idx, 1)
    },
    once(ev, cb) { (emits[ev] ||= []).push(cb) },
    kill(sig?: string) {
      this.history.push(`kill:${sig ?? ''}`)
      this.killed = true
    },
  }
  return proc
}

function emitFake(proc: FakeProc, ev: string, ...args: any[]) {
  const arr = proc.emits[ev]?.slice() || []
  arr.forEach(cb => { try { cb(...args) } catch { /* ignore */ } })
}

/**
 * Small extracted helper that mirrors the production zombie reaper
 * tick: every found proc is handed to the canonical
 * signalAndAwaitClose. A `.catch` handler logs a typed failure and
 * intentionally does NOT call cleanupProcess, mirroring the production
 * contract.
 */
function zombieReapOne(
  proc: FakeProc,
  cleanupKey: string,
  onCleanup: (key: string) => void,
): { closePromise: Promise<void> } {
  const closePromise = signalAndAwaitClose(proc as any, {
    fallbackMs: 30,
    cleanupKey,
    cleanup: onCleanup,
  })
  closePromise.catch(() => {
    // Production: log typed failure; do NOT cleanup; leave tracking.
    // The unit test asserts cleanup was NOT called on rejection.
  })
  return { closePromise }
}

describe('Z — defect #5: zombie reaper fail-closed invariants', () => {
  it('killed=true + null exitCode + null signalCode: reaper hands proc to canonical helper; SIGKILL fallback fires; tracking stays', async () => {
    const proc = makeFakeProc()
    proc.killed = true
    proc.exitCode = null
    proc.signalCode = null
    // Child ignores every signal — exit/signal stay null forever.
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      // Intentionally do NOT emit close, do NOT set exit/signal.
    }) as any

    let cleanupCalls = 0
    const { closePromise } = zombieReapOne(proc, 'zombie-A', () => { cleanupCalls++ })

    let resolved = false
    let rejected: any = null
    closePromise.then(() => { resolved = true }, (e) => { rejected = e })

    // Yield, then assert the helper is still waiting.
    await new Promise(r => setTimeout(r, 10))
    expect(resolved).toBe(false)
    // After the SIGKILL fallback timer fires, the hard ceiling
    // rejection should settle — await the actual chain.
    try { await closePromise } catch { /* expected */ }
    expect(resolved).toBe(false)
    expect(rejected).toBeTruthy()
    expect(rejected).toBeInstanceOf(ProcessCloseTimeoutError)
    expect(proc.history.some(h => h === 'kill:SIGKILL')).toBe(true)
    // CRITICAL: cleanupProcess was NOT called on rejection — tracking
    // is preserved for diagnostic / retry.
    expect(cleanupCalls).toBe(0)
    // The proc is still in the eyes of the helper as not closed.
    expect(proc.exitCode).toBeNull()
    expect(proc.signalCode).toBeNull()
    // No unhandled rejection: our .catch handler attaches a no-op.
  })

  it('already-closed proc: helper resolves immediately; cleanup runs; no SIGKILL needed', async () => {
    const proc = makeFakeProc()
    proc.killed = true
    proc.exitCode = 137
    proc.signalCode = 'SIGKILL'

    let cleanupCalls = 0
    let resolved = false
    await signalAndAwaitClose(proc as any, {
      fallbackMs: 30,
      cleanupKey: 'already-closed',
      cleanup: () => { cleanupCalls++ },
    }).then(() => { resolved = true })
    expect(resolved).toBe(true)
    expect(cleanupCalls).toBe(1)
    // No SIGKILL attempt was needed — the helper short-circuits.
    expect(proc.history).toEqual([])
  })

  it('graceful SIGTERM close: helper resolves on the close event; cleanup runs; no SIGKILL needed', async () => {
    const proc = makeFakeProc()
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      proc.signalCode = 'SIGTERM'
      proc.exitCode = 143
      emitFake(proc, 'close')
    }) as any

    let cleanupCalls = 0
    await signalAndAwaitClose(proc as any, {
      fallbackMs: 50,
      cleanupKey: 'graceful',
      cleanup: () => { cleanupCalls++ },
    })
    expect(cleanupCalls).toBe(1)
    expect(proc.history).toContain('kill:SIGTERM')
    // No SIGKILL needed because the child closed cleanly.
    expect(proc.history).not.toContain('kill:SIGKILL')
  })

  it('reaper promise has an attached .catch handler: no unhandled rejection surfaces', async () => {
    const proc = makeFakeProc()
    proc.kill = ((sig?: string) => {
      proc.history.push(`kill:${sig ?? ''}`)
      proc.killed = true
      // Ignore SIGKILL — child stays alive.
    }) as any

    let unhandled: any = null
    const onUnhandled = (reason: unknown) => { unhandled = reason }
    process.once('unhandledRejection', onUnhandled)

    const { closePromise } = zombieReapOne(proc, 'zombie-unhandled', () => {})
    // Wait long enough for the hard ceiling reject to fire.
    await new Promise(r => setTimeout(r, 80))
    // Give the event loop a chance to surface unhandled rejections.
    await new Promise(r => setTimeout(r, 10))
    // The reaper's `.catch` handler attached during zombieReapOne
    // absorbs the rejection so it never reaches the unhandledRejection
    // event.
    expect(unhandled).toBeNull()
    void closePromise
  })
})
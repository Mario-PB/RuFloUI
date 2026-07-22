/**
 * process-close.ts — extracted lifecycle helpers for tracked child
 * processes. Standalone so they can be exercised by unit tests without
 * booting the rest of the server.
 *
 * Both helpers were extracted from server.ts verbatim (defect fixes #1
 * and #2 of ACC-TASK-QUEUE-002-RACE-REPAIR):
 *   - waitForProcessClose(): the bounded wait for a child to actually
 *     exit, with SIGKILL fallback semantics.
 *   - signalAndAwaitClose(): the canonical cancel-proc pipeline — attach
 *     listeners BEFORE sending SIGTERM, await the actual close, run the
 *     caller-supplied cleanup only AFTER close settles.
 *
 * Both helpers are FAIL-CLOSED. The Promise resolves ONLY when the
 * process has actually closed (exitCode !== null OR signalCode !== null)
 * OR when the process is already terminal at entry. If the hard ceiling
 * elapses with the child still alive, the Promise REJECTS with a typed
 * `ProcessCloseTimeoutError`. Callers (cancelTask, cancelActive) must
 * surface the rejection to the dispatcher so the slot stays leased and
 * the cancellation guard is not released.
 *
 * Defect #1 (ACC-TASK-QUEUE-002-LISTENER-GUARD-FINAL): onError and
 * onClose must NOT confirm exit until `isClosed(proc) === true`. An
 * `error` event without terminal metadata (exitCode/signalCode both
 * still null) is NOT a close. A `close` event without terminal metadata
 * is also NOT a successful close. Only an event that brings
 * isClosed(proc) === true may resolve the wait.
 *
 * Defect #2: signalAndAwaitClose MUST NOT call
 * `proc.removeAllListeners('close'|'error')` — that would strip the
 * server.ts close/error handlers that release scheduler leases, update
 * the task/workflow records, and notify agent activity. The helper
 * installs only its OWN `once` listeners; cleanup removes only its own
 * references via `proc.removeListener`.
 */
import { spawn } from 'child_process'

/** Typed error thrown when the process is still alive after the hard ceiling. */
export class ProcessCloseTimeoutError extends Error {
  readonly code = 'PROCESS_CLOSE_TIMEOUT'
  constructor(message: string) {
    super(message)
    this.name = 'ProcessCloseTimeoutError'
  }
}

/**
 * True ONLY when the process has actually exited. In Node, `proc.killed`
 * is "signal sent", not "process exited" — we never use it to confirm
 * close. An `error` event with null exitCode/signalCode is NOT a close
 * either; we MUST wait for the SIGKILL fallback (or another error) to
 * populate terminal metadata.
 */
export function isClosed(proc: any): boolean {
  return typeof proc.exitCode === 'number' || !!proc.signalCode
}

/**
 * Internal helper. Resolves only when the child has actually closed
 * (isClosed(proc) === true). Rejects with `ProcessCloseTimeoutError`
 * if the hard ceiling elapses while the child is still alive.
 *
 * Defect #1 fix:
 *   - onClose and onError each require isClosed(proc) === true before
 *     settling as success. An error event with null exitCode/signalCode
 *     is logged but NOT treated as a close. A close event with null
 *     exitCode/signalCode is similarly ignored — the SIGKILL fallback
 *     and hard-timeout rejection remain the only way to surface a
 *     failed close.
 *   - cleanup removes ONLY the listeners this helper owns and the
 *     timers this helper scheduled.
 */
function awaitProcessCloseImpl(
  proc: any,
  fallbackMs: number,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (isClosed(proc)) {
      resolve()
      return
    }
    let done = false
    let sigkillTimer: NodeJS.Timeout | null = null
    let hardTimer: NodeJS.Timeout | null = null

    const cleanup = () => {
      if (sigkillTimer) { clearTimeout(sigkillTimer); sigkillTimer = null }
      if (hardTimer) { clearTimeout(hardTimer); hardTimer = null }
      // Remove ONLY the listeners THIS helper installed. We deliberately
      // never call removeAllListeners — that would strip server.ts's
      // own close/error handlers and lose their lease + telemetry
      // bookkeeping.
      try { proc.removeListener('close', onClose) } catch { /* ignore */ }
      try { proc.removeListener('error', onError) } catch { /* ignore */ }
    }

    const settle = (err?: Error) => {
      if (done) return
      done = true
      cleanup()
      if (err) reject(err)
      else resolve()
    }

    /**
     * Defect #1: a `close` event alone is NOT a successful close.
     * It is only a successful close once `isClosed(proc) === true`.
     * If the close fires but terminal metadata is missing, the
     * SIGKILL fallback is sent and the hard timer still owns the
     * terminal settlement. We DO NOT call settle() here.
     */
    const onClose = () => {
      if (done) return
      if (!isClosed(proc)) {
        // Close event arrived without terminal metadata. Try once to
        // force a SIGKILL so the OS reaps the child and the next
        // close/error event will carry terminal metadata. The hard
        // timer still owns terminal settlement.
        try { proc.kill('SIGKILL') } catch { /* ignore */ }
        return
      }
      settle()
    }
    /**
     * Defect #1: an `error` event with null exitCode AND null
     * signalCode is NOT a close. We must NOT settle here — if we
     * did, callers would release the slot while the child is still
     * alive. The SIGKILL fallback fires regardless; the hard timer
     * still owns terminal settlement.
     */
    const onError = () => {
      if (done) return
      if (!isClosed(proc)) {
        // Error event without terminal metadata. Same reasoning as
        // onClose: SIGKILL fallback fires, hard timer owns
        // settlement.
        try { proc.kill('SIGKILL') } catch { /* ignore */ }
        return
      }
      settle()
    }

    // Defensive: never assume the helper's listeners are not already
    // attached (idempotency). Remove any leftover before re-attaching.
    try { proc.removeListener('close', onClose) } catch { /* ignore */ }
    try { proc.removeListener('error', onError) } catch { /* ignore */ }
    proc.once('close', onClose)
    proc.once('error', onError)

    // SIGKILL fallback. Fires unconditionally regardless of proc.killed:
    // in Node, `killed` is only "signal sent", not "process exited".
    sigkillTimer = setTimeout(() => {
      if (done) return
      try {
        if (!isClosed(proc)) {
          proc.kill('SIGKILL')
        }
      } catch { /* ignore */ }
    }, fallbackMs)

    // Hard ceiling — reject if the child is still alive. The SIGKILL
    // above fires at `fallbackMs`; we give the OS one extra second to
    // reap the child. If the child still has null exitCode and null
    // signalCode, it is still alive in some sense and we MUST NOT
    // report success — the caller will treat this as a failure and
    // keep the slot leased.
    hardTimer = setTimeout(() => {
      if (done) return
      if (isClosed(proc)) {
        settle()
        return
      }
      // Try a final SIGKILL in case the previous one was lost.
      try { proc.kill('SIGKILL') } catch { /* ignore */ }
      settle(new ProcessCloseTimeoutError(
        `process still alive after hard ceiling (${fallbackMs + 1000}ms): exitCode=null, signalCode=null`,
      ))
    }, fallbackMs + 1000)
  })
}

/**
 * Bounded wait for a tracked process to close. Resolves ONLY when the
 * child has actually closed (exitCode !== null OR signalCode !== null).
 * Rejects with `ProcessCloseTimeoutError` if the hard ceiling elapses
 * while the child is still alive.
 *
 * Contract:
 *   - NEVER resolve on `proc.killed === true` alone. In Node, killed only
 *     means a signal was sent; the process may still be running.
 *   - SIGKILL MUST be sent if exitCode and signalCode are both still null,
 *     regardless of `proc.killed`. A previous SIGTERM does not imply the
 *     OS has reaped the child.
 *   - Attach `close` / `error` listeners BEFORE sending SIGTERM so the
 *     process cannot close between kill and listener attachment.
 *   - Only consider the process "done" when exitCode !== null OR
 *     signalCode !== null. (Defect #1 fix: error/close events with null
 *     terminal metadata are NOT a successful close.)
 *   - Cleanup all timers + own listeners on every final path (resolve,
 *     reject, SIGKILL fallback, hard timeout). NEVER remove other
 *     listeners — server.ts close/error handlers stay attached.
 *   - Returns `null` for null/undefined proc — caller may treat that as
 *     "nothing to wait for".
 *   - If `proc` is already closed at entry, resolves immediately.
 */
export function waitForProcessClose(
  proc: ReturnType<typeof spawn> | null | undefined,
  fallbackMs = 5000,
): Promise<void> {
  if (!proc) return Promise.resolve()
  if (isClosed(proc)) return Promise.resolve()
  return awaitProcessCloseImpl(proc, fallbackMs)
}

/**
 * Canonical cancel-proc helper: attach close/error listeners FIRST, then
 * send SIGTERM, then await actual close (with SIGKILL fallback).
 *
 * Order (defect #2 — listeners BEFORE SIGTERM):
 *   1. Send SIGTERM ONLY if the child is still alive at the moment we
 *      were about to send the signal. The close/error listeners attached
 *      by awaitProcessCloseImpl are in place BEFORE the kill() call,
 *      so a synchronous close during kill is observed.
 *   2. Await actual close. FAIL-CLOSED: if the hard ceiling elapses
 *      while the child is still alive, REJECT with `ProcessCloseTimeoutError`.
 *      The rejection is propagated to the caller; the caller MUST NOT
 *      release the slot in that case.
 *   3. Cleanup runs ONLY after close has actually settled. If the helper
 *      rejected, cleanup is NOT called — the caller handles the failure.
 *
 * Defect #2 fix: we DO NOT call `proc.removeAllListeners('close'|'error')`
 * here. The server.ts close/error handlers — which release scheduler
 * leases, update TaskRecord / WorkflowRecord, and notify agent activity —
 * must remain attached. We add only our own `once` listeners and the
 * helper's own cleanup removes only those references.
 *
 * If `proc` is undefined the helper resolves immediately and runs
 * cleanup. If `proc` is already closed at entry, the helper resolves
 * immediately and runs cleanup.
 */
export async function signalAndAwaitClose(
  proc: ReturnType<typeof spawn> | null | undefined,
  opts: {
    fallbackMs?: number
    cleanupKey?: string
    cleanup?: (key: string) => void
  } = {},
): Promise<void> {
  const { fallbackMs = 5000, cleanupKey, cleanup } = opts
  if (!proc) {
    if (cleanupKey && cleanup) cleanup(cleanupKey)
    return
  }
  if (isClosed(proc)) {
    if (cleanupKey && cleanup) cleanup(cleanupKey)
    return
  }

  // Defect #2 fix: we DO NOT remove other listeners here. The helper
  // adds its own once listeners inside awaitProcessCloseImpl. Any
  // pre-existing close/error handlers (server.ts scheduler lease
  // release, TaskRecord/WorkflowRecord updates, agent-activity emits)
  // stay attached and continue to fire normally.

  // IMPORTANT: attach listeners BEFORE sending the signal. We start
  // awaitProcessCloseImpl FIRST — its constructor synchronously
  // attaches close/error listeners on proc. Only after those listeners
  // are in place do we send SIGTERM. A fast-close child that emits
  // `close` synchronously inside kill() is observed.
  const closePromise = awaitProcessCloseImpl(proc, fallbackMs)

  // Send SIGTERM ONLY if child is still alive. The listeners are
  // attached NOW, so any synchronous close emit during kill is
  // observed.
  try {
    if (!isClosed(proc)) {
      proc.kill('SIGTERM')
    }
  } catch { /* ignore */ }

  // Wait for actual close OR hard ceiling rejection.
  try {
    await closePromise
  } catch (err) {
    // Cleanup is intentionally NOT called on rejection — the caller is
    // responsible for handling the failure (the slot must stay leased).
    throw err
  }

  // Cleanup runs ONLY after we have confirmed the child has actually
  // closed.
  if (cleanupKey && cleanup) cleanup(cleanupKey)
}
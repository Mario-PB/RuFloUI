/**
 * task-lifecycle.ts — pure helpers for explicit task terminal settlement
 * and canonical cancellation.
 *
 * Both helpers are extracted from server.ts so they can be exercised by
 * behavioral tests without booting the entire Express + WebSocket server.
 *
 * The helpers are parameterized over their dependencies:
 *   - the TaskStore + WorkflowStore maps
 *   - the TaskDispatcher (source of truth for in-flight slots)
 *   - a broadcast function for state-change notifications
 *   - a persist function (debounced save) called on terminal transitions
 *   - a process registry + cleanup fn used by active cancellation
 *   - a getScheduler fn used to release any synthetic subtask leases
 *
 * This lets server.ts pass its module-level state, while tests can pass
 * isolated maps and a mocked scheduler.
 *
 * ── CONTRACT REMINDERS ──────────────────────────────────────────────
 *
 * settleTaskTerminal
 *   - Records the passed result on the TaskRecord before any transition.
 *   - The authoritative dispatcher transition runs first; the broadcast
 *     runs after. A second terminal call is a guaranteed no-op.
 *   - cancelled/interrupted are preserved (never overwritten).
 *
 * cancelTask
 *   - Already-terminal: idempotent no-op.
 *   - pending/preparing: physically removed via dispatcher.cancelPending;
 *     never spawns Claude, never provisions a worktree.
 *   - Active cancel delegates the entire process teardown + slot release
 *     to `dispatcher.cancelActive(taskId, cleanupCallback)`. The
 *     dispatcher:
 *       a) installs the cancellation guard BEFORE running cleanupCallback
 *          so concurrent complete/fail/startTask are no-ops;
 *       b) runs the cleanupCallback which performs
 *          scheduler.cancelTask → signalAndAwaitClose → cleanupProcess
 *          for every captured tracked process;
 *       c) ONLY on successful callback completion does the dispatcher
 *          perform the terminalTransition, clear the guard, release the
 *          slot, and dispatch the next task.
 *   - The TaskRecord.status / WorkflowRecord.status / broadcasts are
 *     updated BEFORE the dispatcher is invoked so the UI guard is in
 *     place by the time the cancel fires SIGTERM.
 *   - completed/failed/interrupted: idempotent no-op.
 */
import { spawn } from 'child_process'
import { TaskDispatcher, DispatcherTaskRecord } from './task-dispatcher'
import { signalAndAwaitClose } from './process-close'

// ── SHARED TYPES ─────────────────────────────────────────────────────

export interface LifecycleTaskRecord {
  id: string
  status: string
  result?: string
  completedAt?: string
  startedAt?: string
  agentResults?: Array<{ index: number; agent: string; task: string; result: string }>
  subtaskStatuses?: Array<'pending' | 'completed' | 'failed' | 'cancelled'>
  // Any other fields (sourceCwd, etc.) are read-only through this helper.
  [key: string]: unknown
}

export interface LifecycleWorkflowRecord {
  id: string
  status: string
  taskId?: string
  completedAt?: string
  steps: Array<{ id?: string; status: string; [key: string]: unknown }>
  result?: string
  [key: string]: unknown
}

export interface LifecycleDeps {
  taskStore: Map<string, LifecycleTaskRecord>
  workflowStore: Map<string, LifecycleWorkflowRecord>
  dispatcher: TaskDispatcher
  runningProcesses: Map<string, ReturnType<typeof spawn>>
  broadcast: (type: string, payload: unknown) => void
  persist: () => void
  syncTaskRecordFromDispatcher: (taskId: string) => void
  cleanupProcess: (key: string) => void
  getScheduler: () => { cancelTask: (taskId: string) => void } | null
}

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled', 'interrupted'])
function terminalStatusSet(status: string | undefined): boolean {
  return !!status && TERMINAL_STATUSES.has(status)
}

function dispatcherIsAlreadyTerminal(dr: DispatcherTaskRecord): boolean {
  return dr.status === 'completed' || dr.status === 'failed' || dr.status === 'cancelled' || dr.status === 'interrupted'
}

// ── SETTLE TASK TERMINAL ─────────────────────────────────────────────

/**
 * Single explicit terminal settlement helper. All non-cancel terminal
 * paths funnel through here so:
 *   - cancelled is NEVER overwritten by a close/error/timeout handler
 *     racing against an explicit user cancel;
 *   - the passed `result` is recorded on the TaskRecord BEFORE the
 *     authoritative dispatcher transition;
 *   - a second terminal call is a guaranteed no-op (returns false).
 *
 * Returns true if this call performed a transition, false if it was a
 * no-op (already terminal, missing task, or repeated call).
 */
export function settleTaskTerminal(
  deps: LifecycleDeps,
  taskId: string,
  desired: 'completed' | 'failed',
  result: string,
): boolean {
  const task = deps.taskStore.get(taskId)
  if (!task) return false
  // 1. cancelled/interrupted is preserved — never overwrite.
  if (task.status === 'cancelled' || task.status === 'interrupted') return false
  const dr = deps.dispatcher.get(taskId)
  if (dr && dispatcherIsAlreadyTerminal(dr)) return false
  // 2. Capture the canonical result string in a local variable BEFORE
  //    any side-effecting call. The dispatcher's statusChange listeners
  //    and the test-overridden syncTaskRecordFromDispatcher can clobber
  //    task.result in transit; we re-assert the canonical value AFTER
  //    the sync from the local capture.
  const canonicalResult = result || task.result || ''
  task.result = canonicalResult
  // 3. Authoritative dispatcher transition first.
  if (desired === 'completed') {
    deps.dispatcher.complete(taskId, canonicalResult || 'completed')
  } else {
    deps.dispatcher.fail(taskId, canonicalResult || 'failed')
  }
  // 4. Mirror to TaskRecord (syncTaskRecordFromDispatcher overwrites
  //    with the dispatcher's authoritative fields including status).
  //    Re-assert the canonical result AFTER the sync so any
  //    listener-driven overwrite does not clobber our canonical
  //    result string.
  deps.syncTaskRecordFromDispatcher(taskId)
  const fresh = deps.taskStore.get(taskId)
  if (fresh) fresh.result = canonicalResult
  // Also pin the original `task` reference in case test code captured
  // a different Map entry earlier in the call.
  if (task !== fresh) task.result = canonicalResult
  // 5. Mirror linked workflow terminal.
  for (const [, wf] of deps.workflowStore.entries()) {
    if (wf.taskId === taskId && wf.status !== 'completed' && wf.status !== 'cancelled' && wf.status !== 'failed') {
      wf.status = desired
      wf.completedAt = task.completedAt || new Date().toISOString()
      wf.result = result
      deps.broadcast('workflow:updated', wf)
    }
  }
  // 6. The dispatcher statusChange listener owns task:updated.
  // Emitting it here as well duplicates Telegram notifications and
  // repeats webhook completion side effects. Workflow broadcasts above
  // remain owned by this helper.
  return true
}

// ── CANCEL TASK ──────────────────────────────────────────────────────

export type CancelMode = 'pending' | 'active' | 'noop'

export interface CancelResult {
  ok: boolean
  alreadyTerminal?: boolean
  mode: CancelMode
  status?: string
}

/**
 * Canonical task cancellation helper. Used by:
 *   - POST /api/tasks/:id/cancel      (HTTP route)
 *   - Telegram /cancel                (TelegramStores.cancelTask)
 *   - /api/workflows/:id/cancel       (linked-task cancel)
 *   - launchViaClaude dispatch-rejection cancellation
 *
 * THROWING CONTRACT:
 *   - The legacy fallback branch (when the dispatcher has no record for
 *     this task) may REJECT with the original typed error thrown by
 *     signalAndAwaitClose (e.g. ProcessCloseTimeoutError) when one or
 *     more captured tracked processes could not be confirmed closed.
 *     When this happens, the helper does NOT return `{ ok: true }`,
 *     does NOT call cleanupProcess for unconfirmed closes, and leaves
 *     the (still-alive) processes in runningProcesses. The TaskRecord
 *     keeps `status = 'cancelled'` as the user's intent, but the
 *     response cannot honestly claim success.
 *
 * Strict ordering (defect #3 — guard BEFORE signal/wait):
 *
 *   1. Terminal guard — completed / failed / cancelled / interrupted
 *      are idempotent no-ops.
 *
 *   2. Pending / preparing cancel — dispatcher.cancelPending physically
 *      removes the queued work. NEVER spawns Claude, NEVER provisions
 *      a worktree.
 *
 *   3. Active cancel. The slot is NOT released by this function. The
 *      dispatcher is the single owner of slot release; this helper
 *      delegates the entire process teardown via
 *      `dispatcher.cancelActive(taskId, cleanupCallback)`. The
 *      dispatcher:
 *        a) installs the cancellation guard BEFORE invoking the
 *           callback, so concurrent complete/fail/startTask are
 *           guaranteed no-ops for this task;
 *        b) awaits the callback (defined below);
 *        c) on callback success, performs the terminal transition,
 *           clears the guard, releases the slot, and schedules the
 *           next dispatch tick.
 *
 *   4. The cleanup callback runs synchronously inside cancelActive.
 *      It is responsible for the bounded process teardown in this
 *      strict order:
 *        i)   scheduler.cancelTask(taskId) — stop new dispatches;
 *        ii)  signalAndAwaitClose for every captured tracked process;
 *             close/error handlers that fire DURING teardown observe
 *             the already-installed cancellation guard, so they cannot
 *             transition the task out of cancelled;
 *        iii) cleanupProcess(key) for each tracked process — called
 *             ONLY after the actual close has settled.
 *
 *   5. TaskRecord.status is updated and the workflow is mirrored
 *      cancelled BEFORE the dispatcher is invoked, so any close
 *      handler racing in during process teardown preserves the
 *      cancelled state.
 *
 *   6. Legacy fallback — when the dispatcher has no record for the
 *      task (e.g. older persisted entries that predate the
 *      dispatcher), the helper still runs the bounded process
 *      teardown via signalAndAwaitClose + cleanupProcess, but it
 *      does NOT use the dispatcher guard path. The legacy branch is
 *      fail-closed: scheduler.cancelTask is awaited BEFORE any SIGTERM,
 *      and if signalAndAwaitClose rejects (e.g.
 *      ProcessCloseTimeoutError) the typed error is propagated to the
 *      caller. cleanupProcess is NOT called for unconfirmed closes, the
 *      process stays in runningProcesses, and the helper does NOT
 *      return `{ ok: true }`. New dispatcher-owned tasks ALWAYS go
 *      through the guard path.
 */
export async function cancelTask(
  deps: LifecycleDeps,
  taskId: string,
  opts: { reason?: string; signalFallbackMs?: number } = {},
): Promise<CancelResult> {
  const task = deps.taskStore.get(taskId)
  if (!task) return { ok: false, mode: 'noop' }
  const dr = deps.dispatcher.get(taskId)

  // 1. Terminal guard — completed / failed / cancelled / interrupted
  //    are all idempotent no-ops. We must NEVER overwrite a real
  //    terminal state.
  if (dr && dispatcherIsAlreadyTerminal(dr)) {
    return { ok: true, alreadyTerminal: true, mode: 'noop', status: dr.status }
  }
  if (terminalStatusSet(task.status)) {
    return { ok: true, alreadyTerminal: true, mode: 'noop', status: task.status }
  }

  // 2. Pending / preparing cancel: physically remove queued work and
  //    record terminal state through the dispatcher's cancelPending.
  //    This NEVER spawns Claude, NEVER launches a worktree.
  if (dr && (dr.status === 'pending' || dr.status === 'preparing')) {
    const { cancelled } = deps.dispatcher.cancelPending(taskId)
    if (cancelled) {
      task.status = 'cancelled'
      task.completedAt = task.completedAt || new Date().toISOString()
      const note = `cancelled (${opts.reason || 'explicit'})`
      task.result = (task.result ? task.result + '\n' : '') + note
      for (const [, wf] of deps.workflowStore.entries()) {
        if (wf.taskId === taskId && wf.status !== 'completed' && wf.status !== 'cancelled') {
          wf.status = 'cancelled'
          wf.completedAt = task.completedAt
          wf.steps.forEach(s => { if (s.status === 'running' || s.status === 'pending') s.status = 'cancelled' })
          wf.result = (wf.result ? wf.result + '\n' : '') + note
          deps.broadcast('workflow:updated', wf)
        }
      }
      deps.syncTaskRecordFromDispatcher(taskId)
      deps.broadcast('task:updated', { ...task, id: taskId })
      deps.persist()
      return { ok: true, mode: 'pending', status: 'cancelled' }
    }
    // cancelPending refused (e.g. status changed between snapshot and
    // call) — fall through to active cancellation semantics.
  }

  // 3. Active cancel. We:
  //    a) capture tracked processes;
  //    b) mark TaskRecord + Workflow cancelled + broadcast (so any
  //       close handler racing in during teardown observes the
  //       cancelled state);
  //    c) delegate to dispatcher.cancelActive with a cleanup callback
  //       that performs scheduler.cancelTask → signalAndAwaitClose →
  //       cleanupProcess.
  //
  //    The dispatcher's contract:
  //      - installs the cancellation guard BEFORE invoking the callback
  //        (so complete/fail/startTask for this task are no-ops);
  //      - runs the callback;
  //      - on callback success, performs the terminal transition,
  //        clears the guard, releases the slot exactly once, and
  //        schedules the next dispatch tick.
  //
  //    On callback rejection (e.g. signalAndAwaitClose rejected with
  //    ProcessCloseTimeoutError), the dispatcher keeps the guard and
  //    the slot leased. The caller observes the rejection via the
  //    promise chain.
  const tracked: Array<{ proc: ReturnType<typeof spawn>; key: string }> = []
  for (const [key, proc] of deps.runningProcesses.entries()) {
    if (key.startsWith(taskId)) tracked.push({ proc, key })
  }

  task.status = 'cancelled'
  task.completedAt = new Date().toISOString()
  const note = `cancelled (${opts.reason || 'explicit'})`
  task.result = (task.result ? task.result + '\n' : '') + note
  for (const [, wf] of deps.workflowStore.entries()) {
    if (wf.taskId === taskId && wf.status !== 'completed' && wf.status !== 'cancelled' && wf.status !== 'failed') {
      wf.status = 'cancelled'
      wf.completedAt = task.completedAt
      wf.steps.forEach(s => { if (s.status === 'running' || s.status === 'pending') s.status = 'cancelled' })
      wf.result = (wf.result ? wf.result + '\n' : '') + note
      deps.broadcast('workflow:updated', wf)
    }
  }
  deps.broadcast('task:updated', { ...task, id: taskId })
  deps.persist()

  if (!dr) {
    // Legacy fallback — dispatcher has no record for this task (older
    // TaskRecord whose persistence predates the dispatcher, or a task
    // created via a path that bypassed createAndEnqueueTask). Run the
    // bounded process teardown directly.
    //
    // FAIL-CLOSED contract:
    //   1. scheduler.cancelTask FIRST — no new subtask dispatches for
    //      this task once we begin teardown.
    //   2. signalAndAwaitClose for every captured tracked process.
    //      signalAndAwaitClose attaches its own listeners BEFORE SIGTERM
    //      and runs cleanupProcess ONLY AFTER close settled.
    //   3. If ANY process-close helper REJECTS (e.g.
    //      ProcessCloseTimeoutError), the failure is propagated to the
    //      caller. We DO NOT call cleanupProcess for an unconfirmed
    //      close. We DO NOT return `{ ok: true }`. The process remains
    //      visible in runningProcesses so a later teardown attempt (or
    //      the operator) can retry. The TaskRecord already reflects the
    //      user's intent ('cancelled') via the broadcast above; we do
    //      NOT mutate that intent based on process teardown outcome.
    //   4. Sync/persist + success response only happen when every
    //      captured process actually closed.
    try {
      try { deps.getScheduler()?.cancelTask(taskId) } catch { /* scheduler may be torn down */ }
      // Build an outer try/catch so we can RE-THROW the typed error
      // (ProcessCloseTimeoutError or similar) up to the caller — do
      // NOT swallow it as the previous implementation did.
      await Promise.all(tracked.map(({ proc, key }) => signalAndAwaitClose(proc, {
        fallbackMs: opts.signalFallbackMs ?? 5000,
        cleanupKey: key,
        cleanup: deps.cleanupProcess,
      })))
    } catch (err) {
      // Fail-closed: at least one process did not actually close.
      // Surface the typed error to the caller. Do NOT return success.
      // cleanupProcess was NOT called for any unconfirmed-close process
      // because signalAndAwaitClose's contract refuses to run cleanup
      // when the promise rejects. The process is still in
      // runningProcesses (the slot stays leased for retry/shutdown).
      throw err
    }
    // All processes actually closed. Now we may sync, persist, and
    // claim success.
    deps.syncTaskRecordFromDispatcher(taskId)
    deps.broadcast('task:updated', { ...task, id: taskId })
    deps.persist()
    return { ok: true, mode: 'active', status: 'cancelled' }
  }

  // Dispatcher-owned path. The dispatcher installs the cancellation
  // guard BEFORE invoking the callback; close/error handlers firing
  // during teardown observe the guard and cannot transition the task
  // out of cancelled.
  await deps.dispatcher.cancelActive(taskId, async () => {
    // (i) Stop new scheduler dispatches for this task.
    try { deps.getScheduler()?.cancelTask(taskId) } catch { /* ignore */ }
    // (ii) Bounded wait for every captured tracked process to actually
    //      close. signalAndAwaitClose attaches its own listeners BEFORE
    //      SIGTERM and runs cleanupProcess only AFTER close settles.
    await Promise.all(tracked.map(({ proc, key }) => signalAndAwaitClose(proc, {
      fallbackMs: opts.signalFallbackMs ?? 5000,
      cleanupKey: key,
      cleanup: deps.cleanupProcess,
    })))
  })
  deps.syncTaskRecordFromDispatcher(taskId)
  // Re-broadcast post-settlement so subscribers see the final
  // authoritative state including the dispatched terminal reason and
  // finishedAt.
  deps.broadcast('task:updated', { ...task, id: taskId })
  deps.persist()
  return { ok: true, mode: 'active', status: 'cancelled' }
}
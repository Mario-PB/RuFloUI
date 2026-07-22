/**
 * task-dispatcher.ts — persistent top-level task queue with safe WRITE
 * isolation via git worktrees.
 *
 * Built on top of the existing GlobalScheduler (preserves its guarantees:
 * model=opus, global cap, priorities, dependencies, typed terminal
 * reasons, cancellation, exactly-one final reviewer).
 *
 * Responsibilities:
 *   1. Accept every new top-level task with status='pending' and stable
 *      priority ordering (critical → high → normal → low, FIFO inside).
 *   2. Cap parallelism at RUFLO_TASK_MAX_IN_FLIGHT (default 10) — falls
 *      back safely to default on bad env values, and rejects invalid
 *      constructor options.
 *   3. Move pending → preparing → in_progress. The public task is
 *      pending/queued while a worktree is being provisioned; it only
 *      becomes in_progress RIGHT BEFORE the launcher runs, after the
 *      worktree is ready (or for READ-ONLY, no provisioning needed).
 *   4. Capacity = preparing + running. Both count against maxInFlight.
 *   5. Pending/preparing cancel is a physical removal — never spawns
 *      Claude, never starts. Active cancel routes through a single
 *      explicit terminal transition helper.
 *   6. Cancel-while-provisioning: re-checked after every await and
 *      immediately before launching Claude. A cancelled task must never
 *      launch.
 *   7. Restart-recovery: side-effect-free hydrate. Old in_progress
 *      records without a live process become `interrupted` (fail-closed).
 *      Terminal records never re-run. Pending records enter the ready
 *      queue exactly once and the dispatcher does NOT launch anything
 *      until the entire hydrate pass is complete.
 *   8. queuePosition reflects the actual priority+FIFO order, not the
 *      raw insertion array.
 *   9. Duplicate enqueue/create does NOT overwrite an existing
 *      TaskRecord — it surfaces the existing authoritative record.
 *  10. cancel/complete/fail transitions go through ONE explicit helper
 *      (`terminalTransition`). A second transition is a no-op.
 *  11. forgetTerminal is allowed only on terminal records.
 *
 * The dispatcher is intentionally small and side-effect-light at module
 * load. server.ts wires it into the task routes, persistence layer and
 * scheduler events.
 */

import { Priority } from './scheduler'
import {
  WorktreeInfo,
  WorktreeManager,
  getWorktreeManager,
  WorktreeError,
} from './task-worktrees'

export type TaskMode = 'WRITE' | 'READ-ONLY'

export const TASK_PRIORITY_ORDER: Record<Priority, number> = {
  critical: 0,
  high: 1,
  normal: 2,
  low: 3,
}

const PRIORITY_DEFAULT: Priority = 'normal'
const MAX_IN_FLIGHT_DEFAULT = 10
const MAX_IN_FLIGHT_MIN = 1
const MAX_IN_FLIGHT_MAX = 1000

export type DispatcherStatus =
  | 'pending'
  | 'preparing'
  | 'in_progress'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'interrupted'

export const TERMINAL_STATUSES: ReadonlySet<DispatcherStatus> = new Set([
  'completed', 'failed', 'cancelled', 'interrupted',
])

export function isTerminalStatus(status: DispatcherStatus): boolean {
  return TERMINAL_STATUSES.has(status)
}

export interface DispatcherTaskRecord {
  id: string
  title: string
  description: string
  mode: TaskMode
  priority: Priority
  /** Authoritatively normalized status. */
  status: DispatcherStatus
  /** Original request cwd. READ-ONLY executes here. WRITE never touches it. */
  sourceCwd: string
  /** Per-WRITE worktree path; absent for READ-ONLY and for failed provisioning. */
  executionCwd?: string
  worktree?: WorktreeInfo
  createdAt: string
  startedAt?: string
  finishedAt?: string
  /** Terminal reason string mirroring scheduler.typed enum, plus dispatcher-only `interrupted`. */
  terminalReason?:
    | 'completed'
    | 'failed'
    | 'explicit-cancel'
    | 'task-cancelled'
    | 'interrupted'
    | 'worktree-failed'
    | 'enqueue-rejected'
    | 'duplicate-enqueue'
  /** Latest attempt counter for re-tries (terminal never auto-reruns). */
  attempt: number
  /** Filled in by server.ts when the actual claude -p child is spawned. */
  running?: boolean
  /** Set when the user explicitly assigned an agent. */
  assignedTo?: string
}

export type TaskLauncher = (task: DispatcherTaskRecord) => Promise<void>

export interface DispatcherOptions {
  maxInFlight?: number
  worktreeManager?: WorktreeManager
  /** Override the default priority for tests. */
  defaultPriority?: Priority
}

function readMaxInFlight(): number {
  const raw = process.env.RUFLO_TASK_MAX_IN_FLIGHT
  if (!raw) return MAX_IN_FLIGHT_DEFAULT
  // Reject anything that isn't a positive finite integer.
  if (!/^\d+$/.test(raw.trim())) return MAX_IN_FLIGHT_DEFAULT
  const n = Number(raw.trim())
  if (!Number.isFinite(n) || n <= 0 || n > 1000) return MAX_IN_FLIGHT_DEFAULT
  return Math.floor(n)
}

function validateMaxInFlight(n: unknown): number {
  if (typeof n !== 'number') return MAX_IN_FLIGHT_DEFAULT
  if (!Number.isFinite(n)) return MAX_IN_FLIGHT_DEFAULT
  if (!Number.isInteger(n)) return MAX_IN_FLIGHT_DEFAULT
  if (n < MAX_IN_FLIGHT_MIN || n > MAX_IN_FLIGHT_MAX) return MAX_IN_FLIGHT_DEFAULT
  return n
}

export interface DispatcherEvents {
  enqueue: { taskId: string }
  statusChange: { taskId: string; status: DispatcherStatus }
  dispatch: { taskId: string }
  preparing: { taskId: string }
  worktreeProvisioned: { taskId: string; worktreePath: string; branchName: string }
  worktreeFailed: { taskId: string; error: string }
  complete: { taskId: string; result: string }
  fail: { taskId: string; error: string }
  cancel: { taskId: string; reason: 'explicit-pending' | 'explicit-active' | 'task-cancel' }
}

type Listener<E extends keyof DispatcherEvents> = (payload: DispatcherEvents[E]) => void

/**
 * Deterministic priority + FIFO + task-id tie-break comparator.
 * Returns 0 only when both records are equivalent in EVERY sort key.
 */
export function compareTasksForOrder(a: DispatcherTaskRecord, b: DispatcherTaskRecord): number {
  const pa = TASK_PRIORITY_ORDER[a.priority]
  const pb = TASK_PRIORITY_ORDER[b.priority]
  if (pa !== pb) return pa - pb
  if (a.createdAt < b.createdAt) return -1
  if (a.createdAt > b.createdAt) return 1
  if (a.id < b.id) return -1
  if (a.id > b.id) return 1
  return 0
}

export class TaskDispatcher {
  private readonly maxInFlight: number
  private readonly worktrees: WorktreeManager
  private readonly defaultPriority: Priority
  private readonly tasks = new Map<string, DispatcherTaskRecord>()
  /** Pending queue (status === 'pending'). Mirrors the actual priority+FIFO order. */
  private readonly ready: string[] = []
  /** Tasks currently being launched or running (counts against maxInFlight). */
  private readonly inFlight = new Map<string, DispatcherTaskRecord>()
  /** Tasks that already reached a terminal state — never re-runs. */
  private readonly terminal = new Map<string, DispatcherTaskRecord>()
  /**
   * Tasks whose active cancellation has been requested but whose terminal
   * transition has not yet fired. A task in `cancelling` keeps its slot
   * (stays in `inFlight`) so the dispatcher cannot promote the next
   * queued task until cleanup actually settles.
   */
  private readonly cancelling = new Map<string, DispatcherTaskRecord>()
  /**
   * Per-task cancellation Promise. Concretely lets concurrent
   * cancelActive() calls share the same async cleanup; a second call
   * observes the same Promise and cannot trigger a double cleanup.
   */
  private readonly cancelCleanups = new Map<string, Promise<void>>()
  /** External launch hooks. Set via setLauncher(). */
  private launcher: TaskLauncher | null = null
  /** Set while a `tryDispatchTick` is running to prevent recursion. */
  private dispatching = false
  /** Set during a hydrate pass — we MUST NOT auto-dispatch from hydrate. */
  private hydrating = false

  private readonly listeners: { [K in keyof DispatcherEvents]: Set<Listener<K>> } = {
    enqueue: new Set(),
    statusChange: new Set(),
    dispatch: new Set(),
    preparing: new Set(),
    worktreeProvisioned: new Set(),
    worktreeFailed: new Set(),
    complete: new Set(),
    fail: new Set(),
    cancel: new Set(),
  }

  constructor(opts: DispatcherOptions = {}) {
    // Constructor opts.maxInFlight is validated the same way as the env var.
    this.maxInFlight = opts.maxInFlight !== undefined
      ? validateMaxInFlight(opts.maxInFlight)
      : readMaxInFlight()
    this.worktrees = opts.worktreeManager ?? getWorktreeManager()
    this.defaultPriority = opts.defaultPriority ?? PRIORITY_DEFAULT
  }

  get maxInFlightValue(): number {
    return this.maxInFlight
  }

  // ── External hooks ──────────────────────────────────────────────

  setLauncher(launcher: TaskLauncher | null): void {
    this.launcher = launcher
  }

  on<E extends keyof DispatcherEvents>(event: E, listener: Listener<E>): () => void {
    this.listeners[event].add(listener)
    return () => this.listeners[event].delete(listener)
  }

  removeAllListeners(): void {
    for (const k of Object.keys(this.listeners) as (keyof DispatcherEvents)[]) {
      this.listeners[k].clear()
    }
  }

  private emit<E extends keyof DispatcherEvents>(event: E, payload: DispatcherEvents[E]): void {
    for (const l of this.listeners[event]) {
      try {
        (l as Listener<E>)(payload)
      } catch {
        // listeners must not break the dispatcher.
      }
    }
  }

  // ── Public API ──────────────────────────────────────────────────

  /**
   * Idempotent enqueue. Reject duplicates with `duplicate-enqueue` and
   * leave the existing record untouched. The taskId is the caller-chosen
   * global id (per-project requirement: queue never duplicates one id).
   */
  enqueue(input: {
    id: string
    title: string
    description: string
    mode: TaskMode
    priority?: Priority
    sourceCwd: string
    assignedTo?: string
  }): { task: DispatcherTaskRecord; created: boolean } {
    const id = String(input.id || '').trim()
    if (!id) throw new Error('task id is required')
    if (this.tasks.has(id)) {
      // Idempotent — surface the existing record, do NOT requeue.
      const existing = this.tasks.get(id)!
      this.emit('enqueue', { taskId: id })
      return { task: existing, created: false }
    }

    const task: DispatcherTaskRecord = {
      id,
      title: String(input.title || '').slice(0, 1024),
      description: String(input.description || '').slice(0, 32_000),
      mode: input.mode,
      priority: input.priority || this.defaultPriority,
      status: 'pending',
      sourceCwd: String(input.sourceCwd || ''),
      createdAt: new Date().toISOString(),
      attempt: 0,
      assignedTo: input.assignedTo ? String(input.assignedTo) : undefined,
    }
    this.tasks.set(id, task)
    this.insertIntoReady(task)
    this.emit('enqueue', { taskId: id })
    this.setStatus(task, 'pending')
    // Try to promote immediately — may be a no-op while another is in flight.
    if (!this.hydrating) {
      queueMicrotask(() => this.tryDispatchTick())
    }
    return { task, created: true }
  }

  /** Look up a record (pending / in-flight / terminal). */
  get(taskId: string): DispatcherTaskRecord | undefined {
    return this.tasks.get(taskId)
  }

  /** Snapshot for tests and persistence. */
  snapshot(): DispatcherTaskRecord[] {
    return [...this.tasks.values()].map(t => ({ ...t }))
  }

  /** Pending count (status === 'pending'). */
  get pendingSize(): number {
    let n = 0
    for (const id of this.ready) {
      const t = this.tasks.get(id)
      if (t && t.status === 'pending') n++
    }
    return n
  }

  /**
   * Capacity used by both preparing and running tasks (blocker 2).
   * Preparing tasks hold a slot so maxInFlight can never be exceeded
   * even if many worktrees are provisioning at once.
   */
  get reservedSize(): number {
    let n = 0
    for (const t of this.tasks.values()) {
      if (t.status === 'preparing' || t.status === 'in_progress') n++
    }
    return n
  }

  get inFlightSize(): number {
    return this.inFlight.size
  }

  get terminalSize(): number {
    return this.terminal.size
  }

  /**
   * Position (1-based) of a pending task in the priority+FIFO ready queue.
   * 0 when the task is not pending. Position reflects the live
   * `compareTasksForOrder` ordering, not the raw insertion array.
   */
  queuePosition(taskId: string): number {
    const t = this.tasks.get(taskId)
    if (!t || t.status !== 'pending') return 0
    const sorted = this.snapshotReadySorted()
    const idx = sorted.findIndex(x => x.id === taskId)
    return idx >= 0 ? idx + 1 : 0
  }

  private snapshotReadySorted(): DispatcherTaskRecord[] {
    const live = this.ready
      .map(id => this.tasks.get(id)!)
      .filter(t => t && t.status === 'pending')
    live.sort(compareTasksForOrder)
    return live
  }

  /** Insert a freshly-created pending task into the ready array in sorted order. */
  private insertIntoReady(task: DispatcherTaskRecord): void {
    this.ready.push(task.id)
    this.reorderReady()
  }

  /** Re-sort the ready array in place to match `compareTasksForOrder`. */
  private reorderReady(): void {
    if (this.ready.length < 2) return
    // Filter to pending-only; preserve only entries still in `tasks` and pending.
    const pendingIds: string[] = []
    for (const id of this.ready) {
      const t = this.tasks.get(id)
      if (t && t.status === 'pending') pendingIds.push(id)
    }
    pendingIds.sort((a, b) => {
      const ta = this.tasks.get(a)
      const tb = this.tasks.get(b)
      if (!ta || !tb) return 0
      return compareTasksForOrder(ta, tb)
    })
    // Replace ready contents atomically with the new sorted slice.
    this.ready.length = 0
    for (const id of pendingIds) this.ready.push(id)
  }

  /**
   * Pending cancel: physically remove the task from the ready queue,
   * never spawn Claude, never provision a worktree. Records terminal
   * status = cancelled with reason = explicit-pending.
   *
   * Cancelling a task that already started is a no-op here — the caller
   * routes the active cancel through `cancelActive` below.
   */
  cancelPending(taskId: string): { cancelled: boolean; terminalReason: DispatcherTaskRecord['terminalReason'] } {
    const t = this.tasks.get(taskId)
    if (!t) return { cancelled: false, terminalReason: undefined }
    if (this.terminal.has(taskId)) {
      // Idempotent: already terminal.
      return { cancelled: true, terminalReason: t.terminalReason }
    }
    if (t.status === 'pending') {
      const idx = this.ready.indexOf(taskId)
      if (idx >= 0) this.ready.splice(idx, 1)
      this.terminalTransition(t, 'cancelled', 'explicit-cancel')
      this.emit('cancel', { taskId, reason: 'explicit-pending' })
      return { cancelled: true, terminalReason: 'explicit-cancel' }
    }
    if (t.status === 'preparing') {
      // Cancel during delayed provision: the launcher boundary check in
      // startTask() will refuse to launch because terminal.has(t.id) is true.
      this.terminalTransition(t, 'cancelled', 'explicit-cancel')
      this.emit('cancel', { taskId, reason: 'explicit-pending' })
      return { cancelled: true, terminalReason: 'explicit-cancel' }
    }
    // Already in_progress or beyond — caller must use cancelActive().
    return { cancelled: false, terminalReason: t.terminalReason }
  }

  /**
   * Cancel an active task.
   *
   * Contract (defect #3 — fail-closed):
   *   - Install the cancellation guard BEFORE awaiting the cancel-callback,
   *     so any subsequent complete/fail/startTask is a guaranteed no-op
   *     for this task.
   *   - The in-flight slot stays leased until the async cleanup callback
   *     ACTUALLY SUCCEEDS. tryDispatchTick is NOT invoked until cleanup
   *     succeeds, so the next queued task cannot launch while the old
   *     Claude child might still be alive.
   *   - The terminal transition + dispatch tick run EXACTLY ONCE, even
   *     across concurrent cancelActive calls (idempotent).
   *   - If `cancelViaScheduler` rejects, the task stays in the
   *     `cancelling` guard, the in-flight slot stays leased, the next
   *     queued task cannot launch, and `cancelActive` rejects with the
   *     same error. reset/dispose can atomically clear that state.
   *   - No 8s safety timeout inside cancelActive: signalAndAwaitClose
   *     is responsible for its own bounded reject. We do NOT clear the
   *     guard on a wall-clock timeout.
   *   - Completed/failed/interrupted: no-op.
   *   - hydrating: cleanup defers a microtask but never auto-dispatches.
   *
   * Implementation note: we keep `t.status` at its current value
   * (in_progress / preparing) while the cancellation guard is active.
   * This makes `reservedSize` continue to count the slot, so the
   * dispatch tick refuses to promote the next queued task. The task is
   * NOT moved into `terminal` until cleanup completes successfully — at
   * which point we perform a normal terminalTransition and the slot is
   * released exactly once.
   */
  async cancelActive(taskId: string, cancelViaScheduler: () => Promise<void> | void): Promise<boolean> {
    const t = this.tasks.get(taskId)
    if (!t) return false
    // Already terminal — idempotent no-op.
    if (this.terminal.has(taskId)) return true
    if (t.status !== 'in_progress' && t.status !== 'preparing') return false

    // Concurrent re-entry: if a cancellation is already in flight,
    // DO NOT start a second cleanup chain. Await the SAME chain —
    // whether it succeeds or rejects. This is the fail-closed
    // idempotency contract: at most one chain runs, every caller
    // observes the same outcome.
    const existing = this.cancelCleanups.get(taskId)
    if (existing) {
      await existing
      // Reaching here means the chain resolved successfully. If the
      // chain rejected, the await would have thrown and we'd never
      // get here.
      return true
    }
    // If the cancelling guard is still set (e.g. a previous cleanup
    // rejected and the guard was kept), refuse to start a new cleanup
    // chain. The caller must dispose() to clear the guard before
    // retrying. This is fail-closed: a stuck cancellation cannot be
    // silently shadowed by a duplicate chain.
    if (this.cancelling.has(taskId)) {
      throw new Error(
        `cancelActive: task ${taskId} is in cancelling guard from a previous failed cleanup; call reset/dispose first`,
      )
    }

    // Install the cancellation guard BEFORE awaiting cancelViaScheduler().
    // From this point onward, complete/fail/startTask see the cancellation
    // guard and refuse to transition the task. The slot stays leased
    // because t.status stays at preparing/in_progress.
    this.cancelling.set(taskId, t)

    // Wire up the shared cleanup chain. The promise resolves OR rejects
    // exactly once regardless of concurrent callers — that's the
    // idempotency contract. The chain is exposed ONLY to concurrent
    // callers while it is in-flight; once it settles, we delete it.
    let resolveCleanup!: () => void
    let rejectCleanup!: (err: unknown) => void
    const cleanupPromise = new Promise<void>((res, rej) => {
      resolveCleanup = res
      rejectCleanup = rej
    })
    this.cancelCleanups.set(taskId, cleanupPromise)
    // Guard against unhandled rejection on the cleanupPromise itself.
    // The chain is observed by concurrent callers (await below) but
    // additionally we attach a no-op catch so the chain never produces
    // an unhandled rejection if no concurrent caller observes it.
    cleanupPromise.catch(() => { /* surfaced via async function throw */ })

    // Emit cancel BEFORE awaiting cleanup so subscribers can observe the
    // intent immediately. The task's status, terminalReason and finishedAt
    // are NOT yet updated — those are written when cleanup completes.
    this.emit('cancel', { taskId, reason: 'task-cancel' })

    // (1) Run the async cleanup. complete/fail/startTask for this task are
    //     no-ops during this window via the `cancelling` guard.
    //     No wall-clock timeout here: signalAndAwaitClose provides its
    //     own bounded reject. We do NOT release the slot on any kind of
    //     timeout. If cleanup never resolves, the task stays in the
    //     guard. The caller may dispose/reset.
    try {
      await Promise.resolve().then(() => cancelViaScheduler())
    } catch (err) {
      // Fail-closed: cleanup callback errored. Do NOT release the slot.
      // The task stays in the cancelling guard and the in-flight slot
      // stays leased. Reject the chain so concurrent callers awaiting
      // the same chain observe the same failure. The async function's
    //     return promise also rejects via `throw err`.
      // Note: we do NOT clear this.cancelling here. The guard remains
      // active until dispose() is called. complete/fail/startTask
      // continue to no-op for this task.
      // No tryDispatchTick: the slot is still leased.
      // Surface the error to the caller via throw.
      rejectCleanup(err)
      throw err
    }

    // (2) Cleanup callback resolved successfully. Authoritative order:
    //     a) terminalTransition(cancelled) — this is the single point
    //        that releases the in-flight slot via inFlight.delete;
    //     b) clear the cancelling guard + the cleanup Promise so a
    //        subsequent retry observes a fresh state;
    //     c) resolve the shared cleanup Promise — concurrent callers
    //        awaiting the same chain unblock ONLY AFTER terminal
    //        transition and slot release have happened;
    //     d) schedule the next dispatch tick.
    //
    //     Defect #4 fix: we deliberately resolve the shared cleanup
    //     Promise LAST. A second cancelActive caller that awaits the
    //     shared chain must observe the task as cancelled and the
    //     slot already released — never a window where concurrent
    //     calls return true while the task is still in_progress.
    if (!this.terminal.has(taskId) && !isTerminalStatus(t.status)) {
      this.terminalTransition(t, 'cancelled', 'task-cancelled')
    }
    this.cancelling.delete(taskId)
    this.cancelCleanups.delete(taskId)
    resolveCleanup()

    // (3) Schedule the next dispatch tick AFTER cleanup settled. Only
    //     promote if the dispatcher is not hydrating — during a hydrate
    //     pass dispatch is only triggered by dispatchAfterHydrate().
    if (!this.hydrating) {
      queueMicrotask(() => this.tryDispatchTick())
    }
    return true
  }

  /** Called by the engine after an in-flight task finishes (success / fail). */
  complete(taskId: string, result: string): void {
    const t = this.tasks.get(taskId)
    if (!t) return
    if (this.terminal.has(taskId)) return // idempotent
    if (t.status !== 'in_progress' && t.status !== 'preparing') return // not dispatcher-owned
    // Cancellation guard: never overwrite a cancelling task.
    if (this.cancelling.has(taskId)) return
    this.terminalTransition(t, 'completed', 'completed')
    this.emit('complete', { taskId, result })
    if (!this.hydrating) {
      queueMicrotask(() => this.tryDispatchTick())
    }
  }

  fail(taskId: string, error: string): void {
    const t = this.tasks.get(taskId)
    if (!t) return
    if (this.terminal.has(taskId)) return
    if (t.status !== 'in_progress' && t.status !== 'preparing') return
    // Cancellation guard: never overwrite a cancelling task.
    if (this.cancelling.has(taskId)) return
    this.terminalTransition(t, 'failed', 'failed')
    this.emit('fail', { taskId, error })
    if (!this.hydrating) {
      queueMicrotask(() => this.tryDispatchTick())
    }
  }

  /**
   * Forget a terminal record. Returns true on success. Refuses to forget
   * anything that is still in flight or pending (operator safety).
   * Worktrees/branches are NEVER auto-removed by the dispatcher.
   */
  forgetTerminal(taskId: string): boolean {
    const t = this.tasks.get(taskId)
    if (!t) return false
    if (!this.terminal.has(taskId)) return false
    this.terminal.delete(taskId)
    this.tasks.delete(taskId)
    return true
  }

  /**
   * Side-effect-free hydrate pass.
   *
   * Restores the dispatcher from a persisted snapshot WITHOUT triggering
   * any launches. After all records are restored, the caller invokes
   * `dispatchAfterHydrate()` exactly once to begin dispatching.
   *
   * Rules:
   *   - terminal records → restored into `terminal`, never re-launched.
   *   - in_progress with a live launcher → restored into inFlight as-is.
   *   - in_progress without a live launcher → marked `interrupted` (fail-closed).
   *   - pending → restored into the ready queue exactly once.
   *   - any other status is restored as-is and never launched.
   */
  hydrateFromSnapshot(snapshot: Array<Partial<DispatcherTaskRecord> & { id: string }>): {
    restored: string[]
    interrupted: string[]
  } {
    const restored: string[] = []
    const interrupted: string[] = []
    // Hydrate starts from a clean state — any leftover cancellation
    // guards from a prior instance must NOT leak into the restored
    // dispatcher's view.
    this.cancelling.clear()
    this.cancelCleanups.clear()
    this.hydrating = true
    try {
      for (const raw of snapshot) {
        if (!raw || !raw.id) continue
        const id = String(raw.id)
        if (this.tasks.has(id)) continue // idempotent restore

        const mode: TaskMode = raw.mode === 'READ-ONLY' ? 'READ-ONLY' : 'WRITE'
        const priority: Priority =
          raw.priority === 'critical' || raw.priority === 'high' || raw.priority === 'low'
            ? raw.priority
            : 'normal'
        const status: DispatcherStatus =
          raw.status === 'pending' || raw.status === 'in_progress' ||
          raw.status === 'completed' || raw.status === 'failed' ||
          raw.status === 'cancelled' || raw.status === 'interrupted' ||
          raw.status === 'preparing'
            ? raw.status
            : 'pending'

        const task: DispatcherTaskRecord = {
          id,
          title: String(raw.title || '').slice(0, 1024),
          description: String(raw.description || '').slice(0, 32_000),
          mode,
          priority,
          status,
          sourceCwd: String(raw.sourceCwd || ''),
          executionCwd: raw.executionCwd,
          worktree: raw.worktree,
          createdAt: String(raw.createdAt || new Date().toISOString()),
          startedAt: raw.startedAt,
          finishedAt: raw.finishedAt,
          terminalReason: raw.terminalReason,
          attempt: typeof raw.attempt === 'number' ? raw.attempt : 0,
          running: raw.running,
          assignedTo: raw.assignedTo,
        }
        this.tasks.set(id, task)

        if (isTerminalStatus(status)) {
          // Terminal records are NEVER re-launched.
          this.terminal.set(id, task)
          continue
        }
        if (status === 'in_progress') {
          // No live process survives a restart — fail-closed.
          this.terminalTransition(task, 'interrupted', 'interrupted')
          this.emit('fail', { taskId: id, error: 'interrupted: no live process after restart' })
          interrupted.push(id)
          continue
        }
        if (status === 'pending' || status === 'preparing') {
          // Both restore into the ready queue exactly once. We normalise
          // 'preparing' back to 'pending' — provisioning is restartable.
          if (status !== 'pending') {
            task.status = 'pending'
          }
          this.insertIntoReady(task)
          restored.push(id)
        }
      }
      this.reorderReady()
    } finally {
      this.hydrating = false
    }
    return { restored, interrupted }
  }

  /** Promote pending tasks after a hydrate. Single explicit entry point. */
  dispatchAfterHydrate(): void {
    queueMicrotask(() => this.tryDispatchTick())
  }

  // ── Dispatch loop ──────────────────────────────────────────────

  /**
   * Single explicit terminal transition helper. All success/fail/cancel
   * paths funnel through here so a second terminal transition is a
   * guaranteed no-op (blocker 9).
   *
   * opts.keepSlot: when true, do NOT remove the task from inFlight —
   * the slot stays leased. Used by cancelActive so the dispatcher's
   * capacity remains honest while the async cleanup callback runs.
   */
  private terminalTransition(
    t: DispatcherTaskRecord,
    status: 'completed' | 'failed' | 'cancelled' | 'interrupted',
    reason: DispatcherTaskRecord['terminalReason'],
    opts: { keepSlot?: boolean } = {},
  ): void {
    if (this.terminal.has(t.id)) return
    if (isTerminalStatus(t.status)) return // already terminal in-place
    if (!opts.keepSlot && (t.status === 'in_progress' || t.status === 'preparing')) {
      this.inFlight.delete(t.id)
    }
    if (t.status === 'pending') {
      const idx = this.ready.indexOf(t.id)
      if (idx >= 0) this.ready.splice(idx, 1)
    }
    this.terminal.set(t.id, t)
    t.status = status
    t.finishedAt = new Date().toISOString()
    t.terminalReason = reason
    t.running = false
    this.emit('statusChange', { taskId: t.id, status })
  }

  private setStatus(t: DispatcherTaskRecord, status: DispatcherStatus): void {
    if (t.status !== status) {
      t.status = status
      this.emit('statusChange', { taskId: t.id, status })
    }
  }

  private async tryDispatchTick(): Promise<void> {
    if (this.dispatching) return
    if (this.hydrating) return
    this.dispatching = true
    try {
      // Re-sort the ready array so queuePosition reflects the live
      // priority+FIFO ordering.
      this.reorderReady()
      while (
        this.reservedSize < this.maxInFlight &&
        this.ready.length > 0
      ) {
        const head = this.ready.shift()!
        const t = this.tasks.get(head)
        if (!t) continue
        if (t.status !== 'pending') continue // already terminal during microtask
        // Reserve a slot BEFORE async work begins so capacity is honest
        // under concurrency (preparing counts towards the cap).
        t.attempt += 1
        this.inFlight.set(t.id, t)
        this.setStatus(t, 'preparing')
        delete t.startedAt // will be set right before launcher
        this.emit('preparing', { taskId: t.id })
        this.emit('dispatch', { taskId: t.id })
        // Fire-and-forget: actual launch is async; errors fall through
        // to fail()/complete(). We never await here because dispatching
        // multiple tasks concurrently is the whole point of the cap.
        void this.startTask(t)
      }
    } finally {
      this.dispatching = false
    }
  }

  /**
   * Walk the full provisioning → launch sequence. Cancel-aware: we
   * re-check the terminal state after every await, and immediately
   * before invoking the launcher. A cancelled task NEVER launches.
   */
  private async startTask(t: DispatcherTaskRecord): Promise<void> {
    // Cancellation guard at the very top — if cancelActive is in flight,
    // do not even start provisioning.
    if (this.cancelling.has(t.id)) return
    if (this.terminal.has(t.id)) return
    try {
      let executionCwd: string
      if (t.mode === 'WRITE') {
        let wt: WorktreeInfo
        try {
          wt = await this.worktrees.provision(t.id, t.sourceCwd)
        } catch (err) {
          // Fail-closed: do not spawn Claude without a worktree.
          const msg = err instanceof WorktreeError ? `${err.code}: ${err.message}` : String((err as Error).message ?? err)
          // The task may have been cancelled during provisioning. If so,
          // keep the cancelled state (don't overwrite it with failed).
          if (this.terminal.has(t.id)) return
          this.emit('worktreeFailed', { taskId: t.id, error: msg })
          this.fail(t.id, `worktree provisioning failed: ${msg}`)
          return
        }
        // Cancel-while-provisioning guard.
        if (this.terminal.has(t.id)) {
          // Worktree is already on disk; keep its metadata for operator
          // cleanup but do not launch Claude or change the cancelled state.
          t.worktree = wt
          t.executionCwd = wt.worktreePath
          return
        }
        t.worktree = wt
        t.executionCwd = wt.worktreePath
        executionCwd = wt.worktreePath
        this.emit('worktreeProvisioned', {
          taskId: t.id,
          worktreePath: wt.worktreePath,
          branchName: wt.branchName,
        })
      } else {
        // READ-ONLY: pass the source cwd straight to Claude.
        executionCwd = t.sourceCwd
        t.executionCwd = executionCwd
      }
      // Boundary check — terminal may have been set while READ-ONLY set
      // executionCwd. Refuse to launch a terminal task.
      if (this.terminal.has(t.id)) return
      if (!this.launcher) {
        // No launcher wired up — keep task in preparing and let the
        // caller complete/fail it explicitly. This branch is mostly for
        // tests.
        return
      }
      // Final pre-launch transition: pending → in_progress with startedAt.
      // This is the only place we set in_progress (blocker 1).
      // IMPORTANT (pre-launch event order): authoritative execution metadata
      // (startedAt, running) MUST be set BEFORE the statusChange emit.
      // Listeners observing `in_progress` must see a fully-populated record
      // — no race where the listener inspects the task while startedAt is
      // still undefined or running is still false.
      this.inFlight.set(t.id, t)
      t.startedAt = new Date().toISOString()
      t.running = true
      this.setStatus(t, 'in_progress')
      // One last guard right before the launcher fires.
      if (this.terminal.has(t.id)) return
      // Cancellation guard: never launch a task whose cancel is in flight.
      if (this.cancelling.has(t.id)) return
      await this.launcher(t)
      // Note: launcher is responsible for calling complete() / fail().
    } catch (err) {
      if (this.terminal.has(t.id)) return
      const msg = err instanceof Error ? err.message : String(err)
      this.fail(t.id, `launch error: ${msg}`)
    }
  }

  /**
   * Reset all cancellation-related transient state. Used by hydrate-from-
   * snapshot AND by resetTaskDispatcher. After this call:
   *   - cancelling is empty;
   *   - cancelCleanups has no dangling promises;
   *   - inFlight and terminal still reflect the dispatcher's prior
   *     state, no automatic transitions are run.
   *
   * In-flight cancelViaScheduler Promises are intentionally NOT awaited
   * here — their settlement is the caller's responsibility (typically the
   * caller chooses to await them explicitly via cancelActive's return
   * value before invoking dispose).
   */
  dispose(): void {
    this.cancelling.clear()
    // Cancel promises are intentionally not rejected: callers may be
    // awaiting them. Leaving them dangling lets those callers resume and
    // see the cleared dispatcher state.
    this.cancelCleanups.clear()
  }
}

// ── Module-level singleton + helpers ─────────────────────────────

let _default: TaskDispatcher | null = null
export function getTaskDispatcher(): TaskDispatcher {
  if (!_default) _default = new TaskDispatcher()
  return _default
}
export function resetTaskDispatcher(): void {
  if (_default) {
    _default.removeAllListeners()
    _default.dispose()
  }
  _default = null
}

/** Test / dispatcher-only helper. */
export function setLauncher(fn: TaskLauncher | null): void {
  getTaskDispatcher().setLauncher(fn)
}

/**
 * Detect MODE from a task title+description using the same regex family
 * the pipeline already trusts. This is the one and only place we parse
 * MODE — server.ts calls into here for both new tasks and continuation.
 */
export function detectTaskMode(input: { packetId?: string; title: string; description: string }): TaskMode {
  const haystack = `${input.packetId || ''}\n${input.title}\n${input.description}`.toLowerCase()
  // Tightest signal first: explicit MODE header in a packet header.
  if (/mode\s*[:=]\s*read[- ]?only\b/.test(haystack)) return 'READ-ONLY'
  if (/mode\s*[:=]\s*(write|edit|modify|impl)\b/.test(haystack)) return 'WRITE'
  // Common slash-command / header forms.
  if (/^mode:\s*read[- ]?only/mi.test(`${input.packetId || ''}\n${input.title}\n${input.description}`)) return 'READ-ONLY'
  if (/^mode:\s*write/mi.test(`${input.packetId || ''}\n${input.title}\n${input.description}`)) return 'WRITE'
  // Body heuristics: explicit READ-ONLY tag near the top wins.
  const head = `${input.title}\n${input.description}`.slice(0, 512).toLowerCase()
  if (/\bread[- ]?only[: -]/i.test(head) || /\b(?:audit|review|analyze|inspect|examine|grep|find)\b/.test(head) && !/\b(implement|refactor|write|edit|fix|add|create)\b/.test(head)) {
    // The "analysis only" heuristic is intentionally conservative — only
    // when no WRITE verb is present in the head do we call it READ-ONLY.
    if (!/\b(implement|refactor|write|edit|fix|add|create)\b/.test(head)) return 'READ-ONLY'
  }
  // Default to WRITE so the worktree safety net applies for ambiguous cases.
  return 'WRITE'
}

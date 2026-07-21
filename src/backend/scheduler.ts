import { AGENT_PROFILES } from './agent-profiles'

export type Priority = 'critical' | 'high' | 'normal' | 'low'

export const PRIORITY_RANK: Record<Priority, number> = {
  critical: 0,
  high: 1,
  normal: 2,
  low: 3,
}

export type SubtaskStatus = 'pending' | 'active' | 'completed' | 'failed' | 'cancelled'

export interface SubtaskRequest {
  id: string
  taskId: string
  capability: string
  description: string
  priority?: Priority
  dependsOn?: string[]
  profileId?: string
  isFinalReviewer?: boolean
  systemPrompt?: string
}

export interface QueuedSubtask extends SubtaskRequest {
  priority: Priority
  dependsOn: string[]
  enqueuedAt: number
  status: SubtaskStatus
  /**
   * Typed reason for the latest terminal transition. Set when the subtask
   * reaches `cancelled` or `failed` so observers / awaitDispatchers can
   * distinguish prerequisite-failed from prerequisite-cancelled from
   * explicit cancel from task-cancel — even after the entry is removed
   * from `pending`.
   */
  terminalReason?:
    | 'completed'
    | 'failed'
    | 'explicit-cancel'
    | 'task-cancelled'
    | 'dependency-failed'
    | 'dependency-cancelled'
    | 'dispatch-timeout'
    | 'scheduled-shutdown'
}

export interface AgentLease {
  profileId: string
  agentId: string
  subtaskId: string
  taskId: string
  startedAt: number
}

export type ReleaseReason = 'close' | 'error' | 'cancel' | 'timeout' | 'shutdown'

export interface SchedulerEvents {
  release: { lease: AgentLease; reason: ReleaseReason }
  error: { lease: AgentLease; error: Error }
  schedule: { subtaskId: string; agentId: string; profileId: string }
  enqueue: { subtaskId: string }
  complete: { subtaskId: string; result: string }
  cancel: { subtaskId: string; reason: 'dependency-failed' | 'dependency-cancelled' | 'explicit' | 'task-cancelled' }
  fail: { subtaskId: string; error: Error }
}

type Listener<E extends keyof SchedulerEvents> = (payload: SchedulerEvents[E]) => void

/**
 * Global work-conserving scheduler for the RuFloUI multi-agent pipeline.
 *
 * Invariants the scheduler enforces:
 *  - **Cancellation is physical**: cancel/cancelTask physically removes the
 *    subtask entry from `pending`; awaitDispatch surfaces a typed reason
 *    (prerequisite failed, prerequisite cancelled, explicit cancel, task
 *    cancelled) using the separately-stored `terminalReason`.
 *  - **Cancellation is idempotent**: cancelling an already-completed/failed
 *    subtask is a no-op and never alters the recorded terminal state.
 *  - **awaitDispatch timeout is fatal**: a timeout removes the queued
 *    subtask from `pending` and rejects with a typed `dispatch-timeout`
 *    error — the subtask NEVER runs later, even if a slot frees up.
 *  - **Tear-down is atomic**: reset/dispose/releaseAll close every active
 *    lease, evict every registered agent, and reject every pending
 *    awaitDispatch with a `scheduled-shutdown` error without dispatching
 *    further work.
 *  - **launchViaClaude safety**: lease holders MUST be granted before
 *    spawning a child process. Successful dispatch → spawn. Rejection →
 *    no spawn. Cancelled while queued → no spawn.
 *  - **fail-closed worker propagation**: if any required worker failed or
 *    was cancelled, the parent must surface `failed` or `cancelled` —
 *    never `completed`.
 */
export class GlobalScheduler {
  private readonly globalMax: number
  private readonly defaultDispatchTimeoutMs: number
  private readonly pending = new Map<string, QueuedSubtask>()
  private readonly completed = new Set<string>()
  /** Active Claude processes keyed by lease id (subtaskId → lease) */
  private readonly activeLeases = new Map<string, AgentLease>()
  /** Active Claude processes keyed by profileId (for per-agent exclusivity) */
  private readonly profileLocks = new Map<string, AgentLease>()
  private readonly agents = new Map<string, { profileId: string }>()
  /** Subtasks that have reached a terminal state — used for dep resolution. */
  private readonly failed = new Set<string>()
  private readonly cancelled = new Set<string>()
  /**
   * Typed terminal reason persisted PER subtask so observers / awaiters
   * can distinguish prerequisite-failed / cancelled / explicit / task /
   * dispatch-timeout / shutdown even after the entry leaves `pending`.
   * The value is the terminal reason OR (for dep-* reasons) the underlying
   * dep id suffix-encoded alongside the reason — use `parseTerminalReason`
   * to unpack.
   */
  private readonly terminalReasons = new Map<string, NonNullable<QueuedSubtask['terminalReason']>>()
  /** For dep-cancel reason, store the dep id that triggered the cascade. */
  private readonly depTriggerMap = new Map<string, string>()
  /** Outstanding awaitDispatch timers to allow forced cleanup. */
  private readonly pendingAwaitDispatchers = new Map<string, Set<{ settle: () => void }>>()

  /** Tear-down flag — gates new dispatches while teardown is in progress. */
  private tearingDown = false

  private readonly listeners: { [K in keyof SchedulerEvents]: Set<Listener<K>> } = {
    release: new Set(),
    error: new Set(),
    schedule: new Set(),
    enqueue: new Set(),
    complete: new Set(),
    cancel: new Set(),
    fail: new Set(),
  }

  constructor(opts: { globalMaxConcurrent?: number; defaultDispatchTimeoutMs?: number } = {}) {
    const env = Number(process.env.RUFLO_GLOBAL_MAX_CONCURRENT)
    const fallback = Number.isFinite(env) && env > 0 ? env : 10
    this.globalMax = opts.globalMaxConcurrent ?? fallback
    // Safe long default so normal Opus workloads never expire under load.
    // Override with opts.defaultDispatchTimeoutMs or RUFLO_DISPATCH_TIMEOUT_MS env.
    const envTimeout = Number(process.env.RUFLO_DISPATCH_TIMEOUT_MS)
    this.defaultDispatchTimeoutMs = opts.defaultDispatchTimeoutMs
      ?? (Number.isFinite(envTimeout) && envTimeout > 0 ? envTimeout : 30 * 60_000)
  }

  // ── Agent registry ───────────────────────────────────────────────

  registerAgents(agents: Array<{ profileId: string; agentId: string }>): void {
    if (this.tearingDown) return
    for (const a of agents) this.agents.set(a.agentId, { profileId: a.profileId })
  }

  unregisterAgent(agentId: string): void {
    const profile = this.agents.get(agentId)?.profileId
    if (profile) {
      const lock = this.profileLocks.get(profile)
      if (lock && lock.agentId === agentId) {
        // Mark the subtask failed and release the lease exactly once.
        if (this.activeLeases.has(lock.subtaskId)) {
          this.activeLeases.delete(lock.subtaskId)
          this.profileLocks.delete(profile)
          this.pending.delete(lock.subtaskId)
          if (!this.failed.has(lock.subtaskId) && !this.completed.has(lock.subtaskId)) {
            this.failed.add(lock.subtaskId)
            this.terminalReasons.set(lock.subtaskId, 'failed')
            this.emit('release', { lease: lock, reason: 'error' })
            this.emit('fail', { subtaskId: lock.subtaskId, error: new Error('Agent unregistered') })
          }
        }
      }
    }
    this.agents.delete(agentId)
  }

  /** Unregister every agent for a profile (used on swarm re-init / purge). */
  unregisterProfile(profileId: string): void {
    for (const [agentId, meta] of [...this.agents.entries()]) {
      if (meta.profileId === profileId) this.unregisterAgent(agentId)
    }
  }

  hasCompatibleAgent(subtask: QueuedSubtask): boolean {
    for (const { profileId } of this.agents.values()) {
      if (this.isProfileMatch(profileId, subtask)) return true
    }
    return false
  }

  // ── Lifecycle ─────────────────────────────────────────────────────

  enqueue(subtask: SubtaskRequest): QueuedSubtask {
    if (this.tearingDown) {
      // Treat enqueue during teardown as an explicit cancel recorded for
      // dependents that may still be observing the subtask.
      this.terminalReasons.set(subtask.id, 'scheduled-shutdown')
      const rejected: QueuedSubtask = {
        ...subtask,
        priority: subtask.priority ?? 'normal',
        dependsOn: subtask.dependsOn ?? [],
        enqueuedAt: Date.now(),
        status: 'cancelled',
        terminalReason: 'scheduled-shutdown',
      }
      this.cancelled.add(subtask.id)
      return rejected
    }
    const merged: QueuedSubtask = {
      ...subtask,
      priority: subtask.priority ?? 'normal',
      dependsOn: subtask.dependsOn ?? [],
      enqueuedAt: Date.now(),
      status: 'pending',
    }
    this.pending.set(merged.id, merged)
    this.emit('enqueue', { subtaskId: merged.id })
    this.maybeDispatch()
    return merged
  }

  /** Mark a subtask complete; frees its slot/lease. */
  complete(subtaskId: string, result: string): void {
    if (this.completed.has(subtaskId)) return
    // Idempotent: never overwrite an already-cancelled terminal state.
    if (this.cancelled.has(subtaskId) || this.failed.has(subtaskId)) return
    this.completed.add(subtaskId)
    this.terminalReasons.set(subtaskId, 'completed')
    const sub = this.pending.get(subtaskId)
    if (sub) sub.status = 'completed'
    this.emit('complete', { subtaskId, result })
    // Release any lease tied to this subtask (close path).
    this.release(subtaskId, 'close')
    // Cancel any dependents that were waiting on this and check downstream.
    this.cancelDependentsOf(subtaskId, 'completed')
    if (!this.tearingDown) this.maybeDispatch()
  }

  /** Report an error and release the slot/lease for that subtask. */
  fail(subtaskId: string, error: Error): void {
    if (this.failed.has(subtaskId) || this.completed.has(subtaskId)) return
    // Idempotent: never overwrite an already-cancelled terminal state.
    if (this.cancelled.has(subtaskId)) return
    const lease = this.activeLeases.get(subtaskId)
    this.failed.add(subtaskId)
    this.terminalReasons.set(subtaskId, 'failed')
    const sub = this.pending.get(subtaskId)
    if (sub) sub.status = 'failed'
    this.release(subtaskId, 'error', error)
    if (lease) this.emit('error', { lease, error })
    this.emit('fail', { subtaskId, error })
    // Cancel dependents: they cannot run because their prereq failed.
    this.cancelDependentsOf(subtaskId, 'dependency-failed', subtaskId)
    if (!this.tearingDown) this.maybeDispatch()
  }

  /**
   * Idempotent release — called from close/error/cancel/timeout/shutdown.
   * Subsequent calls with the same lease are no-ops (no double-release
   * events emitted, no double-dispatch).
   */
  release(subtaskId: string, _reason: ReleaseReason, _error?: Error): void {
    const lease = this.activeLeases.get(subtaskId)
    if (!lease) return
    this.activeLeases.delete(subtaskId)
    // Only delete the profile lock if THIS lease held it.
    const currentLock = this.profileLocks.get(lease.profileId)
    if (currentLock && currentLock.subtaskId === subtaskId) {
      this.profileLocks.delete(lease.profileId)
      this.emit('release', { lease, reason: _reason })
      if (!this.tearingDown) this.maybeDispatch()
    }
    this.pending.delete(subtaskId)
  }

  /**
   * Release every active lease. Used during shutdown / teardown so no
   * new work is dispatched. Caller is responsible for draining pending
   * awaitDispatchers via dispose() / reset.
   */
  releaseAll(reason: ReleaseReason = 'close'): void {
    this.tearingDown = true
    const ids = [...this.activeLeases.keys()]
    for (const id of ids) this.release(id, reason)
  }

  /** Cancel an active subtask; releases slot, never re-runs later. */
  cancel(subtaskId: string): void {
    if (this.completed.has(subtaskId) || this.failed.has(subtaskId)) return
    this.cancelOne(subtaskId, 'explicit')
  }

  /** Cancel every subtask belonging to a task (task-cancel propagation). */
  cancelTask(taskId: string): void {
    // Loop until no new active subtasks of this task appear. Cancelling
    // one active lease may dispatch a queued subtask of the same task
    // (when the cap opens up); we must keep cancelling until stable.
    let progressed = true
    while (progressed) {
      progressed = false
      // Cancel active subtasks for this task
      for (const [id, lease] of [...this.activeLeases.entries()]) {
        if (lease.taskId === taskId) {
          this.cancelOne(id, 'task-cancelled')
          progressed = true
        }
      }
      // Cancel any pending subtasks for this task (including active-status
      // entries that haven't been terminalised yet).
      for (const [id, sub] of [...this.pending.entries()]) {
        if (sub.taskId === taskId && (sub.status === 'pending' || sub.status === 'active')) {
          if (!this.cancelled.has(id) && !this.completed.has(id) && !this.failed.has(id)) {
            this.cancelOne(id, 'task-cancelled')
            progressed = true
          }
        }
      }
    }
  }

  /**
   * Physically cancel a single subtask:
   *  - records typed terminal reason,
   *  - removes entry from `pending`,
   *  - cancels queued dependents with the matching typed reason,
   *  - settles any outstanding awaitDispatch immediately with typed reason.
   *
   * Idempotent: cancelling an already terminal subtask is a no-op and
   * never mutates the recorded terminal state (no mixed-state copy).
   *
   * `triggeringDep` (optional) is the dependency id that initiated the
   * cascade — used by awaitDispatch to surface a typed "prerequisite X
   * failed/cancelled" error.
   */
  private cancelOne(
    subtaskId: string,
    reason: 'dependency-failed' | 'dependency-cancelled' | 'explicit' | 'task-cancelled' | 'dispatch-timeout' | 'scheduled-shutdown',
    triggeringDep?: string,
  ): void {
    if (this.completed.has(subtaskId) || this.failed.has(subtaskId)) {
      // Idempotent: don't record cancel over a completed/failed terminal state.
      return
    }
    const wasPending = this.pending.has(subtaskId)
    const sub = this.pending.get(subtaskId)
    this.pending.delete(subtaskId)
    this.cancelled.add(subtaskId)
    this.terminalReasons.set(subtaskId, mapCancelReasonToTerminal(reason))
    if (triggeringDep) this.depTriggerMap.set(subtaskId, triggeringDep)
    if (sub) sub.status = 'cancelled'
    // Release any active lease for this subtask (idempotent if none).
    this.release(subtaskId, reason === 'dispatch-timeout' || reason === 'scheduled-shutdown' ? 'timeout' : 'cancel')
    if (wasPending || sub) {
      this.emit('cancel', { subtaskId, reason: mapCancelEmitReason(reason) })
      // Settle any awaitDispatchers that were waiting on this subtask.
      this.settleAwaiters(subtaskId, rejectMessageForCancelReason(subtaskId, mapCancelReasonToTerminal(reason)))
    }
    // Cancel queued dependents recursively with the right typed reason.
    const depReason =
      reason === 'dependency-failed' ? 'dependency-failed'
      : 'dependency-cancelled'
    // For dependent cascades, the triggering dep is the subtask being cancelled
    // (i.e. THIS subtask is the dep that just terminated). For the inner
    // `cancelOne(subtaskId, 'explicit')` path on direct user cancel, fall back
    // to subtaskId itself so dependents know which dep failed/cancelled.
    this.cancelDependentsOf(subtaskId, depReason, triggeringDep ?? subtaskId)
    if (!this.tearingDown) this.maybeDispatch()
  }

  private cancelDependentsOf(
    prereqId: string,
    reason: 'dependency-failed' | 'dependency-cancelled' | 'completed' | 'task-cancelled' | 'explicit' | 'dispatch-timeout' | 'scheduled-shutdown',
    triggeringDep?: string,
  ): void {
    // A successful completion of a prereq is NOT a cancellation signal.
    if (reason === 'completed') return
    const dependentIds: string[] = []
    for (const [id, sub] of this.pending.entries()) {
      if (sub.dependsOn.includes(prereqId) && !this.completed.has(id) && !this.failed.has(id)) {
        dependentIds.push(id)
      }
    }
    for (const id of dependentIds) {
      this.cancelOne(id, reason, triggeringDep)
    }
  }

  // ── Status / introspection ──────────────────────────────────────

  get activeCount(): number {
    return this.activeLeases.size
  }

  get globalMaxConcurrent(): number {
    return this.globalMax
  }

  get dispatchTimeoutMs(): number {
    return this.defaultDispatchTimeoutMs
  }

  getProfileLock(profileId: string): AgentLease | undefined {
    return this.profileLocks.get(profileId)
  }

  isSubtaskActive(subtaskId: string): boolean {
    return this.activeLeases.has(subtaskId)
  }

  isSubtaskCancelled(subtaskId: string): boolean {
    return this.cancelled.has(subtaskId)
  }

  isSubtaskFailed(subtaskId: string): boolean {
    return this.failed.has(subtaskId)
  }

  isSubtaskCompleted(subtaskId: string): boolean {
    return this.completed.has(subtaskId)
  }

  isReadyToRun(subtaskId: string): boolean {
    const subtask = this.pending.get(subtaskId)
    if (!subtask) return false
    if (this.completed.has(subtaskId)) return false
    if (this.failed.has(subtaskId) || this.cancelled.has(subtaskId)) return false
    return subtask.dependsOn.every(d => this.completed.has(d))
  }

  /** Pending count — only subtasks in `pending` status (excludes active/terminal). */
  get pendingSize(): number {
    let n = 0
    for (const sub of this.pending.values()) {
      if (sub.status === 'pending') n++
    }
    return n
  }

  /** Direct peek into the pending map (mostly for tests/diagnostics). */
  peekPending(subtaskId: string): QueuedSubtask | undefined {
    return this.pending.get(subtaskId)
  }

  /** Return the recorded typed terminal reason, if any. */
  getTerminalReason(subtaskId: string): NonNullable<QueuedSubtask['terminalReason']> | undefined {
    return this.terminalReasons.get(subtaskId)
  }

  /** True iff the scheduler is in the middle of an atomic teardown. */
  get isTearingDown(): boolean {
    return this.tearingDown
  }

  /**
   * Awaitable dispatch wait — resolves once the subtask is active
   * (running on an agent) or rejected if its dependencies cannot resolve.
   * Includes a timeout (configurable, default 30 min) so callers never
   * spin forever.
   *
   * Always cleans up every listener and timer registered for this await
   * on every resolution path (success, error, cancel, timeout).
   *
   * On timeout the queued subtask is REMOVED from `pending` and rejected
   * with a typed dispatch-timeout error — it can never be dispatched
   * later even if a slot frees up.
   */
  awaitDispatch(subtaskId: string, timeoutMs?: number): Promise<{ agentId: string; profileId: string }> {
    const effectiveTimeout = Math.max(1, timeoutMs ?? this.defaultDispatchTimeoutMs)
    return new Promise((resolve, reject) => {
      // Fast-reject already-terminal subtasks with the correct typed reason.
      const terminalFast = this.fastTerminalRejection(subtaskId)
      if (terminalFast) {
        reject(terminalFast)
        return
      }
      if (this.activeLeases.has(subtaskId)) {
        const lease = this.activeLeases.get(subtaskId)!
        resolve({ agentId: lease.agentId, profileId: lease.profileId })
        return
      }
      if (!this.pending.has(subtaskId)) {
        reject(new Error(`Subtask ${subtaskId} not enqueued`))
        return
      }

      const listenerCleanup: Array<() => void> = []
      let timer: ReturnType<typeof setTimeout> | undefined = undefined
      let settled = false
      let waiter: { settle: () => void } | null = null
      const cleanup = () => {
        if (settled) return
        settled = true
        if (timer) { clearTimeout(timer); timer = undefined }
        for (const off of listenerCleanup) try { off() } catch { /* ignore */ }
        if (waiter) {
          this.pendingAwaitDispatchers.get(subtaskId)?.delete(waiter)
          if (this.pendingAwaitDispatchers.get(subtaskId)?.size === 0) {
            this.pendingAwaitDispatchers.delete(subtaskId)
          }
        }
      }

      waiter = {
        settle: () => {
          // External invoker (cancel/timeout/shutdown) is settling us.
          cleanup()
        },
      }
      const waiters = this.pendingAwaitDispatchers.get(subtaskId)
      if (waiters) waiters.add(waiter)
      else this.pendingAwaitDispatchers.set(subtaskId, new Set([waiter]))

      // Up-front dependency check (so we don't wait full timeout on a doomed subtask).
      const sub = this.pending.get(subtaskId)
      if (sub) {
        for (const d of sub.dependsOn) {
          if (this.failed.has(d)) {
            cleanup()
            reject(new Error(`Subtask ${subtaskId} cannot run: prerequisite ${d} failed`))
            return
          }
          if (this.cancelled.has(d)) {
            cleanup()
            reject(new Error(`Subtask ${subtaskId} cannot run: prerequisite ${d} cancelled`))
            return
          }
        }
      }
      // Re-check after listener registration: a dep may have transitioned
      // to failed/cancelled between our pre-check and the listener install.
      const subAfter = this.pending.get(subtaskId)
      if (subAfter) {
        for (const d of subAfter.dependsOn) {
          if (this.failed.has(d)) {
            cleanup()
            reject(new Error(`Subtask ${subtaskId} cannot run: prerequisite ${d} failed`))
            return
          }
          if (this.cancelled.has(d)) {
            cleanup()
            reject(new Error(`Subtask ${subtaskId} cannot run: prerequisite ${d} cancelled`))
            return
          }
        }
      }

      timer = setTimeout(() => {
        if (settled) return
        // Mark settled FIRST so synchronous emit('cancel') from cancelOne
        // is ignored by the listener — the timeout reason wins.
        settled = true
        clearTimeout(timer); timer = undefined
        for (const off of listenerCleanup) try { off() } catch { /* ignore */ }
        // Physically remove the queued subtask — it can NEVER run later.
        // cancelOne is idempotent for already-completed/failed subtasks.
        this.cancelOne(subtaskId, 'dispatch-timeout')
        if (waiter) {
          this.pendingAwaitDispatchers.get(subtaskId)?.delete(waiter)
          if (this.pendingAwaitDispatchers.get(subtaskId)?.size === 0) {
            this.pendingAwaitDispatchers.delete(subtaskId)
          }
          waiter = null
        }
        reject(new Error(`Scheduler dispatch timeout for ${subtaskId}`))
      }, effectiveTimeout)

      const unsubSchedule = this.on('schedule', payload => {
        if (settled) return
        if (payload.subtaskId === subtaskId) {
          cleanup()
          resolve({ agentId: payload.agentId, profileId: payload.profileId })
        }
      })
      const unsubError = this.on('error', payload => {
        if (settled) return
        if (payload.lease.subtaskId === subtaskId) {
          cleanup()
          reject(payload.error)
        }
      })
      const unsubCancel = this.on('cancel', payload => {
        if (settled) return
        if (payload.subtaskId === subtaskId) {
          cleanup()
          // Look up the persisted terminal reason + triggering dep so the
          // error message stays informative even after the entry leaves pending.
          const terminalReason = this.terminalReasons.get(subtaskId)
          const depTrigger = this.depTriggerMap.get(subtaskId)
          reject(new Error(cancelErrorMessage(subtaskId, payload.reason, terminalReason, depTrigger)))
        }
      })
      listenerCleanup.push(unsubSchedule, unsubError, unsubCancel)
    })
  }

  /** True iff an active lease exists for the given agent. */
  isAgentBusy(agentId: string): boolean {
    for (const lease of this.activeLeases.values()) {
      if (lease.agentId === agentId) return true
    }
    return false
  }

  private fastTerminalRejection(subtaskId: string): Error | null {
    if (this.cancelled.has(subtaskId)) {
      const reason = this.terminalReasons.get(subtaskId)
      // Prefer the stored triggering dep (set by cancelOne when cascading);
      // it survives deletion from `pending`.
      const depId = this.depTriggerMap.get(subtaskId)
      const depSuffix = depId ? ` ${depId}` : ''
      if (reason === 'dependency-failed') {
        return new Error(`Subtask ${subtaskId} cannot run: prerequisite${depSuffix} failed`.replace(/\s+/g, ' '))
      }
      if (reason === 'dependency-cancelled') {
        return new Error(`Subtask ${subtaskId} cannot run: prerequisite${depSuffix} cancelled`.replace(/\s+/g, ' '))
      }
      if (reason === 'dispatch-timeout') return new Error(`Subtask ${subtaskId} dispatch timed out before assignment`)
      if (reason === 'scheduled-shutdown') return new Error(`Subtask ${subtaskId} cancelled: scheduler shutdown`)
      if (reason === 'task-cancelled') return new Error(`Subtask ${subtaskId} task cancelled`)
      return new Error(`Subtask ${subtaskId} cancelled before dispatch`)
    }
    if (this.failed.has(subtaskId)) {
      return new Error(`Subtask ${subtaskId} failed before dispatch`)
    }
    return null
  }

  /** Settle every outstanding waiter for a subtask with the given error. */
  private settleAwaiters(subtaskId: string, message: string): void {
    const set = this.pendingAwaitDispatchers.get(subtaskId)
    if (!set) return
    this.pendingAwaitDispatchers.delete(subtaskId)
    for (const w of set) {
      try { w.settle() } catch { /* ignore */ }
    }
    void message
  }

  /**
   * Atomic teardown. Drops pending, leases, locks and agents, rejects every
   * outstanding awaitDispatch with a `scheduled-shutdown` typed error, and
   * never dispatches new work.
   *
   * Use this from launchViaClaude's process.exit/error path or any
   * outer catch boundary that must guarantee no more work runs.
   */
  dispose(): void {
    if (this.tearingDown) return
    this.tearingDown = true
    // Evict pending entries first so subsequent `maybeDispatch` is a no-op.
    const pendingIds = [...this.pending.keys()]
    for (const id of pendingIds) this.cancelOne(id, 'scheduled-shutdown')
    // Clear leases/locks/agents without invoking `release` (which would
    // re-dispatch under non-tearing-down conditions).
    this.activeLeases.clear()
    this.profileLocks.clear()
    this.agents.clear()
    // Reject every waiter.
    for (const id of [...this.pendingAwaitDispatchers.keys()]) {
      this.settleAwaiters(id, `Scheduler disposed while awaiting ${id}`)
    }
  }

  on<E extends keyof SchedulerEvents>(event: E, listener: Listener<E>): () => void {
    this.listeners[event].add(listener)
    return () => this.listeners[event].delete(listener)
  }

  // ── Internal dispatch logic ─────────────────────────────────────

  /**
   * Promote ready subtasks into active leases, respecting global + per-agent
   * capacity. Called whenever state changes (enqueue/complete/release).
   *
   * Holds "stale" subtasks whose prereqs failed/cancelled so they surface
   * typed errors to their awaiters (handled in awaitDispatch).
   *
   * Never runs during teardown — dispose() / releaseAll() flip
   * `tearingDown` so this method bails immediately.
   */
  private maybeDispatch(): void {
    if (this.tearingDown) return
    if (this.activeLeases.size >= this.globalMax) return

    const ready = [...this.pending.values()]
      .filter(s =>
        s.status === 'pending' &&
        !this.activeLeases.has(s.id) &&
        !this.completed.has(s.id) &&
        !this.failed.has(s.id) &&
        !this.cancelled.has(s.id) &&
        s.dependsOn.every(d => this.completed.has(d)) &&
        // Skip subtasks whose prereqs are failed/cancelled — they will
        // surface a typed error when their awaitDispatch wakes up.
        s.dependsOn.every(d => !this.failed.has(d) && !this.cancelled.has(d)),
      )
      .sort(this.comparePriority)

    for (const subtask of ready) {
      if (this.tearingDown) return
      if (this.activeLeases.size >= this.globalMax) break
      this.tryDispatch(subtask)
    }
  }

  private tryDispatch(subtask: QueuedSubtask): AgentLease | null {
    if (this.tearingDown) return null
    // Skip re-dispatch if the subtask is already active.
    if (this.activeLeases.has(subtask.id)) return null

    const agentId = this.pickAgent(subtask)
    if (!agentId) return null

    const profileId = this.agents.get(agentId)!.profileId
    const lease: AgentLease = {
      profileId,
      agentId,
      subtaskId: subtask.id,
      taskId: subtask.taskId,
      startedAt: Date.now(),
    }
    this.profileLocks.set(profileId, lease)
    this.activeLeases.set(subtask.id, lease)
    subtask.status = 'active'
    this.emit('schedule', { subtaskId: subtask.id, agentId, profileId })
    return lease
  }

  private pickAgent(subtask: QueuedSubtask): string | null {
    for (const [agentId, { profileId }] of this.agents.entries()) {
      if (!this.isProfileMatch(profileId, subtask)) continue
      if (this.profileLocks.has(profileId)) continue
      return agentId
    }
    return null
  }

  private comparePriority(a: QueuedSubtask, b: QueuedSubtask): number {
    const pa = PRIORITY_RANK[a.priority]
    const pb = PRIORITY_RANK[b.priority]
    if (pa !== pb) return pa - pb
    return a.enqueuedAt - b.enqueuedAt
  }

  private isProfileMatch(profileId: string, subtask: QueuedSubtask): boolean {
    if (subtask.profileId) return profileId === subtask.profileId
    return this.agentHasCapability(profileId, subtask.capability)
  }

  /** Resolved capability → profile mapping lives outside scheduler. */
  private agentHasCapability(profileId: string, capability: string): boolean {
    // agentProfiles does not import from scheduler, so this is safe.
    const profile = AGENT_PROFILES.find(p => p.profileId === profileId)
    return !!profile?.capabilities.includes(capability)
  }

  private emit<E extends keyof SchedulerEvents>(event: E, payload: SchedulerEvents[E]): void {
    for (const listener of this.listeners[event]) {
      try {
        (listener as Listener<E>)(payload)
      } catch {
        // listener exceptions must not break scheduler
      }
    }
  }
}

function mapCancelReasonToTerminal(
  reason: 'dependency-failed' | 'dependency-cancelled' | 'explicit' | 'task-cancelled' | 'dispatch-timeout' | 'scheduled-shutdown',
): NonNullable<QueuedSubtask['terminalReason']> {
  if (reason === 'explicit') return 'explicit-cancel'
  return reason
}

function mapCancelEmitReason(
  reason: 'dependency-failed' | 'dependency-cancelled' | 'explicit' | 'task-cancelled' | 'dispatch-timeout' | 'scheduled-shutdown',
): 'dependency-failed' | 'dependency-cancelled' | 'explicit' | 'task-cancelled' {
  if (reason === 'dispatch-timeout' || reason === 'scheduled-shutdown') return 'dependency-cancelled'
  return reason as 'dependency-failed' | 'dependency-cancelled' | 'explicit' | 'task-cancelled'
}

function rejectMessageForCancelReason(subtaskId: string, reason: NonNullable<QueuedSubtask['terminalReason']>): string {
  if (reason === 'dependency-failed') return `Subtask ${subtaskId} cancelled: prerequisite failed`
  if (reason === 'dependency-cancelled') return `Subtask ${subtaskId} cancelled: prerequisite cancelled`
  if (reason === 'explicit-cancel') return `Subtask ${subtaskId} cancelled`
  if (reason === 'task-cancelled') return `Subtask ${subtaskId} cancelled: task cancelled`
  if (reason === 'dispatch-timeout') return `Subtask ${subtaskId} cancelled: dispatch timed out`
  if (reason === 'scheduled-shutdown') return `Subtask ${subtaskId} cancelled: scheduler shutdown`
  return `Subtask ${subtaskId} cancelled`
}

function cancelErrorMessage(
  subtaskId: string,
  reason: 'dependency-failed' | 'dependency-cancelled' | 'explicit' | 'task-cancelled',
  terminalReason?: NonNullable<QueuedSubtask['terminalReason']>,
  triggeringDep?: string,
): string {
  // Prefer terminalReason for richer detail (includes dispatch-timeout /
  // scheduled-shutdown etc.) when available.
  const dep = triggeringDep ? ` ${triggeringDep}` : ''
  if (reason === 'dependency-failed') return `Subtask ${subtaskId} cannot run: prerequisite${dep} failed`.replace(/\s+/g, ' ')
  if (reason === 'dependency-cancelled') return `Subtask ${subtaskId} cannot run: prerequisite${dep} cancelled`.replace(/\s+/g, ' ')
  if (reason === 'task-cancelled') return `Subtask ${subtaskId} task cancelled`
  if (terminalReason === 'dispatch-timeout') return `Subtask ${subtaskId} dispatch timed out before assignment`
  if (terminalReason === 'scheduled-shutdown') return `Subtask ${subtaskId} cancelled: scheduler shutdown`
  return `Subtask ${subtaskId} cancelled`
}

let globalInstance: GlobalScheduler | null = null
export function getGlobalScheduler(): GlobalScheduler {
  if (!globalInstance) globalInstance = new GlobalScheduler()
  return globalInstance
}
export function resetGlobalScheduler(): void {
  if (globalInstance) {
    globalInstance.dispose()
    globalInstance = null
  }
}

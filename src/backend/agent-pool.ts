// Canonical agent pool ownership.
//
// ACC-TASK-QUEUE-002-FINAL-REPAIR invariants:
//   - There is exactly ONE logical pool of agent profiles: AGENT_PROFILES.
//   - The executable runtime pool is ALWAYS derived from AGENT_PROFILES,
//     deterministically, by profileId — never from the in-memory registry
//     alone. Stale registry entries cannot inflate the pool.
//   - Profile identity is the stable key. Duplicate profileIds are
//     rejected from the pipeline; the canonical profile is represented
//     exactly once.
//   - Final-reviewer profile is represented exactly once and last.
//   - Persisted registry hydration MUST prune to the canonical set on
//     every restart (idempotent).
//
// This module is the single source of truth for executable agent pool
// ownership. It is intentionally pure (no module-level mutable state)
// so it can be exercised by behavioural tests in isolation.

import { AGENT_PROFILES } from './agent-profiles'

export interface RuntimeAgent {
  /** Stable runtime identifier, derived from the profileId. */
  id: string
  name: string
  type: string
  profileId: string
}

/**
 * The canonical executable runtime pool — exactly the configured
 * AGENT_PROFILES, in declared order, with deterministic stable ids.
 *
 * The returned array:
 *   - has length === AGENT_PROFILES.length (10 today)
 *   - has unique profileId per element
 *   - is in AGENT_PROFILES declaration order
 *   - final-reviewer profile is the LAST element by construction
 *     (reviewer is declared last in AGENT_PROFILES)
 */
export function buildCanonicalPool(): RuntimeAgent[] {
  return AGENT_PROFILES.map(p => ({
    id: `runtime-${p.profileId}`,
    name: p.name,
    type: p.type,
    profileId: p.profileId,
  }))
}

/**
 * Normalize an arbitrary list of agent records into the canonical pool.
 *
 * The input parameter is ACCEPTED but NOT consulted for membership —
 * the canonical pool is built purely from AGENT_PROFILES so historical /
 * stale entries CANNOT influence the result. The parameter exists so
 * callers can express "I would have picked from this list" and so the
 * function signature is future-compatible with preserved-metrics
 * extraction (intentionally not implemented in this repair).
 */
export function normalizeToCanonicalPool(
  _input: ReadonlyArray<{ id: string; name?: string; type?: string; profileId?: string }>,
): RuntimeAgent[] {
  const orderedKeys = AGENT_PROFILES.map(p => p.profileId)
  const seen = new Set<string>()
  const result: RuntimeAgent[] = []
  for (const key of orderedKeys) {
    if (seen.has(key)) continue
    seen.add(key)
    const profile = AGENT_PROFILES.find(p => p.profileId === key)!
    result.push({
      id: `runtime-${profile.profileId}`,
      name: profile.name,
      type: profile.type,
      profileId: profile.profileId,
    })
  }
  return result
}

/**
 * Prune the agentRegistry map in place to the canonical set:
 *
 *   - removes every entry whose profileId is not in the configured set
 *   - removes duplicate profileIds (first wins, by map insertion order)
 *   - removes entries whose keys are in the terminated set
 *   - is idempotent (a second call is a no-op)
 *
 * Returns the count of entries removed. Used both during restart
 * hydration and as a defensive guard before each pipeline launch.
 */
export function pruneRegistryToCanonical(
  registry: Map<string, { id: string; name: string; type: string; profileId?: string }>,
  terminated: ReadonlySet<string>,
): number {
  const allowed = new Set(AGENT_PROFILES.map(p => p.profileId))
  let removed = 0
  const seen = new Set<string>()
  for (const key of [...registry.keys()]) {
    const entry = registry.get(key)
    if (!entry) {
      registry.delete(key)
      removed++
      continue
    }
    if (terminated.has(key) || terminated.has(entry.id)) {
      registry.delete(key)
      removed++
      continue
    }
    if (!entry.profileId || !allowed.has(entry.profileId)) {
      registry.delete(key)
      removed++
      continue
    }
    if (seen.has(entry.profileId)) {
      registry.delete(key)
      removed++
      continue
    }
    seen.add(entry.profileId)
  }
  return removed
}

/**
 * Prune a serialised PersistedState.agents array (the format used by
 * state.json) to the canonical set. Returns a NEW array — does not
 * mutate input. Safe to call on every restart.
 */
export function prunePersistedAgents(
  agents: ReadonlyArray<[string, { id: string; name: string; type: string; profileId?: string }]>,
  terminated: ReadonlyArray<string>,
): Array<[string, { id: string; name: string; type: string; profileId?: string }]> {
  const terminatedSet = new Set(terminated)
  const allowed = new Set(AGENT_PROFILES.map(p => p.profileId))
  const seen = new Set<string>()
  const result: Array<[string, { id: string; name: string; type: string; profileId?: string }]> = []
  for (const entry of agents) {
    const [key, reg] = entry
    if (terminatedSet.has(key) || terminatedSet.has(reg.id)) continue
    if (!reg.profileId || !allowed.has(reg.profileId)) continue
    if (seen.has(reg.profileId)) continue
    seen.add(reg.profileId)
    result.push(entry)
  }
  return result
}
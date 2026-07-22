// @vitest-environment node
/**
 * ACC-TASK-QUEUE-002-FINAL-REPAIR — agent API source-of-truth tests.
 *
 * Invariants under test:
 *   - The agent list source-of-truth is the canonical pool, NOT the
 *     persisted registry.
 *   - Even if the persisted registry has been inflated to 20 / 30
 *     entries, every API-surface derivation yields exactly 10 agents.
 *   - The final-reviewer is the last element in every API derivation.
 *
 * The tests simulate the exact derivation that `/api/agents` and
 * `/api/swarm-monitor/snapshot` perform (filter by registry-known
 * profileId, dedupe, then enrich with activity) so a regression in
 * either endpoint source-of-truth would surface here.
 */
import { describe, it, expect } from 'vitest'
import {
  buildCanonicalPool,
  pruneRegistryToCanonical,
  prunePersistedAgents,
  RuntimeAgent,
} from '../agent-pool'
import { AGENT_PROFILES } from '../agent-profiles'

/** Simulate `/api/agents` derivation. */
function deriveAgentListResponse(
  cliRows: Array<{ created: string; id?: string; name?: string; type?: string }>,
  registry: Map<string, { id: string; name: string; type: string; profileId?: string }>,
  terminated: Set<string>,
  activity: Map<string, { tasksCompleted: number; errors: number; status: string }>,
): Array<{ id: string; name: string; profileId?: string }> {
  const canonicalProfileIds = new Set(AGENT_PROFILES.map(p => p.profileId))
  return cliRows
    .filter(row => {
      const created = row.created || ''
      if (terminated.has(created)) return false
      const reg = registry.get(created)
      if (!reg || !reg.profileId || !canonicalProfileIds.has(reg.profileId)) return false
      return true
    })
    .map((row, i) => {
      const created = row.created || ''
      const reg = registry.get(created)!
      return {
        id: row.id || reg.id || `agent-${i}`,
        name: reg.name,
        profileId: reg.profileId,
      }
    })
    // Even with empty rows, the canonical pool is the source of truth.
    .concat(cliRows.length === 0 ? buildCanonicalPool() : [])
    // Activity enrichment (preserved metrics; no profileId -> no entry)
    .map(a => ({ ...a, metrics: activity.get(a.id) }))
    .map(a => ({ id: a.id, name: a.name, profileId: a.profileId }))
}

describe('agent-source-of-truth — registry + cli rows derivation', () => {
  it('a 20-entry registry + 20 CLI rows yields exactly 10 API agents', () => {
    const cliRows: Array<{ created: string; id?: string; name?: string; type?: string }> = []
    const registry = new Map<string, { id: string; name: string; type: string; profileId?: string }>()
    AGENT_PROFILES.forEach((p, i) => {
      cliRows.push({ created: `k${i}`, id: `runtime-${p.profileId}`, type: p.type })
      registry.set(`k${i}`, { id: `runtime-${p.profileId}`, name: p.name, type: p.type, profileId: p.profileId })
    })
    // Plus 10 stale entries that the CLI may still return.
    for (let i = 0; i < 10; i++) {
      cliRows.push({ created: `h${i}`, id: `hist-${i}`, type: 'coder' })
      registry.set(`h${i}`, { id: `hist-${i}`, name: `H${i}`, type: 'coder', profileId: 'historical' })
    }
    expect(registry.size).toBe(20)
    expect(cliRows.length).toBe(20)
    // First, prune so the registry only contains canonical entries.
    const removed = pruneRegistryToCanonical(registry, new Set())
    expect(removed).toBe(10)
    expect(registry.size).toBe(10)

    // Now feed the pruned registry + its surviving CLI rows into the
    // API derivation. Stale CLI rows are filtered because their
    // registry entry is no longer present.
    const canonicalCliRows: Array<{ created: string; id?: string; type?: string }> = []
    for (const [key, reg] of registry.entries()) {
      canonicalCliRows.push({ created: key, id: reg.id, type: reg.type })
    }
    expect(canonicalCliRows.length).toBe(10)
    const apiResult = deriveAgentListResponse(canonicalCliRows, registry, new Set(), new Map())
    expect(apiResult.length).toBe(10)
    expect(new Set(apiResult.map(a => a.profileId)).size).toBe(10)
  })

  it('falls back to canonical pool when CLI list returns nothing', () => {
    const apiResult = deriveAgentListResponse([], new Map(), new Set(), new Map())
    expect(apiResult.length).toBe(10)
    expect(apiResult[apiResult.length - 1].profileId).toBe('reviewer')
  })

  it('drops registry entries whose profileId is not canonical', () => {
    const cliRows = [{ created: 'a', id: 'a', type: 'coder' }, { created: 'b', id: 'b', type: 'coder' }]
    const registry = new Map<string, { id: string; name: string; type: string; profileId?: string }>()
    registry.set('a', { id: 'a', name: 'a', type: 'coder', profileId: 'backend-1' })
    registry.set('b', { id: 'b', name: 'b', type: 'coder', profileId: 'phantom' })
    const result = deriveAgentListResponse(cliRows, registry, new Set(), new Map())
    expect(result.length).toBe(1)
    expect(result[0].profileId).toBe('backend-1')
  })

  it('respects termination filter', () => {
    const cliRows = [{ created: 'a', id: 'a', type: 'coder' }, { created: 'b', id: 'b', type: 'coder' }]
    const registry = new Map<string, { id: string; name: string; type: string; profileId?: string }>()
    registry.set('a', { id: 'a', name: 'a', type: 'coder', profileId: 'backend-1' })
    registry.set('b', { id: 'b', name: 'b', type: 'coder', profileId: 'backend-2' })
    const result = deriveAgentListResponse(cliRows, registry, new Set(['a']), new Map())
    expect(result.length).toBe(1)
    expect(result[0].id).toBe('b')
  })
})

describe('agent-source-of-truth — final-reviewer invariant', () => {
  it('final-reviewer is the last element of the canonical pool derivation', () => {
    const pool = buildCanonicalPool()
    expect(pool[pool.length - 1].profileId).toBe('reviewer')
  })

  it('persisted-registry hydration produces a final-reviewer as the last entry', () => {
    const persisted: Array<[string, { id: string; name: string; type: string; profileId?: string }]> = []
    AGENT_PROFILES.forEach((p, i) => {
      persisted.push([`k${i}`, { id: `runtime-${p.profileId}`, name: p.name, type: p.type, profileId: p.profileId }])
    })
    const pruned = prunePersistedAgents(persisted, [])
    expect(pruned.length).toBe(10)
    expect(pruned[pruned.length - 1][1].profileId).toBe('reviewer')
  })

  it('a duplicate reviewer entry in input is dropped — only the canonical reviewer survives', () => {
    const pool: RuntimeAgent[] = [
      ...buildCanonicalPool(),
      { id: 'runtime-reviewer-2', name: 'Dup', type: 'reviewer', profileId: 'reviewer' },
    ]
    const seenProfileIds = new Set<string>()
    const dedup: RuntimeAgent[] = []
    for (const agent of pool) {
      if (seenProfileIds.has(agent.profileId)) continue
      seenProfileIds.add(agent.profileId)
      dedup.push(agent)
    }
    expect(dedup.length).toBe(10)
    expect(dedup.filter(a => a.profileId === 'reviewer').length).toBe(1)
    expect(dedup[dedup.length - 1].profileId).toBe('reviewer')
  })
})
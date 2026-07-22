// @vitest-environment node
/**
 * ACC-TASK-QUEUE-002-FINAL-REPAIR — agent pool ownership regression tests.
 *
 * Invariants under test:
 *   - Legacy persisted registry with 20 / stale entries normalizes to
 *     exactly the configured 10 profiles.
 *   - Two sequential launches still expose / execute 10, never 20.
 *   - Duplicate profile IDs cannot enter a pipeline.
 *   - Final-reviewer is represented exactly once and last.
 *   - No stale agent is dispatched to the scheduler.
 *   - Canonical pool is the single source of truth for executable
 *     agent identity.
 *
 * These tests are pure module-level exercises of agent-pool.ts; they do
 * NOT touch the network, worktrees, or the global scheduler instance.
 */
import { describe, it, expect } from 'vitest'
import {
  buildCanonicalPool,
  normalizeToCanonicalPool,
  pruneRegistryToCanonical,
  prunePersistedAgents,
  RuntimeAgent,
} from '../agent-pool'
import { AGENT_PROFILES, findProfileById } from '../agent-profiles'

describe('agent-pool — buildCanonicalPool', () => {
  it('returns exactly AGENT_PROFILES.length entries', () => {
    const pool = buildCanonicalPool()
    expect(pool.length).toBe(AGENT_PROFILES.length)
    expect(pool.length).toBe(10)
  })

  it('every entry has a unique profileId', () => {
    const pool = buildCanonicalPool()
    const ids = new Set(pool.map(p => p.profileId))
    expect(ids.size).toBe(pool.length)
  })

  it('preserves declared AGENT_PROFILES order', () => {
    const pool = buildCanonicalPool()
    expect(pool.map(p => p.profileId)).toEqual(AGENT_PROFILES.map(p => p.profileId))
  })

  it('final-reviewer is the LAST element', () => {
    const pool = buildCanonicalPool()
    const last = pool[pool.length - 1]
    expect(last.profileId).toBe('reviewer')
    expect(last.type).toBe('reviewer')
    expect(findProfileById(last.profileId)?.capabilities).toContain('final-review')
  })

  it('each entry exposes profileId and a deterministic runtime id', () => {
    const pool = buildCanonicalPool()
    for (const agent of pool) {
      expect(agent.profileId).toBeTruthy()
      expect(agent.id).toBe(`runtime-${agent.profileId}`)
      expect(agent.name).toBeTruthy()
      expect(agent.type).toBeTruthy()
    }
  })

  it('returns a fresh array each call (no shared mutation)', () => {
    const a = buildCanonicalPool()
    const b = buildCanonicalPool()
    expect(a).not.toBe(b)
    expect(a).toEqual(b)
  })
})

describe('agent-pool — normalizeToCanonicalPool (legacy 20-entry registry)', () => {
  it('a 20-entry stale registry normalizes to 10', () => {
    const stale: Array<{ id: string; type: string; profileId?: string }> = []
    for (let i = 0; i < 20; i++) {
      stale.push({ id: `stale-${i}`, type: 'coder', profileId: 'stale-profile' })
    }
    const pool = normalizeToCanonicalPool(stale)
    expect(pool.length).toBe(10)
    expect(new Set(pool.map(p => p.profileId))).toEqual(new Set(AGENT_PROFILES.map(p => p.profileId)))
  })

  it('input containing duplicates of canonical profileIds still yields 10 entries', () => {
    const dup: Array<{ id: string; type: string; profileId?: string }> = []
    AGENT_PROFILES.forEach(p => dup.push({ id: `dup1-${p.profileId}`, type: p.type, profileId: p.profileId }))
    AGENT_PROFILES.forEach(p => dup.push({ id: `dup2-${p.profileId}`, type: p.type, profileId: p.profileId }))
    expect(dup.length).toBe(20)
    const pool = normalizeToCanonicalPool(dup)
    expect(pool.length).toBe(10)
  })

  it('ignores profileIds that are not in the canonical set', () => {
    const polluted: Array<{ id: string; type: string; profileId?: string }> = [
      ...AGENT_PROFILES.map(p => ({ id: `c-${p.profileId}`, type: p.type, profileId: p.profileId })),
      { id: 'ghost-1', type: 'coder', profileId: 'phantom-coder' },
      { id: 'ghost-2', type: 'tester', profileId: 'unknown' },
      { id: 'ghost-3', type: 'reviewer', profileId: 'extra-reviewer' },
    ]
    const pool = normalizeToCanonicalPool(polluted)
    expect(pool.length).toBe(10)
    expect(pool.find(a => a.profileId === 'phantom-coder')).toBeUndefined()
    expect(pool.find(a => a.profileId === 'extra-reviewer')).toBeUndefined()
  })
})

describe('agent-pool — pruneRegistryToCanonical (sequential launch guard)', () => {
  it('removes stale entries on every call (idempotent)', () => {
    const registry = new Map<string, { id: string; name: string; type: string; profileId?: string }>()
    // Simulate the leaked state: 10 canonical + 10 historical duplicates
    AGENT_PROFILES.forEach((p, i) => {
      registry.set(`k${i}-a`, { id: `runtime-${p.profileId}`, name: p.name, type: p.type, profileId: p.profileId })
      registry.set(`k${i}-b`, { id: `stale-${i}`, name: 'Stale', type: 'coder', profileId: 'stale' })
    })
    expect(registry.size).toBe(20)

    const terminated = new Set<string>()
    const removed = pruneRegistryToCanonical(registry, terminated)
    expect(removed).toBe(10)
    expect(registry.size).toBe(10)

    // Idempotent: a second call removes nothing.
    const removed2 = pruneRegistryToCanonical(registry, terminated)
    expect(removed2).toBe(0)
    expect(registry.size).toBe(10)
  })

  it('simulates two sequential task launches staying at 10', () => {
    const registry = new Map<string, { id: string; name: string; type: string; profileId?: string }>()
    const terminated = new Set<string>()

    // First launch: registry seeded with 20 (10 canonical + 10 stale).
    AGENT_PROFILES.forEach((p, i) => {
      registry.set(`s1-${i}-a`, { id: `runtime-${p.profileId}`, name: p.name, type: p.type, profileId: p.profileId })
      registry.set(`s1-${i}-b`, { id: `s1-stale-${i}`, name: 'Stale', type: 'coder', profileId: 'stale' })
    })

    pruneRegistryToCanonical(registry, terminated)
    expect(registry.size).toBe(10)

    // Second launch: registry starts at 10; no new entries should inflate it
    // because the canonical pool is the single source of truth.
    const pool = buildCanonicalPool()
    for (const agent of pool) {
      registry.set(`s2-${agent.profileId}`, { id: agent.id, name: agent.name, type: agent.type, profileId: agent.profileId })
    }
    pruneRegistryToCanonical(registry, terminated)
    expect(registry.size).toBe(10)
    const profileIds = new Set<string>()
    for (const v of registry.values()) profileIds.add(v.profileId!)
    expect(profileIds.size).toBe(10)
  })

  it('drops entries whose profileId is not in the canonical set', () => {
    const registry = new Map<string, { id: string; name: string; type: string; profileId?: string }>()
    registry.set('a', { id: 'a', name: 'a', type: 'coder', profileId: 'backend-1' })
    registry.set('b', { id: 'b', name: 'b', type: 'coder', profileId: 'nonexistent' })
    registry.set('c', { id: 'c', name: 'c', type: 'coder' }) // no profileId
    const removed = pruneRegistryToCanonical(registry, new Set())
    expect(removed).toBe(2)
    expect(registry.size).toBe(1)
    expect(registry.get('a')).toBeDefined()
  })

  it('drops entries whose keys are in the terminated set', () => {
    const registry = new Map<string, { id: string; name: string; type: string; profileId?: string }>()
    registry.set('a', { id: 'a', name: 'a', type: 'coder', profileId: 'backend-1' })
    registry.set('b', { id: 'b', name: 'b', type: 'coder', profileId: 'backend-2' })
    const removed = pruneRegistryToCanonical(registry, new Set(['a']))
    expect(removed).toBe(1)
    expect(registry.size).toBe(1)
    expect(registry.get('a')).toBeUndefined()
  })

  it('deduplicates duplicate profileIds (first wins)', () => {
    const registry = new Map<string, { id: string; name: string; type: string; profileId?: string }>()
    registry.set('first', { id: 'first', name: 'first', type: 'coder', profileId: 'backend-1' })
    registry.set('dup', { id: 'dup', name: 'dup', type: 'coder', profileId: 'backend-1' })
    const removed = pruneRegistryToCanonical(registry, new Set())
    expect(removed).toBe(1)
    expect(registry.size).toBe(1)
    expect(registry.get('first')).toBeDefined()
    expect(registry.get('dup')).toBeUndefined()
  })
})

describe('agent-pool — prunePersistedAgents (restart hydration)', () => {
  it('prunes 20 historical entries to 10 canonical on restart', () => {
    const persisted: Array<[string, { id: string; name: string; type: string; profileId?: string }]> = []
    for (let i = 0; i < 10; i++) {
      persisted.push([`h${i}`, { id: `historical-${i}`, name: 'H', type: 'coder', profileId: 'historical' }])
    }
    AGENT_PROFILES.forEach((p, i) => {
      persisted.push([`k${i}`, { id: `runtime-${p.profileId}`, name: p.name, type: p.type, profileId: p.profileId }])
    })
    expect(persisted.length).toBe(20)
    const pruned = prunePersistedAgents(persisted, [])
    expect(pruned.length).toBe(10)
  })

  it('is idempotent', () => {
    const persisted: Array<[string, { id: string; name: string; type: string; profileId?: string }]> = []
    AGENT_PROFILES.forEach((p, i) => {
      persisted.push([`k${i}`, { id: `runtime-${p.profileId}`, name: p.name, type: p.type, profileId: p.profileId }])
    })
    const pruned1 = prunePersistedAgents(persisted, [])
    const pruned2 = prunePersistedAgents(pruned1, [])
    expect(pruned2.length).toBe(10)
    expect(pruned2).toEqual(pruned1)
  })

  it('does not mutate its input array', () => {
    const persisted: Array<[string, { id: string; name: string; type: string; profileId?: string }]> = []
    AGENT_PROFILES.forEach((p, i) => {
      persisted.push([`k${i}`, { id: `runtime-${p.profileId}`, name: p.name, type: p.type, profileId: p.profileId }])
    })
    persisted.push(['x', { id: 'x', name: 'x', type: 'coder', profileId: 'bogus' }])
    const before = persisted.length
    prunePersistedAgents(persisted, [])
    expect(persisted.length).toBe(before)
  })

  it('removes entries whose key is in the terminated set', () => {
    const persisted: Array<[string, { id: string; name: string; type: string; profileId?: string }]> = []
    AGENT_PROFILES.forEach((p, i) => {
      persisted.push([`k${i}`, { id: `runtime-${p.profileId}`, name: p.name, type: p.type, profileId: p.profileId }])
    })
    const terminated = ['k0']
    const pruned = prunePersistedAgents(persisted, terminated)
    expect(pruned.length).toBe(9)
  })
})

describe('agent-pool — final-reviewer invariant', () => {
  it('buildCanonicalPool contains reviewer exactly once and as the last element', () => {
    const pool = buildCanonicalPool()
    const reviewers = pool.filter(p => p.profileId === 'reviewer')
    expect(reviewers.length).toBe(1)
    expect(pool[pool.length - 1].profileId).toBe('reviewer')
  })

  it('a duplicate reviewer entry in input is dropped by normalize', () => {
    const input: RuntimeAgent[] = [
      ...buildCanonicalPool(),
      { id: 'runtime-reviewer-2', name: 'Dup', type: 'reviewer', profileId: 'reviewer' },
    ]
    const normalized = normalizeToCanonicalPool(input)
    expect(normalized.length).toBe(10)
    expect(normalized.filter(p => p.profileId === 'reviewer').length).toBe(1)
  })

  it('final-reviewer position is preserved by the fallback chain (reviewer never appears earlier)', () => {
    const pool = buildCanonicalPool()
    const reviewerIdx = pool.findIndex(p => p.profileId === 'reviewer')
    expect(reviewerIdx).toBe(pool.length - 1)
  })
})
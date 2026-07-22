// @vitest-environment node
/**
 * ACC-TASK-QUEUE-002-FINAL-REPAIR — planner JSON parsing + bounded
 * fallback regression tests.
 *
 * Invariants under test:
 *   - Bare JSON arrays parse correctly.
 *   - Fenced JSON (``` or ```json) parses correctly.
 *   - Surrounding explanatory prose is stripped before parsing.
 *   - Malformed planner output yields null (caller enters fallback).
 *   - Bounded deterministic fallback:
 *       * uses ONLY canonical AGENT_PROFILES capabilities,
 *       * bounded to 2 subtasks for READ-ONLY, 3 for WRITE,
 *       * never includes a final-review capability (the pipeline
 *         appends exactly one canonical final-review separately),
 *       * bounded — never fans out through the entire stale registry.
 */
import { describe, it, expect } from 'vitest'
import { parsePlannerOutput, buildDeterministicFallback } from '../planner-parse'
import { AGENT_PROFILES } from '../agent-profiles'

describe('planner-parse — parsePlannerOutput', () => {
  it('accepts a bare JSON array', () => {
    const text = JSON.stringify([
      { capability: 'backend', task: 'do x', depends_on: [] },
      { capability: 'qa', task: 'test x', depends_on: [0] },
    ])
    const result = parsePlannerOutput(text)
    expect(result).not.toBeNull()
    expect(result!.length).toBe(2)
  })

  it('accepts a fenced JSON block (```json)', () => {
    const text = 'Some prose\n\n```json\n[{"capability":"backend","task":"x","depends_on":[]}]\n```\n\nMore prose'
    const result = parsePlannerOutput(text)
    expect(result).not.toBeNull()
    expect(result!.length).toBe(1)
  })

  it('accepts a fenced JSON block (```)', () => {
    const text = 'Here is the plan:\n```\n[{"capability":"research","task":"explore","depends_on":[]}]\n```'
    const result = parsePlannerOutput(text)
    expect(result).not.toBeNull()
    expect(result!.length).toBe(1)
  })

  it('accepts surrounding explanatory prose around a JSON array', () => {
    const text = `Sure, here's my plan:

[
  {"capability":"backend","task":"build feature","depends_on":[]},
  {"capability":"qa","task":"validate feature","depends_on":[0]},
  {"capability":"review","task":"review feature","depends_on":[0,1]}
]

Let me know if you'd like me to elaborate.`
    const result = parsePlannerOutput(text)
    expect(result).not.toBeNull()
    expect(result!.length).toBe(3)
  })

  it('accepts the old "agent" field shape', () => {
    const text = JSON.stringify([{ agent: 'coder', task: 'do thing', depends_on: [] }])
    const result = parsePlannerOutput(text)
    expect(result).not.toBeNull()
    expect(result![0].agent).toBe('coder')
  })

  it('returns null for empty input', () => {
    expect(parsePlannerOutput('')).toBeNull()
  })

  it('returns null for prose with no JSON', () => {
    const text = 'I think we should approach this carefully. Let me outline the strategy first.'
    expect(parsePlannerOutput(text)).toBeNull()
  })

  it('returns null for malformed JSON', () => {
    const text = '[{"capability":"backend","task":"x","depends_on":' // truncated
    expect(parsePlannerOutput(text)).toBeNull()
  })

  it('returns null for an array of non-subtask objects', () => {
    const text = '[{"foo":"bar"}]'
    expect(parsePlannerOutput(text)).toBeNull()
  })

  it('returns null for empty array', () => {
    expect(parsePlannerOutput('[]')).toBeNull()
  })

  it('parses balanced arrays even when surrounded by stray brackets', () => {
    const text = 'Note: [see appendix] then plan: [{"capability":"backend","task":"x","depends_on":[]}] end.'
    const result = parsePlannerOutput(text)
    expect(result).not.toBeNull()
    expect(result!.length).toBe(1)
  })
})

describe('planner-parse — buildDeterministicFallback (bounded, canonical only)', () => {
  it('WRITE fallback contains exactly 2 subtasks (impl + qa, final-review appended later by pipeline)', () => {
    const fb = buildDeterministicFallback('Add a /api/foo endpoint', false)
    expect(fb.length).toBe(2)
  })

  it('READ-ONLY fallback contains exactly 1 subtask (research, final-review appended later by pipeline)', () => {
    const fb = buildDeterministicFallback('Audit the codebase', true)
    expect(fb.length).toBe(1)
  })

  it('fallback chain uses ONLY canonical AGENT_PROFILES capabilities', () => {
    const fb = buildDeterministicFallback('Implement X', false)
    const caps = new Set(AGENT_PROFILES.flatMap(p => p.capabilities))
    for (const sub of fb) {
      expect(caps.has(sub.capability)).toBe(true)
    }
  })

  it('fallback chain does NOT include final-review capability (the pipeline appends it)', () => {
    const fb = buildDeterministicFallback('Implement X', false)
    for (const sub of fb) {
      expect(sub.capability).not.toBe('final-review')
      expect(sub.capability).not.toBe('review')
    }
    const fbRo = buildDeterministicFallback('Audit X', true)
    for (const sub of fbRo) {
      expect(sub.capability).not.toBe('final-review')
      expect(sub.capability).not.toBe('review')
    }
  })

  it('fallback chain has deterministic dependency chain (qa depends on implementation)', () => {
    const fb = buildDeterministicFallback('Implement X', false)
    expect(fb[0].depends_on).toEqual([])
    expect(fb[1].depends_on).toEqual([0])
  })

  it('fallback chain is bounded — never fans out through more than 3 subtasks (impl + qa, +pipeline-appended final-review)', () => {
    const fb = buildDeterministicFallback('Implement a huge feature', false)
    // The fallback itself returns impl + qa (2). The pipeline then
    // appends exactly one final-review. So total fallback-shape subtasks
    // from this function alone is bounded by 2.
    expect(fb.length).toBeLessThanOrEqual(2)
    const fbRo = buildDeterministicFallback('Audit everything', true)
    expect(fbRo.length).toBeLessThanOrEqual(1)
  })

  it('READ-ONLY fallback uses research capability', () => {
    const fb = buildDeterministicFallback('Audit X', true)
    expect(fb[0].capability).toBe('research')
  })

  it('WRITE fallback starts with implementation capability (backend)', () => {
    const fb = buildDeterministicFallback('Implement X', false)
    expect(fb[0].capability).toBe('backend')
    expect(fb[1].capability).toBe('qa')
  })
})
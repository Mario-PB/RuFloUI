// Planner JSON extraction + bounded fallback chain.
//
// ACC-TASK-QUEUE-002-FINAL-REPAIR invariants:
//   - The planner may emit the JSON array bare, fenced with ``` / ```json,
//     wrapped in explanatory prose, or a mixture. All four shapes must
//     parse identically.
//   - On ANY parse failure (no array, malformed JSON, empty array after
//     parsing) we MUST enter a bounded deterministic fallback chain that
//     references ONLY canonical AGENT_PROFILES capabilities — never stale
//     registry entries or unknown capabilities.
//   - The fallback is bounded: 3 subtasks for WRITE (implementation /
//     qa / final-review), 2 for READ-ONLY (research / final-review).
//   - Exactly one terminal-reviewer at the END of the chain. The
//     canonical final-review is enforced by the pipeline's
//     stripTerminalReviewerAliases + append-final-review pass; the
//     fallback respects it by NOT including a terminal-review capability
//     in the chain so the appended canonical final-review remains the
//     sole and last entry.

import { AGENT_PROFILES } from './agent-profiles'

/**
 * The bounded capabilities the deterministic fallback is allowed to use.
 * These are guaranteed to be present on AGENT_PROFILES today; if a
 * capability disappears from AGENT_PROFILES in the future the fallback
 * would be silent (returning []), which is a louder failure mode than
 * dispatching an unknown capability to the scheduler.
 */
const FALLBACK_CAPABILITIES = {
  implementation: 'backend',
  qa: 'qa',
  readOnlyResearch: 'research',
} as const

/**
 * Robustly parse a planner Claude response into a JSON array of subtasks.
 *
 * Accepts:
 *   - Bare JSON array (e.g. `[{"capability":"...","task":"...","depends_on":[]}]`)
 *   - Fenced JSON (` ```json ... ``` ` or ` ``` ... ``` `)
 *   - Surrounding explanatory prose around any of the above
 *
 * Returns `null` when the input does not contain a parseable array of
 * objects. Empty arrays are also reported as `null` (callers MUST treat
 * null as "use bounded fallback").
 */
export function parsePlannerOutput(
  text: string,
): Array<{ capability?: string; agent?: string; task: string; depends_on?: number[] }> | null {
  if (!text) return null

  // Strategy 1: the whole input parses as a JSON array.
  const trimmed = text.trim()
  if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
    const arr = tryParse(trimmed)
    if (arr) return arr
  }

  // Strategy 2: extract a fenced JSON block (```json ... ``` or ``` ... ```)
  const fenced = extractFencedBlock(text)
  if (fenced) {
    const arr = tryParse(fenced)
    if (arr) return arr
  }

  // Strategy 3: find the first '[' that starts a balanced array and parse it.
  const balanced = extractBalancedArray(text)
  if (balanced) {
    const arr = tryParse(balanced)
    if (arr) return arr
  }

  return null
}

function tryParse(s: string): Array<{ capability?: string; agent?: string; task: string; depends_on?: number[] }> | null {
  try {
    const value = JSON.parse(s)
    if (Array.isArray(value) && value.length > 0 && value.every(isSubtaskShape)) {
      return value as Array<{ capability?: string; agent?: string; task: string; depends_on?: number[] }>
    }
    return null
  } catch {
    return null
  }
}

function isSubtaskShape(v: unknown): boolean {
  if (!v || typeof v !== 'object') return false
  const o = v as Record<string, unknown>
  return typeof o.task === 'string' && o.task.length > 0
}

function extractFencedBlock(text: string): string | null {
  // Matches ```json ... ``` or ``` ... ``` (greedy between fences).
  const fenceRe = /```(?:json)?\s*([\s\S]*?)```/m
  const m = text.match(fenceRe)
  return m ? m[1].trim() : null
}

function extractBalancedArray(text: string): string | null {
  // Find every potential '[' start, then take the first one whose
  // balanced substring parses as a JSON array of subtasks. This way
  // prose like "see [appendix]" cannot hijack the parse — if the
  // balanced substring starting at the first '[' doesn't yield a
  // valid array, we try later positions until one works.
  let searchFrom = 0
  while (searchFrom < text.length) {
    const start = text.indexOf('[', searchFrom)
    if (start === -1) return null
    let depth = 0
    let inString = false
    let escape = false
    let endIdx = -1
    for (let i = start; i < text.length; i++) {
      const ch = text[i]
      if (escape) { escape = false; continue }
      if (ch === '\\') { escape = true; continue }
      if (ch === '"') { inString = !inString; continue }
      if (inString) continue
      if (ch === '[') depth++
      else if (ch === ']') {
        depth--
        if (depth === 0) { endIdx = i; break }
      }
    }
    if (endIdx >= 0) {
      const candidate = text.slice(start, endIdx + 1)
      // Quick validation: must start with '[' and contain at least one
      // object. Full parsing happens in tryParse; this just rejects
      // obviously-empty brackets like "[see appendix]".
      if (candidate.includes('{')) {
        try {
          const value = JSON.parse(candidate)
          if (Array.isArray(value) && value.length > 0) return candidate
        } catch {
          /* keep searching */
        }
      }
      // Advance past this '[' so we look for the next candidate.
      searchFrom = start + 1
      continue
    }
    return null
  }
  return null
}

/**
 * The deterministic fallback chain — used when the planner output is
 * unparseable OR has zero subtasks after parsing.
 *
 * Constraints:
 *   - Uses ONLY canonical AGENT_PROFILES capabilities (look-up via
 *     FALLBACK_CAPABILITIES, verified at runtime against AGENT_PROFILES).
 *   - Final-review capability is NOT emitted here — the pipeline's
 *     canonical pass appends one exactly-once, ensuring it is LAST.
 *   - WRITE: 3 subtasks (implementation → qa) before final-review.
 *   - READ-ONLY: 1 subtask (research) before final-review.
 */
export function buildDeterministicFallback(
  taskDesc: string,
  isReadOnly: boolean,
): Array<{ capability: string; task: string; depends_on: number[] }> {
  // Defensive: every capability used here MUST exist on at least one
  // canonical profile. If not, return an empty chain (the pipeline's
  // canonical pass still appends the final-reviewer so the chain is
  // well-formed, and the scheduler will fail to dispatch an unknown
  // capability which surfaces a typed error to the parent task).
  const backendOk = AGENT_PROFILES.some(p => p.capabilities.includes(FALLBACK_CAPABILITIES.implementation))
  const qaOk = AGENT_PROFILES.some(p => p.capabilities.includes(FALLBACK_CAPABILITIES.qa))
  const researchOk = AGENT_PROFILES.some(p => p.capabilities.includes(FALLBACK_CAPABILITIES.readOnlyResearch))

  if (isReadOnly) {
    if (!researchOk) return []
    return [
      {
        capability: FALLBACK_CAPABILITIES.readOnlyResearch,
        task: `Analyze the following request and produce findings: ${taskDesc}`,
        depends_on: [],
      },
    ]
  }

  if (!backendOk || !qaOk) return []
  return [
    {
      capability: FALLBACK_CAPABILITIES.implementation,
      task: `Implement the requested change: ${taskDesc}`,
      depends_on: [],
    },
    {
      capability: FALLBACK_CAPABILITIES.qa,
      task: 'Add or update tests covering the implementation and run the test suite.',
      depends_on: [0],
    },
  ]
}
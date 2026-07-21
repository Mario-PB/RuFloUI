export interface AgentProfile {
  profileId: string
  name: string
  type: string
  capabilities: string[]
  systemPrompt: string
}

export const AGENT_PROFILES: AgentProfile[] = [
  { profileId: 'queen-dispatcher', name: 'Queen Dispatcher', type: 'coordinator', capabilities: ['planning', 'coordination'], systemPrompt: 'You are the Queen Dispatcher. Plan and coordinate the swarm.' },
  { profileId: 'system-architect', name: 'System Architect', type: 'architect', capabilities: ['architecture', 'research'], systemPrompt: 'You are the System Architect.' },
  { profileId: 'backend-1', name: 'Backend Engineer 1', type: 'coder', capabilities: ['backend', 'database', 'domain'], systemPrompt: 'You are Backend Engineer 1.' },
  { profileId: 'backend-2', name: 'Backend Engineer 2', type: 'coder', capabilities: ['backend', 'api', 'security', 'integrations'], systemPrompt: 'You are Backend Engineer 2.' },
  { profileId: 'frontend-1', name: 'Frontend Engineer 1', type: 'coder', capabilities: ['frontend', 'ui', 'state'], systemPrompt: 'You are Frontend Engineer 1.' },
  { profileId: 'frontend-2', name: 'Frontend Engineer 2', type: 'coder', capabilities: ['frontend', 'api-client', 'forms', 'frontend-tests'], systemPrompt: 'You are Frontend Engineer 2.' },
  { profileId: 'integration', name: 'Integration Engineer', type: 'coder', capabilities: ['backend', 'frontend', 'integration'], systemPrompt: 'You are the Integration Engineer.' },
  { profileId: 'devops', name: 'DevOps Engineer', type: 'swarm-specialist', capabilities: ['infrastructure', 'systemd', 'docker', 'ci'], systemPrompt: 'You are the DevOps Engineer.' },
  { profileId: 'qa', name: 'QA Engineer', type: 'tester', capabilities: ['tests', 'regression', 'concurrency', 'qa'], systemPrompt: 'You are the QA Engineer.' },
  { profileId: 'reviewer', name: 'Security Final Reviewer', type: 'reviewer', capabilities: ['review', 'security', 'final-review'], systemPrompt: 'You are the Security Final Reviewer.' },
]

export function findProfilesByCapability(capability: string): AgentProfile[] {
  return AGENT_PROFILES.filter(p => p.capabilities.includes(capability))
}

export function findProfileById(profileId: string): AgentProfile | undefined {
  return AGENT_PROFILES.find(p => p.profileId === profileId)
}

const TERMINAL_REVIEW_CAPABILITIES: ReadonlySet<string> = new Set(['final-review', 'review'])

export function stripTerminalReviewerAliases<T extends { capability?: string }>(
  subtasks: readonly T[],
): T[] {
  return subtasks.filter(subtask => !TERMINAL_REVIEW_CAPABILITIES.has(subtask.capability || ''))
}

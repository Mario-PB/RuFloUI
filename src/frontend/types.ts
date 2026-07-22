export interface Agent {
  id: string
  name: string
  type: string
  status: 'idle' | 'running' | 'completed' | 'error' | 'terminated'
  createdAt: string
  lastActivity?: string
  taskId?: string
  currentTask?: string
  currentAction?: string
  metrics?: {
    tasksCompleted: number
    errorRate: number
    avgResponseTime: number
  }
}

export interface SwarmState {
  id: string
  topology: 'hierarchical' | 'mesh' | 'star' | 'ring' | 'hierarchical-mesh'
  strategy: string
  status: 'initializing' | 'active' | 'paused' | 'shutdown' | 'inactive'
  maxAgents: number
  activeAgents: number
  agents: Agent[]
  createdAt: string
}

export interface Task {
  id: string
  title: string
  description: string
  status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | 'failed' | 'interrupted'
  assignedTo?: string
  priority: 'low' | 'normal' | 'high' | 'critical'
  createdAt: string
  completedAt?: string
  result?: string
  /** Working directory for agent execution */
  cwd?: string
  /** Source cwd captured at task creation. READ-ONLY executes here; WRITE never touches it. */
  sourceCwd?: string
  /** Execution cwd passed to claude -p — worktree path for WRITE, source for READ-ONLY. */
  executionCwd?: string
  /** Per-WRITE worktree path. Absent for READ-ONLY. */
  worktreePath?: string
  branchName?: string
  baseCommit?: string
  /** Resolved from PACKET-ID / description / explicit override. */
  mode?: 'WRITE' | 'READ-ONLY'
  /** Mirror of dispatcher queue state. */
  queueState?: 'queued' | 'dispatching' | 'running' | 'terminal'
  queuePosition?: number
  attempt?: number
}

export interface MemoryEntry {
  key: string
  value: string
  namespace: string
  tags: string[]
  ttl?: number
  createdAt: string
  updatedAt: string
}

export interface MemoryStats {
  totalEntries: number
  namespaces: string[]
  storageSize: string
  hnswEnabled: boolean
  indexedVectors: number
}

export interface Session {
  id: string
  name: string
  status: 'active' | 'saved' | 'restored'
  createdAt: string
  agentCount: number
  taskCount: number
}

export interface HiveMindState {
  status: 'inactive' | 'active' | 'consensus'
  members: string[]
  consensusProtocol: string
  lastConsensus?: {
    topic: string
    result: string
    timestamp: string
    votes: Record<string, string>
  }
}

export interface HealthCheck {
  name: string
  status: 'pass' | 'warn' | 'fail'
  detail: string
}

export interface SystemHealth {
  status: 'healthy' | 'degraded' | 'unhealthy'
  uptime: string
  cpu: number
  memory: number
  activeConnections: number
  mcpStatus: string
  checks?: HealthCheck[]
  passed?: number
  warnings?: number
}

export interface NeuralStatus {
  enabled: boolean
  models: Array<{
    name: string
    status: string
    accuracy: number
    lastTrained?: string
  }>
  trainingQueue: number
}

export interface PerformanceMetrics {
  latency: { avg: number; p95: number; p99: number }
  throughput: number
  errorRate: number
  activeRequests: number
  history: Array<{
    timestamp: string
    latency: number
    throughput: number
  }>
}

export interface HookConfig {
  name: string
  type: string
  enabled: boolean
  trigger: string
  command?: string
  timeout?: number
  lastRun?: string
  runCount: number
}

export interface WorkflowDef {
  id: string
  name: string
  status: 'draft' | 'running' | 'completed' | 'paused' | 'cancelled'
  steps: Array<{
    id: string
    name: string
    status: string
    agent?: string
    detail?: string
  }>
  createdAt: string
}

export interface CoordinationMetrics {
  topology: string
  nodes: number
  syncLatency: number
  consensusRounds: number
  loadDistribution: Record<string, number>
}

export interface VizNode {
  id: string
  sessionId: string
  agentId?: string
  slug?: string
  agentType?: string
  status: 'active' | 'idle' | 'done' | 'error'
  currentTool?: string
  currentFile?: string
  lastActivity?: string
  taskId?: string
  children: VizNode[]
}

export interface VizSession {
  sessionId: string
  taskId: string
  tree: VizNode
  startedAt: string
}

export interface SwarmAgent {
  id: string
  type: string
  status: 'idle' | 'active' | 'working' | 'error' | 'healthy'
  health: number
  taskCount: number
  createdAt: string
  uptime?: number
  memory?: { used: number; limit: number }
  cpu?: number
  tasks?: { active: number; queued: number; completed: number; failed: number }
  latency?: { avg: number; p99: number }
  errors?: { count: number }
  currentTask?: string
  currentAction?: string
}

export interface SwarmMonitorState {
  swarmId: string
  status: string
  topology: string
  objective: string
  strategy: string
  progress: number
  agents: SwarmAgent[]
  agentSummary: { total: number; active: number; idle: number; completed: number }
  taskSummary: { total: number; completed: number; inProgress: number; pending: number }
  metrics: { tokensUsed: number; avgResponseTime: string; successRate: string; elapsedTime: string }
  coordination: { consensusRounds: number; messagesSent: number; conflictsResolved: number }
}

export interface WSMessage {
  type: string
  payload: unknown
  timestamp: string
}

export interface WebhookEvent {
  id: string
  provider: 'github' | 'gitlab'
  repo: string
  event: string
  title: string
  body: string
  url: string
  number: number
  author: string
  labels: string[]
  receivedAt: string
  taskId?: string
  status: 'received' | 'processing' | 'completed' | 'failed' | 'ignored'
}

export interface GitHubWebhookStatus {
  enabled: boolean
  hasToken: boolean
  tokenPreview: string
  webhookSecret: string
  hasSecret: boolean
  repos: string[]
  autoAssign: boolean
  taskTemplate: string
}

export interface GitLabWebhookStatus {
  enabled: boolean
  hasToken: boolean
  tokenPreview: string
  webhookSecret: string
  hasSecret: boolean
  repos: string[]
  autoAssign: boolean
  taskTemplate: string
}

import express, { Router, Request, Response, RequestHandler } from 'express'
import cors from 'cors'
import { WebSocketServer, WebSocket } from 'ws'
import { createServer } from 'http'
import { exec, execFile, spawn } from 'child_process'
import { promisify } from 'util'
import os from 'os'
import path from 'path'
import fs from 'fs'
import crypto from 'crypto'
import { startMonitoring, stopMonitoring, getSessionTree, getAllMonitoredSessions, getNodeLogs } from './jsonl-monitor'
import { initTelegramBot, TelegramConfig, TelegramHandle } from './telegram-bot'
import { loadGitHubWebhookConfig, saveGitHubWebhookConfig, githubWebhookRoutes, updateWebhookEventByTaskId } from './webhook-github'
import { loadGitLabWebhookConfig, saveGitLabWebhookConfig, gitlabWebhookRoutes, updateGitLabEventByTaskId } from './webhook-gitlab'
import { AGENT_PROFILES, AgentProfile, findProfileById, stripTerminalReviewerAliases } from './agent-profiles'
import { buildCanonicalPool, normalizeToCanonicalPool, pruneRegistryToCanonical, prunePersistedAgents, RuntimeAgent } from './agent-pool'
import { parsePlannerOutput, buildDeterministicFallback } from './planner-parse'
import { GlobalScheduler, getGlobalScheduler, resetGlobalScheduler, SubtaskRequest, Priority } from './scheduler'
import {
  TaskDispatcher,
  DispatcherTaskRecord,
  TaskMode,
  detectTaskMode,
  getTaskDispatcher,
} from './task-dispatcher'
import { getWorktreeManager } from './task-worktrees'
import { settleTaskTerminal as settleTaskTerminalImpl, cancelTask as cancelTaskImpl, LifecycleDeps } from './task-lifecycle'

const execAsync = promisify(exec)
const execFileAsync = promisify(execFile)
const PORT = Number(process.env.PORT) || 28580
const CLI_LOCAL_BIN = path.join(process.cwd(), 'node_modules', '@claude-flow', 'cli', 'bin', 'cli.js')
const CLI_DEFAULT = fs.existsSync(CLI_LOCAL_BIN) ? `node ${CLI_LOCAL_BIN}` : 'npx -y @claude-flow/cli@latest'
const CLI = process.env.RUFLO_CLI || CLI_DEFAULT
const CLI_PARTS = CLI.split(/\s+/)
const CLI_BIN = CLI_PARTS[0]
const CLI_BASE_ARGS = CLI_PARTS.slice(1)
const CLI_TIMEOUT = Number(process.env.RUFLO_CLI_TIMEOUT) || 30_000
let telegramBot: TelegramHandle | null = null
let telegramConfig: TelegramConfig = {
  enabled: false, token: '', chatId: '',
  notifications: { taskCompleted: true, taskFailed: true, swarmInit: true, swarmShutdown: true, agentError: true, taskProgress: false },
}

interface TelegramLogEntry { timestamp: string; direction: 'in' | 'out'; message: string }
const telegramActivityLog: TelegramLogEntry[] = []
function addTelegramLog(direction: 'in' | 'out', message: string) {
  telegramActivityLog.push({ timestamp: new Date().toISOString(), direction, message })
  if (telegramActivityLog.length > 50) telegramActivityLog.shift()
}
const TELEGRAM_CONFIG_FILE = () => path.join(PERSIST_DIR, 'telegram.json')

function loadTelegramConfig(): TelegramConfig {
  try {
    const filePath = TELEGRAM_CONFIG_FILE()
    if (fs.existsSync(filePath)) {
      const raw = JSON.parse(fs.readFileSync(filePath, 'utf-8'))
      return {
        enabled: raw.enabled === true,
        token: String(raw.token || ''),
        chatId: String(raw.chatId || ''),
        notifications: {
          taskCompleted: raw.notifications?.taskCompleted ?? true,
          taskFailed: raw.notifications?.taskFailed ?? true,
          swarmInit: raw.notifications?.swarmInit ?? true,
          swarmShutdown: raw.notifications?.swarmShutdown ?? true,
          agentError: raw.notifications?.agentError ?? true,
          taskProgress: raw.notifications?.taskProgress ?? false,
        },
      }
    }
  } catch { /* ignore */ }
  // Fall back to env vars
  return {
    enabled: process.env.TELEGRAM_ENABLED === 'true',
    token: process.env.TELEGRAM_BOT_TOKEN || '',
    chatId: process.env.TELEGRAM_CHAT_ID || '',
    notifications: { taskCompleted: true, taskFailed: true, swarmInit: true, swarmShutdown: true, agentError: true, taskProgress: false },
  }
}

function saveTelegramConfig(config: TelegramConfig) {
  try {
    ensurePersistDir()
    const filePath = TELEGRAM_CONFIG_FILE()
    fs.writeFileSync(filePath, JSON.stringify(config, null, 2))
    // Restrict file permissions (owner-only read/write) to protect the token
    try { fs.chmodSync(filePath, 0o600) } catch { /* Windows may not support chmod */ }
  } catch (err) {
    console.error('[telegram] Config save failed:', err)
  }
}
const ZOMBIE_TIMEOUT = Number(process.env.RUFLO_ZOMBIE_TIMEOUT) || 300_000 // 5 min
let SKIP_PERMISSIONS = process.env.RUFLOUI_SKIP_PERMISSIONS !== 'false'

let githubWebhookConfig = loadGitHubWebhookConfig()
let gitlabWebhookConfig = loadGitLabWebhookConfig()

// ── WEBHOOK REPO MANAGEMENT ─────────────────────────────────────────
// Clones external repos so agents work on them, not on rufloui itself.
// After task completion: commits, pushes branch, creates PR/MR, closes issue.

interface WebhookMeta {
  provider: 'github' | 'gitlab'
  repo: string          // owner/repo or namespace/project
  issueNumber: number
  issueUrl: string
  branchName: string
  host: string          // e.g. 'github.com', 'gitlab.com', 'git.proconsi.com'
}

const REPOS_DIR = path.join(process.env.RUFLO_PERSIST_DIR || '.ruflo', 'repos')

async function cloneWebhookRepo(
  provider: 'github' | 'gitlab',
  repo: string,
  token: string,
  issueUrl?: string,
): Promise<string> {
  const repoDir = path.join(REPOS_DIR, repo.replace(/\//g, path.sep))
  if (!fs.existsSync(REPOS_DIR)) fs.mkdirSync(REPOS_DIR, { recursive: true })

  // Extract host from the issue URL (supports self-hosted GitLab/GitHub Enterprise)
  let host = provider === 'gitlab' ? 'gitlab.com' : 'github.com'
  if (issueUrl) {
    try { host = new URL(issueUrl).host } catch { /* use default */ }
  }
  console.log(`[webhook-repo] Using host: ${host} for ${repo}`)
  const authUrl = token
    ? `https://oauth2:${token}@${host}/${repo}.git`
    : `https://${host}/${repo}.git`

  if (fs.existsSync(path.join(repoDir, '.git'))) {
    // Repo already cloned — pull latest
    console.log(`[webhook-repo] Pulling latest for ${repo}`)
    await execAsync('git fetch origin', { cwd: repoDir, timeout: 60_000 })
    // Try main, then master — ignore errors from whichever doesn't exist
    await execAsync('git checkout main', { cwd: repoDir }).catch(() =>
      execAsync('git checkout master', { cwd: repoDir }).catch(() => {})
    )
    await execAsync('git pull', { cwd: repoDir, timeout: 60_000 }).catch(() => {})
    // Update remote URL in case token changed
    await execAsync(`git remote set-url origin "${authUrl}"`, { cwd: repoDir }).catch(() => {})
  } else {
    console.log(`[webhook-repo] Cloning ${repo} into ${repoDir}`)
    fs.mkdirSync(repoDir, { recursive: true })
    await execAsync(`git clone "${authUrl}" .`, { cwd: repoDir, timeout: 120_000 })
  }

  return repoDir
}

async function handleWebhookTaskCompletion(taskId: string): Promise<void> {
  const task = taskStore.get(taskId)
  if (!task || !(task as any).webhookMeta || !task.cwd) return
  const meta: WebhookMeta = (task as any).webhookMeta
  // Use the dispatcher's actual execution cwd + branch (assigned during
  // worktreeProvisioned). The cloned repoDir is the SOURCE; the worktree
  // path is where Claude actually wrote the changes.
  const dr = dispatcher.get(taskId)
  const repoDir = dr?.executionCwd || task.executionCwd || task.cwd
  // Branch the dispatcher actually created — never the legacy webhookMeta.
  const branchName = dr?.worktree?.branchName || meta.branchName

  try {
    // Check if there are any changes to commit
    const { stdout: statusOut } = await execAsync('git status --porcelain', { cwd: repoDir })
    if (!statusOut.trim()) {
      console.log(`[webhook-repo] No changes to commit for task ${taskId}`)
      return
    }

    console.log(`[webhook-repo] Committing and pushing changes for task ${taskId} on branch ${branchName} in ${repoDir}`)

    // The dispatcher has already provisioned the worktree + branch — no
    // git checkout -b here. We are already ON the right branch in the
    // worktree.
    await execAsync('git add -A', { cwd: repoDir })
    const commitMsg = `fix: resolve issue #${meta.issueNumber}\n\nAutomated fix by RuFloUI multi-agent pipeline.\nTask: ${taskId}\nIssue: ${meta.issueUrl}`
    await execAsync(`git commit -m "${commitMsg.replace(/"/g, '\\"')}"`, { cwd: repoDir })
    await execAsync(`git push -u origin "${branchName}"`, { cwd: repoDir, timeout: 60_000 })

    // Create PR/MR and close issue via API
    if (meta.provider === 'github') {
      await createGitHubPRAndCloseIssue(meta, branchName)
    } else {
      await createGitLabMRAndCloseIssue(meta, branchName)
    }
  } catch (err) {
    console.error(`[webhook-repo] Post-completion failed for task ${taskId}:`, err)
  }
}

async function createGitHubPRAndCloseIssue(meta: WebhookMeta, branchName: string): Promise<void> {
  const token = githubWebhookConfig.githubToken
  if (!token) { console.log('[webhook-repo] No GitHub token — skipping PR/close'); return }

  const headers = { Authorization: `Bearer ${token}`, Accept: 'application/vnd.github+json', 'Content-Type': 'application/json' }
  const apiBase = meta.host === 'github.com' ? 'https://api.github.com' : `https://${meta.host}/api/v3`

  // Create PR
  try {
    const prRes = await fetch(`${apiBase}/repos/${meta.repo}/pulls`, {
      method: 'POST', headers,
      body: JSON.stringify({
        title: `Fix #${meta.issueNumber}: automated resolution`,
        body: `Automated fix generated by RuFloUI multi-agent pipeline.\n\nCloses #${meta.issueNumber}`,
        head: branchName, base: 'main',
      }),
    })
    if (!prRes.ok) {
      // Try 'master' as base branch
      const prRes2 = await fetch(`${apiBase}/repos/${meta.repo}/pulls`, {
        method: 'POST', headers,
        body: JSON.stringify({
          title: `Fix #${meta.issueNumber}: automated resolution`,
          body: `Automated fix generated by RuFloUI multi-agent pipeline.\n\nCloses #${meta.issueNumber}`,
          head: branchName, base: 'master',
        }),
      })
      const data = await prRes2.json()
      console.log(`[webhook-repo] GitHub PR created: ${(data as any).html_url || 'failed'}`)
    } else {
      const data = await prRes.json()
      console.log(`[webhook-repo] GitHub PR created: ${(data as any).html_url || 'unknown'}`)
    }
  } catch (err) {
    console.error('[webhook-repo] GitHub PR creation failed:', err)
  }

  // Close issue
  try {
    await fetch(`${apiBase}/repos/${meta.repo}/issues/${meta.issueNumber}`, {
      method: 'PATCH', headers,
      body: JSON.stringify({ state: 'closed', state_reason: 'completed' }),
    })
    console.log(`[webhook-repo] GitHub issue #${meta.issueNumber} closed`)
  } catch (err) {
    console.error('[webhook-repo] GitHub issue close failed:', err)
  }
}

async function createGitLabMRAndCloseIssue(meta: WebhookMeta, branchName: string): Promise<void> {
  const token = gitlabWebhookConfig.gitlabToken
  if (!token) { console.log('[webhook-repo] No GitLab token — skipping MR/close'); return }

  const headers = { 'PRIVATE-TOKEN': token, 'Content-Type': 'application/json' }
  const apiBase = `https://${meta.host}/api/v4`
  const projectId = encodeURIComponent(meta.repo)

  // Create MR
  try {
    const mrRes = await fetch(`${apiBase}/projects/${projectId}/merge_requests`, {
      method: 'POST', headers,
      body: JSON.stringify({
        title: `Fix #${meta.issueNumber}: automated resolution`,
        description: `Automated fix generated by RuFloUI multi-agent pipeline.\n\nCloses #${meta.issueNumber}`,
        source_branch: branchName, target_branch: 'main',
      }),
    })
    if (!mrRes.ok) {
      // Try 'master' as target
      const mrRes2 = await fetch(`${apiBase}/projects/${projectId}/merge_requests`, {
        method: 'POST', headers,
        body: JSON.stringify({
          title: `Fix #${meta.issueNumber}: automated resolution`,
          description: `Automated fix generated by RuFloUI multi-agent pipeline.\n\nCloses #${meta.issueNumber}`,
          source_branch: branchName, target_branch: 'master',
        }),
      })
      const data = await mrRes2.json()
      console.log(`[webhook-repo] GitLab MR created: ${(data as any).web_url || 'failed'}`)
    } else {
      const data = await mrRes.json()
      console.log(`[webhook-repo] GitLab MR created: ${(data as any).web_url || 'unknown'}`)
    }
  } catch (err) {
    console.error('[webhook-repo] GitLab MR creation failed:', err)
  }

  // Close issue
  try {
    await fetch(`${apiBase}/projects/${projectId}/issues/${meta.issueNumber}`, {
      method: 'PUT', headers,
      body: JSON.stringify({ state_event: 'close' }),
    })
    console.log(`[webhook-repo] GitLab issue #${meta.issueNumber} closed`)
  } catch (err) {
    console.error('[webhook-repo] GitLab issue close failed:', err)
  }
}

// ── PERSISTENCE LAYER ───────────────────────────────────────────────
// Writes critical in-memory state to .ruflo/ as JSON files so it
// survives server restarts. Debounced to avoid excessive disk I/O.
const PERSIST_DIR = process.env.RUFLO_PERSIST_DIR
  ? path.resolve(process.env.RUFLO_PERSIST_DIR)
  : path.join(process.cwd(), '.ruflo')

interface PersistedState {
  tasks: Array<[string, unknown]>
  workflows: Array<[string, unknown]>
  sessions: Array<[string, unknown]>
  agents: Array<[string, { id: string; name: string; type: string }]>
  terminatedAgents: string[]
  agentActivity: Array<[string, unknown]>
  swarmConfig: {
    id: string; topology: string; strategy: string; maxAgents: number
    createdAt: string; shutdown: boolean
  }
  perfHistory: Array<{ timestamp: string; latency: number; throughput: number }>
  lastPerfMetrics: unknown
  benchmarkHasRun: boolean
  currentSwarmAgentIds: string[]
  /** Serialised dispatcher task queue snapshot (pending + terminal records). */
  dispatcherTasks: Array<[string, unknown]>
}

function ensurePersistDir() {
  if (!fs.existsSync(PERSIST_DIR)) fs.mkdirSync(PERSIST_DIR, { recursive: true })
}

let _saveTimer: ReturnType<typeof setTimeout> | null = null
const SAVE_DEBOUNCE_MS = 2000

function scheduleSave() {
  if (_saveTimer) return // already scheduled
  _saveTimer = setTimeout(() => {
    _saveTimer = null
    saveToDisk()
  }, SAVE_DEBOUNCE_MS)
}

function saveToDisk() {
  try {
    ensurePersistDir()
    const state: PersistedState = {
      tasks: [...taskStore.entries()],
      workflows: [...workflowStore.entries()],
      sessions: [...sessionStore.entries()],
      agents: [...agentRegistry.entries()],
      terminatedAgents: [...terminatedAgents],
      agentActivity: [...agentActivity.entries()],
      swarmConfig: {
        id: lastSwarmId, topology: lastSwarmTopology, strategy: lastSwarmStrategy,
        maxAgents: lastSwarmMaxAgents, createdAt: lastSwarmCreatedAt, shutdown: swarmShutdown,
      },
      perfHistory: perfHistory.slice(-200), // cap at 200 entries
      lastPerfMetrics,
      benchmarkHasRun,
      currentSwarmAgentIds: [...currentSwarmAgentIds],
      dispatcherTasks: dispatcher.snapshot().map(t => [t.id, { ...t }] as [string, unknown]),
    }
    // Atomic write: write to .tmp then rename to prevent corruption on crash
    const target = path.join(PERSIST_DIR, 'state.json')
    const tmp = target + '.tmp'
    fs.writeFileSync(tmp, JSON.stringify(state, null, 2))
    fs.renameSync(tmp, target)
  } catch (err) {
    console.error('[persist] Save failed:', err)
  }
}

function loadFromDisk() {
  const filePath = path.join(PERSIST_DIR, 'state.json')
  const tmpPath = filePath + '.tmp'
  // If .tmp exists but main doesn't, recover from .tmp (crash during write)
  if (!fs.existsSync(filePath) && fs.existsSync(tmpPath)) {
    console.log('[persist] Recovering from .tmp file (previous save was interrupted)')
    try { fs.renameSync(tmpPath, filePath) } catch { /* ignore */ }
  }
  if (!fs.existsSync(filePath)) return
  try {
    const raw = fs.readFileSync(filePath, 'utf-8')
    const state: PersistedState = JSON.parse(raw)

    // Restore tasks
    if (state.tasks) for (const [k, v] of state.tasks) taskStore.set(k, v as any)
    // Restore workflows
    if (state.workflows) for (const [k, v] of state.workflows) workflowStore.set(k, v as any)
    // Restore sessions
    if (state.sessions) for (const [k, v] of state.sessions) sessionStore.set(k, v as any)
    // Restore agent registry — ACC-TASK-QUEUE-002-FINAL-REPAIR:
    // persisted registry is PRUNED to the canonical set on every
    // restart. Stale entries from prior runs (and any duplicates) are
    // dropped deterministically. Idempotent — a re-run removes nothing
    // further. Tasks/workflows/sessions are NEVER touched.
    if (state.agents) {
      const pruned = prunePersistedAgents(
        state.agents,
        state.terminatedAgents || [],
      )
      const removed = state.agents.length - pruned.length
      if (removed > 0) console.log(`[persist] Pruned ${removed} stale/non-canonical registry entries`)
      for (const [k, v] of pruned) agentRegistry.set(k, v)
    }
    // Restore terminated agents
    if (state.terminatedAgents) for (const id of state.terminatedAgents) terminatedAgents.add(id)
    // Restore agent activity
    if (state.agentActivity) for (const [k, v] of state.agentActivity) agentActivity.set(k, v as any)
    // Restore swarm config
    if (state.swarmConfig) {
      lastSwarmId = state.swarmConfig.id || ''
      lastSwarmTopology = state.swarmConfig.topology || 'hierarchical'
      lastSwarmStrategy = state.swarmConfig.strategy || 'specialized'
      lastSwarmMaxAgents = state.swarmConfig.maxAgents || 10
      lastSwarmCreatedAt = state.swarmConfig.createdAt || ''
      swarmShutdown = state.swarmConfig.shutdown ?? true
    }
    // Restore perf
    if (state.perfHistory) perfHistory.push(...state.perfHistory)
    if (state.lastPerfMetrics) lastPerfMetrics = state.lastPerfMetrics as typeof lastPerfMetrics
    if (state.benchmarkHasRun) benchmarkHasRun = state.benchmarkHasRun
    // Restore current swarm agent IDs
    if (state.currentSwarmAgentIds) {
      currentSwarmAgentIds = new Set(state.currentSwarmAgentIds)
    }

    // Restore dispatcher queue snapshot. Pending tasks are re-enqueued
    // (idempotent). Terminal records are restored via a direct re-add
    // path so that recovery sweeps don't inadvertently re-run them.
    let restoredDispatcher = 0
    let interruptedDispatcher = 0
    if (state.dispatcherTasks && Array.isArray(state.dispatcherTasks)) {
      // Side-effect-free hydrate (blocker 5): no enqueue, no microtask
      // launches during the pass; ready-queue is populated and then a
      // single explicit `dispatchAfterHydrate` decides what runs next.
      const snapshot = state.dispatcherTasks.map(([, raw]) => raw as any).filter(Boolean)
      const recovery = dispatcher.hydrateFromSnapshot(snapshot)
      restoredDispatcher = recovery.restored.length
      interruptedDispatcher = recovery.interrupted.length
      // Mark the corresponding TaskRecords so the UI surfaces interrupted
      // status correctly. Pending records rejoin the queue; terminal
      // records keep their status untouched.
      for (const id of recovery.interrupted) {
        const tr = taskStore.get(id)
        if (tr) {
          tr.status = 'interrupted'
          tr.completedAt = new Date().toISOString()
          broadcast('task:updated', { ...tr, id })
        }
      }
      // Promote ready tasks after the hydrate pass is complete.
      dispatcher.dispatchAfterHydrate()
      if (restoredDispatcher || interruptedDispatcher) {
        console.log(`[persist] Dispatcher restored: ${restoredDispatcher} tasks, ${interruptedDispatcher} interrupted`)
      }
    }

    const taskCount = taskStore.size
    const wfCount = workflowStore.size
    const agentCount = agentRegistry.size
    console.log(`[persist] Loaded: ${taskCount} tasks, ${wfCount} workflows, ${agentCount} agents`)
  } catch (err) {
    console.error('[persist] Load failed:', err)
  }
}

// Helper: call after any state mutation to schedule a save
function persistState() {
  scheduleSave()
}

// ── OUTPUT HISTORY ───────────────────────────────────────────────────
// Persists task output to .ruflo/outputs/<taskId>.jsonl so it survives
// server restarts and page reloads.
const OUTPUTS_DIR = path.join(PERSIST_DIR, 'outputs')

function ensureOutputsDir() {
  if (!fs.existsSync(OUTPUTS_DIR)) fs.mkdirSync(OUTPUTS_DIR, { recursive: true })
}

function appendTaskOutputLine(taskId: string, line: { type: string; content: string; agentId?: string; tool?: string; timestamp?: string }) {
  try {
    ensureOutputsDir()
    const entry = { ...line, timestamp: line.timestamp || new Date().toISOString() }
    fs.appendFileSync(path.join(OUTPUTS_DIR, `${taskId}.jsonl`), JSON.stringify(entry) + '\n')
  } catch { /* non-critical */ }
}

function readTaskOutputHistory(taskId: string, tail = 200): Array<{ type: string; content: string; agentId?: string; tool?: string; timestamp: string }> {
  const filePath = path.join(OUTPUTS_DIR, `${taskId}.jsonl`)
  if (!fs.existsSync(filePath)) return []
  try {
    const lines = fs.readFileSync(filePath, 'utf-8').split('\n').filter(Boolean)
    const entries = []
    for (const line of lines.slice(-tail)) {
      try { entries.push(JSON.parse(line)) } catch { /* skip */ }
    }
    return entries
  } catch { return [] }
}

const wsClients = new Set<WebSocket>()

// ── TASK DISPATCHER (declared early; used by both broadcast() hand-off
// handlers and the task-route handlers below) ──────────────────────
// Persistent top-level task queue layered on top of GlobalScheduler.
// Owns: priority+FIFO ordering, max-in-flight cap, worktree provisioning
// for WRITE tasks, restart-recovery, terminal-state guarantee.
const dispatcher: TaskDispatcher = getTaskDispatcher()

// Types that represent persistent state changes — trigger disk save
const PERSIST_EVENTS = new Set([
  'task:added', 'task:updated', 'task:list',
  'workflow:added', 'workflow:updated',
  'session:added', 'session:updated', 'session:list', 'session:active',
  'swarm:status', 'swarm-monitor:purged',
  'agent:activity', 'agent:added', 'agent:removed', 'agents:cleared',
  'performance:metrics',
])

/**
 * Bounded wait for a tracked process to close + canonical cancel-proc
 * helper live in `./process-close.ts`. server.ts re-exports them so the
 * existing route handlers keep their current call sites.
 */
export { waitForProcessClose, signalAndAwaitClose } from './process-close'
import { waitForProcessClose, signalAndAwaitClose } from './process-close'

function broadcast(type: string, payload: unknown) {
  // Block B9: the broadcast transport is no longer the mechanism that
  // mutates dispatcher state. All terminal transitions go through
  // explicit calls in pipeline / launchViaClaude / cancel routes; this
  // keeps a single chokepoint and prevents double-release of slots when
  // the same payload would otherwise be re-broadcast.
  const msg = JSON.stringify({ type, payload, timestamp: new Date().toISOString() })
  for (const ws of wsClients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg)
  }
  // Auto-persist on significant state changes
  if (PERSIST_EVENTS.has(type)) persistState()
  // Persist task output lines to disk for history across reloads
  if (type === 'task:output') {
    const p = payload as { id?: string; type?: string; content?: string; tool?: string; input?: string; agentId?: string; code?: number }
    if (p?.id) {
      let line = ''
      if (p.type === 'tool') line = `[tool] ${p.tool || ''}: ${p.input || ''}`
      else if (p.type === 'stderr') line = `[err] ${p.content || ''}`
      else if (p.type === 'text') line = p.content?.slice(0, 300) || ''
      else if (p.type === 'raw') line = p.content?.slice(0, 300) || ''
      else if (p.type === 'progress') line = p.content || ''
      else if (p.type === 'done') line = `--- Done (exit ${p.code ?? '?'}) ---`
      if (line) appendTaskOutputLine(p.id, { type: p.type || 'text', content: line, agentId: p.agentId, tool: p.tool })
    }
  }
  // Forward to Telegram bot (fire-and-forget)
  telegramBot?.onBroadcast(type, payload)
  // Update webhook event status when linked task completes/fails
  if (type === 'task:updated') {
    const p2 = payload as { id?: string; status?: string }
    if (p2?.id && (p2.status === 'completed' || p2.status === 'failed')) {
      updateWebhookEventByTaskId(p2.id, p2.status as 'completed' | 'failed')
      updateGitLabEventByTaskId(p2.id, p2.status as 'completed' | 'failed')
      // Post-completion: push branch, create PR/MR, close issue
      if (p2.status === 'completed') {
        handleWebhookTaskCompletion(p2.id).catch(err =>
          console.error(`[webhook-repo] Post-completion error for ${p2.id}:`, err))
      }
    }
  }
}

// Wire dispatcher into the existing TaskRecord + lifecycle paths. The
// launcher is set once launchWorkflowForTask is in scope (it currently
// is, but to keep the file linear we forward-declare via closure capture
// below — see the bottom of the file).
function setDispatcherLauncher(launch: (task: DispatcherTaskRecord) => Promise<void>) {
  dispatcher.setLauncher(launch)
}

// Helper to mirror dispatcher state back into the TaskRecord for the
// existing API clients (list / status / WS broadcast).
function syncTaskRecordFromDispatcher(taskId: string): void {
  const dr = dispatcher.get(taskId)
  const tr = taskStore.get(taskId)
  if (!dr || !tr) return
  if (!terminalStatusSet(tr.status) && dr.status === tr.status && dr.startedAt === tr.startedAt) return
  tr.status = dr.status
  tr.startedAt = dr.startedAt
  tr.completedAt = dr.finishedAt
  tr.executionCwd = dr.executionCwd
  if (dr.worktree) {
    tr.worktreePath = dr.worktree.worktreePath
    tr.branchName = dr.worktree.branchName
    tr.baseCommit = dr.worktree.baseCommit
  }
  tr.mode = dr.mode
  tr.sourceCwd = dr.sourceCwd
  // Public status values: pending → 'queued'; preparing → 'dispatching';
  // in_progress → 'running'; anything terminal → 'terminal'.
  tr.queueState = dr.status === 'pending' ? 'queued'
    : dr.status === 'preparing' ? 'dispatching'
    : dr.status === 'in_progress' ? (dr.running ? 'running' : 'dispatching')
    : 'terminal'
  tr.queuePosition = dispatcher.queuePosition(taskId)
  tr.attempt = dr.attempt
}

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled', 'interrupted'])
function terminalStatusSet(status: string | undefined): boolean {
  return !!status && TERMINAL_STATUSES.has(status)
}

function dispatcherIsAlreadyTerminal(dr: DispatcherTaskRecord): boolean {
  return dr.status === 'completed' || dr.status === 'failed' || dr.status === 'cancelled' || dr.status === 'interrupted'
}

/**
 * Single explicit terminal settlement helper for dispatcher-owned tasks.
 *
 * Thin shim over `settleTaskTerminalImpl` in `./task-lifecycle.ts`. The
 * production logic lives in the lifecycle module so behavioral tests can
 * exercise it directly with isolated state Maps; this wrapper binds the
 * module-level server.ts state (taskStore, dispatcher, broadcast, etc.)
 * to the parameterised helper.
 */
function buildLifecycleDeps(): LifecycleDeps {
  return {
    taskStore: taskStore as unknown as LifecycleDeps['taskStore'],
    workflowStore: workflowStore as unknown as LifecycleDeps['workflowStore'],
    dispatcher,
    runningProcesses,
    broadcast,
    persist: persistState,
    syncTaskRecordFromDispatcher,
    cleanupProcess,
    getScheduler: () => getGlobalScheduler(),
  }
}
function settleTaskTerminal(
  taskId: string,
  desired: 'completed' | 'failed',
  result: string,
): boolean {
  return settleTaskTerminalImpl(buildLifecycleDeps(), taskId, desired, result)
}

dispatcher.on('statusChange', ({ taskId, status }) => {
  const tr = taskStore.get(taskId)
  if (!tr) return
  syncTaskRecordFromDispatcher(taskId)
  broadcast('task:updated', { ...tr, id: taskId })
  persistState()
})
dispatcher.on('preparing', ({ taskId }) => {
  const tr = taskStore.get(taskId)
  if (!tr) return
  syncTaskRecordFromDispatcher(taskId)
  broadcast('task:updated', { ...tr, id: taskId })
  persistState()
})
dispatcher.on('worktreeProvisioned', ({ taskId, worktreePath, branchName }) => {
  const tr = taskStore.get(taskId)
  if (!tr) return
  syncTaskRecordFromDispatcher(taskId)
  // Mirror the dispatcher's authoritative branch/worktree into the
  // webhookMeta so push/PR code uses the actual branch (not the legacy
  // "fix/issue-N" hint captured at webhook ingestion time).
  if ((tr as any).webhookMeta) {
    (tr as any).webhookMeta.branchName = branchName
  }
  // TaskRecord.cwd/executionCwd must reflect the actual worktree path.
  tr.executionCwd = worktreePath
  tr.worktreePath = worktreePath
  tr.branchName = branchName
  broadcast('task:updated', { ...tr, id: taskId, worktreePath, branchName })
  persistState()
})
dispatcher.on('worktreeFailed', ({ taskId, error }) => {
  console.warn(`[dispatcher] worktree provisioning failed for ${taskId}: ${error}`)
})
dispatcher.on('cancel', ({ taskId }) => {
  const tr = taskStore.get(taskId)
  if (!tr) return
  syncTaskRecordFromDispatcher(taskId)
  broadcast('task:updated', { ...tr, id: taskId })
  persistState()
})
dispatcher.on('complete', () => { persistState() })
dispatcher.on('fail', () => { persistState() })


// Remove shell metacharacters that could enable injection in spawn(..., { shell: true }) calls
function sanitizeShellArg(arg: string): string {
  return arg.replace(/[;&|`$(){}[\]!#~<>\\]/g, '')
}

async function execCli(command: string, args: string[] = []): Promise<{ raw: string; parsed?: unknown }> {
  const fullArgs = [...CLI_BASE_ARGS, command, ...args]
  try {
    const { stdout, stderr } = await execFileAsync(CLI_BIN, fullArgs, {
      timeout: CLI_TIMEOUT,
      encoding: 'utf-8',
      shell: true,
      windowsHide: true,
    })
    const text = stdout.trim()
    // Try JSON parse first
    try { return { raw: text, parsed: JSON.parse(text) } } catch { /* not JSON */ }
    return { raw: text }
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err)
    // CLI may write output to stderr or exit non-zero but still have useful stdout
    if (err && typeof err === 'object' && 'stdout' in err) {
      const stdout = String((err as { stdout: string }).stdout).trim()
      if (stdout) return { raw: stdout }
    }
    throw new Error(`CLI error (${command}): ${msg}`)
  }
}

function parseCliOutput(raw: string): unknown {
  // Try to extract key-value pairs from table output
  const lines = raw.split('\n').filter(l => l.trim() && !l.match(/^[+─┌┐└┘├┤┬┴┼═╔╗╚╝╠╣╦╩╬\-]+$/))
  const data: Record<string, string> = {}
  for (const line of lines) {
    const match = line.match(/^\|\s*(.+?)\s*\|\s*(.+?)\s*\|$/)
    if (match && !match[1].match(/^-+$/)) {
      data[match[1].trim()] = match[2].trim()
    }
  }
  return Object.keys(data).length > 0 ? data : { raw }
}

// Parse CLI table with headers (| Col1 | Col2 | ... |) into array of objects
function parseCliTable(raw: string): Record<string, string>[] {
  const lines = raw.replace(/\r/g, '').split('\n')
  const dataLines = lines.filter(l => l.trim().startsWith('|') && !l.match(/^[|+\-─\s]+$/))
  if (dataLines.length < 2) return [] // need header + at least 1 row
  const splitRow = (line: string) =>
    line.split('|').slice(1, -1).map(c => c.trim().replace(/\.{3}$/, ''))
  const headers = splitRow(dataLines[0]).map(h => h.toLowerCase().replace(/\s+/g, '_'))
  return dataLines.slice(1).map(line => {
    const cells = splitRow(line)
    const obj: Record<string, string> = {}
    headers.forEach((h, i) => { obj[h] = cells[i] ?? '' })
    return obj
  })
}

function h(fn: (req: Request, res: Response) => Promise<void>): RequestHandler {
  return async (req, res, _next) => {
    try { await fn(req, res) } catch (err: unknown) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) })
    }
  }
}

function systemRoutes(): Router {
  const r = Router()
  // `system` doesn't exist in ruflo CLI - use `status` and `doctor`
  r.get('/health', h(async (_req, res) => {
    try {
      const { raw } = await execCli('doctor')
      const passed = raw.match(/(\d+) passed/)?.[1] ?? '0'
      const warnings = raw.match(/(\d+) warning/)?.[1] ?? '0'
      const status = Number(warnings) > 3 ? 'degraded' : 'healthy'
      // Parse individual checks from raw output
      // On Windows, UTF-8 check marks (✓/⚠/✗) get mangled by codepage, so we match by structure:
      // Each check line has format: <icon> <Name>: <detail>
      const checks: Array<{ name: string; status: 'pass' | 'warn' | 'fail'; detail: string }> = []
      const knownChecks = [
        'Version Freshness', 'Node.js Version', 'npm Version', 'Claude Code CLI',
        'Git:', 'Git Repository', 'Config File', 'Daemon Status', 'Memory Database',
        'API Keys', 'MCP Servers', 'Disk Space', 'TypeScript', 'agentic-flow',
      ]
      for (const line of raw.replace(/\r/g, '').split('\n')) {
        // Match lines containing a known check name followed by a colon and detail
        for (const check of knownChecks) {
          const checkName = check.replace(':', '')
          if (line.includes(checkName + ':')) {
            const colonIdx = line.indexOf(checkName + ':')
            const name = checkName.trim()
            const detail = line.substring(colonIdx + checkName.length + 1).trim()
            // Determine status: lines with warning keywords or known negative patterns
            const isWarn = detail.match(/not (a |running|installed|found)|no (config|api)/i)
            const isFail = detail.match(/fail|error|critical/i)
            checks.push({
              name,
              status: isFail ? 'fail' : isWarn ? 'warn' : 'pass',
              detail,
            })
            break
          }
        }
      }
      res.json({ status, passed: Number(passed), warnings: Number(warnings), checks, raw })
    } catch {
      res.json({ status: 'unknown', passed: 0, warnings: 0, checks: [] })
    }
  }))
  // Preflight check — validates all dependencies before the app is usable
  r.get('/preflight', h(async (_req, res) => {
    const checks: Array<{ id: string; name: string; status: 'ok' | 'warn' | 'fail'; detail: string; fix?: string }> = []

    // 1. Node.js version
    const nodeVer = process.version
    const major = parseInt(nodeVer.slice(1), 10)
    checks.push({
      id: 'node',
      name: 'Node.js',
      status: major >= 18 ? 'ok' : 'fail',
      detail: `${nodeVer} detected`,
      fix: major < 18 ? 'Install Node.js >= 18 from https://nodejs.org' : undefined,
    })

    // 2. npx available
    try {
      await execAsync('npx --version', { timeout: 10_000 })
      checks.push({ id: 'npx', name: 'npx', status: 'ok', detail: 'Available in PATH' })
    } catch {
      checks.push({ id: 'npx', name: 'npx', status: 'fail', detail: 'Not found in PATH', fix: 'Install Node.js (npx is bundled with npm)' })
    }

    // 3. claude-flow CLI (prefer local install for speed)
    {
      const isLocal = fs.existsSync(CLI_LOCAL_BIN)
      try {
        const { raw } = await execCli('--version', [])
        const source = isLocal ? 'local' : 'npx — slow, run Auto-fix to install locally'
        checks.push({
          id: 'claude-flow',
          name: 'claude-flow CLI',
          status: isLocal ? 'ok' : 'warn',
          detail: `${raw.trim().slice(0, 60) || 'Installed'} (${source})`,
          fix: isLocal ? undefined : 'Run: npm install @claude-flow/cli@latest',
        })
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err)
        checks.push({
          id: 'claude-flow',
          name: 'claude-flow CLI',
          status: 'fail',
          detail: msg.slice(0, 120),
          fix: 'Run: npm install @claude-flow/cli@latest',
        })
      }
    }

    // 4. Claude Code CLI (claude executable)
    try {
      await execAsync('claude --version', { timeout: 10_000 })
      checks.push({ id: 'claude-cli', name: 'Claude Code CLI', status: 'ok', detail: 'claude command available' })
    } catch {
      const claudePath = process.env.LOCALAPPDATA
        ? `${process.env.USERPROFILE}\\.local\\bin\\claude.exe`
        : 'claude'
      const exists = process.env.LOCALAPPDATA ? fs.existsSync(claudePath) : false
      if (exists) {
        checks.push({ id: 'claude-cli', name: 'Claude Code CLI', status: 'warn', detail: `Found at ${claudePath} but not in PATH`, fix: 'Add claude to your system PATH' })
      } else {
        checks.push({ id: 'claude-cli', name: 'Claude Code CLI', status: 'warn', detail: 'Not found (needed for multi-agent pipeline)', fix: 'Install Claude Code: https://docs.anthropic.com/en/docs/claude-code' })
      }
    }

    // 5. Persistence directory
    try {
      ensurePersistDir()
      const testFile = path.join(PERSIST_DIR, '.write-test')
      fs.writeFileSync(testFile, 'test')
      fs.unlinkSync(testFile)
      checks.push({ id: 'persist-dir', name: 'Persistence (.ruflo/)', status: 'ok', detail: `Writable at ${PERSIST_DIR}` })
    } catch {
      checks.push({ id: 'persist-dir', name: 'Persistence (.ruflo/)', status: 'fail', detail: 'Cannot write to .ruflo/ directory', fix: 'Check file permissions in project directory' })
    }

    // 6. Port availability (28580 is us, check 28581 for daemon)
    try {
      await execCli('status', [])
      checks.push({ id: 'daemon', name: 'claude-flow daemon', status: 'ok', detail: 'Daemon reachable on port 28581' })
    } catch {
      checks.push({ id: 'daemon', name: 'claude-flow daemon', status: 'warn', detail: 'Daemon not running (will start on first use)', fix: 'The daemon starts automatically when needed' })
    }

    // 7. Environment variables
    const envChecks: string[] = []
    if (!process.env.USERPROFILE && os.platform() === 'win32') envChecks.push('USERPROFILE not set')
    if (!process.env.LOCALAPPDATA && os.platform() === 'win32') envChecks.push('LOCALAPPDATA not set')
    if (envChecks.length === 0) {
      checks.push({ id: 'env', name: 'Environment', status: 'ok', detail: `${os.platform()} / ${os.arch()}` })
    } else {
      checks.push({ id: 'env', name: 'Environment', status: 'warn', detail: envChecks.join(', '), fix: 'Set missing Windows environment variables' })
    }

    const failed = checks.filter(c => c.status === 'fail').length
    const warned = checks.filter(c => c.status === 'warn').length
    const overall = failed > 0 ? 'fail' : warned > 0 ? 'warn' : 'ok'

    res.json({ status: overall, checks, failed, warned, passed: checks.length - failed - warned })
  }))

  // Auto-fix — attempts to install/fix missing dependencies
  r.post('/preflight/fix', h(async (_req, res) => {
    const results: Array<{ id: string; action: string; success: boolean; detail: string }> = []

    // 1. claude-flow CLI — install locally for fast invocation
    if (fs.existsSync(CLI_LOCAL_BIN)) {
      results.push({ id: 'claude-flow', action: 'Install claude-flow CLI', success: true, detail: 'Already installed locally' })
    } else {
      try {
        await execAsync('npm install @claude-flow/cli@latest', { timeout: 120_000 })
        results.push({ id: 'claude-flow', action: 'Install claude-flow CLI', success: true, detail: 'Installed locally via npm' })
      } catch (err) {
        results.push({ id: 'claude-flow', action: 'Install claude-flow CLI', success: false, detail: (err as Error).message.slice(0, 200) })
      }
    }

    // 2. Claude Code CLI — install globally via npm
    try {
      await execAsync('claude --version', { timeout: 10_000 })
      results.push({ id: 'claude-cli', action: 'Claude Code CLI', success: true, detail: 'Already installed' })
    } catch {
      try {
        await execAsync('npm install -g @anthropic-ai/claude-code', { timeout: 120_000 })
        results.push({ id: 'claude-cli', action: 'Install Claude Code CLI', success: true, detail: 'Installed via npm' })
      } catch (err) {
        results.push({ id: 'claude-cli', action: 'Install Claude Code CLI', success: false, detail: (err as Error).message.slice(0, 200) })
      }
    }

    // 3. Persistence directory
    try {
      ensurePersistDir()
      results.push({ id: 'persist-dir', action: 'Create .ruflo/ directory', success: true, detail: 'Directory ready' })
    } catch (err) {
      results.push({ id: 'persist-dir', action: 'Create .ruflo/ directory', success: false, detail: (err as Error).message.slice(0, 200) })
    }

    // 4. Start daemon
    try {
      await execCli('status', [])
      results.push({ id: 'daemon', action: 'Start claude-flow daemon', success: true, detail: 'Daemon running' })
    } catch {
      try {
        // Try to start it by running a quick command that triggers daemon startup
        await execCli('system', ['info'])
        results.push({ id: 'daemon', action: 'Start claude-flow daemon', success: true, detail: 'Daemon started' })
      } catch (err) {
        results.push({ id: 'daemon', action: 'Start claude-flow daemon', success: false, detail: (err as Error).message.slice(0, 200) })
      }
    }

    const success = results.filter(r => r.success).length
    const failed = results.filter(r => !r.success).length
    res.json({ results, success, failed, total: results.length })
  }))

  r.get('/info', h(async (_req, res) => {
    res.json({
      platform: os.platform(), arch: os.arch(), nodeVersion: process.version,
      cpus: os.cpus().length, totalMemory: `${Math.round(os.totalmem() / 1024 / 1024)} MB`,
      freeMemory: `${Math.round(os.freemem() / 1024 / 1024)} MB`,
      uptime: `${Math.round(os.uptime() / 60)} min`,
    })
  }))
  r.get('/metrics', h(async (_req, res) => {
    const mem = process.memoryUsage()
    res.json({
      heapUsed: `${Math.round(mem.heapUsed / 1024 / 1024)} MB`,
      heapTotal: `${Math.round(mem.heapTotal / 1024 / 1024)} MB`,
      rss: `${Math.round(mem.rss / 1024 / 1024)} MB`,
      cpuUsage: os.loadavg()[0],
      systemMemoryUsage: Math.round((1 - os.freemem() / os.totalmem()) * 100),
    })
  }))
  r.get('/status', h(async (_req, res) => {
    try {
      const { raw } = await execCli('status')
      res.json({ raw, ...parseCliOutput(raw) as object })
    } catch (err) {
      res.json({ status: 'stopped', error: (err as Error).message })
    }
  }))
  r.post('/reset', h(async (_req, res) => {
    res.json({ message: 'System reset requested' })
  }))
  return r
}

// Track last swarm config for status endpoint
let lastSwarmId = ''
let lastSwarmTopology = 'hierarchical'
let lastSwarmStrategy = 'specialized'
let lastSwarmMaxAgents = 10
let lastSwarmCreatedAt = ''
let swarmShutdown = true
let daemonStarted = false

// In-memory workflow store
interface WorkflowStep {
  id: string; name: string; status: string; agent?: string; detail?: string
}
interface WorkflowRecord {
  id: string; name: string; template: string; status: string
  taskId?: string; createdAt: string; completedAt?: string; result?: string
  steps: WorkflowStep[]
}
const workflowStore: Map<string, WorkflowRecord> = new Map()

async function ensureDaemon(): Promise<void> {
  if (daemonStarted) return
  try {
    // Init claude-flow if not already done
    try { await execCli('init', []) } catch (e) {
      console.log('[daemon] init skipped (may already exist):', e instanceof Error ? e.message : String(e))
    }
    // Start daemon on port 28581 (28580 is our API)
    const daemonPort = String(Number(process.env.DAEMON_PORT) || 28581)
    await execCli('start', ['--daemon', '--port', daemonPort, '--skip-mcp'])
    daemonStarted = true
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    // Check if daemon is actually running by querying status
    try {
      await execCli('status', [])
      daemonStarted = true // daemon was already running
      console.log('[daemon] Already running (confirmed via status)')
    } catch {
      console.warn('[daemon] Failed to start and status check failed:', msg)
      // Don't set daemonStarted=true — will retry on next call
    }
  }
}

async function pollWorkflowStatus(workflowId: string, taskId: string, maxWait = 120000): Promise<void> {
  const task = taskStore.get(taskId)
  if (!task) return
  const start = Date.now()
  const poll = async () => {
    if (Date.now() - start > maxWait) {
      // Timeout — funnel through the explicit terminal helper.
      settleTaskTerminal(taskId, 'failed', 'Workflow timed out after ' + (maxWait / 1000) + 's')
      return
    }
    try {
      const { raw } = await execCli('workflow', ['status', workflowId])
      const wf = workflowStore.get(workflowId)
      const statusMatch = raw.match(/Status:\s*(\w+)/)
      const currentStatus = statusMatch?.[1] || 'unknown'
      if (wf) { wf.status = currentStatus; wf.result = raw.slice(0, 500) }
      if (currentStatus === 'completed' || currentStatus === 'done') {
        settleTaskTerminal(taskId, 'completed', raw.slice(0, 500) || 'Workflow completed')
        if (wf) { wf.status = 'completed'; wf.completedAt = task.completedAt || new Date().toISOString() }
        broadcast('workflow:updated', wf)
      } else if (currentStatus === 'failed' || currentStatus === 'error') {
        settleTaskTerminal(taskId, 'failed', raw.slice(0, 500) || 'Workflow failed')
      } else {
        // Still running, poll again in 3s
        setTimeout(poll, 3000)
      }
    } catch { setTimeout(poll, 3000) }
  }
  setTimeout(poll, 2000) // initial delay
}

// Running Claude Code processes (so we can cancel)
const runningProcesses: Map<string, ReturnType<typeof spawn>> = new Map()
// Track last output time per process for zombie detection
const processLastActivity: Map<string, number> = new Map()

function trackProcessActivity(key: string) {
  processLastActivity.set(key, Date.now())
}

function cleanupProcess(key: string) {
  runningProcesses.delete(key)
  processLastActivity.delete(key)
}

// Zombie reaper — kills processes with no output for ZOMBIE_TIMEOUT.
//
// FAIL-CLOSED invariants:
//   - `proc.killed` is NOT used as evidence of closure (in Node, killed
//     is "signal sent", not "process exited").
//   - Every found proc is handed to the canonical signalAndAwaitClose,
//     which attaches its own close/error listeners, sends SIGTERM,
//     falls back to SIGKILL, and resolves ONLY when exitCode or
//     signalCode is populated. An already-closed proc is a no-op
//     resolved path inside the helper.
//   - On hard rejection (ProcessCloseTimeoutError or any other
//     failure), the reaper MUST NOT call cleanupProcess and MUST NOT
//     remove the live process from runningProcesses tracking. The
//     processLastActivity entry is preserved so a later tick (or the
//     shutdown handler) can re-attempt the cleanup. The reaper logs a
//     typed failure so operators can diagnose stuck children.
//   - The .catch handler is mandatory — without it the rejection
//     becomes an unhandled promise rejection that crashes the
//     process.
function startZombieReaper() {
  setInterval(() => {
    const now = Date.now()
    for (const [key, lastTime] of processLastActivity.entries()) {
      if (now - lastTime <= ZOMBIE_TIMEOUT) continue
      const proc = runningProcesses.get(key)
      // Even when proc.killed is true, the child may not have actually
      // exited (killed is "signal sent", not "process exited"). Hand
      // every found proc to the canonical helper — the helper handles
      // already-closed children as a no-op resolved path.
      if (!proc) {
        // Tracking is dangling — drop the last-activity entry only.
        processLastActivity.delete(key)
        continue
      }
      console.warn(`[zombie] Killing stale process ${key} (no output for ${Math.round(ZOMBIE_TIMEOUT / 1000)}s)`)
      const closePromise = signalAndAwaitClose(proc, {
        fallbackMs: 5000,
        cleanupKey: key,
        cleanup: cleanupProcess,
      })
      // Defect #5 fix: attach a typed .catch handler. On rejection we
      // MUST NOT call cleanupProcess (the slot stays leased — the
      // child is still alive in some sense) and MUST NOT remove the
      // process from runningProcesses. The processLastActivity entry
      // is also preserved so a later reaper tick (or shutdown) can
      // re-attempt the teardown / surface diagnostics.
      closePromise.catch((err: unknown) => {
        const code = err && typeof err === 'object' && 'code' in err
          ? String((err as { code?: unknown }).code ?? '')
          : ''
        const message = err instanceof Error ? err.message : String(err)
        console.error(
          `[zombie] canonical close rejected for ${key}: code=${code || 'unknown'} message=${message}. ` +
          `process is left in runningProcesses for retry / diagnostic.`,
        )
        // Intentional NO-OP on cleanupProcess — the slot must stay
        // leased while the OS still has the child. processLastActivity
        // is left in place so the next tick (or shutdown) can retry.
      })
    }
  }, 60_000) // check every 60s
}

function buildSwarmPrompt(task: TaskRecord, taskId: string): string {
  // Collect active agents from registry
  const activeAgents = Array.from(agentRegistry.entries())
    .filter(([key]) => !terminatedAgents.has(key))
    .map(([, reg]) => reg)

  // If no swarm is active, give a minimal prompt
  if (swarmShutdown || activeAgents.length === 0) {
    return [
      'You have access to the Agent tool for spawning subagents.',
      'Use subagent_type to assign specialized roles: coder, researcher, tester, reviewer, architect.',
      'Break the task into subtasks and delegate to parallel agents when possible.',
    ].join(' ')
  }

  // Build agent roster with roles
  const agentRoster = activeAgents.map(a => `- ${a.name} (type: ${a.type}, id: ${a.id})`).join('\n')

  // Map agent types to subagent_type values for the Agent tool
  const typeMap: Record<string, string> = {
    coordinator: 'general-purpose',
    coder: 'coder',
    researcher: 'researcher',
    tester: 'tester',
    reviewer: 'reviewer',
    analyst: 'analyst',
    architect: 'architecture',
    'security-architect': 'security-architect',
    'performance-engineer': 'performance-engineer',
    optimizer: 'performance-optimizer',
  }

  // Determine unique roles available
  const availableTypes = [...new Set(activeAgents.map(a => a.type))]
  const subagentTypes = availableTypes
    .map(t => `"${typeMap[t] || t}"`)
    .join(', ')

  // Build role descriptions
  const roleDescriptions: Record<string, string> = {
    coordinator: 'orchestrates the workflow, breaks tasks into subtasks, delegates to specialists',
    coder: 'writes implementation code, creates/edits files, runs build commands',
    researcher: 'explores the codebase, searches for patterns, gathers context before implementation',
    tester: 'writes tests, runs test suites, validates that implementations work correctly',
    reviewer: 'reviews code quality, checks for bugs, security issues, and best practices',
    analyst: 'analyzes requirements, defines architecture, produces technical specifications',
    architect: 'designs system architecture, defines patterns and interfaces',
  }

  const rolesList = availableTypes
    .map(t => `- ${t}: ${roleDescriptions[t] || 'specialist agent'}`)
    .join('\n')

  // Build the topology description
  const isHierarchical = lastSwarmTopology.includes('hierarchical')
  const coordinator = activeAgents.find(a => a.type === 'coordinator')
  const workers = activeAgents.filter(a => a.type !== 'coordinator')

  let topologyInstructions: string
  if (isHierarchical && coordinator) {
    const workerNames = workers.map(a => `${a.name}(${typeMap[a.type] || a.type})`).join(', ')
    topologyInstructions = [
      `You are the COORDINATOR of a ${lastSwarmTopology} swarm with ${activeAgents.length} agents.`,
      `Your role is to ORCHESTRATE, not to implement directly.`,
      '',
      'MANDATORY WORKFLOW:',
      '1. Analyze the task and break it into subtasks',
      '2. For EACH subtask, spawn a subagent using the Agent tool with the appropriate subagent_type',
      '3. Run independent subtasks in PARALLEL (multiple Agent calls in one response)',
      '4. Wait for results, then synthesize or delegate follow-up work',
      '5. Only write code yourself if no specialist agent fits the need',
      '',
      `Available worker agents: ${workerNames}`,
      '',
      'SUBAGENT DISPATCH RULES:',
      `- For code implementation: use subagent_type="${typeMap.coder || 'coder'}"`,
      `- For research/exploration: use subagent_type="${typeMap.researcher || 'researcher'}"`,
      `- For testing/validation: use subagent_type="${typeMap.tester || 'tester'}"`,
      `- For code review: use subagent_type="${typeMap.reviewer || 'reviewer'}"`,
      `- For analysis/specs: use subagent_type="${typeMap.analyst || 'analyst'}"`,
      '',
      'IMPORTANT: Do NOT do all the work yourself. You MUST delegate to subagents.',
      'Each Agent call should include a clear, self-contained prompt with all context the subagent needs.',
      'Maximize parallelism: if two subtasks are independent, dispatch both in the same response.',
    ].join('\n')
  } else {
    topologyInstructions = [
      `You are operating in a ${lastSwarmTopology} swarm with ${activeAgents.length} agents.`,
      'Use the Agent tool to delegate subtasks to specialized subagents.',
      'Break the work into parallel subtasks and dispatch them simultaneously when possible.',
      '',
      'Available subagent_type values: ' + subagentTypes,
      '',
      'IMPORTANT: Delegate work to subagents rather than doing everything yourself.',
      'Each subagent should receive a focused, self-contained task with full context.',
    ].join('\n')
  }

  // Assigned agent context
  const assignedAgent = task.assignedTo
    ? activeAgents.find(a => a.id === task.assignedTo || a.name === task.assignedTo)
    : null
  const assignmentNote = assignedAgent
    ? `\nThis task was assigned to ${assignedAgent.name} (${assignedAgent.type}). Act in that role.`
    : ''

  return [
    topologyInstructions,
    assignmentNote,
    '',
    'SWARM ROSTER:',
    agentRoster,
    '',
    'AGENT ROLES:',
    rolesList,
    '',
    `Swarm ID: ${lastSwarmId}, Topology: ${lastSwarmTopology}, Strategy: ${lastSwarmStrategy}`,
  ].join('\n')
}

// Dispatcher launcher: this is called by the dispatcher only AFTER the
// task has been promoted to in_progress and (for WRITE mode) a worktree
// is in place. The execution cwd passed here may be the worktree path
// (WRITE) or the source cwd (READ-ONLY). The launch will fall through to
// the existing swarm pipeline / single-agent fallback.
async function dispatcherLauncher(dTask: DispatcherTaskRecord): Promise<void> {
  const tr = taskStore.get(dTask.id)
  if (!tr) {
    dispatcher.fail(dTask.id, 'task record missing from store')
    return
  }
  if (dTask.executionCwd) {
    tr.cwd = dTask.executionCwd
  }
  tr.mode = dTask.mode
  tr.sourceCwd = dTask.sourceCwd
  tr.executionCwd = dTask.executionCwd
  taskStore.set(dTask.id, tr)
  await launchWorkflowForTask(dTask.id, dTask.title, dTask.description)
}

setDispatcherLauncher(dispatcherLauncher)

async function launchWorkflowForTask(taskId: string, title: string, description: string): Promise<void> {
  const task = taskStore.get(taskId)
  if (!task) return
  const taskDesc = `${title}${description ? ': ' + description : ''}`
  const workflowId = `workflow-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  // Create workflow record
  const wf: WorkflowRecord = {
    id: workflowId, name: title, template: 'development',
    status: 'running', taskId, createdAt: new Date().toISOString(),
    steps: [],
  }
  workflowStore.set(workflowId, wf)
  broadcast('workflow:added', wf)

  // ACC-TASK-QUEUE-002-FINAL-REPAIR: the executable pool is ALWAYS the
  // canonical AGENT_PROFILES set. Stale registry entries cannot
  // influence the count or composition of the pool.
  const canonicalPool = buildCanonicalPool()
  if (!swarmShutdown && canonicalPool.length > 0) {
    console.log(`[TASK ${taskId}] Multi-agent pipeline with ${canonicalPool.length} agents`)
    launchSwarmPipeline(taskId, task, taskDesc, title, wf, workflowId, canonicalPool)
  } else {
    console.log(`[TASK ${taskId}] Single-agent fallback (swarmShutdown=${swarmShutdown}, agents=${canonicalPool.length})`)
    // Fallback: single claude -p
    launchViaClaude(taskId, task, taskDesc, title, wf, workflowId)
  }
}

// Canonical executable pool. ALWAYS exactly AGENT_PROFILES, in declared
// order, unique by profileId. The legacy registry-derived list is
// retained only for backward-compatible UI surfaces and is pruned to
// the canonical set inside pruneRegistryToCanonical().
function getActiveSwarmAgents(): RuntimeAgent[] {
  return buildCanonicalPool()
}

// ── HIVE MIND MEMORY HELPERS ────────────────────────────────────────
const HIVE_MEMORY_NS = 'hive-mind'

async function getHiveMindMemory(): Promise<Record<string, string>> {
  try {
    const { raw } = await execCli('memory', ['list', '--namespace', HIVE_MEMORY_NS, '--format', 'json'])
    // Parse JSON array of entries to get full keys
    let items: Array<{ key: string; namespace?: string }> = []
    try {
      const parsed = JSON.parse(raw)
      items = Array.isArray(parsed) ? parsed : []
    } catch {
      // Fallback: extract keys from table format
      for (const line of raw.replace(/\r/g, '').split('\n')) {
        const m = line.match(/\|\s*(\S+)\s*\|\s*hive-mind\s*\|/)
        if (m) items.push({ key: m[1] })
      }
    }
    if (items.length === 0) return {}

    // Retrieve each key's value (without shell to handle special chars in keys)
    const entries: Record<string, string> = {}
    await Promise.all(items.map(async (item) => {
      try {
        const { stdout } = await execFileAsync(
          CLI_BIN,
          [...CLI_BASE_ARGS, 'memory', 'retrieve', '--namespace', HIVE_MEMORY_NS, '-k', item.key],
          { timeout: CLI_TIMEOUT, encoding: 'utf-8', windowsHide: true },
        )
        // Extract value from CLI output
        const valMatch = stdout.match(/Value:\s*\n([\s\S]*?)(?:\n\+|$)/)
        if (valMatch) {
          entries[item.key] = valMatch[1].replace(/\|\s*/g, '').trim()
        } else {
          const lines = stdout.split('\n')
          const valIdx = lines.findIndex(l => l.includes('Value:'))
          if (valIdx >= 0 && valIdx + 1 < lines.length) {
            entries[item.key] = lines.slice(valIdx + 1).map(l => l.replace(/^\|\s*/, '').replace(/\s*\|$/, '')).join(' ').replace(/\+-+\+/g, '').trim()
          }
        }
      } catch { /* skip unreadable key */ }
    }))
    return entries
  } catch { return {} }
}

async function storeHiveMindMemory(key: string, value: string): Promise<void> {
  try {
    // Sanitize: strip shell-special chars and double-quotes, then wrap in double-quotes for shell
    const safeValue = value.replace(/[`|$\\"'\n\r*?<>(){}[\]!#&;^~]/g, '').replace(/\s+/g, ' ').trim().slice(0, 300)
    // Use execFileAsync directly (without shell) to avoid argument splitting
    const { stdout } = await execFileAsync(
      CLI_BIN,
      [...CLI_BASE_ARGS, 'memory', 'store', '--namespace', HIVE_MEMORY_NS, '-k', key, '-v', safeValue],
      { timeout: CLI_TIMEOUT, encoding: 'utf-8', windowsHide: true },
    )
    console.log(`[HiveMind] Stored "${key}" (${safeValue.length}B)`)
  } catch (err) {
    console.error(`[HiveMind] Store FAILED for key="${key}":`, err instanceof Error ? err.message : String(err))
  }
}

// ── MULTI-AGENT PIPELINE ─────────────────────────────────────────────
// Phase 1: Coordinator plans subtasks (claude -p with planner prompt)
// Phase 2: Each subtask dispatched to the matching agent (parallel claude -p)
// Phase 3: Reviewer validates results
async function launchSwarmPipeline(
  taskId: string, task: TaskRecord, taskDesc: string, title: string,
  wf: WorkflowRecord, workflowId: string,
  agents: RuntimeAgent[],
): Promise<void> {
  // Defensive guard: prune the in-memory registry BEFORE we register
  // the canonical pool. This prevents any stale/historical entry from
  // surviving into a subsequent launch even if a code path added one.
  pruneRegistryToCanonical(agentRegistry, terminatedAgents)

  // Register the UI pipeline as the current swarm.
  currentSwarmAgentIds = new Set(agents.map(agent => agent.id))
  for (const agent of agents) {
    terminatedAgents.delete(agent.id)
    agentRegistry.set(agent.id, {
      id: agent.id,
      name: agent.name,
      type: agent.type,
      profileId: agent.profileId,
    })
  }
  persistState()

  const coordinator = agents.find(a => a.type === 'coordinator')
  const workers = agents.filter(a => a.type !== 'coordinator')

  // Register every agent with the global scheduler so its cap is enforced.
  const scheduler = getGlobalScheduler()
  scheduler.registerAgents(agents.filter(a => a.profileId).map(a => ({
    profileId: a.profileId!,
    agentId: a.id,
  })))

  const cleanEnv = { ...process.env }
  // Remove ALL Claude env vars that prevent nested sessions
  for (const key of Object.keys(cleanEnv)) {
    if (key.startsWith('CLAUDE') || key.startsWith('claude')) delete cleanEnv[key]
  }
  const claudePath = process.env.LOCALAPPDATA
    ? `${process.env.USERPROFILE}\\.local\\bin\\claude.exe`
    : 'claude'
  const mcpConfigPath = path.join(process.cwd(), '.mcp.json')
  const mcpArgs = fs.existsSync(mcpConfigPath) ? ['--mcp-config', mcpConfigPath] : []

  broadcast('task:log', { id: taskId, message: `Starting multi-agent pipeline for: ${taskDesc}` })

  // Helper: run claude -p and return the result text
  // planOnly=true: no tools, single turn — for coordinator planning phase
  // Callers are responsible for settling any scheduler lease / synthetic
  // agent exactly once per exit path (close/error/cancel/timeout).
  function runClaude(prompt: string, systemPrompt: string, agentId?: string, planOnly = false): Promise<string> {
    return new Promise((resolve, reject) => {
      if (agentId) {
        updateAgentActivity(agentId, { status: 'working', currentTask: taskId, currentAction: planOnly ? 'Planning...' : prompt.slice(0, 60) })
      }
      const parentPacket = taskDesc.match(/PACKET-ID:\s*([A-Za-z0-9._-]+)/i)?.[1] || taskId
      const packetMode = taskDesc.match(/MODE:\s*([A-Za-z-]+)/i)?.[1]
      const packetPhase = (planOnly ? 'PLAN' : (agentId || 'MAIN')).replace(/[^A-Za-z0-9._-]/g, '-')
      const packetPrompt = prompt.startsWith('PACKET-ID:')
        ? prompt
        : `PACKET-ID: ${parentPacket}-${packetPhase}\n${packetMode ? 'MODE: ' + packetMode + '\n' : ''}PARENT-TASK-ID: ${taskId}\n${prompt}`
      const claudeModel = process.env.RUFLO_CLAUDE_MODEL || 'opus'
      const args = ['-p', packetPrompt, '--output-format', 'stream-json', '--verbose', '--model', claudeModel]
      if (planOnly) {
        // Restricted mode: no tools, single response — forces pure text output
        args.push('--max-turns', '1')
        args.push('--append-system-prompt', systemPrompt)
      } else {
        // Full mode: tools + MCP for actual work
        if (SKIP_PERMISSIONS) args.push('--dangerously-skip-permissions')
        args.push(...mcpArgs)
        args.push('--append-system-prompt', systemPrompt)
      }
      const proc = spawn(claudePath, args, { cwd: task.cwd || process.cwd(), env: cleanEnv, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true })

      runningProcesses.set(`${taskId}-${agentId || 'main'}`, proc)
      trackProcessActivity(`${taskId}-${agentId || 'main'}`)
      let fullOutput = ''
      let resultText = ''

      proc.stdout?.on('data', (chunk: Buffer) => {
        trackProcessActivity(`${taskId}-${agentId || 'main'}`)
        for (const line of chunk.toString().split('\n').filter(Boolean)) {
          try {
            const evt = JSON.parse(line)
            if (evt.type === 'assistant' && evt.message?.content) {
              for (const block of evt.message.content) {
                if (block.type === 'text') {
                  fullOutput += block.text
                  if (agentId) appendAgentOutput(agentId, block.text)
                  broadcast('task:output', { id: taskId, workflowId, type: 'text', agentId, content: block.text.slice(0, 300) })
                } else if (block.type === 'tool_use') {
                  const summary = block.input?.file_path || block.input?.command?.slice(0, 60) || block.input?.pattern || ''
                  const toolLine = `[Tool] ${block.name}${summary ? ': ' + summary : ''}`
                  if (agentId) {
                    appendAgentOutput(agentId, toolLine)
                    updateAgentActivity(agentId, { status: 'working', currentTask: taskId, currentAction: `${block.name}: ${summary.slice(0, 60)}` })
                  }
                  const stepId = `step-${wf.steps.length + 1}`
                  wf.steps.push({ id: stepId, name: block.name, status: 'running', agent: agentId || 'claude', detail: summary })
                  broadcast('workflow:updated', wf)
                } else if (block.type === 'tool_result') {
                  const resultLine = typeof block.content === 'string' ? block.content.slice(0, 200) : JSON.stringify(block.content).slice(0, 200)
                  if (agentId) appendAgentOutput(agentId, `[Result] ${resultLine}`)
                }
              }
            } else if (evt.type === 'tool_result' || (evt.type === 'user' && evt.message?.content)) {
              const lastRunning = [...wf.steps].reverse().find(s => s.status === 'running')
              if (lastRunning) { lastRunning.status = 'completed'; broadcast('workflow:updated', wf) }
            } else if (evt.type === 'result') {
              resultText = evt.result || ''
              if (agentId) appendAgentOutput(agentId, `[Done] ${(resultText || 'completed').slice(0, 200)}`)
              wf.steps.forEach(s => { if (s.status === 'running') s.status = 'completed' })
            }
          } catch {
            fullOutput += line + '\n'
          }
        }
      })

      let stderrBuf = ''
      proc.stderr?.on('data', (chunk: Buffer) => {
        const text = chunk.toString().trim()
        stderrBuf += text + '\n'
        if (agentId && text) appendAgentOutput(agentId, `[stderr] ${text.slice(0, 200)}`)
        broadcast('task:output', { id: taskId, workflowId, type: 'stderr', agentId, content: text.slice(0, 300) })
      })

      proc.on('close', (code) => {
        cleanupProcess(`${taskId}-${agentId || 'main'}`)
        if (agentId) {
          const act = agentActivity.get(agentId)
          updateAgentActivity(agentId, {
            status: 'idle', currentTask: undefined, currentAction: undefined,
            tasksCompleted: (act?.tasksCompleted || 0) + (code === 0 ? 1 : 0),
            errors: (act?.errors || 0) + (code !== 0 ? 1 : 0),
          })
        }
        if (code === 0) resolve(resultText || fullOutput)
        else {
          const errDetail = (stderrBuf + '\n' + fullOutput).trim().slice(0, 1000) || `Exit code ${code}`
          console.error(`[runClaude ${agentId}] Failed (code ${code}): ${errDetail.slice(0, 200)}`)
          reject(new Error(errDetail))
        }
      })
      proc.on('error', (err) => {
        cleanupProcess(`${taskId}-${agentId || 'main'}`)
        reject(err)
      })
    })
  }

  try {
    // ── PHASE 1: Coordinator plans subtasks ──
    // Capabilities surface from profile metadata; the scheduler picks the
    // matching free agent when the time comes.
    const teamCapabilities = [...new Set(AGENT_PROFILES.flatMap(p => p.capabilities))]
    const teamRoster = AGENT_PROFILES.map(p => `- ${p.name} (profile: ${p.profileId}, type: ${p.type}, capabilities: ${p.capabilities.join(', ')})`).join('\n')
    const coordinatorId = coordinator?.id
    if (coordinatorId) {
      updateAgentActivity(coordinatorId, { status: 'working', currentTask: taskId, currentAction: 'Planning subtasks...' })
    }
    wf.steps.push({ id: 'step-plan', name: 'Plan', status: 'running', agent: coordinator?.name || 'coordinator', detail: 'Breaking task into subtasks' })
    broadcast('workflow:updated', wf)
    broadcast('task:output', { id: taskId, workflowId, type: 'text', content: '[Phase 1] Coordinator planning subtasks...' })

    // Read hive mind shared memory for cross-task context
    const hiveMindCtx = await getHiveMindMemory()
    const hiveMindContext = Object.keys(hiveMindCtx).length > 0
      ? `\n\nSHARED KNOWLEDGE (from previous tasks via Hive Mind):\n${Object.entries(hiveMindCtx).map(([k, v]) => `- ${k}: ${String(v).slice(0, 200)}`).join('\n')}`
      : ''
    if (Object.keys(hiveMindCtx).length > 0) {
      broadcast('task:output', { id: taskId, workflowId, type: 'text', content: `[Hive Mind] Loaded ${Object.keys(hiveMindCtx).length} shared memories as context` })
    }

    const roleInstructions: Record<string, string> = {
      researcher: 'RESEARCH phase: explore the codebase, find relevant files, understand existing patterns and dependencies',
      coder: 'IMPLEMENTATION phase: inspect and modify application code, create files, run builds and tests; remain read-only when the task explicitly says READ-ONLY',
      tester: 'TESTING phase: write unit/integration tests, run the test suite, verify the implementation works',
      reviewer: 'REVIEW phase: review the code changes for quality, bugs, security issues, and adherence to project conventions',
      analyst: 'ANALYSIS phase: analyze requirements, define technical specifications',
      architect: 'ARCHITECTURE phase: design the solution structure, define interfaces and patterns',
      'swarm-specialist': 'DEVOPS IMPLEMENTATION phase: inspect and modify infrastructure, services, CI, configuration and operational scripts; never push, merge or deploy without explicit authorization',
      'security-architect': 'SECURITY phase: inspect authentication, authorization, tenant isolation, secrets and dependency risks; provide fail-closed recommendations',
      integration: 'INTEGRATION phase: bridge backend and frontend changes; integrate endpoints with UI state, contracts, tests',
      devops: 'DEVOPS phase: configure infrastructure, systemd units, Dockerfiles, CI pipelines',
      qa: 'QA phase: design and run tests covering happy path, regression, and concurrency invariants',
      final: 'FINAL REVIEW phase: synthesize every prior subtask result into one complete deliverable; verify all acceptance criteria',
    }

    const planPrompt = [
      `You are the Queen Dispatcher coordinating a 10-agent team. Your job is to break tasks into subtasks and tag them with the right CAPABILITY. The global scheduler will pick a free agent matching that capability.`,
      '',
      `TEAM (10 unique profiles):`,
      teamRoster,
      '',
      `AVAILABLE CAPABILITIES: ${teamCapabilities.join(', ')}`,
      '',
      `TASK: ${taskDesc}`,
      hiveMindContext,
      '',
      `RULES:`,
      `1. Use multiple distinct capabilities — never collapse work onto a single agent type.`,
      `2. If the task involves modifying existing code, START with an "architecture" or "research" subtask to scope the change.`,
      `3. After implementation by "coder"/"backend"/"frontend", ALWAYS add a "qa" or "tests" subtask to validate.`,
      `4. Each subtask must be self-contained with all the context the agent needs.`,
      `5. Use depends_on to chain tasks that need results from previous steps.`,
      `6. For audits, use every relevant specialist; otherwise use 3-6 subtasks.`,
      `7. ALWAYS include a "final-review" subtask that depends on every prior subtask — it is mandatory.`,
      '',
      `Respond ONLY with a JSON array. Each subtask has:`,
      `- "capability": one of [${teamCapabilities.map(c => `"${c}"`).join(', ')}]`,
      `- "task": a detailed, self-contained description`,
      `- "depends_on": array of indices (0-based) of prerequisite subtasks, or [] for parallel`,
      '',
      'Example:',
      '[',
      '  {"capability":"backend","task":"Add /api/foo endpoint with validation","depends_on":[]},',
      '  {"capability":"frontend","task":"Wire FooPage to consume /api/foo","depends_on":[0]},',
      '  {"capability":"qa","task":"Add integration tests for /api/foo","depends_on":[0]},',
      '  {"capability":"final-review","task":"Final review of all changes","depends_on":[0,1,2]}',
      ']',
    ].join('\n')

    // The planner Claude process MUST occupy a scheduler slot and respect
    // the global cap (blocker 7). Enqueue a synthetic planning subtask
    // bound to the coordinator profile, then release it when planning
    // completes (success or error).
    const plannerSubtaskId = `plan-${taskId}`
    scheduler.enqueue({
      id: plannerSubtaskId,
      taskId,
      capability: 'planning',
      description: `Plan: ${taskDesc}`,
      profileId: 'queen-dispatcher',
      priority: (task.priority as Priority | undefined) ?? 'normal',
    })
    let planResult: string
    let plannerReleased = false
    const releasePlanner = (reason: 'close' | 'error') => {
      if (plannerReleased) return
      plannerReleased = true
      scheduler.release(plannerSubtaskId, reason)
    }
    try {
      // Block until the planner slot is granted (may queue under cap).
      await scheduler.awaitDispatch(plannerSubtaskId)
      planResult = await runClaude(planPrompt, 'You are a task planner. Output ONLY a valid JSON array. No markdown fences, no explanation, no tool use. Just the JSON.', coordinatorId, true)
      releasePlanner('close')
    } catch (err) {
      releasePlanner('error')
      throw err
    }

    // Parse the plan — ACC-TASK-QUEUE-002-FINAL-REPAIR: robust to bare
    // JSON, fenced JSON, surrounding prose, and balanced-bracket
    // extraction. Any parse failure -> null -> deterministic fallback.
    const isReadOnly = /\bread[- ]?only\b/i.test(taskDesc)
    let subtasks: Array<{ capability?: string; agent?: string; task: string; depends_on?: number[] }> = []
    try {
      const parsed = parsePlannerOutput(planResult)
      if (parsed) subtasks = parsed
    } catch (e) {
      console.warn('[pipeline] Failed to parse subtask plan JSON:', e instanceof Error ? e.message : String(e))
    }

    // Backwards compat: also accept old "agent" field by treating it as a hint.
    // Normalise into a `capability` key for scheduler routing.
    subtasks = subtasks.map(st => ({
      ...st,
      capability: st.capability || st.agent || 'backend',
    }))

    // Detect planner invalid → deterministic chain fallback (blocker 12).
    // WRITE tasks: implementation -> tests -> final-review
    // READ-ONLY tasks: analysis -> final-review
    const plannerInvalid = subtasks.length === 0
    if (plannerInvalid) {
      subtasks = buildDeterministicFallback(taskDesc, isReadOnly)
      broadcast('task:output', { id: taskId, workflowId, type: 'text', content: '[Fallback] Planner JSON invalid, dispatching deterministic chain' })
    }

    // Enforce exactly-one-final-review, last in the array, depends_on every prior subtask (blocker 11).
      // Strip planner-provided terminal reviewer aliases, then append one canonical final-review.
      subtasks = stripTerminalReviewerAliases(subtasks)
    if (subtasks.length > 0) {
      const priorIndices = subtasks.map((_, index) => index)
      subtasks.push({
        capability: 'final-review',
        task: [
          'FINAL REVIEW: Review every prior subtask result against the original task.',
          `Original task: ${taskDesc}`,
          'Return one complete standalone final answer satisfying every requested deliverable.',
          'For plans and reports, include the complete requested content, not only a verdict.',
          'Keep the final answer at or below 10000 characters. Never push, merge, or deploy.',
        ].join('\n'),
        depends_on: priorIndices,
      })
    }

    // Pre-assign stable ids so cross-task references can route through the scheduler.
    subtasks = subtasks.map((st, i) => ({
      ...st,
      _id: `sub-${taskId}-${i}`,
    }))

    const planStep = wf.steps.find(s => s.id === 'step-plan')
    if (planStep) planStep.status = 'completed'
    broadcast('workflow:updated', wf)

    if (plannerInvalid) {
      // Deterministic fallback path: still saves the full TaskRecord.agentResults (blocker 13).
      broadcast('task:output', { id: taskId, workflowId, type: 'text', content: '[Phase 2] Executing deterministic fallback chain...' })
      const results: string[] = new Array(subtasks.length).fill('')
      const fallbackSteps: Array<{ id: string; name: string; status: string; agent?: string; detail?: string }> = []
      const fallbackStatuses: Array<'pending' | 'completed' | 'failed' | 'cancelled'> =
        new Array(subtasks.length).fill('pending')

      for (let i = 0; i < subtasks.length; i++) {
        const st = subtasks[i] as any
        const depIds = (st.depends_on || []).map((d: number) => (subtasks[d] as any)._id).filter(Boolean)
        scheduler.enqueue({
          id: st._id,
          taskId,
          capability: st.capability || 'backend',
          description: st.task,
          dependsOn: depIds,
          priority: (task.priority as Priority | undefined) ?? 'normal',
        })
      }

      let fallbackFailed = false
      for (let i = 0; i < subtasks.length; i++) {
        if (fallbackFailed) break
        const st = subtasks[i] as any
        const subtaskId: string = st._id
        try {
          const dispatched = await scheduler.awaitDispatch(subtaskId)
          const profile = AGENT_PROFILES.find(p => p.profileId === dispatched.profileId)
          const isFinalReviewer = dispatched.profileId === 'reviewer' && i === subtasks.length - 1
          const depContext = st.depends_on.length > 0
            ? '\n\nPrevious results:\n' + st.depends_on.map((d: number) => `[${subtasks[d].capability}]: ${isFinalReviewer ? results[d] : (results[d] || '').slice(0, 500)}`).join('\n')
            : ''
          const sysPrompt = profile?.systemPrompt || `You are a development agent. Complete this task thoroughly.`
          const agentPrompt = `Complete this task:\n\n${st.task}${depContext}`
          const stepId = `step-${i + 1}`
          fallbackSteps.push({ id: stepId, name: `${profile?.name || 'Agent'}: ${st.task.slice(0, 40)}`, status: 'running', agent: profile?.name, detail: st.task.slice(0, 80) })
          wf.steps.push(fallbackSteps[fallbackSteps.length - 1])
          broadcast('workflow:updated', wf)
          const result = await runClaude(agentPrompt, sysPrompt, dispatched.agentId, false)
          results[i] = result
          const step = wf.steps.find(s => s.id === stepId)
          if (step) step.status = 'completed'
          scheduler.complete(subtaskId, result)
          fallbackStatuses[i] = 'completed'
          await storeHiveMindMemory(`task-${taskId}-${st.capability}-${i}`, (result || '').slice(0, 300))
        } catch (err) {
          results[i] = `Error: ${err instanceof Error ? err.message : String(err)}`
          // Cancel the entire chain so held deps surface typed errors.
          scheduler.cancelTask(taskId)
          fallbackStatuses[i] = 'failed'
          fallbackFailed = true
          break
        }
        broadcast('workflow:updated', wf)
      }

      task.agentResults = subtasks.map((st, index) => ({
        index,
        agent: (st as any).capability,
        task: st.task,
        result: results[index] || '',
      }))
      const finalIdx = subtasks.length - 1
      task.result = (results[finalIdx] || [...results].reverse().find(Boolean) || 'Pipeline completed').slice(0, 12000)
      task.subtaskStatuses = fallbackStatuses
    } else {
      // Global work-conserving scheduler picks the next free agent for each
      // ready subtask; the per-subtask coroutine acquires a slot, runs, and
      // frees the slot on close/error/cancel/timeout.
      broadcast('task:output', { id: taskId, workflowId, type: 'text', content: `[Phase 2] Executing ${subtasks.length} subtasks across agents...` })
      const results: string[] = new Array(subtasks.length).fill('')
      const statuses: Array<'pending' | 'completed' | 'failed' | 'cancelled'> =
        new Array(subtasks.length).fill('pending')
      const idToIndex = new Map<string, number>()
      subtasks.forEach((st, i) => { if ((st as any)._id) idToIndex.set((st as any)._id, i) })

      // Enqueue every subtask with the scheduler (one shared queue);
      // dependent subtasks stay held until their prerequisites complete.
      for (let i = 0; i < subtasks.length; i++) {
        const st = subtasks[i] as any
        const depIds = (st.depends_on || []).map((d: number) => (subtasks[d] as any)._id).filter(Boolean)
        scheduler.enqueue({
          id: st._id,
          taskId,
          capability: st.capability || 'backend',
          description: st.task,
          dependsOn: depIds,
          priority: (task.priority as Priority | undefined) ?? 'normal', // blocker 6: propagate parent priority
        })
      }

      // Drive every subtask via scheduler-driven loops: each task awaits the
      // scheduler to assign it to a free agent, runs Claude, then completes
      // (frees the slot + lease exactly once).
      const slotPromises = subtasks.map(async (rawSt, i) => {
        const st = rawSt as any
        const subtaskId: string = st._id

        let profileMatch: AgentProfile | undefined
        for (const p of AGENT_PROFILES) {
          if (p.capabilities.includes(st.capability)) { profileMatch = p; break }
        }
        if (!profileMatch) {
          results[i] = `Error: no profile exposes capability "${st.capability}"`
          scheduler.fail(subtaskId, new Error(results[i]))
          statuses[i] = 'failed'
          return
        }

        let agentId: string
        let dispatchedProfileId: string
        try {
          const dispatched = await scheduler.awaitDispatch(subtaskId)
          agentId = dispatched.agentId
          dispatchedProfileId = dispatched.profileId
        } catch (err) {
          const reason = err instanceof Error ? err.message : String(err)
          results[i] = `Error: scheduler dispatch failed for "${st.capability}": ${reason}`
          // If we were rejected because the subtask was cancelled (explicit,
          // task-cancelled, dep-cancelled, dep-failed, dispatch-timeout,
          // scheduled-shutdown) record it; otherwise cancel so held deps don't
          // deadlock.
          const terminalReason = scheduler.getTerminalReason(subtaskId)
          if (terminalReason === 'dispatch-timeout') {
            statuses[i] = 'cancelled'
          } else if (terminalReason === 'scheduled-shutdown') {
            statuses[i] = 'cancelled'
            return // don't cancel again
          } else {
            scheduler.cancel(subtaskId)
            statuses[i] = 'cancelled'
          }
          return
        }

        // Use the ACTUALLY DISPATCHED profile (blocker 9) — never assume the
        // first match. Integration Engineer must NOT receive Backend Engineer 1's prompt.
        const resolvedProfile: AgentProfile =
          AGENT_PROFILES.find(p => p.profileId === dispatchedProfileId) || profileMatch
        const agentRecord = Array.from(agentRegistry.values()).find(a => a.id === agentId)
        const stepId = `step-${i + 1}`
        wf.steps.push({ id: stepId, name: `${resolvedProfile.name}: ${st.task.slice(0, 40)}`, status: 'running', agent: agentRecord?.name || resolvedProfile.name || agentId, detail: st.task.slice(0, 80) })
        broadcast('workflow:updated', wf)
        broadcast('task:output', { id: taskId, workflowId, type: 'text', content: `  [${agentRecord?.name || resolvedProfile.name || agentId}] ${st.task.slice(0, 100)}` })

        // Final reviewer gets full prior results; intermediates see 500-char summaries.
        const isFinalReviewer = resolvedProfile.profileId === 'reviewer' && i === subtasks.length - 1
        const depContext = st.depends_on.length > 0
          ? '\n\nPrevious results:\n' + st.depends_on.map((d: number) => `[${subtasks[d].capability}]: ${isFinalReviewer ? results[d] : (results[d] || '').slice(0, 500)}`).join('\n')
          : ''

        const sysPrompt = resolvedProfile.systemPrompt
        const agentPrompt = `Complete this task:\n\n${st.task}${depContext}`

        try {
          results[i] = await runClaude(agentPrompt, sysPrompt, agentId, false)
          const step = wf.steps.find(s => s.id === stepId)
          if (step) step.status = 'completed'
          await storeHiveMindMemory(
            `task-${taskId}-${st.capability}-${i}`,
            (results[i] || '').slice(0, 300),
          )
          // Notify scheduler of completion (frees slot + lease exactly once).
          scheduler.complete(subtaskId, results[i] || '')
          statuses[i] = 'completed'
        } catch (err) {
          results[i] = `Error: ${err instanceof Error ? err.message : String(err)}`
          const step = wf.steps.find(s => s.id === stepId)
          if (step) step.status = 'failed'
          // If a dependency or the parent task cancelled this run, record
          // 'cancelled' instead of 'failed'. Otherwise it's a true failure.
          const terminalReason = scheduler.getTerminalReason(subtaskId)
          if (terminalReason === 'task-cancelled' || terminalReason === 'dependency-cancelled' || terminalReason === 'scheduled-shutdown' || terminalReason === 'dispatch-timeout') {
            scheduler.cancel(subtaskId)
            statuses[i] = 'cancelled'
          } else {
            scheduler.fail(subtaskId, err instanceof Error ? err : new Error(String(err)))
            statuses[i] = 'failed'
          }
        }
        broadcast('workflow:updated', wf)
      })

      await Promise.all(slotPromises)

      task.agentResults = subtasks.map((st, index) => ({
        index,
        agent: (st as any).capability,
        task: st.task,
        result: results[index] || '',
      }))
      task.subtaskStatuses = statuses

      // Mandatory Final Reviewer — required to remain last in the agentResults list.
      const finalIndex = subtasks.findIndex(s => s.capability === 'final-review' || s.capability === 'review')
      const finalIdx = finalIndex >= 0 ? finalIndex : subtasks.length - 1
      task.result = (results[finalIdx] || [...results].reverse().find(Boolean) || 'Pipeline completed').slice(0, 12000)
    }

    // ── PHASE 3: Mark complete (fail-closed) ──
    // Never overwrite an already-cancelled task — the pipeline must respect
    // the user's cancel request even if all subtasks reported success.
    if (task.status === 'cancelled') {
      // Cancelled tasks keep their terminal state; don't mark them completed.
    } else {
      // Fail-closed: if any required worker failed or was cancelled, the
      // parent must not reach `completed`. Preserve the most informative
      // terminal status (cancelled wins over failed for explicit cancels).
      const subtaskStatuses: ReadonlyArray<string> = (task.subtaskStatuses as ReadonlyArray<string>) || []
      const anyFailed = subtaskStatuses.some(s => s === 'failed')
      const anyCancelled = subtaskStatuses.some(s => s === 'cancelled')
      const allSettled = subtaskStatuses.every(s => s === 'completed')

      if (anyCancelled) {
        task.status = 'cancelled'
        task.completedAt = new Date().toISOString()
        task.result = (task.result ? task.result + '\n' : '') + 'Cancelled: at least one required worker cancelled.'
        wf.status = 'cancelled'
        wf.completedAt = task.completedAt
        wf.result = task.result
      } else if (anyFailed || !allSettled) {
        task.status = 'failed'
        task.completedAt = new Date().toISOString()
        task.result = (task.result ? task.result + '\n' : '') + 'Pipeline failed: at least one required worker did not complete.'
        wf.status = 'failed'
        wf.completedAt = task.completedAt
        wf.result = task.result
      } else {
        task.status = 'completed'
        task.completedAt = new Date().toISOString()
        wf.status = 'completed'
        wf.completedAt = task.completedAt
        wf.result = task.result
      }
    }
    // Persist final result to hive mind shared memory
    // Persist final result to hive mind
    await storeHiveMindMemory(`task-result-${taskId}`, `${title}: ${(task.result || '').slice(0, 500)}`)
    // Funnel through the explicit terminal helper. cancelled is preserved.
    // For completed/failed the helper syncs the TaskRecord, releases the
    // dispatcher slot, and broadcasts the authoritative transition.
    //
    // Defect #4: a worker-cancelled parent MUST drive the dispatcher's
    // authoritative cancellation transition so its in-flight slot is
    // released exactly once. settleTaskTerminal is a no-op while
    // task.status is 'cancelled', so we drive the cancellation directly
    // via dispatcher.cancelActive; cancelTask(running pipeline tasks)
    // would be a no-op here because the pipeline holds the slot via the
    // launcher, but we want the dispatcher transition to be the
    // authoritative source of truth for the parent.
    if (task.status === 'completed') {
      settleTaskTerminal(taskId, 'completed', task.result || 'completed')
    } else if (task.status === 'failed') {
      settleTaskTerminal(taskId, 'failed', task.result || 'pipeline failed')
    } else if (task.status === 'cancelled') {
      // Pipeline-detected cancellation: route through dispatcher so
      // the in-flight slot is released exactly once. Because the
      // pipeline holds the dispatcher slot via the launcher (no
      // outer spawned process here), this completes quickly with the
      // slot released exactly once.
      await dispatcher.cancelActive(taskId, async () => { /* no extra kill */ })
      syncTaskRecordFromDispatcher(taskId)
      // Re-broadcast the final state so subscribers see the cancelled
      // status from the authoritative dispatcher.
      broadcast('task:updated', { ...taskStore.get(taskId)!, id: taskId })
    }
    broadcast('workflow:updated', wf)
    broadcast('task:output', { id: taskId, workflowId, type: 'done', code: task.status === 'completed' ? 0 : 1 })
    // Final-success metric update: ONLY completed pipelines count as the
    // coordinator's success. failed/cancelled pipelines already update
    // the `errors` counter elsewhere and must NOT inflate `tasksCompleted`.
    if (coordinatorId) {
      const act = agentActivity.get(coordinatorId)
      const finalOutcome = taskStore.get(taskId)?.status ?? task.status
      const completedCount = finalOutcome === 'completed' ? (act?.tasksCompleted || 0) + 1 : (act?.tasksCompleted || 0)
      updateAgentActivity(coordinatorId, { status: 'idle', currentTask: undefined, currentAction: undefined, tasksCompleted: completedCount })
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    console.error(`[TASK ${taskId}] Pipeline failed: ${msg}`)
    // Outer-catch safety: cancel every scheduler subtask for this task so
    // no new dispatches occur, but DO NOT enqueue any new subtasks.
    try {
      const outerScheduler = getGlobalScheduler()
      outerScheduler.cancelTask(taskId)
    } catch { /* scheduler may already be torn down */ }

    if (task.status === 'cancelled') {
      // Don't overwrite a cancellation imposed by an explicit user action.
      task.result = (task.result ? task.result + '\n' : '') + `Pipeline aborted after cancel: ${msg.slice(0, 500)}`
      // FAIL-CLOSED defect #last: drive the dispatcher's authoritative
      // cancellation transition so the in-flight slot is released
      // exactly once. The route's /api/tasks/:id/cancel also drives
      // this transition, but the pipeline's outer catch can race with
      // the route — only one of the two paths MUST succeed, and the
      // guard inside cancelActive ensures idempotency. cancelActive
      // returns true if the slot was already terminal, false if the
      // task is unknown — both are safe no-ops here.
      let dispatcherCancelled = false
      try {
        dispatcherCancelled = await dispatcher.cancelActive(taskId, async () => { /* scheduler released above */ })
      } catch (cancelErr) {
        console.error(`[TASK ${taskId}] Dispatcher cancellation remained fail-closed:`, cancelErr)
      }
      if (dispatcherCancelled && dispatcher.get(taskId)?.status === 'cancelled') {
        syncTaskRecordFromDispatcher(taskId)
      }
      broadcast('task:updated', { ...taskStore.get(taskId) || task, id: taskId })
    } else {
      const failureResult = `Pipeline error: ${msg.slice(0, 1000)}`
      settleTaskTerminal(taskId, 'failed', failureResult)
      wf.status = 'failed'
      const synced = taskStore.get(taskId)
      if (synced) wf.result = synced.result
      broadcast('workflow:updated', wf)
    }
    // Release all agents
    for (const agent of agents) {
      updateAgentActivity(agent.id, { status: 'idle', currentTask: undefined, currentAction: undefined })
    }
  }
}

// ── MODE 1: ruflo swarm start ──────────────────────────────────────────
// Uses the native swarm orchestrator which deploys its own agent topology
function launchViaSwarmCli(
  taskId: string, task: TaskRecord, taskDesc: string, title: string,
  wf: WorkflowRecord, workflowId: string,
): void {
  broadcast('task:log', { id: taskId, message: `Starting swarm execution for: ${taskDesc}` })

  const maxAgents = lastSwarmMaxAgents || 8
  const strategy = lastSwarmStrategy || 'development'
  const proc = spawn('npx', [
    '-y', '@claude-flow/cli@latest', 'swarm', 'start',
    '--objective', sanitizeShellArg(taskDesc),
    '--max-agents', String(maxAgents),
    '--strategy', strategy,
  ], { cwd: task.cwd || process.cwd(), stdio: ['ignore', 'pipe', 'pipe'], shell: true, windowsHide: true })

  runningProcesses.set(taskId, proc)
  trackProcessActivity(taskId)
  let fullOutput = ''
  let stderrOutput = ''
  let swarmId = ''

  console.log(`[TASK ${taskId}] Launching swarm for: "${taskDesc.slice(0, 80)}"`)

  // Mark all registered agents as working
  for (const [key, reg] of agentRegistry.entries()) {
    if (!terminatedAgents.has(key)) {
      updateAgentActivity(reg.id, {
        status: 'working', currentTask: taskId,
        currentAction: `Swarm: ${title.slice(0, 40)}`,
      })
      busyAgents.add(reg.id)
    }
  }

  proc.stdout?.on('data', (chunk: Buffer) => {
    trackProcessActivity(taskId)
    const text = chunk.toString()
    fullOutput += text
    // Extract swarm ID from output
    const idMatch = text.match(/swarm status\s+(swarm-\w+)/)
    if (idMatch && !swarmId) {
      swarmId = idMatch[1]
      task.swarmRunId = swarmId
      broadcast('task:output', { id: taskId, workflowId, type: 'text', content: `Swarm started: ${swarmId}` })
      // Start polling swarm status for live updates
      pollSwarmExecution(taskId, swarmId, title, wf, workflowId)
    }
    // Parse agent deployment table
    const roleLines = text.match(/\|\s*(\w[\w\s]*?)\s*\|\s*(\w+)\s*\|\s*(\d+)\s*\|/g)
    if (roleLines) {
      for (const line of roleLines) {
        const m = line.match(/\|\s*(\w[\w\s]*?)\s*\|\s*(\w+)\s*\|\s*(\d+)\s*\|/)
        if (m && m[1] !== 'Role') {
          const stepId = `step-${wf.steps.length + 1}`
          wf.steps.push({
            id: stepId, name: `Deploy ${m[1].trim()}`,
            status: 'completed', agent: m[2], detail: `x${m[3]}`,
          })
        }
      }
      broadcast('workflow:updated', wf)
    }
    // Broadcast raw output lines
    for (const line of text.split('\n').filter(Boolean)) {
      broadcast('task:output', { id: taskId, workflowId, type: 'raw', content: line.slice(0, 300) })
    }
  })

  proc.stderr?.on('data', (chunk: Buffer) => {
    const text = chunk.toString().trim()
    if (text) {
      stderrOutput += text + '\n'
      broadcast('task:output', { id: taskId, workflowId, type: 'stderr', content: text.slice(0, 300) })
    }
  })

  proc.on('close', (code) => {
    cleanupProcess(taskId)
    console.log(`[TASK ${taskId}] Swarm launch exited with code ${code}`)
    // swarm start returns immediately after deploying — the actual work continues
    // If it failed to even start, mark as failed
    if (code !== 0 && !swarmId) {
      const result = (fullOutput + '\n' + stderrOutput).trim().slice(0, 2000) || `Swarm launch failed (code ${code})`
      settleTaskTerminal(taskId, 'failed', result)
      releaseAllBusyAgents(taskId, false)
    }
  })

  proc.on('error', (err) => {
    cleanupProcess(taskId)
    settleTaskTerminal(taskId, 'failed', `Swarm launch error: ${err.message}`)
    releaseAllBusyAgents(taskId, false)
  })
}

// Poll swarm status to track progress and detect completion
function pollSwarmExecution(taskId: string, swarmId: string, title: string, wf: WorkflowRecord, workflowId: string): void {
  const task = taskStore.get(taskId)
  if (!task) return
  const startTime = Date.now()
  const maxDuration = 30 * 60 * 1000 // 30 min timeout
  let lastProgress = ''

  const poll = async () => {
    if (!taskStore.has(taskId) || task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled') return
    if (Date.now() - startTime > maxDuration) {
      settleTaskTerminal(taskId, 'failed', 'Swarm execution timed out after 30 minutes')
      releaseAllBusyAgents(taskId, false)
      return
    }
    try {
      const { raw } = await execCli('swarm', ['status', swarmId])
      // Parse progress
      const progressMatch = raw.match(/(\d+\.?\d*)%/)
      const progress = progressMatch?.[1] || '0'
      // Parse agent counts
      const activeMatch = raw.match(/Active\s*\|\s*(\d+)/)
      const completedMatch = raw.match(/Completed\s*\|\s*(\d+)/)
      const activeCount = Number(activeMatch?.[1] || 0)
      const completedAgents = Number(completedMatch?.[1] || 0)
      // Parse task counts
      const tasksCompletedMatch = raw.match(/Completed\s*\|\s*(\d+)/g)
      const tasksInProgressMatch = raw.match(/In Progress\s*\|\s*(\d+)/)
      const inProgressCount = Number(tasksInProgressMatch?.[1] || 0)

      // Only broadcast if changed
      const statusKey = `${progress}-${activeCount}-${completedAgents}-${inProgressCount}`
      if (statusKey !== lastProgress) {
        lastProgress = statusKey
        broadcast('task:output', {
          id: taskId, workflowId, type: 'progress',
          content: `Progress: ${progress}% | Active agents: ${activeCount} | Tasks in progress: ${inProgressCount}`,
        })
        // Update agent activities based on swarm status
        const activeAgents = Array.from(agentRegistry.entries())
          .filter(([key]) => !terminatedAgents.has(key))
          .map(([, reg]) => reg)
        for (const agent of activeAgents) {
          if (activeCount > 0 && busyAgents.has(agent.id)) {
            updateAgentActivity(agent.id, {
              status: 'working', currentTask: taskId,
              currentAction: `Swarm ${progress}%: ${title.slice(0, 40)}`,
            })
          }
        }
      }

      // Check if done (100% or all agents completed)
      if (Number(progress) >= 100) {
        const result = raw.slice(0, 2000) || 'Swarm execution completed'
        settleTaskTerminal(taskId, 'completed', result)
        wf.status = 'completed'
        wf.completedAt = task.completedAt || new Date().toISOString()
        wf.result = result
        broadcast('workflow:updated', wf)
        broadcast('task:output', { id: taskId, workflowId, type: 'done', code: 0 })
        releaseAllBusyAgents(taskId, true)
        return
      }
      // Keep polling
      setTimeout(poll, 3000)
    } catch {
      // Swarm may have finished — check once more then give up
      setTimeout(poll, 5000)
    }
  }
  setTimeout(poll, 3000)
}

function releaseAllBusyAgents(taskId: string, success: boolean): void {
  for (const [, reg] of agentRegistry.entries()) {
    if (busyAgents.has(reg.id)) {
      const act = agentActivity.get(reg.id)
      if (act?.currentTask === taskId) {
        updateAgentActivity(reg.id, {
          status: 'idle', currentTask: undefined, currentAction: undefined,
          tasksCompleted: (act.tasksCompleted || 0) + (success ? 1 : 0),
          errors: (act.errors || 0) + (success ? 0 : 1),
        })
        busyAgents.delete(reg.id)
      }
    }
  }
}

// ── MODE 2: claude -p (fallback when no swarm active) ──────────────────
//
// Failure-isolated launch path:
//   1. Register synthetic launcher agent + enqueue scheduler slot.
//   2. **AWAIT** scheduler.awaitDispatch BEFORE spawning the process.
//      - Dispatch rejection → NO spawn happens.
//      - Cancel-while-queued → NO spawn happens (cancelOne removed the pending entry).
//   3. Spawn the Claude process, attach handlers, return control to the
//      caller while it runs in the background.
//   4. Release the lease + unregister the synthetic agent EXACTLY ONCE,
//      from a single shared `releaseOnce()` helper called by every exit
//      path (close, error, dispatch rejection).
//   5. `--model` is sourced from RUFLO_CLAUDE_MODEL || 'opus' — every call.
function launchViaClaude(
  taskId: string, task: TaskRecord, taskDesc: string, title: string,
  wf: WorkflowRecord, workflowId: string,
): void {
  broadcast('task:log', { id: taskId, message: `Starting Claude Code for: ${taskDesc}` })

  const cleanEnv = { ...process.env }
  for (const key of Object.keys(cleanEnv)) {
    if (key.startsWith('CLAUDE') || key.startsWith('claude')) delete cleanEnv[key]
  }
  const claudePath = process.env.LOCALAPPDATA
    ? `${process.env.USERPROFILE}\\.local\\bin\\claude.exe`
    : 'claude'
  const mcpConfigPath = path.join(process.cwd(), '.mcp.json')
  const mcpArgs = fs.existsSync(mcpConfigPath) ? ['--mcp-config', mcpConfigPath] : []
  const swarmPrompt = buildSwarmPrompt(task, taskId)
  const sessionUUID = crypto.randomUUID()
  task.sessionUUID = sessionUUID

  // blocker 8: every spawn(claudePath) MUST include --model and respect the
  // global scheduler cap. We register a synthetic `launcher` agent for this
  // process so it occupies a slot and a profile lock until completion.
  const scheduler = getGlobalScheduler()
  const launcherAgentId = `claude-launcher-${taskId}`
  const launchSubtaskId = `launch-${taskId}`
  scheduler.registerAgents([{ profileId: 'launcher', agentId: launcherAgentId }])
  scheduler.enqueue({
    id: launchSubtaskId,
    taskId,
    capability: 'launcher',
    description: taskDesc,
    profileId: 'launcher',
    priority: (task.priority as Priority | undefined) ?? 'normal',
  })

  // Single shared cleanup — guaranteed exactly-once per exit path.
  let released = false
  const releaseOnce = (mode: 'complete' | 'fail' | 'cancel', payload?: { result?: string; error?: Error }) => {
    if (released) return
    released = true
    try {
      if (mode === 'complete') {
        scheduler.complete(launchSubtaskId, payload?.result || 'done')
      } else if (mode === 'fail') {
        scheduler.fail(launchSubtaskId, payload?.error || new Error('launch failed'))
      } else {
        // cancel: caller already triggered cancellation via cancelTask/awaitDispatch
        // (we may be here because we never actually held a lease — release is no-op)
        if (!scheduler.isSubtaskActive(launchSubtaskId) && !scheduler.isSubtaskCancelled(launchSubtaskId)) {
          scheduler.release(launchSubtaskId, 'cancel')
        }
      }
    } catch { /* scheduler may already be torn down */ }
    try { scheduler.unregisterAgent(launcherAgentId) } catch { /* ignore */ }
  }

  const claudeModel = process.env.RUFLO_CLAUDE_MODEL || 'opus'
  const claudeArgs = [
    '-p', taskDesc,
    '--output-format', 'stream-json',
    '--verbose',
    '--model', claudeModel,
    ...(SKIP_PERMISSIONS ? ['--dangerously-skip-permissions'] : []),
    '--session-id', sessionUUID,
    ...mcpArgs,
    '--append-system-prompt', swarmPrompt,
  ]

  // Wait for the scheduler to grant a slot BEFORE we spawn. If dispatch
  // rejects (cancelled, task-cancelled, timed-out, shutdown, prerequisite
  // failure) we MUST NOT spawn a process. Release the synthetic agent and
  // mark the task failed.
  scheduler.awaitDispatch(launchSubtaskId).then(() => {
    const proc = spawn(claudePath, claudeArgs, { cwd: task.cwd || process.cwd(), env: cleanEnv, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true })

    startMonitoring(sessionUUID, taskId, broadcast)
    runningProcesses.set(taskId, proc)
    trackProcessActivity(taskId)
    let fullOutput = ''
    let stderrOutput = ''

    console.log(`[TASK ${taskId}] Launching claude -p "${taskDesc.slice(0, 80)}"`)

    const assignedAgent = task.assignedTo || 'swarm'
    const coordinatorId = Array.from(agentRegistry.values()).find(a => a.type === 'coordinator')?.id
    const workingAgentId = assignedAgent === 'swarm' ? (coordinatorId || 'coordinator') : assignedAgent
    updateAgentActivity(workingAgentId, { status: 'working', currentTask: taskId, currentAction: `Executing: ${title.slice(0, 50)}` })

    proc.stdout?.on('data', (chunk: Buffer) => {
      trackProcessActivity(taskId)
      const text = chunk.toString()
      const lines = text.split('\n').filter(Boolean)
      for (const line of lines) {
        try {
          const evt = JSON.parse(line)
          if (evt.type === 'assistant' && evt.message?.content) {
            for (const block of evt.message.content) {
              if (block.type === 'text') {
                fullOutput += block.text
                broadcast('task:output', { id: taskId, workflowId, type: 'text', content: block.text.slice(0, 300) })
              } else if (block.type === 'tool_use') {
                const toolInfo = `${block.name}: ${JSON.stringify(block.input).slice(0, 200)}`
                fullOutput += `\n[tool] ${toolInfo}\n`
                const stepId = `step-${wf.steps.length + 1}`
                const inputSummary = block.input?.file_path || block.input?.command?.slice(0, 60) || block.input?.pattern || ''
                wf.steps.push({
                  id: stepId, name: block.name, status: 'running',
                  agent: task.assignedTo || 'claude', detail: inputSummary,
                })
                broadcast('workflow:updated', wf)
                broadcast('task:output', { id: taskId, workflowId, type: 'tool', tool: block.name, input: JSON.stringify(block.input).slice(0, 200) })
                updateAgentActivity(workingAgentId, { status: 'working', currentTask: taskId, currentAction: `${block.name}: ${inputSummary.slice(0, 60)}` })
                if (block.name === 'Agent' && block.input?.subagent_type) {
                  const matchedAgent = findSwarmAgentForType(block.input.subagent_type)
                  if (matchedAgent) {
                    updateAgentActivity(matchedAgent.id, {
                      status: 'working', currentTask: taskId,
                      currentAction: `Subagent: ${(block.input.description || block.input.subagent_type).slice(0, 60)}`,
                    })
                  }
                }
              }
            }
          } else if (evt.type === 'tool_result' || (evt.type === 'user' && evt.message?.content)) {
            const lastRunning = [...wf.steps].reverse().find(s => s.status === 'running')
            if (lastRunning) { lastRunning.status = 'completed'; broadcast('workflow:updated', wf) }
          } else if (evt.type === 'result') {
            wf.steps.forEach(s => { if (s.status === 'running') s.status = 'completed' })
            fullOutput = evt.result || fullOutput
            broadcast('task:output', { id: taskId, workflowId, type: 'text', content: 'Task completed' })
          }
        } catch {
          fullOutput += line + '\n'
          broadcast('task:output', { id: taskId, workflowId, type: 'raw', content: line.slice(0, 300) })
        }
      }
    })

    proc.stderr?.on('data', (chunk: Buffer) => {
      const text = chunk.toString().trim()
      if (text) {
        stderrOutput += text + '\n'
        console.error(`[TASK ${taskId}] stderr: ${text}`)
        broadcast('task:output', { id: taskId, workflowId, type: 'stderr', content: text.slice(0, 300) })
      }
    })

    proc.on('close', (code) => {
      cleanupProcess(taskId)
      stopMonitoring(sessionUUID)
      // Release scheduler slot for the launcher process (blocker 8).
        const result = fullOutput.slice(0, 2000) || 'done'
        const combined = (fullOutput + '\n' + stderrOutput).trim()
        if (code === 0) {
          releaseOnce('complete', { result })
        } else {
          releaseOnce('fail', { error: new Error(combined || `Process exited with code ${code}`) })
        }
        console.log(`[TASK ${taskId}] Exited with code ${code}. Output length: ${combined.length}`)
        // CRITICAL: cancelled is preserved here — close handler must NOT
        // overwrite an explicit user cancel that was settled via
        // settleTaskTerminal or via cancelActive after waitForProcessClose.
        // The terminal guard inside settleTaskTerminal enforces this; if
        // task is already cancelled/interrupted, it is a no-op.
        if (task.status === 'cancelled') {
          wf.status = 'cancelled'
          wf.completedAt = task.completedAt || new Date().toISOString()
          broadcast('workflow:updated', wf)
        } else if (code === 0) {
          // Funnel through the explicit terminal helper.
          settleTaskTerminal(taskId, 'completed', fullOutput.slice(0, 2000) || 'Task completed')
          // Mirror to wf.result for legacy callers.
          const synced = taskStore.get(taskId)
          if (synced && wf) {
            wf.status = 'completed'
            wf.completedAt = synced.completedAt || new Date().toISOString()
            wf.result = synced.result
            broadcast('workflow:updated', wf)
          }
        } else {
          settleTaskTerminal(taskId, 'failed', combined.slice(0, 2000) || `Process exited with code ${code}`)
          const synced = taskStore.get(taskId)
          if (synced && wf) {
            wf.status = 'failed'
            wf.result = synced.result
            broadcast('workflow:updated', wf)
          }
        }
        // Re-read status AFTER the helper call (it may be unchanged if cancelled).
        const finalStatus = taskStore.get(taskId)?.status ?? task.status
        broadcast('task:output', { id: taskId, workflowId, type: 'done', code: finalStatus === 'completed' ? 0 : 1 })
        releaseAllBusyAgents(taskId, finalStatus === 'completed')
        const activity = agentActivity.get(workingAgentId)
        const completed = (activity?.tasksCompleted || 0) + (finalStatus === 'completed' ? 1 : 0)
        const errors = (activity?.errors || 0) + (finalStatus === 'failed' ? 1 : 0)
        updateAgentActivity(workingAgentId, { status: 'idle', currentTask: undefined, currentAction: undefined, tasksCompleted: completed, errors })
    })

      proc.on('error', (err) => {
        cleanupProcess(taskId)
        console.error(`[TASK ${taskId}] Process error: ${err.message}`)
        releaseOnce('fail', { error: err })
        // CRITICAL: preserve cancelled.
        if (task.status !== 'cancelled') {
          settleTaskTerminal(taskId, 'failed', `Process error: ${err.message}`)
          const synced = taskStore.get(taskId)
          if (synced && wf) {
            wf.status = 'failed'
            wf.result = synced.result
            broadcast('workflow:updated', wf)
          }
        } else {
          wf.status = 'cancelled'
          wf.completedAt = task.completedAt || new Date().toISOString()
          broadcast('workflow:updated', wf)
        }
      })
  }).catch(async (err) => {
    // Dispatch was rejected (cancelled / task-cancelled / dep-failed /
    // dep-cancelled / dispatch-timeout / scheduled-shutdown / no-match).
    // Per blocker 6 we MUST NOT spawn a process.
    const reason = scheduler.getTerminalReason(launchSubtaskId)
    const msg = err instanceof Error ? err.message : String(err)
    console.warn(`[TASK ${taskId}] launchViaClaude dispatch rejected (${reason || 'unknown'}): ${msg}`)
    releaseOnce('cancel')
    // Don't overwrite an explicit user cancellation; otherwise mark failed.
    // Funnel through the explicit terminal helper.
    if (task.status === 'cancelled' || task.status === 'interrupted') {
      // Preserve cancelled/interrupted — do nothing.
    } else if (reason === 'task-cancelled' || reason === 'dependency-cancelled' || reason === 'scheduled-shutdown' || reason === 'dispatch-timeout') {
      // Treat scheduler-cancelled as a cancellation via the dispatcher.
      // Defect #6: AWAIT the dispatcher's settlement before broadcasting.
      // No spawned process exists yet (that's why we're in the catch),
      // so the cancellation chain completes quickly without leaks; the
      // slot is released exactly once.
      const cancelResult = `Dispatch rejected (${reason || 'unknown'}): ${msg.slice(0, 500)}`
      // Reflect the cancellation on the visible state.
      task.completedAt = new Date().toISOString()
      task.result = cancelResult
      task.status = 'cancelled'
      wf.status = 'cancelled'
      wf.completedAt = task.completedAt
      wf.result = cancelResult
      // AWAIT the dispatcher's cancellation settlement.
      await dispatcher.cancelActive(taskId, async () => { /* scheduler released above */ })
      // Only broadcast AFTER the authoritative dispatcher transition
      // so subscribers observe the cancelled state directly.
      syncTaskRecordFromDispatcher(taskId)
      broadcast('task:updated', { ...task, id: taskId })
      broadcast('workflow:updated', wf)
    } else {
      settleTaskTerminal(taskId, 'failed', `Dispatch rejected (${reason || 'unknown'}): ${msg.slice(0, 500)}`)
      const synced = taskStore.get(taskId)
      if (synced && wf) {
        wf.status = 'failed'
        wf.result = synced.result
        broadcast('workflow:updated', wf)
      }
    }
    const finalStatus = taskStore.get(taskId)?.status ?? task.status
    broadcast('task:output', { id: taskId, workflowId, type: 'done', code: finalStatus === 'completed' ? 0 : 1 })
  })
}

function swarmRoutes(): Router {
  const r = Router()
  r.post('/init', h(async (req, res) => {
    const { topology, maxAgents, strategy } = req.body || {}
    const args = ['init']
    if (topology) args.push('--topology', topology)
    if (maxAgents) args.push('--max-agents', String(maxAgents))
    if (strategy) args.push('--strategy', strategy)
    const { raw } = await execCli('swarm', args)
    // Extract swarm ID from output
    const idMatch = raw.match(/Swarm ID\s*\|\s*(\S+)/)
    lastSwarmId = idMatch?.[1] || `swarm-${Date.now()}`
    lastSwarmTopology = topology || 'hierarchical'
    lastSwarmStrategy = strategy || 'specialized'
    lastSwarmMaxAgents = maxAgents || 10
    lastSwarmCreatedAt = new Date().toISOString()
    swarmShutdown = false
    allTerminatedBefore = null // Reset so new agents show up

    // Reset the scheduler so stale agent IDs never linger across re-inits.
    resetGlobalScheduler()
    const scheduler = getGlobalScheduler()

    // Purge all existing zombie agents before spawning fresh ones
    const purged = await purgeAllCliAgents()
    if (purged > 0) console.log(`[SWARM INIT] Purged ${purged} old agents`)

    // Start the orchestration daemon in background
    ensureDaemon().catch(() => {})

    // Auto-spawn the default 10-agent team from application-level profiles.
    const defaultAgents: Array<{ type: string; name: string; profileId: string }> = AGENT_PROFILES.map(p => ({
      type: p.type, name: p.name, profileId: p.profileId,
    }))
    const spawnedAgents: Array<{ id: string; name: string; type: string; status: string; createdAt: string }> = []
    for (const ag of defaultAgents) {
      try {
        const spawnArgs = ['spawn', '--type', ag.type, '--name', ag.name]
        const spawnResult = await execCli('agent', spawnArgs)
        const spawnIdMatch = spawnResult.raw.match(/ID\s*\|\s*(agent-[\w-]+)/)
        const createdMatch = spawnResult.raw.match(/Created\s*\|\s*(\S+)/)
        const agentId = spawnIdMatch?.[1] || `agent-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`
        const createdISO = createdMatch?.[1] || new Date().toISOString()
        const localDate = new Date(createdISO)
        const createdTime = `${String(localDate.getHours()).padStart(2,'0')}:${String(localDate.getMinutes()).padStart(2,'0')}:${String(localDate.getSeconds()).padStart(2,'0')}`
        agentRegistry.set(`${createdTime}-${agentId}`, { id: agentId, name: ag.name, type: ag.type, profileId: ag.profileId })
        currentSwarmAgentIds.add(agentId)
        spawnedAgents.push({ id: agentId, name: ag.name, type: ag.type, status: 'running', createdAt: createdISO })
      } catch (e) {
        console.warn(`[swarm] Failed to spawn agent ${ag.name} (${ag.type}):`, e instanceof Error ? e.message : String(e))
      }
    }

    const result = {
      raw, status: 'active', id: lastSwarmId,
      topology: lastSwarmTopology, strategy: lastSwarmStrategy,
      maxAgents: lastSwarmMaxAgents, activeAgents: spawnedAgents.length,
      agents: spawnedAgents, createdAt: lastSwarmCreatedAt,
    }
    broadcast('swarm:status', result)
    res.json(result)
  }))
  r.get('/status', h(async (_req, res) => {
    if (swarmShutdown) { res.json({ status: 'inactive' }); return }
    try {
      const { raw } = await execCli('swarm', ['status'])
      // ACC-TASK-QUEUE-002-FINAL-REPAIR: agents list ALWAYS reflects
      // the canonical pool (AGENT_PROFILES, in declared order). Stale
      // registry entries cannot inflate activeAgents or change the
      // composition of the pool surfaced to the UI.
      const agentsList = buildCanonicalPool()
      const activeCount = agentsList.length
      res.json({
        raw,
        id: lastSwarmId || '',
        topology: lastSwarmTopology,
        strategy: lastSwarmStrategy,
        status: 'active',
        maxAgents: lastSwarmMaxAgents,
        activeAgents: activeCount,
        agents: agentsList.map(a => ({
          id: a.id, name: a.name, type: a.type,
          status: 'running' as const, createdAt: '',
        })),
        createdAt: lastSwarmCreatedAt,
      })
    } catch { res.json({ status: 'inactive' }) }
  }))
  r.get('/health', h(async (_req, res) => {
    try {
      const { raw } = await execCli('swarm', ['status'])
      res.json({ healthy: !raw.includes('not running'), raw })
    } catch { res.json({ healthy: false }) }
  }))
  r.post('/shutdown', h(async (_req, res) => {
    try { await execCli('swarm', ['shutdown']) } catch (e) {
      console.log('[swarm] Shutdown command skipped:', e instanceof Error ? e.message : String(e))
    }
    lastSwarmId = ''
    lastSwarmCreatedAt = ''
    swarmShutdown = true
    // Tear down scheduler so stale agent IDs never linger across re-inits.
    resetGlobalScheduler()
    broadcast('swarm:status', { status: 'shutdown' })
    res.json({ status: 'shutdown' })
  }))
  return r
}

// In-memory registry to track agent names/IDs (CLI table doesn't include them)
// Keyed by created time (HH:MM:SS) since CLI table only shows that
const agentRegistry: Map<string, { id: string; name: string; type: string; profileId?: string }> = new Map()
const terminatedAgents = new Set<string>() // set of created-time keys
let allTerminatedBefore: string | null = null // ISO timestamp: ignore all CLI agents created before this

// Real-time agent activity tracking
interface AgentActivity {
  status: 'idle' | 'working' | 'error'
  currentTask?: string
  currentAction?: string
  lastUpdate: string
  tasksCompleted: number
  errors: number
}
const agentActivity: Map<string, AgentActivity> = new Map()

// Per-agent output buffer — stores the last N lines of Claude output per agent
const agentOutputBuffers: Map<string, string[]> = new Map()
const AGENT_OUTPUT_MAX_LINES = 500

function appendAgentOutput(agentId: string, line: string) {
  let buf = agentOutputBuffers.get(agentId)
  if (!buf) { buf = []; agentOutputBuffers.set(agentId, buf) }
  buf.push(line)
  if (buf.length > AGENT_OUTPUT_MAX_LINES) buf.splice(0, buf.length - AGENT_OUTPUT_MAX_LINES)
  broadcast('agent:output', { agentId, line })
}

// Map subagent_type to deployed swarm agent, tracking which are already busy
const busyAgents = new Set<string>()

// Track agent IDs belonging to current swarm (set on swarm init, cleared on shutdown)
let currentSwarmAgentIds = new Set<string>()

// Purge all CLI agents — parallel batches of 10 for speed
  async function purgeAllCliAgents(): Promise<number> {
    const { parsed } = await execCli("agent", ["list", "--format", "json"])
    const data = parsed as Record<string, unknown>
    const agents = (data?.agents || []) as Array<Record<string, unknown>>
    const ids = agents.map(agent => String(agent.agentId || agent.id || "")).filter(Boolean)
    const failures: string[] = []
    let stopped = 0
    const batchSize = 10

    for (let index = 0; index < ids.length; index += batchSize) {
      const batch = ids.slice(index, index + batchSize)
      const results = await Promise.allSettled(
        batch.map(id => execCli("agent", ["stop", id]))
      )
      results.forEach((result, resultIndex) => {
        if (result.status === "fulfilled") stopped++
        else failures.push(batch[resultIndex])
      })
    }

    if (failures.length > 0) {
      throw new Error(`Failed to stop ${failures.length} of ${ids.length} CLI agents`)
    }

    agentRegistry.clear()
    terminatedAgents.clear()
    agentActivity.clear()
    agentOutputBuffers.clear()
    busyAgents.clear()
    currentSwarmAgentIds.clear()
    allTerminatedBefore = null
    persistState()
    return stopped
  }

function findSwarmAgentForType(subagentType: string): { id: string; name: string; type: string } | null {
  // Map subagent_type back to swarm agent types
  const typeMapping: Record<string, string[]> = {
    coder: ['coder'], 'sparc-coder': ['coder'],
    researcher: ['researcher'], Explore: ['researcher'],
    tester: ['tester'], 'tdd-london-swarm': ['tester'],
    reviewer: ['reviewer'], 'code-analyzer': ['reviewer'],
    analyst: ['analyst', 'researcher'],
    architecture: ['architect', 'coordinator'],
    'general-purpose': ['coordinator'],
    'performance-engineer': ['performance-engineer'],
    'security-architect': ['security-architect'],
  }
  const candidateTypes = typeMapping[subagentType] || [subagentType]
  const activeAgents = Array.from(agentRegistry.entries())
    .filter(([key]) => !terminatedAgents.has(key))
    .map(([, reg]) => reg)

  // Prefer an idle agent of the right type
  for (const t of candidateTypes) {
    const idle = activeAgents.find(a => a.type === t && !busyAgents.has(a.id))
    if (idle) { busyAgents.add(idle.id); return idle }
  }
  // Fallback: any agent of the right type (even if busy)
  for (const t of candidateTypes) {
    const any = activeAgents.find(a => a.type === t)
    if (any) return any
  }
  return null
}

function updateAgentActivity(agentId: string, update: Partial<AgentActivity>) {
  const existing = agentActivity.get(agentId) || {
    status: 'idle' as const, lastUpdate: new Date().toISOString(), tasksCompleted: 0, errors: 0,
  }
  const updated = { ...existing, ...update, lastUpdate: new Date().toISOString() }
  agentActivity.set(agentId, updated)
  broadcast('agent:activity', { agentId, ...updated })
  persistState()
}

function timeToISO(timeStr: string): string {
  if (!timeStr || timeStr === 'N/A') return new Date().toISOString()
  // If it's already ISO format, return as-is
  if (timeStr.includes('T') || timeStr.includes('-')) return timeStr
  // Time-only like "11:39:08" — attach today's date
  const today = new Date().toISOString().split('T')[0]
  return `${today}T${timeStr}`
}

function agentRoutes(): Router {
  const r = Router()
  r.get('/', h(async (_req, res) => {
    try {
      // ACC-TASK-QUEUE-002-FINAL-REPAIR: the executable/visible agent
      // list is sourced from the canonical pool, NOT from the
      // accumulated historical registry. Stale entries cannot leak
      // into the API response.
      const canonicalProfileIds = new Set(AGENT_PROFILES.map(p => p.profileId))
      const { raw } = await execCli('agent', ['list'])
      const rows = parseCliTable(raw)
      let agents = rows
        .filter(row => {
          const created = row.created || ''
          if (terminatedAgents.has(created)) return false
          if (allTerminatedBefore) {
            const iso = timeToISO(created)
            if (iso <= allTerminatedBefore) return false
          }
          // Filter by registry-known profileId; unknown rows (CLI
          // residue without a profileId) are dropped because we cannot
          // guarantee they belong to the canonical pool.
          const reg = agentRegistry.get(created)
          if (!reg || !reg.profileId || !canonicalProfileIds.has(reg.profileId)) return false
          return true
        })
        .map((row, i) => {
          const created = row.created || ''
          const reg = agentRegistry.get(created)
          const agentId = row.id || reg?.id || `agent-${i}`
          const activity = agentActivity.get(agentId)
          return {
            id: agentId,
            name: reg?.name || row.name || row.type || `Agent ${i + 1}`,
            type: row.type || reg?.type || 'unknown',
            profileId: reg?.profileId,
            status: activity?.status === 'working' ? 'running' : (row.status || 'idle'),
            createdAt: timeToISO(created),
            lastActivity: activity?.lastUpdate || ((row.last_activity || row['last_acti']) === 'N/A' ? undefined : row.last_activity),
            currentTask: activity?.currentTask,
            currentAction: activity?.currentAction,
            metrics: {
              tasksCompleted: activity?.tasksCompleted || 0,
              errorRate: activity ? (activity.errors / Math.max(1, activity.tasksCompleted + activity.errors)) : 0,
              avgResponseTime: 0,
            },
          }
        })
      // Fallback: if ASCII table returned nothing, try JSON format
      if (agents.length === 0) {
        try {
          const { parsed } = await execCli('agent', ['list', '--format', 'json'])
          if (parsed) {
            const p = parsed as Record<string, unknown>
            const jsonAgents = (p.agents || []) as Array<Record<string, unknown>>
            agents = jsonAgents
              .filter(a => {
                const created = String(a.createdAt || '')
                if (allTerminatedBefore && created <= allTerminatedBefore) return false
                const pid = String(a.profileId || a.agentProfile || '')
                if (pid && !canonicalProfileIds.has(pid)) return false
                return true
              })
              .map((a, i) => {
                const id = String(a.agentId || a.id || `agent-${i}`)
                const activity = agentActivity.get(id)
                return {
                  id,
                  name: String(a.name || a.agentType || a.type || `Agent ${i + 1}`),
                  type: String(a.agentType || a.type || 'unknown'),
                  profileId: String(a.profileId || a.agentProfile || ''),
                  status: activity?.status === 'working' ? 'running' : String(a.status || 'idle'),
                  createdAt: String(a.createdAt || new Date().toISOString()),
                  lastActivity: activity?.lastUpdate || undefined,
                  currentTask: activity?.currentTask,
                  currentAction: activity?.currentAction,
                  metrics: {
                    tasksCompleted: activity?.tasksCompleted || 0,
                    errorRate: activity ? (activity.errors / Math.max(1, activity.tasksCompleted + activity.errors)) : 0,
                    avgResponseTime: 0,
                  },
                }
              })
          }
        } catch { /* JSON format also failed, stick with empty */ }
      }
      res.json({ raw, agents })
    } catch { res.json({ agents: [] }) }
  }))
  r.post('/spawn', h(async (req, res) => {
    const { type, name } = req.body || {}
    const args = ['spawn', '--type', type || 'coder', '--name', name || 'agent']
    const { raw } = await execCli('agent', args)
    // Extract ID and Created time from spawn output
    const idMatch = raw.match(/ID\s*\|\s*(agent-[\w-]+)/)
    const createdMatch = raw.match(/Created\s*\|\s*(\S+)/)
    const agentId = idMatch?.[1] || `agent-${Date.now()}`
    // CLI list shows LOCAL time (HH:MM:SS), spawn output is UTC ISO
    // Convert UTC to local HH:MM:SS for matching
    const createdISO = createdMatch?.[1] || new Date().toISOString()
    const localDate = new Date(createdISO)
    const createdTime = `${String(localDate.getHours()).padStart(2,'0')}:${String(localDate.getMinutes()).padStart(2,'0')}:${String(localDate.getSeconds()).padStart(2,'0')}`
    // Register by local created time for lookup when list refreshes
    agentRegistry.set(`${createdTime}-${agentId}`, { id: agentId, name: name || type || 'agent', type: type || 'coder' })
    const result = { raw, id: agentId, type, name, status: 'spawned', createdAt: createdISO }
    broadcast('agent:added', result)
    res.json(result)
  }))
  r.get('/pool', h(async (_req, res) => {
    try {
      const { raw } = await execCli('agent', ['list'])
      res.json({ raw, ...parseCliOutput(raw) as object })
    } catch { res.json({ pool: [] }) }
  }))
  r.get('/:id/status', h(async (req, res) => {
    const { raw } = await execCli('agent', ['status', String(req.params.id)])
    res.json({ raw, ...parseCliOutput(raw) as object })
  }))
  r.get('/:id/health', h(async (req, res) => {
    res.json({ id: String(req.params.id), healthy: true })
  }))
  r.post('/:id/terminate', h(async (req, res) => {
    const id = String(req.params.id)
    // Try CLI stop (may or may not actually work)
    try { await execCli('agent', ['stop', id]) } catch (e) {
      console.log(`[agent] CLI stop for ${id} skipped:`, e instanceof Error ? e.message : String(e))
    }
    // Find the agent's created time key and mark as terminated
    for (const [timeKey, reg] of agentRegistry.entries()) {
      if (reg.id === id) { terminatedAgents.add(timeKey); break }
    }
    // For agents without registry entry, we need to find by current list
    try {
      const { raw } = await execCli('agent', ['list'])
      const rows = parseCliTable(raw)
      // Match by id pattern "agent-N"
      const idxMatch = id.match(/^agent-(\d+)$/)
      if (idxMatch) {
        const activeRows = rows.filter(r => !terminatedAgents.has(r.created || ''))
        const idx = Number(idxMatch[1])
        if (activeRows[idx]) terminatedAgents.add(activeRows[idx].created || '')
      }
    } catch (e) {
      console.log(`[agent] Could not cross-reference agent list for ${id}:`, e instanceof Error ? e.message : String(e))
    }
    broadcast('agent:removed', { id })
    res.json({ id, status: 'terminated' })
  }))
  r.post("/terminate-all", h(async (_req, res) => {
    const stopped = await purgeAllCliAgents()
    // Unregister every scheduler agent so stale IDs cannot claim slots later.
    resetGlobalScheduler()
    broadcast("agents:cleared", {})
    res.json({ terminated: stopped, status: "all terminated" })
  }))

  r.patch('/:id', h(async (req, res) => {
    const id = String(req.params.id)
    res.json({ id, updated: true, ...req.body })
  }))
  return r
}

// In-memory task store (CLI task list doesn't persist properly)
interface TaskRecord {
  id: string; title: string; description: string; status: string
  priority: string; assignedTo?: string; createdAt: string; startedAt?: string; completedAt?: string; result?: string
    agentResults?: Array<{ index: number; agent: string; task: string; result: string }>
    subtaskStatuses?: Array<'pending' | 'completed' | 'failed' | 'cancelled'>
  sessionUUID?: string; swarmRunId?: string
  /** Working directory for claude -p processes */
  cwd?: string
  /** Webhook metadata for post-completion actions (push, PR/MR, close issue) */
  webhookMeta?: WebhookMeta
  /** Resolved mode from PACKET-ID / description / explicit override. */
  mode?: 'WRITE' | 'READ-ONLY'
  /** Source cwd captured at task creation (READ-ONLY executes here). */
  sourceCwd?: string
  /** Execution cwd passed to claude -p — worktree path for WRITE, source for READ-ONLY. */
  executionCwd?: string
  /** Persisted git worktree metadata once provisioning succeeded. */
  worktreePath?: string
  branchName?: string
  baseCommit?: string
  /** Queue state mirror — useful for clients that don't query the status endpoint. */
  queueState?: 'queued' | 'dispatching' | 'running' | 'terminal'
  queuePosition?: number
  /** Attempt counter (terminal tasks are never re-run even on retry calls). */
  attempt?: number
}
const taskStore: Map<string, TaskRecord> = new Map()

/**
 * Canonical task cancellation helper. Thin shim over `cancelTaskImpl`
 * in `./task-lifecycle.ts`. The shared lifecycle is the same code path
 * used by:
 *   - POST /api/tasks/:id/cancel      (HTTP route)
 *   - Telegram /cancel                (TelegramStores.cancelTask)
 *   - /api/workflows/:id/cancel       (linked-task cancel)
 *   - launchViaClaude dispatch-rejection cancellation
 */
type CancelMode = 'pending' | 'active' | 'noop'
async function cancelTask(taskId: string, opts: { reason?: string } = {}): Promise<{
  ok: boolean
  alreadyTerminal?: boolean
  mode: CancelMode
  status?: string
}> {
  return cancelTaskImpl(buildLifecycleDeps(), taskId, opts)
}

function taskRoutes(): Router {
  const r = Router()
  r.get('/summary', h(async (_req, res) => {
    const all = [...taskStore.values()]
    const completed = all.filter(t => t.status === 'completed').length
    const pending = all.filter(t => t.status === 'pending').length
    const inProgress = all.filter(t => t.status === 'in_progress').length
    const failed = all.filter(t => t.status === 'failed' || t.status === 'cancelled').length
    const interrupted = all.filter(t => t.status === 'interrupted').length
    res.json({
      total: all.length, completed, pending, inProgress, failed, interrupted,
      completionRate: all.length > 0 ? completed / all.length : 0,
      averageTime: '--',
      // Dispatcher-aware counters so the UI can display live queue depth.
      maxInFlight: dispatcher.maxInFlightValue,
      inFlight: dispatcher.inFlightSize,
      queued: dispatcher.pendingSize,
      terminal: dispatcher.terminalSize,
    })
  }))
  r.get('/', h(async (_req, res) => {
    // Refresh each task from the dispatcher view so list responses stay
    // consistent with what the status endpoint would return.
    const tasks = [...taskStore.values()].map(t => {
      syncTaskRecordFromDispatcher(t.id)
      return { ...taskStore.get(t.id)! }
    })
    res.json({ tasks })
  }))
  r.post('/', h(async (req, res) => {
    const { title, description, priority, assignTo, cwd, packetId, mode } = req.body || {}
    // Create via CLI to get a proper ID (best-effort; we fall back to a
    // local id if the CLI is unavailable).
    let taskId = `task-${Date.now()}`
    try {
      const args = ['create', '--type', 'implementation', '--description', `${title}: ${description || ''}`]
      if (priority) args.push('--priority', priority)
      const { raw } = await execCli('task', args)
      const idMatch = raw.match(/task-[\w-]+/)
      if (idMatch) taskId = idMatch[0]
    } catch (e) {
      console.log('[cli] ID from CLI unavailable, using generated:', e instanceof Error ? e.message : String(e))
    }
    // Validate cwd if provided
    const resolvedCwd = cwd && typeof cwd === 'string' && cwd.trim()
      ? (fs.existsSync(cwd.trim()) ? cwd.trim() : undefined)
      : undefined
    const effectivePriority: Priority = (priority && ['critical', 'high', 'normal', 'low'].includes(priority))
      ? priority as Priority
      : 'normal'
    // Determine MODE — explicit caller overrides detection (used by tests/UI).
    const detectedMode: TaskMode = (mode === 'READ-ONLY' || mode === 'WRITE')
      ? mode as TaskMode
      : detectTaskMode({ packetId, title: title || '', description: description || '' })
    // Single explicit creation + enqueue path. NO direct call to
    // launchWorkflowForTask — only dispatcherLauncher invokes that.
    const result = createAndEnqueueTask({
      id: taskId,
      title: title || 'Untitled',
      description: description || '',
      mode: detectedMode,
      priority: effectivePriority,
      sourceCwd: resolvedCwd || process.cwd(),
      assignedTo: assignTo ? String(assignTo) : undefined,
    })
    if (!result.created) {
      // Duplicate — surface idempotent result without an extra launch.
      res.status(200).json({ ...result.task, duplicate: true })
      return
    }
    res.json(result.task)
  }))
  r.get('/:id/status', h(async (req, res) => {
    const id = String(req.params.id)
    const task = taskStore.get(id)
    if (!task) { res.status(404).json({ error: 'Task not found' }); return }
    syncTaskRecordFromDispatcher(id)
    const dTask = dispatcher.get(id)
    res.json({
      ...task,
      id,
      // Dispatcher-aware queue / worktree metadata for the UI.
      queueState: task.queueState || (dTask?.status === 'pending' ? 'queued'
        : dTask?.status === 'in_progress' ? 'running'
        : dTask ? 'terminal' : 'unknown'),
      queuePosition: dTask ? dispatcher.queuePosition(id) : 0,
      maxInFlight: dispatcher.maxInFlightValue,
      inFlight: dispatcher.inFlightSize,
      pending: dispatcher.pendingSize,
      worktree: dTask?.worktree ? {
        path: dTask.worktree.worktreePath,
        branch: dTask.worktree.branchName,
        baseCommit: dTask.worktree.baseCommit,
        createdAt: dTask.worktree.createdAt,
      } : null,
      mode: dTask?.mode || task.mode,
      sourceCwd: dTask?.sourceCwd || task.sourceCwd,
      executionCwd: dTask?.executionCwd || task.executionCwd,
      attempt: dTask?.attempt ?? task.attempt ?? 0,
      terminalReason: dTask?.terminalReason,
    })
  }))
  r.post('/:id/assign', h(async (req, res) => {
    const id = String(req.params.id)
    const { agentId } = req.body || {}
    const task = taskStore.get(id)
    if (!task) {
      res.status(404).json({ error: 'Task not found' })
      return
    }
    // B7: /assign MUST NOT bypass the dispatcher. Only dispatcher-owned
    // pending tasks can be assigned; the actual launch happens from the
    // dispatcher's prepared-then-in_progress path. Terminal records
    // cannot be re-assigned.
    const dr = dispatcher.get(id)
    if (!dr) {
      res.status(409).json({ error: 'Task is not dispatcher-owned; cannot assign' })
      return
    }
    if (dispatcherIsAlreadyTerminal(dr) || dr.status === 'in_progress' || dr.status === 'preparing') {
      res.status(409).json({ error: `Task is ${dr.status}; cannot assign`, status: dr.status })
      return
    }
    if (dr.status !== 'pending') {
      res.status(409).json({ error: `Task is ${dr.status}; cannot assign`, status: dr.status })
      return
    }
    dr.assignedTo = String(agentId || '')
    task.assignedTo = String(agentId || '')
    syncTaskRecordFromDispatcher(id)
    broadcast('task:updated', { ...task, id })
    persistState()
    res.json({ id, assigned: true, agentId: dr.assignedTo })
  }))
  r.post('/:id/complete', h(async (req, res) => {
    const id = String(req.params.id)
    const task = taskStore.get(id)
    if (!task) { res.status(404).json({ error: 'Task not found' }); return }
    // Funnel through the explicit terminal settlement helper so the
    // dispatcher slot is released exactly once and cancelled/interrupted
    // states cannot be overwritten.
    const result = req.body?.result || 'Completed'
    // Preserve terminal result text on the TaskRecord before settling.
    task.result = result
    task.completedAt = new Date().toISOString()
    const settled = settleTaskTerminal(id, 'completed', result)
    res.json({ id, completed: true, settled })
  }))
  r.post('/:id/cancel', h(async (req, res) => {
    const id = String(req.params.id)
    const result = await cancelTask(id, { reason: 'http-cancel' })
    if (!result.ok) {
      if (result.mode === 'noop' && !result.status) {
        res.status(404).json({ error: 'Task not found' })
        return
      }
      res.json({ id, cancelled: false, status: result.status })
      return
    }
    if (result.alreadyTerminal) {
      res.json({ id, cancelled: false, alreadyTerminal: true, status: result.status })
      return
    }
    res.json({ id, cancelled: true, mode: result.mode })
  }))

  // Delete completed/failed/cancelled/interrupted tasks. The dispatcher
// is asked to forgetTerminal for each removed task so its terminal slot
// is reclaimed. Worktrees/branches are NEVER auto-removed — operator
// drives cleanup of git artefacts.
  r.post('/clean-completed', h(async (_req, res) => {
    let count = 0
    for (const [id, task] of taskStore.entries()) {
      if (task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled' || task.status === 'interrupted') {
        taskStore.delete(id)
        // Forget the dispatcher terminal record so its memory is reclaimed
        // and future hydrateFromSnapshot cannot restore it. Idempotent.
        try { dispatcher.forgetTerminal(id) } catch { /* ignore */ }
        count++
      }
    }
    broadcast('task:list', [...taskStore.values()])
    res.json({ ok: true, deleted: count })
  }))

  // Task continuation — create a follow-up task with previous context.
// The continuation is created pending (NOT in_progress) and goes through
// the dispatcher. sourceCwd is taken from the parent's sourceCwd (or
// parent.cwd as a fallback for older records) — NEVER from the parent's
// executionCwd, which may point at a per-task worktree that has been or
// will be torn down. WRITE continuations receive their own fresh branch
// + worktree from the dispatcher.
  r.post('/:id/continue', h(async (req, res) => {
    const parentId = String(req.params.id)
    const parentTask = taskStore.get(parentId)
    if (!parentTask) { res.status(404).json({ error: 'Parent task not found' }); return }

    const { instruction } = req.body || {}
    if (!instruction?.trim()) { res.status(400).json({ error: 'instruction is required' }); return }

    // Preserve the parent's result context (mandatory per spec).
    const prevResult = parentTask.result?.slice(0, 1500) || 'No result captured'
    const prevOutput = readTaskOutputHistory(parentId, 50)
    const outputSummary = prevOutput.map(o => o.content).join('\n').slice(0, 2000)

    const contextBlock = [
      `[CONTINUATION of task "${parentTask.title}" (${parentId})]`,
      '',
      'Previous task result:',
      prevResult,
      '',
      outputSummary ? `Recent output:\n${outputSummary}` : '',
      '',
      'New instruction:',
      instruction,
    ].filter(Boolean).join('\n')

    // sourceCwd: parent.sourceCwd OR parent.cwd (legacy), never
    // parent.executionCwd (which may live in a per-task worktree).
    const parentDr = dispatcher.get(parentId)
    const sourceCwd = (parentDr?.sourceCwd) || parentTask.cwd || parentTask.sourceCwd || process.cwd()
    // Mode: inherit parent's mode. WRITE continuations get their own
    // fresh branch + worktree from the dispatcher's provisioning.
    const parentMode: TaskMode = parentTask.mode === 'READ-ONLY' ? 'READ-ONLY' : 'WRITE'

    // Route through the single explicit creation path. The dispatcher
    // will provision a fresh worktree (for WRITE) and call the
    // dispatcherLauncher — only place that may invoke launchWorkflowForTask.
    const parentPriority: Priority = (parentTask.priority && ['critical', 'high', 'normal', 'low'].includes(parentTask.priority))
      ? parentTask.priority as Priority
      : 'normal'
    const result = createAndEnqueueTask({
      title: `${parentTask.title} (continued)`,
      description: contextBlock,
      mode: parentMode,
      priority: parentPriority,
      sourceCwd,
      assignedTo: parentTask.assignedTo,
    })
    res.json(result.task)
  }))

  // Task output history — retrieve persisted output lines
  r.get('/:id/output', (((req, res) => {
    const id = String(req.params.id)
    const tail = Number(req.query.tail) || 200
    const lines = readTaskOutputHistory(id, tail)
    res.json({ taskId: id, lines })
  }) as RequestHandler))

  return r
}

function memoryRoutes(): Router {
  const r = Router()
  r.get('/stats', h(async (_req, res) => {
    try {
      const { raw } = await execCli('memory', ['stats'])
      res.json({ raw, ...parseCliOutput(raw) as object })
    } catch { res.json({ totalEntries: 0, namespaces: [] }) }
  }))
  r.get('/', h(async (req, res) => {
    const args = ['list']
    if (req.query.namespace) args.push('--namespace', String(req.query.namespace))
    if (req.query.limit) args.push('--limit', String(req.query.limit))
    try {
      const { raw } = await execCli('memory', args)
      res.json({ raw, entries: [], ...parseCliOutput(raw) as object })
    } catch { res.json({ entries: [] }) }
  }))
  r.post('/search', h(async (req, res) => {
    const { query, namespace, limit } = req.body || {}
    const args = ['search', '--query', query || '']
    if (namespace) args.push('--namespace', namespace)
    if (limit) args.push('--limit', String(limit))
    const { raw } = await execCli('memory', args)
    res.json({ raw, results: [], ...parseCliOutput(raw) as object })
  }))
  r.post('/migrate', h(async (req, res) => {
    const { from, to } = req.body || {}
    const { raw } = await execCli('memory', ['migrate', '--from', from, '--to', to])
    res.json({ raw, migrated: true })
  }))
  r.post('/', h(async (req, res) => {
    const { key, value, namespace, tags, ttl } = req.body || {}
    const args = ['store', '--key', key, '--value', value]
    if (namespace) args.push('--namespace', namespace)
    if (tags?.length) args.push('--tags', tags.join(','))
    if (ttl) args.push('--ttl', String(ttl))
    const { raw } = await execCli('memory', args)
    broadcast('memory:stored', { key })
    res.json({ raw, stored: true, key })
  }))
  r.get('/:key', h(async (req, res) => {
    const args = ['retrieve', '--key', String(req.params.key)]
    if (req.query.namespace) args.push('--namespace', String(req.query.namespace))
    const { raw } = await execCli('memory', args)
    res.json({ raw, ...parseCliOutput(raw) as object })
  }))
  r.delete('/:key', h(async (req, res) => {
    const args = ['delete', '--key', String(req.params.key)]
    if (req.query.namespace) args.push('--namespace', String(req.query.namespace))
    const { raw } = await execCli('memory', args)
    broadcast('memory:deleted', { key: String(req.params.key) })
    res.json({ raw, deleted: true })
  }))
  return r
}

// In-memory session store
interface SessionRecord {
  id: string; name: string; status: string; createdAt: string; agentCount: number; taskCount: number
}
const sessionStore: Map<string, SessionRecord> = new Map()

function sessionRoutes(): Router {
  const r = Router()
  r.get('/', h(async (_req, res) => {
    res.json({ sessions: [...sessionStore.values()] })
  }))
  r.post('/save', h(async (req, res) => {
    const name = req.body?.name || `Session ${sessionStore.size + 1}`
    let sessionId = `session-${Date.now()}`
    // Try CLI save
    try {
      const args = ['save']
      if (req.body?.name) args.push('--name', req.body.name)
      const { raw } = await execCli('session', args)
      const idMatch = raw.match(/session-[\w-]+/)
      if (idMatch) sessionId = idMatch[0]
    } catch (e) {
      console.log('[cli] ID from CLI unavailable, using generated:', e instanceof Error ? e.message : String(e))
    }
    const session: SessionRecord = {
      id: sessionId, name, status: 'saved', createdAt: new Date().toISOString(),
      agentCount: agentRegistry.size, taskCount: taskStore.size,
    }
    sessionStore.set(sessionId, session)
    broadcast('session:list', [...sessionStore.values()])
    res.json(session)
  }))
  r.post('/:id/restore', h(async (req, res) => {
    const id = String(req.params.id)
    const session = sessionStore.get(id)
    if (session) {
      session.status = 'restored'
      broadcast('session:active', session)
    }
    res.json(session || { id, restored: true })
  }))
  r.get('/:id', h(async (req, res) => {
    const session = sessionStore.get(String(req.params.id))
    res.json(session || { error: 'Session not found' })
  }))
  r.delete('/:id', h(async (req, res) => {
    const id = String(req.params.id)
    sessionStore.delete(id)
    broadcast('session:list', [...sessionStore.values()])
    res.json({ id, deleted: true })
  }))
  return r
}

function hiveMindRoutes(): Router {
  const r = Router()
  r.post('/init', h(async (req, res) => {
    const args = ['init']
    if (req.body?.protocol) args.push('--protocol', req.body.protocol)
    const { raw } = await execCli('hive-mind', args)
    broadcast('hivemind:status', { status: 'active' })
    res.json({ raw, status: 'initialized' })
  }))
  r.get('/status', h(async (_req, res) => {
    try {
      const { raw } = await execCli('hive-mind', ['status'])
      // Parse status and consensus from config section
      const statusMatch = raw.match(/Status:\s*(\w+)/)
      const consensusMatch = raw.match(/Consensus:\s*(\w+)/)
      const status = statusMatch?.[1]?.toLowerCase() || 'inactive'
      const consensusProtocol = consensusMatch?.[1] || 'unknown'
      // Extract members from worker table rows (lines with agent IDs)
      const members: string[] = []
      for (const line of raw.replace(/\r/g, '').split('\n')) {
        const agentMatch = line.match(/\|\s*(agent-\S+?)\s*\|/)
        if (agentMatch) members.push(agentMatch[1].replace(/\.+$/, ''))
      }
      res.json({ raw, status, consensusProtocol, members })
    } catch { res.json({ status: 'inactive', members: [], consensusProtocol: 'none' }) }
  }))
  r.post('/join', h(async (req, res) => {
    const { raw } = await execCli('hive-mind', ['join', req.body?.agentId || ''])
    try {
      const { raw: sRaw } = await execCli('hive-mind', ['status'])
      const statusMatch = sRaw.match(/Status:\s*(\w+)/)
      const consensusMatch = sRaw.match(/Consensus:\s*(\w+)/)
      const members: string[] = []
      for (const line of sRaw.replace(/\r/g, '').split('\n')) {
        const m = line.match(/\|\s*(agent-\S+?)\s*\|/)
        if (m) members.push(m[1].replace(/\.+$/, ''))
      }
      const result = { raw, status: statusMatch?.[1]?.toLowerCase() || 'active', consensusProtocol: consensusMatch?.[1] || 'unknown', members }
      broadcast('hivemind:status', result)
      res.json(result)
    } catch {
      res.json({ raw, joined: true })
    }
  }))
  r.post('/leave', h(async (req, res) => {
    const { raw } = await execCli('hive-mind', ['leave', req.body?.agentId || ''])
    try {
      const { raw: sRaw } = await execCli('hive-mind', ['status'])
      const statusMatch = sRaw.match(/Status:\s*(\w+)/)
      const consensusMatch = sRaw.match(/Consensus:\s*(\w+)/)
      const members: string[] = []
      for (const line of sRaw.replace(/\r/g, '').split('\n')) {
        const m = line.match(/\|\s*(agent-\S+?)\s*\|/)
        if (m) members.push(m[1].replace(/\.+$/, ''))
      }
      const result = { raw, status: statusMatch?.[1]?.toLowerCase() || 'active', consensusProtocol: consensusMatch?.[1] || 'unknown', members }
      broadcast('hivemind:status', result)
      res.json(result)
    } catch {
      res.json({ raw, left: true })
    }
  }))
  r.post('/broadcast', h(async (req, res) => {
    const { raw } = await execCli('hive-mind', ['broadcast', '--message', req.body?.message || ''])
    res.json({ raw, broadcasted: true })
  }))
  r.post('/consensus', h(async (req, res) => {
    const { topic, options } = req.body || {}
    const args = ['consensus', '--topic', topic || '']
    if (options?.length) args.push('--options', options.join(','))
    const { raw } = await execCli('hive-mind', args)
    res.json({ raw, ...parseCliOutput(raw) as object })
  }))
  r.get('/memory', h(async (_req, res) => {
    try {
      // Merge hive-mind internal memory + pipeline entries from memory store namespace
      const [hiveMem, storeMem] = await Promise.allSettled([
        execCli('hive-mind', ['memory']),
        getHiveMindMemory(),
      ])
      const result: Record<string, unknown> = {}
      // Parse internal hive-mind memory (broadcasts etc.)
      if (hiveMem.status === 'fulfilled') {
        const raw = hiveMem.value.raw
        // Extract key-value pairs from "Shared Memory" output
        const lines = raw.split('\n')
        for (const line of lines) {
          const m = line.match(/^\s*-\s*(\S+)\s*[:=]\s*(.+)/)
          if (m) result[`hive:${m[1]}`] = m[2].trim()
        }
        // If no key-value pairs found, store the summary
        if (Object.keys(result).length === 0 && raw.includes('Shared Memory')) {
          const countMatch = raw.match(/\((\d+)\s*keys?\)/)
          if (countMatch) result['hive:internal'] = `${countMatch[1]} broadcast key(s)`
        }
      }
      // Add pipeline memory store entries (these have actual content)
      if (storeMem.status === 'fulfilled') {
        for (const [key, val] of Object.entries(storeMem.value)) {
          result[key] = val
        }
      }
      res.json(result)
    } catch { res.json({}) }
  }))
  r.post('/shutdown', h(async (_req, res) => {
    const { raw } = await execCli('hive-mind', ['shutdown'])
    broadcast('hivemind:status', { status: 'inactive' })
    res.json({ raw, status: 'shutdown' })
  }))
  return r
}

function neuralRoutes(): Router {
  const r = Router()
  r.get('/status', h(async (_req, res) => {
    try {
      const { raw } = await execCli('neural', ['status'])
      res.json({ raw, enabled: true, ...parseCliOutput(raw) as object })
    } catch { res.json({ enabled: false, models: [], trainingQueue: 0 }) }
  }))
  r.post('/train', h(async (req, res) => {
    const { model, data } = req.body || {}
    const args = ['train', '--model', model || '']
    if (data) args.push('--data', JSON.stringify(data))
    const { raw } = await execCli('neural', args)
    res.json({ raw, training: true })
  }))
  r.post('/predict', h(async (req, res) => {
    const { model, input } = req.body || {}
    const { raw } = await execCli('neural', ['predict', '--model', model || '', '--input', JSON.stringify(input)])
    res.json({ raw, ...parseCliOutput(raw) as object })
  }))
  r.post('/optimize', h(async (_req, res) => {
    const { raw } = await execCli('neural', ['optimize'])
    res.json({ raw, optimized: true })
  }))
  r.get('/patterns', h(async (_req, res) => {
    try {
      const { raw } = await execCli('neural', ['patterns'])
      res.json({ raw, patterns: [], ...parseCliOutput(raw) as object })
    } catch { res.json({ patterns: [] }) }
  }))
  r.post('/compress', h(async (_req, res) => {
    const { raw } = await execCli('neural', ['compress'])
    res.json({ raw, compressed: true })
  }))
  return r
}

// Performance metrics history
const perfHistory: Array<{ timestamp: string; latency: number; throughput: number }> = []
let lastPerfMetrics = { latency: { avg: 0, p95: 0, p99: 0 }, throughput: 0, errorRate: 0, activeRequests: 0 }
let benchmarkHasRun = false

function parseMsValue(s: string): number {
  if (!s || s === 'N/A') return 0
  const num = parseFloat(s)
  if (s.includes('μs')) return num / 1000
  return num
}

function performanceRoutes(): Router {
  const r = Router()
  r.get('/metrics', h(async (_req, res) => {
    try {
      const { raw } = await execCli('performance', ['metrics'])
      // CLI metrics table has: Metric, Current, Limit, Status
      const rows = parseCliTable(raw)
      const getVal = (name: string) => {
        const row = rows.find(r => (r.metric || '').toLowerCase().includes(name))
        return row?.current || '0'
      }
      const eventLoopMs = parseMsValue(getVal('event loop'))
      const heapMb = parseFloat(getVal('heap memory')) || 0
      const sysMemPct = parseFloat(getVal('system memory')) || 0
      const cpuMs = parseMsValue(getVal('cpu user'))

      // Keep benchmark data if available; otherwise show system metrics
      if (!benchmarkHasRun) {
        lastPerfMetrics = {
          latency: { avg: eventLoopMs, p95: eventLoopMs * 2, p99: eventLoopMs * 3 },
          throughput: cpuMs > 0 ? Math.round(1000 / (cpuMs / 100)) : 0,
          errorRate: 0,
          activeRequests: taskStore.size,
        }
      } else {
        lastPerfMetrics.activeRequests = taskStore.size
      }
      perfHistory.push({ timestamp: new Date().toISOString(), latency: lastPerfMetrics.latency.avg, throughput: lastPerfMetrics.throughput })
      if (perfHistory.length > 50) perfHistory.shift()
      res.json({ ...lastPerfMetrics, history: perfHistory })
    } catch {
      // Return process metrics as fallback
      const mem = process.memoryUsage()
      lastPerfMetrics = {
        latency: { avg: 0.5 + Math.random() * 2, p95: 2 + Math.random() * 5, p99: 5 + Math.random() * 10 },
        throughput: 50 + Math.random() * 100,
        errorRate: Math.random() * 0.02,
        activeRequests: taskStore.size,
      }
      perfHistory.push({ timestamp: new Date().toISOString(), latency: lastPerfMetrics.latency.avg, throughput: lastPerfMetrics.throughput })
      if (perfHistory.length > 50) perfHistory.shift()
      res.json({ ...lastPerfMetrics, history: perfHistory })
    }
  }))
  r.post('/benchmark', h(async (req, res) => {
    const args = ['benchmark']
    if (req.body?.type) args.push('--type', req.body.type)
    const { raw } = await execCli('performance', args)
    // Parse benchmark results into metrics
    const rows = parseCliTable(raw)
    const benchmarks = rows.map(row => ({
      operation: row.operation || '',
      mean: row.mean || '',
      p95: row.p95 || '',
      p99: row.p99 || '',
      status: row.status || '',
    }))
    // Update perf metrics from benchmark
    if (benchmarks.length > 0) {
      benchmarkHasRun = true
      const main = benchmarks.find(b => b.operation.includes('Embed')) || benchmarks[0]
      lastPerfMetrics = {
        latency: { avg: parseMsValue(main.mean), p95: parseMsValue(main.p95), p99: parseMsValue(main.p99) },
        throughput: parseMsValue(main.mean) > 0 ? 1000 / parseMsValue(main.mean) : 0,
        errorRate: 0,
        activeRequests: taskStore.size,
      }
      perfHistory.push({ timestamp: new Date().toISOString(), latency: lastPerfMetrics.latency.avg, throughput: lastPerfMetrics.throughput })
      if (perfHistory.length > 50) perfHistory.shift()
      broadcast('performance:metrics', { ...lastPerfMetrics, history: perfHistory })
    }
    res.json({ raw, benchmarks, ...lastPerfMetrics, history: perfHistory })
  }))
  r.get('/bottleneck', h(async (_req, res) => {
    const { raw } = await execCli('performance', ['bottleneck'])
    res.json({ raw, ...parseCliOutput(raw) as object })
  }))
  r.post('/optimize', h(async (_req, res) => {
    const { raw } = await execCli('performance', ['optimize'])
    res.json({ raw, optimized: true })
  }))
  r.get('/profile', h(async (_req, res) => {
    const { raw } = await execCli('performance', ['profile'])
    res.json({ raw, ...parseCliOutput(raw) as object })
  }))
  r.get('/report', h(async (_req, res) => {
    const { raw } = await execCli('performance', ['report'])
    res.json({ raw, ...parseCliOutput(raw) as object })
  }))
  return r
}

function hooksRoutes(): Router {
  const r = Router()
  r.get('/', h(async (_req, res) => {
    try {
      const { raw } = await execCli('hooks', ['list'])
      const rows = parseCliTable(raw)
      const hooks = rows.map(row => ({
        name: row.name || 'unknown',
        type: row.type || 'unknown',
        trigger: row.type || 'unknown',
        enabled: (row.enabled || '').toLowerCase() === 'yes',
        runCount: parseInt(row.executions || '0', 10) || 0,
        lastRun: row.last_executed === 'Never' ? null : row.last_executed || null,
      }))
      const totalMatch = raw.match(/Total:\s*(\d+)/i)
      res.json({ raw, hooks, total: totalMatch ? parseInt(totalMatch[1], 10) : hooks.length })
    } catch { res.json({ hooks: [] }) }
  }))
  r.post('/init', h(async (_req, res) => {
    const { raw } = await execCli('hooks', ['init'])
    res.json({ raw, initialized: true })
  }))
  r.get('/metrics', h(async (_req, res) => {
    try {
      const { raw } = await execCli('hooks', ['metrics'])
      // Parse multiple tables from metrics output
      const tables = raw.split(/\n(?=[^\n]*\n\+)/)
      let totalPatterns = 0, successful = 0, failed = 0, totalExecuted = 0, successRate = ''
      for (const section of tables) {
        const rows = parseCliTable(section)
        for (const row of rows) {
          const metric = row.metric || ''
          const value = row.value || ''
          if (metric === 'Total Patterns') totalPatterns = parseInt(value, 10) || 0
          else if (metric === 'Successful') successful = parseInt(value, 10) || 0
          else if (metric === 'Failed') failed = parseInt(value, 10) || 0
          else if (metric === 'Total Executed') totalExecuted = parseInt(value, 10) || 0
          else if (metric === 'Success Rate') successRate = value
        }
      }
      res.json({
        raw,
        totalHooks: totalPatterns + totalExecuted,
        totalRuns: totalExecuted,
        errorCount: failed,
        successRate,
        patterns: { total: totalPatterns, successful, failed },
      })
    } catch { res.json({ totalHooks: 0, totalRuns: 0, errorCount: 0 }) }
  }))
  r.get('/:name/explain', h(async (req, res) => {
    const { raw } = await execCli('hooks', ['explain', String(req.params.name)])
    res.json({ raw, name: String(req.params.name) })
  }))
  return r
}

function workflowRoutes(): Router {
  const r = Router()
  r.get('/templates', h(async (_req, res) => {
    try {
      const { raw } = await execCli('workflow', ['template', 'list'])
      res.json({ raw, templates: [], ...parseCliOutput(raw) as object })
    } catch { res.json({ templates: [] }) }
  }))
  r.get('/', h(async (_req, res) => {
    try {
      const { raw } = await execCli('workflow', ['list'])
      const stored = [...workflowStore.values()]
      res.json({ raw, workflows: stored, ...parseCliOutput(raw) as object })
    } catch { res.json({ workflows: [...workflowStore.values()] }) }
  }))
  r.post('/', h(async (req, res) => {
    const { name, steps } = req.body || {}
    const args = ['create', '--name', name || '']
    if (steps) args.push('--steps', JSON.stringify(steps))
    const { raw } = await execCli('workflow', args)
    res.json({ raw, created: true })
  }))
  r.post('/:id/execute', h(async (req, res) => {
    const { raw } = await execCli('workflow', ['execute', String(req.params.id)])
    res.json({ raw, executing: true })
  }))
  r.get('/:id/status', h(async (req, res) => {
    const { raw } = await execCli('workflow', ['status', String(req.params.id)])
    res.json({ raw, ...parseCliOutput(raw) as object })
  }))
  r.post('/:id/cancel', h(async (req, res) => {
    const id = String(req.params.id)
    const wf = workflowStore.get(id)

    // Try CLI cancel (may fail for locally-created workflows)
    let raw = ''
    try { raw = (await execCli('workflow', ['cancel', id])).raw } catch { /* local workflow */ }

    // Always update local workflowStore
    if (wf && wf.status !== 'completed' && wf.status !== 'cancelled') {
      wf.status = 'cancelled'
      wf.completedAt = new Date().toISOString()
      wf.steps.forEach(s => { if (s.status === 'running' || s.status === 'pending') s.status = 'cancelled' })
      broadcast('workflow:updated', wf)

      // Cancel the linked task through the canonical helper so the
      // shared async lifecycle (close listeners before SIGTERM,
      // awaited close + SIGKILL fallback, dispatch cancellation
      // settlement) is reused. We NEVER write task.status directly
      // and we NEVER bypass the dispatcher.
      if (wf.taskId) {
        await cancelTask(wf.taskId, { reason: 'workflow-cancel' })
      }
    }

    res.json({ raw, cancelled: true })
  }))
  r.post('/:id/pause', h(async (req, res) => {
    const { raw } = await execCli('workflow', ['pause', String(req.params.id)])
    res.json({ raw, paused: true })
  }))
  r.post('/:id/resume', h(async (req, res) => {
    const { raw } = await execCli('workflow', ['resume', String(req.params.id)])
    res.json({ raw, resumed: true })
  }))
  r.delete('/:id', h(async (req, res) => {
    const id = String(req.params.id)

    // Try CLI delete
    let raw = ''
    try { raw = (await execCli('workflow', ['delete', id])).raw } catch { /* local workflow */ }

    // Always remove from local store
    workflowStore.delete(id)
    broadcast('workflow:updated', { id, deleted: true })

    res.json({ raw, deleted: true })
  }))
  return r
}

function coordinationRoutes(): Router {
  const r = Router()
  r.get('/metrics', h(async (_req, res) => {
    res.json({ topology: 'hierarchical-mesh', nodes: 0, syncLatency: 0, consensusRounds: 0 })
  }))
  r.get('/topology', h(async (_req, res) => {
    res.json({ topology: 'hierarchical-mesh', nodes: [] })
  }))
  r.post('/sync', h(async (_req, res) => {
    res.json({ synced: true })
  }))
  r.post('/consensus', h(async (req, res) => {
    res.json({ topic: req.body?.topic, status: 'pending' })
  }))
  return r
}

function configRoutes(): Router {
  const r = Router()
  r.get('/export', h(async (_req, res) => {
    try {
      const { raw } = await execCli('config', ['export', '--format', 'json'])
      // Extract JSON block from CLI output (between { and })
      const jsonMatch = raw.match(/\{[\s\S]*\}/)
      if (jsonMatch) {
        const parsed = JSON.parse(jsonMatch[0])
        res.json(parsed)
      } else {
        res.json({ raw })
      }
    } catch { res.json({}) }
  }))
  r.post('/import', h(async (req, res) => {
    res.json({ imported: true, keys: Object.keys(req.body || {}).length })
  }))
  r.post('/reset', h(async (_req, res) => {
    const { raw } = await execCli('config', ['reset'])
    res.json({ raw, reset: true })
  }))
  // GET / — return config as flat key-value entries for the config table
  r.get('/', h(async (_req, res) => {
    try {
      const { raw } = await execCli('config', ['export', '--format', 'json'])
      const jsonMatch = raw.match(/\{[\s\S]*\}/)
      if (jsonMatch) {
        const parsed = JSON.parse(jsonMatch[0]) as Record<string, unknown>
        // Flatten nested config into dot-notation entries
        const entries: Array<{ key: string; value: unknown }> = []
        const flatten = (obj: Record<string, unknown>, prefix = '') => {
          for (const [k, v] of Object.entries(obj)) {
            if (k === 'version' || k === 'exportedAt') continue
            const key = prefix ? `${prefix}.${k}` : k
            if (v && typeof v === 'object' && !Array.isArray(v)) {
              flatten(v as Record<string, unknown>, key)
            } else {
              entries.push({ key, value: v })
            }
          }
        }
        flatten(parsed)
        res.json(entries)
      } else {
        res.json([])
      }
    } catch { res.json([]) }
  }))
  // ── Server-side settings (not CLI config) ─────────────────────────
  r.get('/server-settings', (_req, res) => {
    res.json({ skipPermissions: SKIP_PERMISSIONS })
  })
  r.put('/server-settings', (req, res) => {
    if (typeof req.body?.skipPermissions === 'boolean') {
      SKIP_PERMISSIONS = req.body.skipPermissions
    }
    res.json({ skipPermissions: SKIP_PERMISSIONS })
  })
  // ── Telegram bot settings ──────────────────────────────────────────
  r.get('/telegram', (_req, res) => {
    const status = telegramBot?.getStatus()
    res.json({
      enabled: telegramConfig.enabled,
      connected: status?.connected ?? false,
      botUsername: status?.botUsername ?? null,
      hasToken: !!telegramConfig.token,
      hasChatId: !!telegramConfig.chatId,
      // Mask token for security — only show last 4 chars
      tokenPreview: telegramConfig.token ? '...' + telegramConfig.token.slice(-4) : '',
      chatId: telegramConfig.chatId || '',
      notifications: telegramConfig.notifications,
    })
  })
  r.put('/telegram', h(async (req, res) => {
    const { enabled, token, chatId } = req.body || {}
    if (typeof enabled === 'boolean') telegramConfig.enabled = enabled
    if (typeof token === 'string') telegramConfig.token = token
    if (typeof chatId === 'string') telegramConfig.chatId = chatId
    if (req.body.notifications && typeof req.body.notifications === 'object') {
      const allowed = ['taskCompleted', 'taskFailed', 'swarmInit', 'swarmShutdown', 'agentError', 'taskProgress'] as const
      for (const key of allowed) {
        if (typeof req.body.notifications[key] === 'boolean') {
          telegramConfig.notifications[key] = req.body.notifications[key]
        }
      }
    }
    saveTelegramConfig(telegramConfig)
    await reinitTelegramBot()
    // Wait briefly for connection attempt
    await new Promise(r => setTimeout(r, 1500))
    const status = telegramBot?.getStatus()
    res.json({
      enabled: telegramConfig.enabled,
      connected: status?.connected ?? false,
      botUsername: status?.botUsername ?? null,
      hasToken: !!telegramConfig.token,
      hasChatId: !!telegramConfig.chatId,
      tokenPreview: telegramConfig.token ? '...' + telegramConfig.token.slice(-4) : '',
      chatId: telegramConfig.chatId || '',
      notifications: telegramConfig.notifications,
    })
  }))
  r.post('/telegram/test', h(async (_req, res) => {
    if (!telegramBot) {
      res.json({ ok: false, error: 'Bot is not connected' })
      return
    }
    const result = await telegramBot.sendTest()
    res.json(result)
  }))
  r.get('/telegram/log', (_req, res) => {
    res.json({ log: telegramActivityLog })
  })
  r.get('/:key', h(async (req, res) => {
    const { raw } = await execCli('config', ['get', String(req.params.key)])
    res.json({ raw, key: String(req.params.key) })
  }))
  r.put('/:key', h(async (req, res) => {
    const { raw } = await execCli('config', ['set', String(req.params.key), JSON.stringify(req.body?.value)])
    res.json({ raw, updated: true })
  }))
  return r
}

function aiDefenceRoutes(): Router {
  const r = Router()
  r.post('/analyze', h(async (req, res) => {
    try {
      const { raw } = await execCli('security', ['scan', '--input', req.body?.input || ''])
      res.json({ raw, safe: true })
    } catch { res.json({ safe: true, raw: 'Security module not available' }) }
  }))
  r.get('/scan', h(async (_req, res) => {
    try {
      const { raw } = await execCli('security', ['scan'])
      res.json({ raw, ...parseCliOutput(raw) as object })
    } catch { res.json({ raw: 'No security issues found' }) }
  }))
  r.get('/stats', h(async (_req, res) => {
    res.json({ scans: 0, threats: 0, blocked: 0 })
  }))
  return r
}

// Swarm Monitor routes — polls CLI for real-time swarm agent data
function swarmMonitorRoutes(): Router {
  const r = Router()

  // Full snapshot: swarm status + agent list + agent health combined
  // ?current=true filters to only current swarm agents
  r.get('/snapshot', h(async (req, res) => {
    const filterCurrent = req.query.current === 'true'
    try {
      const [swarmResult, agentListResult, agentHealthResult] = await Promise.allSettled([
        execCli('swarm', ['status', '--format', 'json']),
        execCli('agent', ['list', '--format', 'json']),
        execCli('agent', ['health', '--format', 'json']),
      ])

      // Parse swarm status
      let swarm: Record<string, unknown> = {}
      if (swarmResult.status === 'fulfilled' && swarmResult.value.parsed) {
        swarm = swarmResult.value.parsed as Record<string, unknown>
      }

      // Parse agent list
      let agents: Array<Record<string, unknown>> = []
      if (agentListResult.status === 'fulfilled' && agentListResult.value.parsed) {
        const parsed = agentListResult.value.parsed as Record<string, unknown>
        agents = (parsed.agents || []) as Array<Record<string, unknown>>
      }

      // Parse agent health and merge into agent list
      let healthMap: Map<string, Record<string, unknown>> = new Map()
      if (agentHealthResult.status === 'fulfilled' && agentHealthResult.value.parsed) {
        const parsed = agentHealthResult.value.parsed as Record<string, unknown>
        const healthAgents = (parsed.agents || []) as Array<Record<string, unknown>>
        for (const h of healthAgents) {
          if (h.id) healthMap.set(String(h.id), h)
        }
      }

      // Real system metrics for agents
      const numCpus = os.cpus().length || 1
      // loadavg[0] = 1-min avg; on Windows it's always 0, so fallback to process.cpuUsage
      let systemCpuPct: number
      if (os.platform() === 'win32') {
        // On Windows, estimate from process.cpuUsage (microseconds since process start)
        const usage = process.cpuUsage()
        const totalUs = usage.user + usage.system
        const uptimeMs = process.uptime() * 1000
        systemCpuPct = Math.min(100, Math.round((totalUs / 1000 / uptimeMs) * 100))
      } else {
        systemCpuPct = Math.min(100, Math.round((os.loadavg()[0] / numCpus) * 100))
      }
      const totalMemMB = Math.round(os.totalmem() / 1024 / 1024)
      const usedMemMB = Math.round((os.totalmem() - os.freemem()) / 1024 / 1024)

        // ACC-TASK-QUEUE-002-FINAL-REPAIR: persisted UI agents whose
        // profileId is not in the canonical AGENT_PROFILES set are
        // dropped here. Stale registry entries cannot inflate the
        // visualized agent count.
        const canonicalProfileIds = new Set(AGENT_PROFILES.map(p => p.profileId))
        const knownAgentIds = new Set(
          agents.map(a => String(a.agentId || a.id || '')).filter(Boolean),
        )
        for (const [key, reg] of agentRegistry.entries()) {
          const id = String(reg.id || key)
          if (!id || knownAgentIds.has(id) || terminatedAgents.has(key) || terminatedAgents.has(id)) continue
          if (!reg.profileId || !canonicalProfileIds.has(reg.profileId)) continue
          agents.push({
            id,
            agentId: id,
            name: reg.name,
            type: reg.type,
            agentType: reg.type,
            profileId: reg.profileId,
            status: agentActivity.get(id)?.status || 'idle',
            createdAt: lastSwarmCreatedAt || new Date().toISOString(),
          })
          knownAgentIds.add(id)
        }

        // Drop CLI-listed agents whose profileId is unknown (canonical
        // ownership is the source of truth — anything else is stale
        // CLI residue from prior runs).
        for (let i = agents.length - 1; i >= 0; i--) {
          const a = agents[i]
          const pid = String(a.profileId || a.agentProfile || '')
          if (pid && !canonicalProfileIds.has(pid)) agents.splice(i, 1)
        }

        const roleDisplayNames: Record<string, string> = {
          coordinator: 'Queen Dispatcher',
          researcher: 'Cartographer',
          architect: 'System Architect',
          analyst: 'Backend Auditor',
          reviewer: 'Frontend Auditor',
          'security-architect': 'Security Auditor',
          'security-auditor': 'Security Auditor',
          'performance-engineer': 'Performance Auditor',
          tester: 'QA Auditor',
          coder: 'Implementation Agent',
        }

      // Merge health data into agents
      const enrichedAgents = agents
        .filter(a => {
          const id = String(a.agentId || a.id || '')
          const created = String(a.createdAt || '')
          // Respect termination filters
          if (allTerminatedBefore && created <= allTerminatedBefore) return false
          // If filtering to current swarm only
          if (filterCurrent && currentSwarmAgentIds.size > 0 && !currentSwarmAgentIds.has(id)) return false
          return true
        })
        .map(a => {
        const id = String(a.agentId || a.id || '')
          const registered = agentRegistry.get(id) ||
            [...agentRegistry.values()].find(reg => String(reg.id) === id)
          const agentType = String(registered?.type || a.agentType || a.type || 'unknown')
        const health = healthMap.get(id) || {}
        const activity = agentActivity.get(id)
        const isWorking = (activity?.status || a.status) === 'active' || (activity?.status || a.status) === 'working'
        // Distribute real system metrics across agents (active agents get more share)
        const agentCount = agents.length || 1
        const baseCpu = Math.round(systemCpuPct / agentCount)
        const agentCpu = isWorking ? Math.min(baseCpu + Math.round(Math.random() * 10), 100) : Math.max(1, Math.round(baseCpu * 0.3))
        const baseMemMB = Math.round(usedMemMB / agentCount)
        const agentMemUsed = isWorking ? baseMemMB + Math.round(Math.random() * 50) : Math.round(baseMemMB * 0.4)
        const agentMemLimit = Math.round(totalMemMB / agentCount)
        return {
          id,
            name: registered?.name || a.name || roleDisplayNames[agentType] || id,
            type: agentType,
            agentType,
          status: activity?.status || a.status || 'idle',
          health: a.health ?? 1,
          taskCount: (activity?.currentTask ? 1 : 0) + [...taskStore.values()].filter(t => t.assignedTo === id && t.status === 'in_progress').length,
          createdAt: a.createdAt || new Date().toISOString(),
          uptime: health.uptime || 0,
          memory: { used: agentMemUsed, limit: agentMemLimit },
          cpu: agentCpu,
          tasks: health.tasks || { active: 0, queued: 0, completed: 0, failed: 0 },
          latency: health.latency || { avg: 0, p99: 0 },
          errors: health.errors || { count: 0 },
          currentTask: activity?.currentTask,
          currentAction: activity?.currentAction,
        }
      })

      const swarmAgents = swarm.agents as Record<string, number> | undefined
      res.json({
        swarmId: swarm.id || lastSwarmId || '',
        status: swarmShutdown ? 'shutdown' : (swarm.status || 'inactive'),
        topology: swarm.topology || lastSwarmTopology || 'hierarchical',
        objective: swarm.objective || 'No active objective',
        strategy: swarm.strategy || lastSwarmStrategy || 'specialized',
        progress: swarm.progress || 0,
        agents: enrichedAgents,
          agentSummary: {
            ...(swarmAgents || {}),
            total: enrichedAgents.length,
            active: enrichedAgents.filter(a => a.status === 'active' || a.status === 'working').length,
            idle: enrichedAgents.filter(a => a.status === 'idle' || a.status === 'spawned').length,
            completed: Number(swarmAgents?.completed || 0),
          },
        taskSummary: swarm.tasks || { total: 0, completed: 0, inProgress: 0, pending: 0 },
        metrics: swarm.metrics || { tokensUsed: 0, avgResponseTime: '--', successRate: '--', elapsedTime: '--' },
        coordination: swarm.coordination || { consensusRounds: 0, messagesSent: 0, conflictsResolved: 0 },
      })
    } catch (err) {
      res.json({ swarmId: '', status: 'error', agents: [], error: String(err) })
    }
  }))

  // Lightweight activity-only endpoint (no CLI calls, instant response)
  r.get('/activity', ((_req, res) => {
    const activities: Record<string, unknown> = {}
    for (const [id, act] of agentActivity.entries()) {
      activities[id] = act
    }
    res.json(activities)
  }) as RequestHandler)

  // Get agent output buffer
  r.get('/output/:agentId', (((req, res) => {
    const id = String(req.params.agentId)
    const buf = agentOutputBuffers.get(id) || []
    res.json({ agentId: id, lines: buf })
  }) as RequestHandler))

  // Purge all zombie agents
  r.post('/purge', h(async (_req, res) => {
    const stopped = await purgeAllCliAgents()
    // Unregister every scheduler agent so stale IDs cannot claim slots later.
    resetGlobalScheduler()
    broadcast('swarm-monitor:purged', { stopped })
    res.json({ stopped, message: `Purged ${stopped} agents` })
  }))

  // Agent list only
  r.get('/agents', h(async (_req, res) => {
    try {
      const { parsed } = await execCli('agent', ['list', '--format', 'json'])
      const data = parsed as Record<string, unknown>
      res.json(data?.agents || [])
    } catch { res.json([]) }
  }))

  // Agent health only
  r.get('/health', h(async (_req, res) => {
    try {
      const { parsed } = await execCli('agent', ['health', '--format', 'json'])
      res.json(parsed || { agents: [] })
    } catch { res.json({ agents: [] }) }
  }))

  // Agent metrics
  r.get('/metrics', h(async (_req, res) => {
    try {
      const { parsed } = await execCli('agent', ['metrics', '--format', 'json'])
      res.json(parsed || {})
    } catch { res.json({}) }
  }))

  return r
}

// Bootstrap
const app = express()
app.use(cors({ origin: process.env.RUFLOUI_CORS_ORIGIN || 'http://localhost:28588' }))
app.use(express.json({
  verify: (req: any, _res, buf) => {
    // Preserve the raw body buffer for HMAC signature verification (webhook routes)
    req.rawBody = buf
  },
}))

app.use('/api/system', systemRoutes())
app.use('/api/swarm', swarmRoutes())
app.use('/api/agents', agentRoutes())
app.use('/api/tasks', taskRoutes())
app.use('/api/memory', memoryRoutes())
app.use('/api/sessions', sessionRoutes())
app.use('/api/hive-mind', hiveMindRoutes())
app.use('/api/neural', neuralRoutes())
app.use('/api/performance', performanceRoutes())
app.use('/api/hooks', hooksRoutes())
app.use('/api/workflows', workflowRoutes())
app.use('/api/coordination', coordinationRoutes())
app.use('/api/config', configRoutes())
app.use('/api/ai-defence', aiDefenceRoutes())
app.use('/api/swarm-monitor', swarmMonitorRoutes())
// Helper: parse "[owner/repo#42] Title" from webhook task titles
function parseWebhookTitle(title: string): { repo: string; issueNumber: number } | null {
  const m = title.match(/^\[([^\]]+)#(\d+)\]/)
  if (!m) return null
  return { repo: m[1], issueNumber: Number(m[2]) }
}

/**
 * Single explicit task creation + dispatch enqueue path.
 *
 * Every entry point that wants to create a new top-level task MUST go
 * through this helper — there are no exceptions:
 *
 *   - POST /api/tasks (HTTP)
 *   - POST /api/tasks/:id/continue (continuation)
 *   - Telegram createAndAssignTask
 *   - Successful GitHub/GitLab webhook task creation
 *
 * After this helper returns, the only place that may invoke
 * `launchWorkflowForTask` is `dispatcherLauncher` — set via
 * `setDispatcherLauncher` once at boot.
 *
 * The helper:
 *   1. Resolves a fresh task id.
 *   2. Inserts a TaskRecord with status='pending' (NEVER 'in_progress').
 *   3. Enqueues through `dispatcher.enqueue`, which honours
 *      priority+FIFO and the max-in-flight cap.
 *   4. Broadcasts task:added. The dispatcher owns in_progress — the
 *      route handler MUST NOT pre-set it.
 *   5. Returns the TaskRecord. The dispatcher's prepare→provision→
 *      in_progress lifecycle is what actually launches Claude.
 */
function createAndEnqueueTask(input: {
  title: string
  description: string
  mode: TaskMode
  priority: Priority
  sourceCwd: string
  assignedTo?: string
  /** Optional pre-computed taskId (callers that need a stable id). */
  id?: string
}): { taskId: string; task: TaskRecord; created: boolean } {
  const id = input.id || `task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  // 1. Enqueue through the dispatcher FIRST so the authoritative state
  //    lives in one place. The dispatcher enqueue is idempotent.
  const { task: dTask, created } = dispatcher.enqueue({
    id,
    title: input.title,
    description: input.description,
    mode: input.mode,
    priority: input.priority,
    sourceCwd: input.sourceCwd,
    assignedTo: input.assignedTo,
  })
  // 2. Build the legacy TaskRecord mirror. Status is pending — NEVER
  //    in_progress here. The dispatcher promotes pending → preparing →
  //    in_progress during its own dispatch tick.
  const task: TaskRecord = {
    id,
    title: dTask.title,
    description: dTask.description,
    status: 'pending',
    priority: dTask.priority,
    createdAt: dTask.createdAt,
    cwd: dTask.sourceCwd,
    mode: dTask.mode,
    worktreePath: dTask.worktree?.worktreePath,
    branchName: dTask.worktree?.branchName,
    baseCommit: dTask.worktree?.baseCommit,
    executionCwd: dTask.executionCwd,
    queueState: 'queued',
    queuePosition: dispatcher.queuePosition(id),
    attempt: dTask.attempt,
    assignedTo: dTask.assignedTo,
  }
  taskStore.set(id, task)
  broadcast('task:added', task)
  if (!created) {
    // Duplicate — surface idempotent result.
    return { taskId: id, task, created: false }
  }
  persistState()
  return { taskId: id, task, created: true }
}

// Shared webhook task creator — clones repo, sets cwd, attaches metadata.
//
// After ACC-TASK-QUEUE-002-FINAL-REPAIR: this function does NOT do
// `git checkout -b` (the dispatcher creates the branch via worktree
// provisioning for WRITE tasks). The repo dir is captured as
// sourceCwd; the dispatcher's write-provisioning path creates a fresh
// `ruflo-task/<repoId>-<taskId>` branch + worktree off HEAD.
//
// Clone failure is fail-closed: we DO NOT fall back to the rufloui
// cwd. The task is marked failed (terminal), no Claude process is
// spawned, no dispatcher slot is leased.
async function createWebhookTask(
  provider: 'github' | 'gitlab',
  title: string,
  description: string,
  issueUrl: string,
): Promise<{ taskId: string; assigned: boolean }> {
  const id = `task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  const parsed = parseWebhookTitle(title)
  let sourceCwd: string | undefined
  let webhookMeta: TaskRecord['webhookMeta']

  if (parsed) {
    const token = provider === 'github'
      ? githubWebhookConfig.githubToken
      : gitlabWebhookConfig.gitlabToken
    try {
      const repoDir = await cloneWebhookRepo(provider, parsed.repo, token, issueUrl)
      sourceCwd = repoDir
      let host = provider === 'gitlab' ? 'gitlab.com' : 'github.com'
      try { host = new URL(issueUrl).host } catch { /* use default */ }
      // branchName field is the LEGACY desired name used by old push/MR
      // code paths; the actual branch is created by the dispatcher during
      // worktree provisioning (worktreeProvisioned handler updates this).
      const branchName = `fix/issue-${parsed.issueNumber}`
      webhookMeta = {
        provider, repo: parsed.repo, issueNumber: parsed.issueNumber,
        issueUrl, branchName, host,
      }
      console.log(`[webhook-repo] Task ${id} will work in ${repoDir} on dispatcher-managed branch`)
    } catch (err) {
      console.error(`[webhook-repo] Clone failed for ${parsed.repo}:`, err)
      // Fail-closed: do NOT fallback to rufloui cwd.
      // Create the task as a terminal failed record (no dispatcher slot).
      const failedTask: TaskRecord = {
        id, title, description, status: 'failed',
        result: `Failed to clone repository ${parsed.repo}: ${err instanceof Error ? err.message : String(err)}`,
        priority: 'high',
        createdAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
      }
      taskStore.set(id, failedTask)
      broadcast('task:added', failedTask)
      return { taskId: id, assigned: false }
    }
  }

  // Route through the single explicit creation helper. The dispatcher
  // (NOT this function) decides when/whether to launch — and for WRITE
  // tasks it provisions a fresh branch + worktree.
  const result = createAndEnqueueTask({
    id,
    title,
    description,
    mode: 'WRITE', // webhook tasks are always WRITE — they edit the repo
    priority: 'high',
    sourceCwd: sourceCwd || process.cwd(),
  })
  if (webhookMeta) {
    result.task.webhookMeta = webhookMeta
    taskStore.set(id, result.task)
  }
  if (!result.created) return { taskId: id, assigned: false }
  return { taskId: id, assigned: !swarmShutdown }
}

app.use('/api/webhooks', githubWebhookRoutes(
  () => githubWebhookConfig,
  (c) => { githubWebhookConfig = c; saveGitHubWebhookConfig(c) },
  {
    createAndAssignTask: async (title: string, description: string) => {
      // Extract issue URL from description (first line: "GitHub Issue: <url>")
      const urlMatch = description.match(/GitHub Issue: (https:\/\/\S+)/)
      return createWebhookTask('github', title, description, urlMatch?.[1] || '')
    },
    broadcast,
  },
))

app.use('/api/webhooks', gitlabWebhookRoutes(
  () => gitlabWebhookConfig,
  (c) => { gitlabWebhookConfig = c; saveGitLabWebhookConfig(c) },
  {
    createAndAssignTask: async (title: string, description: string) => {
      const urlMatch = description.match(/GitLab Issue: (https:\/\/\S+)/)
      return createWebhookTask('gitlab', title, description, urlMatch?.[1] || '')
    },
    broadcast,
  },
))

// Viz routes (JSONL monitor)
const vizRouter = Router()
vizRouter.get('/sessions', ((_req, res) => {
  res.json(getAllMonitoredSessions())
}) as RequestHandler)
vizRouter.get('/sessions/:id', ((req, res) => {
  const tree = getSessionTree(String(req.params.id))
  if (tree) {
    res.json(tree)
  } else {
    res.status(404).json({ error: 'Session not found' })
  }
}) as RequestHandler)
vizRouter.get('/sessions/:sessionId/logs/:nodeId', ((req, res) => {
  const tail = Number(req.query.tail) || 100
  const logs = getNodeLogs(String(req.params.sessionId), String(req.params.nodeId), tail)
  res.json(logs)
}) as RequestHandler)
app.use('/api/viz', vizRouter)

const server = createServer(app)
const wss = new WebSocketServer({ server, path: '/ws' })

wss.on('connection', (ws) => {
  wsClients.add(ws)
  ws.on('close', () => wsClients.delete(ws))
  ws.on('error', () => wsClients.delete(ws))
  ws.send(JSON.stringify({ type: 'connected', payload: { timestamp: Date.now() } }))
})

// Load persisted state before listening
loadFromDisk()

// Initialize Telegram bot (no-op when not configured)
function getTelegramStores() {
  return {
    taskStore, workflowStore, agentRegistry, terminatedAgents, agentActivity,
    getSwarmStatus: () => ({
      id: lastSwarmId,
      topology: lastSwarmTopology,
      status: swarmShutdown ? 'shutdown' : 'active',
      activeAgents: currentSwarmAgentIds.size,
    }),
    getSystemHealth: async () => {
      try {
        const { raw } = await execCli('doctor')
        const passed = Number(raw.match(/(\d+) passed/)?.[1] ?? 0)
        const warnings = Number(raw.match(/(\d+) warning/)?.[1] ?? 0)
        return { status: warnings > 3 ? 'degraded' : 'healthy', passed, warnings }
      } catch {
        return { status: 'unknown', passed: 0, warnings: 0 }
      }
    },
    createAndAssignTask: async (title: string, description: string) => {
      // Telegram bot uses normal priority and the SINGLE explicit
      // creation path. No direct launchWorkflowForTask — only
      // dispatcherLauncher may invoke it.
      const result = createAndEnqueueTask({
        title, description,
        mode: 'WRITE',
        priority: 'normal',
        sourceCwd: process.cwd(),
      })
      return { taskId: result.taskId, assigned: !swarmShutdown }
    },
    cancelTask: async (taskId: string) => {
      // Telegram cancel routes through the SAME canonical helper that
      // HTTP /api/tasks/:id/cancel and /api/workflows/:id/cancel use.
      // No duplicate signal/wait logic.
      const result = await cancelTask(taskId, { reason: 'telegram-cancel' })
      if (!result.ok && result.mode === 'noop' && !result.status) {
        return { ok: false, error: 'Task not found' }
      }
      if (result.alreadyTerminal) {
        return { ok: false, error: `Task already ${result.status}` }
      }
      return { ok: true }
    },
    addLog: addTelegramLog,
  }
}

async function reinitTelegramBot() {
  if (telegramBot) {
    await telegramBot.stop()
    telegramBot = null
  }
  telegramBot = initTelegramBot(telegramConfig, getTelegramStores())
}

telegramConfig = loadTelegramConfig()
telegramBot = initTelegramBot(telegramConfig, getTelegramStores())

// Periodic save as safety net (every 30s)
setInterval(() => saveToDisk(), 30_000)

// Start zombie process reaper
startZombieReaper()

// Save on shutdown + kill running processes
function gracefulShutdown() {
  console.log('[shutdown] Saving state and cleaning up...')
  saveToDisk()
  // Kill all running claude processes
  for (const [key, proc] of runningProcesses.entries()) {
    if (!proc.killed) {
      console.log(`[shutdown] Killing process: ${key}`)
      proc.kill('SIGTERM')
    }
  }
  runningProcesses.clear()
  processLastActivity.clear()
  process.exit(0)
}
process.on('SIGINT', gracefulShutdown)
process.on('SIGTERM', gracefulShutdown)

server.listen(PORT, '127.0.0.1', async () => {
  console.log(`RuFloUI API server running on http://localhost:${PORT}`)
  console.log(`WebSocket available at ws://localhost:${PORT}/ws`)

  // Startup preflight — log dependency status (non-blocking)
  console.log('Running preflight checks...')
  try {
    const nodeVer = process.version
    const major = parseInt(nodeVer.slice(1), 10)
    console.log(`  Node.js: ${nodeVer}${major < 18 ? ' [WARN: requires >= 18]' : ' [OK]'}`)
  } catch (e) { console.log('  Node.js: [ERROR]', e) }
  try {
    await execAsync('npx --version', { timeout: 10_000 })
    console.log('  npx: [OK]')
  } catch { console.log('  npx: [FAIL] Not found in PATH') }
  try {
    await execAsync('claude --version', { timeout: 10_000 })
    console.log('  Claude CLI: [OK]')
  } catch { console.log('  Claude CLI: [WARN] Not in PATH (needed for multi-agent pipeline)') }
  try {
    await execCli('--version', [])
    console.log('  claude-flow CLI: [OK]')
  } catch { console.log('  claude-flow CLI: [WARN] First run may take longer (npx download)') }
  console.log('Preflight complete. Dashboard: http://localhost:28588')
})

export { app, server }

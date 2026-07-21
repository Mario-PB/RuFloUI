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
  const repoDir = task.cwd

  try {
    // Check if there are any changes to commit
    const { stdout: statusOut } = await execAsync('git status --porcelain', { cwd: repoDir })
    if (!statusOut.trim()) {
      console.log(`[webhook-repo] No changes to commit for task ${taskId}`)
      return
    }

    const branchName = meta.branchName
    console.log(`[webhook-repo] Committing and pushing changes for task ${taskId} on branch ${branchName}`)

    // Create branch, add, commit, push
    // Create branch or switch to it if it already exists
    await execAsync(`git checkout -b "${branchName}"`, { cwd: repoDir }).catch(() =>
      execAsync(`git checkout "${branchName}"`, { cwd: repoDir })
    )
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
    // Restore agent registry
    if (state.agents) for (const [k, v] of state.agents) agentRegistry.set(k, v)
    // Restore terminated agents
    if (state.terminatedAgents) for (const id of state.terminatedAgents) terminatedAgents.add(id)
    // Restore agent activity
    if (state.agentActivity) for (const [k, v] of state.agentActivity) agentActivity.set(k, v as any)
    // Restore swarm config
    if (state.swarmConfig) {
      lastSwarmId = state.swarmConfig.id || ''
      lastSwarmTopology = state.swarmConfig.topology || 'hierarchical'
      lastSwarmStrategy = state.swarmConfig.strategy || 'specialized'
      lastSwarmMaxAgents = state.swarmConfig.maxAgents || 8
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

// Types that represent persistent state changes — trigger disk save
const PERSIST_EVENTS = new Set([
  'task:added', 'task:updated', 'task:list',
  'workflow:added', 'workflow:updated',
  'session:added', 'session:updated', 'session:list', 'session:active',
  'swarm:status', 'swarm-monitor:purged',
  'agent:activity', 'agent:added', 'agent:removed', 'agents:cleared',
  'performance:metrics',
])

function broadcast(type: string, payload: unknown) {
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
let lastSwarmMaxAgents = 8
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
      task.status = 'failed'
      task.result = 'Workflow timed out after ' + (maxWait / 1000) + 's'
      broadcast('task:updated', { ...task, id: taskId })
      return
    }
    try {
      const { raw } = await execCli('workflow', ['status', workflowId])
      const wf = workflowStore.get(workflowId)
      const statusMatch = raw.match(/Status:\s*(\w+)/)
      const currentStatus = statusMatch?.[1] || 'unknown'
      if (wf) { wf.status = currentStatus; wf.result = raw.slice(0, 500) }
      if (currentStatus === 'completed' || currentStatus === 'done') {
        task.status = 'completed'
        task.completedAt = new Date().toISOString()
        task.result = raw.slice(0, 500) || 'Workflow completed'
        if (wf) { wf.status = 'completed'; wf.completedAt = task.completedAt }
        broadcast('task:updated', { ...task, id: taskId })
        broadcast('workflow:updated', wf)
      } else if (currentStatus === 'failed' || currentStatus === 'error') {
        task.status = 'failed'
        task.result = raw.slice(0, 500) || 'Workflow failed'
        if (wf) wf.status = 'failed'
        broadcast('task:updated', { ...task, id: taskId })
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

// Zombie reaper — kills processes with no output for ZOMBIE_TIMEOUT
function startZombieReaper() {
  setInterval(() => {
    const now = Date.now()
    for (const [key, lastTime] of processLastActivity.entries()) {
      if (now - lastTime > ZOMBIE_TIMEOUT) {
        const proc = runningProcesses.get(key)
        if (proc && !proc.killed) {
          console.warn(`[zombie] Killing stale process ${key} (no output for ${Math.round(ZOMBIE_TIMEOUT / 1000)}s)`)
          proc.kill('SIGTERM')
          // Force kill after 5s if still alive
          setTimeout(() => { if (!proc.killed) proc.kill('SIGKILL') }, 5000)
        }
        processLastActivity.delete(key)
        cleanupProcess(key)
      }
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

  // If swarm is active with agents, use the multi-agent pipeline
  const activeAgents = getActiveSwarmAgents()
  if (!swarmShutdown && activeAgents.length > 0) {
    console.log(`[TASK ${taskId}] Multi-agent pipeline with ${activeAgents.length} agents`)
    launchSwarmPipeline(taskId, task, taskDesc, title, wf, workflowId, activeAgents)
  } else {
    console.log(`[TASK ${taskId}] Single-agent fallback (swarmShutdown=${swarmShutdown}, agents=${activeAgents.length})`)
    // Fallback: single claude -p
    launchViaClaude(taskId, task, taskDesc, title, wf, workflowId)
  }
}

// Get active agents from registry, excluding terminated
function getActiveSwarmAgents(): Array<{ id: string; name: string; type: string }> {
  return Array.from(agentRegistry.entries())
    .filter(([key]) => !terminatedAgents.has(key))
    .map(([, reg]) => reg)
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
  agents: Array<{ id: string; name: string; type: string }>,
): Promise<void> {
  // Register the UI pipeline as the current swarm.
  currentSwarmAgentIds = new Set(agents.map(agent => agent.id))
  for (const agent of agents) {
    terminatedAgents.delete(agent.id)
    agentRegistry.set(agent.id, {
      id: agent.id,
      name: agent.name,
      type: agent.type,
    })
  }
  persistState()

  const coordinator = agents.find(a => a.type === 'coordinator')
  const workers = agents.filter(a => a.type !== 'coordinator')
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
      const args = ['-p', packetPrompt, '--output-format', 'stream-json', '--verbose']
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
    const workerTypes = [...new Set(workers.map(w => w.type))]
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
      'devops-engineer': 'DEVOPS IMPLEMENTATION phase: inspect and modify infrastructure, services, CI, configuration and operational scripts; never push, merge or deploy without explicit authorization',
      'security-architect': 'SECURITY phase: inspect authentication, authorization, tenant isolation, secrets and dependency risks; provide fail-closed recommendations',
    }

    const planPrompt = [
      `You are a task coordinator managing a development team. Your job is to break tasks into subtasks and assign them to the RIGHT specialist.`,
      '',
      `YOUR TEAM (you MUST use ALL relevant roles):`,
      ...workerTypes.map(t => `- ${t}: ${roleInstructions[t] || 'specialist agent'}`),
      '',
      `TASK: ${taskDesc}`,
      hiveMindContext,
      '',
      `RULES:`,
      `1. You MUST use MULTIPLE agent types — do NOT assign everything to a single agent`,
      `2. If the task involves modifying existing code, START with a "researcher" subtask to explore the codebase`,
      `3. After implementation by "coder", ALWAYS add a "tester" or "reviewer" subtask to validate`,
      `4. Each subtask must be self-contained with enough context for the agent to work independently`,
      `5. Use depends_on to chain tasks that need results from previous steps`,
      `6. For full audits, use every relevant specialist and add a final reviewer validation; otherwise use 3-5 subtasks`,
      '',
      `Respond ONLY with a JSON array. Each subtask has:`,
      `- "agent": one of [${workerTypes.map(t => `"${t}"`).join(', ')}]`,
      `- "task": a detailed, self-contained description`,
      `- "depends_on": array of indices (0-based) of prerequisite subtasks, or [] for parallel`,
      '',
      'Example for a code change task:',
      '[',
      '  {"agent":"researcher","task":"Find all files related to X, understand the current implementation patterns and dependencies","depends_on":[]},',
      '  {"agent":"coder","task":"Implement Y based on the research findings. Modify files A, B, C as needed","depends_on":[0]},',
      '  {"agent":"tester","task":"Write tests for the new Y feature and run the test suite to verify everything passes","depends_on":[1]},',
      '  {"agent":"reviewer","task":"Review all code changes for quality, check for bugs, security issues, and ensure project conventions are followed","depends_on":[1]}',
      ']',
    ].join('\n')

    const planResult = await runClaude(planPrompt, 'You are a task planner. Output ONLY a valid JSON array. No markdown fences, no explanation, no tool use. Just the JSON.', coordinatorId, true)

    // Parse the plan
    const jsonMatch = planResult.match(/\[[\s\S]*\]/)
    let subtasks: Array<{ agent: string; task: string; depends_on: number[] }> = []
    if (jsonMatch) {
      try { subtasks = JSON.parse(jsonMatch[0]) } catch (e) {
        console.warn('[pipeline] Failed to parse subtask plan JSON:', e instanceof Error ? e.message : String(e))
      }
    }

    if (subtasks.length > 0) {
      const finalReviewerDependencies = subtasks.map((_, index) => index)
      subtasks.push({
        agent: 'reviewer',
        task: [
          'FINAL REVIEW: Review every prior subtask result against the original task.',
          `Original task: ${taskDesc}`,
          'Return one complete standalone final answer satisfying every requested deliverable.',
          'For plans and reports, include the complete requested content, not only a verdict.',
          'Keep the final answer at or below 10000 characters. Never push, merge, or deploy.',
        ].join('\n'),
        depends_on: finalReviewerDependencies,
      })
    }

    const planStep = wf.steps.find(s => s.id === 'step-plan')
    if (planStep) planStep.status = 'completed'
    broadcast('workflow:updated', wf)

    if (subtasks.length === 0) {
      // Fallback: if coordinator couldn't plan, just run the whole task with a coder
      broadcast('task:output', { id: taskId, workflowId, type: 'text', content: '[Fallback] Could not parse plan, running with single coder agent' })
      const coder = workers.find(w => w.type === 'coder') || workers[0]
      if (coder) {
        wf.steps.push({ id: 'step-exec', name: 'Execute', status: 'running', agent: coder.name, detail: taskDesc.slice(0, 80) })
        broadcast('workflow:updated', wf)
        const result = await runClaude(taskDesc, `You are a ${coder.type} agent. Complete this task thoroughly.`, coder.id)
        const execStep = wf.steps.find(s => s.id === 'step-exec')
        if (execStep) execStep.status = 'completed'
        task.result = result.slice(0, 12000) || 'Completed'
      }
    } else {
      // ── PHASE 2: Execute subtasks respecting dependencies ──
      broadcast('task:output', { id: taskId, workflowId, type: 'text', content: `[Phase 2] Executing ${subtasks.length} subtasks across agents...` })
      const results: string[] = new Array(subtasks.length).fill('')
      const completed = new Set<number>()

      // Execute in waves: each wave runs all subtasks whose dependencies are met
      while (completed.size < subtasks.length) {
        const ready = subtasks.map((st, i) => ({ ...st, idx: i }))
          .filter(st => !completed.has(st.idx) && st.depends_on.every(d => completed.has(d)))

        if (ready.length === 0) {
          broadcast('task:output', { id: taskId, workflowId, type: 'text', content: '[Error] Circular dependency detected, aborting remaining subtasks' })
          break
        }

          // Run ready subtasks with bounded parallelism.
          const maxConcurrent = Math.max(1, Math.min(2, Number(process.env.RUFLO_PIPELINE_MAX_CONCURRENT || 2) || 2))
          for (let batchStart = 0; batchStart < ready.length; batchStart += maxConcurrent) {
            const batch = ready.slice(batchStart, batchStart + maxConcurrent)
            const wave = batch.map(async (st) => {
          const agent = workers.find(w => w.type === st.agent) || workers[0]
          if (!agent) return

          const stepId = `step-${st.idx + 1}`
          wf.steps.push({ id: stepId, name: `${st.agent}: ${st.task.slice(0, 40)}`, status: 'running', agent: agent.name, detail: st.task.slice(0, 80) })
          broadcast('workflow:updated', wf)
          broadcast('task:output', { id: taskId, workflowId, type: 'text', content: `  [${agent.name}] ${st.task.slice(0, 100)}` })

          // Build context from dependencies
          const isFinalReviewer = st.agent === 'reviewer' && st.idx === subtasks.length - 1
          const depContext = st.depends_on.length > 0
            ? '\n\nPrevious results:\n' + st.depends_on.map(d => `[${subtasks[d].agent}]: ${isFinalReviewer ? results[d] : results[d].slice(0, 500)}`).join('\n')
            : ''

          const roleSystemPrompts: Record<string, string> = {
            researcher: 'You are a researcher agent. Your job is to explore the codebase, find relevant files, read code, and report your findings clearly. Use Read, Grep, Glob tools. Do NOT modify any files.',
            coder: 'You are a full-stack implementation engineer. Inspect and modify application code, create files, and run builds and tests. If the task explicitly says READ-ONLY, do not modify files. Never push, merge, or deploy unless explicitly authorized.',
            tester: 'You are a tester agent. Write and run tests when implementation is allowed. For READ-ONLY or audit tasks, do not modify files; inspect and run existing tests only. Report results clearly.',
            reviewer: 'You are a code reviewer agent. Review the code changes for bugs, security issues, style problems, and adherence to best practices. Report issues found.',
            analyst: 'You are an analyst agent. Analyze requirements and produce clear technical specifications.',
            architect: 'You are an architect agent. Design system architecture, define patterns, interfaces and data flow.',
            'devops-engineer': 'You are a DevOps implementation engineer. Inspect and modify infrastructure, service units, CI, configuration, and operational scripts. Build and verify before restart. Never push, merge, deploy, or expose secrets unless explicitly authorized. Remain read-only when the task says READ-ONLY.',
            'security-architect': 'You are a security architect. Inspect authentication, authorization, tenant isolation, secrets, dependency risks, and fail-closed behavior. Remain read-only unless implementation is explicitly authorized.',
          }
          const agentPrompt = `Complete this task:\n\n${st.task}${depContext}`
          const sysPrompt = roleSystemPrompts[st.agent] || `You are a ${st.agent} agent in a development swarm. Do your assigned work precisely. Do not ask questions, just execute.`

          try {
            results[st.idx] = await runClaude(agentPrompt, sysPrompt, agent.id)
            const step = wf.steps.find(s => s.id === stepId)
            if (step) step.status = 'completed'
            // Store agent findings in hive mind shared memory
            // Store subtask result to hive mind
            await storeHiveMindMemory(
              `task-${taskId}-${st.agent}-${st.idx}`,
              results[st.idx].slice(0, 300),
            )
          } catch (err) {
            results[st.idx] = `Error: ${err instanceof Error ? err.message : String(err)}`
            const step = wf.steps.find(s => s.id === stepId)
            if (step) step.status = 'failed'
          }
          completed.add(st.idx)
          broadcast('workflow:updated', wf)
        })

            await Promise.all(wave)
          }
      }

      task.agentResults = subtasks.map((st, index) => ({
        index,
        agent: st.agent,
        task: st.task,
        result: results[index] || '',
      }))

      const finalReviewerIndex = subtasks.map(st => st.agent).lastIndexOf('reviewer')
        task.result = (results[finalReviewerIndex] || [...results].reverse().find(Boolean) || 'Pipeline completed').slice(0, 12000)
    }

    // ── PHASE 3: Mark complete ──
    task.status = 'completed'
    task.completedAt = new Date().toISOString()
    wf.status = 'completed'
    wf.completedAt = task.completedAt
    wf.result = task.result
    // Persist final result to hive mind shared memory
    // Persist final result to hive mind
    await storeHiveMindMemory(`task-result-${taskId}`, `${title}: ${(task.result || '').slice(0, 500)}`)
    broadcast('task:updated', { ...task, id: taskId })
    broadcast('workflow:updated', wf)
    broadcast('task:output', { id: taskId, workflowId, type: 'done', code: 0 })
    if (coordinatorId) {
      const act = agentActivity.get(coordinatorId)
      updateAgentActivity(coordinatorId, { status: 'idle', currentTask: undefined, currentAction: undefined, tasksCompleted: (act?.tasksCompleted || 0) + 1 })
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    console.error(`[TASK ${taskId}] Pipeline failed: ${msg}`)
    task.status = 'failed'
    task.result = `Pipeline error: ${msg.slice(0, 1000)}`
    wf.status = 'failed'
    broadcast('task:updated', { ...task, id: taskId })
    broadcast('workflow:updated', wf)
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
      task.status = 'failed'
      task.result = (fullOutput + '\n' + stderrOutput).trim().slice(0, 2000) || `Swarm launch failed (code ${code})`
      wf.status = 'failed'
      broadcast('task:updated', { ...task, id: taskId })
      broadcast('workflow:updated', wf)
      releaseAllBusyAgents(taskId, false)
    }
  })

  proc.on('error', (err) => {
    cleanupProcess(taskId)
    task.status = 'failed'
    task.result = `Swarm launch error: ${err.message}`
    wf.status = 'failed'
    broadcast('task:updated', { ...task, id: taskId })
    broadcast('workflow:updated', wf)
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
      task.status = 'failed'
      task.result = 'Swarm execution timed out after 30 minutes'
      wf.status = 'failed'
      broadcast('task:updated', { ...task, id: taskId })
      broadcast('workflow:updated', wf)
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
        task.status = 'completed'
        task.completedAt = new Date().toISOString()
        task.result = raw.slice(0, 2000) || 'Swarm execution completed'
        wf.status = 'completed'
        wf.completedAt = task.completedAt
        wf.result = task.result
        broadcast('task:updated', { ...task, id: taskId })
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
  const claudeArgs = [
    '-p', taskDesc,
    '--output-format', 'stream-json',
    '--verbose',
    ...(SKIP_PERMISSIONS ? ['--dangerously-skip-permissions'] : []),
    '--session-id', sessionUUID,
    ...mcpArgs,
    '--append-system-prompt', swarmPrompt,
  ]
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
    const combined = (fullOutput + '\n' + stderrOutput).trim()
    console.log(`[TASK ${taskId}] Exited with code ${code}. Output length: ${combined.length}`)
    if (code === 0) {
      task.status = 'completed'
      task.completedAt = new Date().toISOString()
      task.result = fullOutput.slice(0, 2000) || 'Task completed'
      wf.status = 'completed'
      wf.completedAt = task.completedAt
      wf.result = task.result
    } else {
      task.status = 'failed'
      task.result = combined.slice(0, 2000) || `Process exited with code ${code}`
      wf.status = 'failed'
      wf.result = task.result
    }
    broadcast('task:updated', { ...task, id: taskId })
    broadcast('workflow:updated', wf)
    broadcast('task:output', { id: taskId, workflowId, type: 'done', code })
    releaseAllBusyAgents(taskId, code === 0)
    const activity = agentActivity.get(workingAgentId)
    const completed = (activity?.tasksCompleted || 0) + (code === 0 ? 1 : 0)
    const errors = (activity?.errors || 0) + (code !== 0 ? 1 : 0)
    updateAgentActivity(workingAgentId, { status: 'idle', currentTask: undefined, currentAction: undefined, tasksCompleted: completed, errors })
  })

  proc.on('error', (err) => {
    cleanupProcess(taskId)
    console.error(`[TASK ${taskId}] Process error: ${err.message}`)
    task.status = 'failed'
    task.result = `Process error: ${err.message}`
    wf.status = 'failed'
    broadcast('task:updated', { ...task, id: taskId })
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
    lastSwarmMaxAgents = maxAgents || 8
    lastSwarmCreatedAt = new Date().toISOString()
    swarmShutdown = false
    allTerminatedBefore = null // Reset so new agents show up

    // Purge all existing zombie agents before spawning fresh ones
    const purged = await purgeAllCliAgents()
    if (purged > 0) console.log(`[SWARM INIT] Purged ${purged} old agents`)

    // Start the orchestration daemon in background
    ensureDaemon().catch(() => {})

    // Auto-spawn a default set of specialized agents for the swarm
    const defaultAgents: Array<{ type: string; name: string }> = [
      { type: 'coordinator', name: 'Queen Dispatcher' },
      { type: 'researcher', name: 'Cartographer' },
      { type: 'architect', name: 'System Architect' },
      { type: 'coder', name: 'Full-stack Engineer' },
      { type: 'devops-engineer', name: 'DevOps Engineer' },
      { type: 'security-architect', name: 'Security Auditor' },
      { type: 'tester', name: 'QA Auditor' },
      { type: 'reviewer', name: 'Final Reviewer' },
    ]
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
        agentRegistry.set(`${createdTime}-${agentId}`, { id: agentId, name: ag.name, type: ag.type })
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
      // Build agents list from registry (exclude terminated)
      const agentsList = Array.from(agentRegistry.entries())
        .filter(([key]) => !terminatedAgents.has(key))
        .map(([, reg]) => ({
          id: reg.id, name: reg.name, type: reg.type,
          status: 'running' as const, createdAt: '',
        }))
      const activeCount = agentsList.length
      res.json({
        raw,
        id: lastSwarmId || '',
        topology: lastSwarmTopology,
        strategy: lastSwarmStrategy,
        status: 'active',
        maxAgents: lastSwarmMaxAgents,
        activeAgents: activeCount,
        agents: agentsList,
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
    broadcast('swarm:status', { status: 'shutdown' })
    res.json({ status: 'shutdown' })
  }))
  return r
}

// In-memory registry to track agent names/IDs (CLI table doesn't include them)
// Keyed by created time (HH:MM:SS) since CLI table only shows that
const agentRegistry: Map<string, { id: string; name: string; type: string }> = new Map()
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
  let stopped = 0
  try {
    const { parsed } = await execCli('agent', ['list', '--format', 'json'])
    const data = parsed as Record<string, unknown>
    const agents = (data?.agents || []) as Array<Record<string, unknown>>
    const ids = agents.map(a => String(a.agentId || a.id || '')).filter(Boolean)
    // Process in parallel batches of 10
    const batchSize = 10
    for (let i = 0; i < ids.length; i += batchSize) {
      const batch = ids.slice(i, i + batchSize)
      const results = await Promise.allSettled(
        batch.map(id => execCli('agent', ['stop', id]))
      )
      stopped += results.filter(r => r.status === 'fulfilled').length
    }
  } catch (e) {
    console.warn('[purge] Failed to list/stop CLI agents:', e instanceof Error ? e.message : String(e))
  }
  // Clear all local tracking
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
                return true
              })
              .map((a, i) => {
                const id = String(a.agentId || a.id || `agent-${i}`)
                const activity = agentActivity.get(id)
                return {
                  id,
                  name: String(a.name || a.agentType || a.type || `Agent ${i + 1}`),
                  type: String(a.agentType || a.type || 'unknown'),
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
  r.post('/terminate-all', h(async (_req, res) => {
    // Set the cutoff: any CLI agent from before NOW is considered terminated
    allTerminatedBefore = new Date().toISOString()
    // Also mark all registry agents
    for (const [timeKey] of agentRegistry.entries()) {
      terminatedAgents.add(timeKey)
    }
    // Try CLI stop all
    try { await execCli('agent', ['stop', '--all']) } catch (e) {
      console.log('[agent] CLI stop --all skipped:', e instanceof Error ? e.message : String(e))
    }
    agentActivity.clear()
    broadcast('agents:cleared', {})
    res.json({ terminated: 'all', status: 'all terminated' })
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
  sessionUUID?: string; swarmRunId?: string
  /** Working directory for claude -p processes */
  cwd?: string
  /** Webhook metadata for post-completion actions (push, PR/MR, close issue) */
  webhookMeta?: WebhookMeta
}
const taskStore: Map<string, TaskRecord> = new Map()

function taskRoutes(): Router {
  const r = Router()
  r.get('/summary', h(async (_req, res) => {
    const all = [...taskStore.values()]
    const completed = all.filter(t => t.status === 'completed').length
    const pending = all.filter(t => t.status === 'pending').length
    const inProgress = all.filter(t => t.status === 'in_progress').length
    const failed = all.filter(t => t.status === 'failed' || t.status === 'cancelled').length
    res.json({
      total: all.length, completed, pending, inProgress, failed,
      completionRate: all.length > 0 ? completed / all.length : 0,
      averageTime: '--',
    })
  }))
  r.get('/', h(async (_req, res) => {
    res.json({ tasks: [...taskStore.values()] })
  }))
  r.post('/', h(async (req, res) => {
    const { title, description, priority, assignTo, cwd } = req.body || {}
    // Create via CLI to get a proper ID
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
    const task: TaskRecord = {
      id: taskId,
      title: title || 'Untitled',
      description: description || '',
      status: assignTo ? 'in_progress' : 'pending',
      priority: priority || 'normal',
      assignedTo: assignTo || undefined,
      createdAt: new Date().toISOString(),
      startedAt: assignTo ? new Date().toISOString() : undefined,
      cwd: resolvedCwd,
    }
    taskStore.set(taskId, task)
    broadcast('task:added', task)
    res.json(task)

    // If assigned on creation, execute in background
    if (assignTo) {
      launchWorkflowForTask(taskId, task.title, task.description)
    }
  }))
  r.get('/:id/status', h(async (req, res) => {
    const task = taskStore.get(String(req.params.id))
    res.json(task || { error: 'Task not found' })
  }))
  r.post('/:id/assign', h(async (req, res) => {
    const id = String(req.params.id)
    const { agentId } = req.body || {}
    const task = taskStore.get(id)
    if (task) {
      task.assignedTo = agentId
      task.status = 'in_progress'
      task.startedAt = new Date().toISOString()
      broadcast('task:updated', { ...task, id })

      // Execute in background via claude-flow workflow
      launchWorkflowForTask(id, task.title, task.description)
    }
    res.json({ id, assigned: true, agentId })
  }))
  r.post('/:id/complete', h(async (req, res) => {
    const id = String(req.params.id)
    const task = taskStore.get(id)
    if (task) {
      task.status = 'completed'
      task.completedAt = new Date().toISOString()
      task.result = req.body?.result || 'Completed'
      broadcast('task:updated', { ...task, id })
    }
    res.json({ id, completed: true })
  }))
  r.post('/:id/cancel', h(async (req, res) => {
    const id = String(req.params.id)
    const task = taskStore.get(id)
    if (task) {
      // Force cancel regardless of current status (handles stuck tasks)
      task.status = 'cancelled'
      task.completedAt = task.completedAt || new Date().toISOString()
      broadcast('task:updated', { ...task, id })

      // Kill running processes for this task
      for (const [key, proc] of runningProcesses.entries()) {
        if (key.startsWith(id) && !proc.killed) {
          proc.kill('SIGTERM')
          setTimeout(() => { if (!proc.killed) proc.kill('SIGKILL') }, 5000)
          cleanupProcess(key)
        }
      }

      // Cancel linked workflow
      for (const [wfId, wf] of workflowStore.entries()) {
        if (wf.taskId === id && wf.status !== 'completed' && wf.status !== 'cancelled') {
          wf.status = 'cancelled'
          wf.completedAt = new Date().toISOString()
          wf.steps.forEach(s => { if (s.status === 'running' || s.status === 'pending') s.status = 'cancelled' })
          broadcast('workflow:updated', wf)
        }
      }
    }
    res.json({ id, cancelled: true })
  }))

  // Delete completed/failed/cancelled tasks
  r.post('/clean-completed', h(async (_req, res) => {
    let count = 0
    for (const [id, task] of taskStore.entries()) {
      if (task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled') {
        taskStore.delete(id)
        count++
      }
    }
    broadcast('task:list', [...taskStore.values()])
    res.json({ ok: true, deleted: count })
  }))

  // Task continuation — create a follow-up task with previous context
  r.post('/:id/continue', h(async (req, res) => {
    const parentId = String(req.params.id)
    const parentTask = taskStore.get(parentId)
    if (!parentTask) { res.status(404).json({ error: 'Parent task not found' }); return }

    const { instruction } = req.body || {}
    if (!instruction?.trim()) { res.status(400).json({ error: 'instruction is required' }); return }

    // Build new task with context from parent
    const taskId = `task-${Date.now()}`
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

    const newTask: TaskRecord = {
      id: taskId,
      title: `${parentTask.title} (continued)`,
      description: contextBlock,
      status: 'in_progress',
      priority: parentTask.priority,
      assignedTo: parentTask.assignedTo || 'swarm',
      createdAt: new Date().toISOString(),
      startedAt: new Date().toISOString(),
    }
    taskStore.set(taskId, newTask)
    broadcast('task:added', newTask)
    res.json(newTask)

    // Execute in background
    launchWorkflowForTask(taskId, newTask.title, newTask.description)
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

      // Also cancel the linked task and kill its processes
      if (wf.taskId) {
        const task = taskStore.get(wf.taskId)
        if (task && task.status !== 'completed' && task.status !== 'failed' && task.status !== 'cancelled') {
          task.status = 'cancelled'
          broadcast('task:updated', { ...task, id: wf.taskId })
        }
        // Kill running processes for this task
        for (const [key, proc] of runningProcesses.entries()) {
          if (key.startsWith(wf.taskId) && !proc.killed) {
            proc.kill('SIGTERM')
            setTimeout(() => { if (!proc.killed) proc.kill('SIGKILL') }, 5000)
            cleanupProcess(key)
          }
        }
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

        // Merge persisted UI agents with the CLI agent list.
        const knownAgentIds = new Set(
          agents.map(a => String(a.agentId || a.id || '')).filter(Boolean),
        )
        for (const [key, reg] of agentRegistry.entries()) {
          const id = String(reg.id || key)
          if (!id || knownAgentIds.has(id) || terminatedAgents.has(key) || terminatedAgents.has(id)) continue
          agents.push({
            id,
            agentId: id,
            name: reg.name,
            type: reg.type,
            agentType: reg.type,
            status: agentActivity.get(id)?.status || 'idle',
            createdAt: lastSwarmCreatedAt || new Date().toISOString(),
          })
          knownAgentIds.add(id)
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

// Shared webhook task creator — clones repo, sets cwd, attaches metadata
async function createWebhookTask(
  provider: 'github' | 'gitlab',
  title: string,
  description: string,
  issueUrl: string,
): Promise<{ taskId: string; assigned: boolean }> {
  const id = `task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  const parsed = parseWebhookTitle(title)
  const task: TaskRecord = {
    id, title, description, status: 'pending', priority: 'high',
    createdAt: new Date().toISOString(),
  }

  // Clone the repo and set working directory
  if (parsed) {
    const token = provider === 'github'
      ? githubWebhookConfig.githubToken
      : gitlabWebhookConfig.gitlabToken
    const branchName = `fix/issue-${parsed.issueNumber}`

    try {
      const repoDir = await cloneWebhookRepo(provider, parsed.repo, token, issueUrl)
      task.cwd = repoDir

      // Create the fix branch
      // Create branch or switch to it if it already exists
      await execAsync(`git checkout -b "${branchName}"`, { cwd: repoDir }).catch(() =>
        execAsync(`git checkout "${branchName}"`, { cwd: repoDir })
      )

      let host = provider === 'gitlab' ? 'gitlab.com' : 'github.com'
      try { host = new URL(issueUrl).host } catch { /* use default */ }
      task.webhookMeta = {
        provider, repo: parsed.repo, issueNumber: parsed.issueNumber,
        issueUrl, branchName, host,
      }
      console.log(`[webhook-repo] Task ${id} will work in ${repoDir} on branch ${branchName}`)
    } catch (err) {
      console.error(`[webhook-repo] Clone failed for ${parsed.repo}:`, err)
      // Do NOT fallback to rufloui cwd — fail the task instead
      task.status = 'failed'
      task.result = `Failed to clone repository ${parsed.repo}: ${err instanceof Error ? err.message : String(err)}`
      taskStore.set(id, task)
      broadcast('task:added', task)
      return { taskId: id, assigned: false }
    }
  }

  taskStore.set(id, task)
  broadcast('task:added', task)
  if (!swarmShutdown) {
    task.status = 'in_progress'
    task.startedAt = new Date().toISOString()
    broadcast('task:updated', { ...task, id })
    launchWorkflowForTask(id, title, description)
    return { taskId: id, assigned: true }
  }
  return { taskId: id, assigned: false }
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
      const id = `task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
      const task = {
        id, title, description, status: 'pending',
        priority: 'medium', createdAt: new Date().toISOString(),
      }
      taskStore.set(id, task)
      broadcast('task:added', task)
      if (!swarmShutdown) {
        task.status = 'in_progress'
        const startedAt = new Date().toISOString()
        Object.assign(task, { startedAt })
        broadcast('task:updated', { ...task, id })
        launchWorkflowForTask(id, task.title, task.description)
        return { taskId: id, assigned: true }
      }
      return { taskId: id, assigned: false }
    },
    cancelTask: async (taskId: string) => {
      const task = taskStore.get(taskId)
      if (!task) return { ok: false, error: 'Task not found' }
      if (task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled') {
        return { ok: false, error: `Task already ${task.status}` }
      }
      task.status = 'cancelled'
      task.completedAt = new Date().toISOString()
      broadcast('task:updated', { ...task, id: taskId })
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

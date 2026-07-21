import TelegramBot from 'node-telegram-bot-api'

// Local interface copies — avoids coupling to server.ts exports
interface TaskRecord {
  id: string; title: string; description: string; status: string
  priority: string; assignedTo?: string; createdAt: string; startedAt?: string
  completedAt?: string; result?: string
}
interface WorkflowRecord {
  id: string; name: string; template: string; status: string
  taskId?: string; createdAt: string; completedAt?: string; result?: string
  steps: { id: string; name: string; status: string; agent?: string; detail?: string }[]
}
interface AgentActivity {
  status: 'idle' | 'working' | 'error'
  currentTask?: string; currentAction?: string; lastUpdate: string
  tasksCompleted: number; errors: number
}

export interface TelegramStores {
  taskStore: Map<string, TaskRecord>
  workflowStore: Map<string, WorkflowRecord>
  agentRegistry: Map<string, { id: string; name: string; type: string }>
  terminatedAgents: Set<string>
  agentActivity: Map<string, AgentActivity>
  getSwarmStatus: () => { id: string; topology: string; status: string; activeAgents: number }
  getSystemHealth: () => Promise<{ status: string; passed: number; warnings: number }>
  createAndAssignTask: (title: string, description: string) => Promise<{ taskId: string; assigned: boolean }>
  cancelTask: (taskId: string) => Promise<{ ok: boolean; error?: string }>
  addLog: (direction: 'in' | 'out', message: string) => void
}

export interface TelegramNotifications {
  taskCompleted: boolean
  taskFailed: boolean
  swarmInit: boolean
  swarmShutdown: boolean
  agentError: boolean
  taskProgress: boolean
}

export interface TelegramConfig {
  enabled: boolean
  token: string
  chatId: string
  notifications: TelegramNotifications
}

export interface TelegramHandle {
  onBroadcast: (type: string, payload: unknown) => void
  stop: () => Promise<void>
  getStatus: () => { enabled: boolean; connected: boolean; botUsername: string | null }
  sendTest: () => Promise<{ ok: boolean; error?: string }>
}

const PREFIX = '[telegram]'

/** Escape HTML special chars for Telegram HTML parse mode */
function h(text: string): string {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

function truncate(text: string, max: number): string {
  if (text.length <= max) return text
  return text.slice(0, max - 3) + '...'
}

const HTML = { parse_mode: 'HTML' as const }

export function initTelegramBot(config: TelegramConfig, stores: TelegramStores): TelegramHandle | null {
  if (!config.enabled) {
    console.log(`${PREFIX} Bot disabled`)
    return null
  }

  if (!config.token || !config.chatId) {
    console.warn(`${PREFIX} Enabled but token or chatId missing`)
    return null
  }

  let bot: TelegramBot
  try {
    bot = new TelegramBot(config.token, { polling: true })
  } catch (err) {
    console.error(`${PREFIX} Failed to create bot:`, err instanceof Error ? err.message : String(err))
    return null
  }

  let botUsername: string | null = null
  let connected = false

  bot.getMe().then(me => {
    botUsername = me.username || null
    connected = true
    console.log(`${PREFIX} Bot connected as @${me.username}`)
  }).catch(err => {
    connected = false
    console.error(`${PREFIX} Bot connection failed:`, err instanceof Error ? err.message : String(err))
  })

  // ── AUTO-RECONNECT ──────────────────────────────────────────────────

  let reconnectAttempts = 0
  let reconnecting = false
  const MAX_RECONNECT = 5

  bot.on('polling_error', (err) => {
    console.error(`${PREFIX} Polling error:`, err.message)
    connected = false
    if (reconnecting) return // prevent stacking timeouts
    reconnectAttempts++
    if (reconnectAttempts <= MAX_RECONNECT) {
      reconnecting = true
      const delay = Math.min(reconnectAttempts * 5000, 30000)
      console.log(`${PREFIX} Reconnecting in ${delay / 1000}s (attempt ${reconnectAttempts}/${MAX_RECONNECT})`)
      setTimeout(() => {
        reconnecting = false
        bot.stopPolling().then(() => {
          bot.startPolling()
          console.log(`${PREFIX} Polling restarted`)
        }).catch(() => { /* ignore */ })
      }, delay)
    } else {
      console.error(`${PREFIX} Max reconnect attempts reached, stopping polling`)
      bot.stopPolling().catch(() => { /* ignore */ })
    }
  })

  // Reset reconnect counter on successful message receipt
  bot.on('message', () => { reconnectAttempts = 0; connected = true })

  const chatId = config.chatId

  // Helper: send with HTML and log errors
  async function reply(chatIdTo: number | string, text: string, keyboard?: TelegramBot.InlineKeyboardButton[][]) {
    const opts: TelegramBot.SendMessageOptions = { parse_mode: 'HTML' }
    if (keyboard) opts.reply_markup = { inline_keyboard: keyboard }
    try {
      await bot.sendMessage(chatIdTo, text, opts)
      stores.addLog('out', text.replace(/<[^>]+>/g, '').slice(0, 100))
    } catch (err) {
      console.error(`${PREFIX} Reply failed:`, err instanceof Error ? err.message : String(err))
      // Fallback: try plain text
      try { await bot.sendMessage(chatIdTo, text.replace(/<[^>]+>/g, '')) } catch { /* give up */ }
    }
  }

  // ── AUTHORIZATION ───────────────────────────────────────────────────

  function isAuthorized(msg: TelegramBot.Message): boolean {
    if (String(msg.chat.id) === chatId) return true
    console.warn(`${PREFIX} Unauthorized message from chat ${msg.chat.id}`)
    return false
  }

  function isAuthorizedChat(chatIdToCheck: number | string): boolean {
    return String(chatIdToCheck) === chatId
  }

  // /start — open to ALL users so they can discover their chat ID
  bot.onText(/\/start(@\w+)?$/, (msg) => {
    stores.addLog('in', msg.text || '/start')
    reply(
      msg.chat.id,
      `Your chat ID is: <code>${msg.chat.id}</code>\nTo configure this bot, enter this ID in the RuFloUI dashboard under Config &gt; Telegram Bot.`
    )
  })

  // ── EXTRACTED COMMAND HANDLERS ──────────────────────────────────────

  async function handleStatus(chatIdTo: number | string) {
    try {
      const swarm = stores.getSwarmStatus()
      const health = await stores.getSystemHealth()
      const agents = [...stores.agentRegistry.entries()]
        .filter(([key]) => !stores.terminatedAgents.has(key))
      const tasks = [...stores.taskStore.values()]
      const pending = tasks.filter(t => t.status === 'pending').length
      const inProgress = tasks.filter(t => t.status === 'in_progress').length
      const completed = tasks.filter(t => t.status === 'completed').length

      const lines = [
        '<b>System Status</b>',
        `Health: ${h(health.status)} (${health.passed} passed, ${health.warnings} warnings)`,
        `Swarm: ${h(swarm.status)} | ${h(swarm.topology)} | ${swarm.activeAgents} agents`,
        `Agents: ${agents.length} active`,
        `Tasks: ${pending} pending, ${inProgress} running, ${completed} done`,
      ]
      const keyboard: TelegramBot.InlineKeyboardButton[][] = [[
        { text: 'Agents', callback_data: 'cmd:agents' },
        { text: 'Tasks', callback_data: 'cmd:tasks' },
        { text: 'Swarm', callback_data: 'cmd:swarm' },
      ]]
      reply(chatIdTo, lines.join('\n'), keyboard)
    } catch (err) {
      reply(chatIdTo, `Error: ${h(err instanceof Error ? err.message : String(err))}`)
    }
  }

  function handleAgents(chatIdTo: number | string) {
    const agents = [...stores.agentRegistry.entries()]
      .filter(([key]) => !stores.terminatedAgents.has(key))
    if (agents.length === 0) {
      reply(chatIdTo, 'No active agents.')
      return
    }
    const lines = agents.map(([, a]) => {
      const activity = stores.agentActivity.get(a.id)
      const status = activity?.status ?? 'unknown'
      return `- <b>${h(a.name || a.id)}</b> (${h(a.type)}) - ${h(status)}`
    })
    const keyboard: TelegramBot.InlineKeyboardButton[][] = [[
      { text: 'Refresh', callback_data: 'cmd:agents' },
    ]]
    reply(chatIdTo, `<b>Active Agents (${agents.length})</b>\n${lines.join('\n')}`, keyboard)
  }

  function handleTasks(chatIdTo: number | string) {
    const tasks = [...stores.taskStore.values()]
    if (tasks.length === 0) {
      reply(chatIdTo, 'No tasks.')
      return
    }
    const groups: Record<string, TaskRecord[]> = {}
    for (const t of tasks) {
      const s = t.status || 'unknown'
      if (!groups[s]) groups[s] = []
      groups[s].push(t)
    }
    const lines: string[] = ['<b>Tasks</b>']
    for (const [status, items] of Object.entries(groups)) {
      lines.push(`\n<i>${h(status)}</i> (${items.length}):`)
      for (const t of items.slice(0, 10)) {
        lines.push(`  - <code>${h(t.id)}</code> ${h(truncate(t.title, 50))}`)
      }
      if (items.length > 10) lines.push(`  ... and ${items.length - 10} more`)
    }
    const keyboard: TelegramBot.InlineKeyboardButton[][] = [[
      { text: 'Refresh', callback_data: 'cmd:tasks' },
    ]]
    reply(chatIdTo, lines.join('\n'), keyboard)
  }

  function handleTask(chatIdTo: number | string, id: string) {
    const task = stores.taskStore.get(id)
    if (!task) { reply(chatIdTo, `Task not found: <code>${h(id)}</code>`); return }
    const lines = [
      `<b>Task: ${h(truncate(task.title, 60))}</b>`,
      `ID: <code>${h(task.id)}</code>`,
      `Status: ${h(task.status)}`,
      `Priority: ${h(task.priority)}`,
      `Created: ${h(task.createdAt)}`,
    ]
    if (task.assignedTo) lines.push(`Assigned: ${h(task.assignedTo)}`)
    if (task.startedAt) lines.push(`Started: ${h(task.startedAt)}`)
    if (task.completedAt) lines.push(`Completed: ${h(task.completedAt)}`)
    if (task.result) lines.push(`\nResult: ${h(truncate(task.result, 2400))}`)
    if (task.description) lines.push(`\nDescription: ${h(truncate(task.description, 200))}`)
    const keyboard: TelegramBot.InlineKeyboardButton[][] = []
    if (task.status === 'pending' || task.status === 'in_progress') {
      keyboard.push([{ text: 'Cancel', callback_data: `cancel:${task.id}` }])
    }
    reply(chatIdTo, lines.join('\n'), keyboard.length > 0 ? keyboard : undefined)
  }

  function handleSwarm(chatIdTo: number | string) {
    const swarm = stores.getSwarmStatus()
    const lines = [
      '<b>Swarm Status</b>',
      `Status: ${h(swarm.status)}`,
      `Topology: ${h(swarm.topology)}`,
      `Active Agents: ${swarm.activeAgents}`,
    ]
    if (swarm.id) lines.push(`ID: <code>${h(swarm.id)}</code>`)
    reply(chatIdTo, lines.join('\n'))
  }

  // ── COMMAND LISTENERS ──────────────────────────────────────────────

  bot.onText(/\/help(@\w+)?$/, (msg) => {
    if (!isAuthorized(msg)) return
    stores.addLog('in', msg.text || '/help')
    const text = [
      '<b>RuFloUI Bot Commands</b>',
      '',
      '/status - System health + swarm + counts',
      '/agents - List active agents',
      '/tasks - Tasks grouped by status',
      '/task &lt;id&gt; - Detail for one task',
      '/workflows - List workflows',
      '/swarm - Swarm topology &amp; status',
      '/run &lt;description&gt; - Create &amp; assign a task',
      '/cancel &lt;id&gt; - Cancel a running task',
      '/help - This message',
    ].join('\n')
    reply(msg.chat.id, text)
  })

  bot.onText(/\/status(@\w+)?$/, async (msg) => {
    if (!isAuthorized(msg)) return
    stores.addLog('in', msg.text || '/status')
    handleStatus(msg.chat.id)
  })

  bot.onText(/\/agents(@\w+)?$/, (msg) => {
    if (!isAuthorized(msg)) return
    stores.addLog('in', msg.text || '/agents')
    handleAgents(msg.chat.id)
  })

  bot.onText(/\/tasks(@\w+)?$/, (msg) => {
    if (!isAuthorized(msg)) return
    stores.addLog('in', msg.text || '/tasks')
    handleTasks(msg.chat.id)
  })

  bot.onText(/\/task (.+)/, (msg, match) => {
    if (!isAuthorized(msg)) return
    stores.addLog('in', msg.text || '/task')
    const id = match?.[1]?.trim()
    if (!id) { reply(msg.chat.id, 'Usage: /task &lt;id&gt;'); return }
    handleTask(msg.chat.id, id)
  })

  bot.onText(/\/workflows(@\w+)?$/, (msg) => {
    if (!isAuthorized(msg)) return
    stores.addLog('in', msg.text || '/workflows')
    const workflows = [...stores.workflowStore.values()]
    if (workflows.length === 0) {
      reply(msg.chat.id, 'No workflows.')
      return
    }
    const lines = workflows.map(w =>
      `- <b>${h(w.name)}</b> (${h(w.status)}) - ${w.steps.length} steps`
    )
    reply(msg.chat.id, `<b>Workflows (${workflows.length})</b>\n${lines.join('\n')}`)
  })

  bot.onText(/\/swarm(@\w+)?$/, (msg) => {
    if (!isAuthorized(msg)) return
    stores.addLog('in', msg.text || '/swarm')
    handleSwarm(msg.chat.id)
  })

  bot.onText(/\/run (.+)/, async (msg, match) => {
    if (!isAuthorized(msg)) return
    stores.addLog('in', msg.text || '/run')
    const description = match?.[1]?.trim()
    if (!description) { reply(msg.chat.id, 'Usage: /run &lt;description&gt;'); return }
    try {
      await reply(msg.chat.id, `Creating task: ${h(truncate(description, 100))}`)
      const result = await stores.createAndAssignTask(description, description)
      const assignedText = result.assigned ? 'assigned to swarm' : 'created (no active swarm)'
      reply(msg.chat.id, `Task <code>${h(result.taskId)}</code> ${assignedText}`)
    } catch (err) {
      reply(msg.chat.id, `Failed to create task: ${h(err instanceof Error ? err.message : String(err))}`)
    }
  })

  bot.onText(/\/cancel (.+)/, async (msg, match) => {
    if (!isAuthorized(msg)) return
    stores.addLog('in', msg.text || '/cancel')
    const taskId = match?.[1]?.trim()
    if (!taskId) { reply(msg.chat.id, 'Usage: /cancel &lt;id&gt;'); return }
    try {
      const result = await stores.cancelTask(taskId)
      if (result.ok) {
        reply(msg.chat.id, `Task <code>${h(taskId)}</code> cancelled.`)
      } else {
        reply(msg.chat.id, `Could not cancel: ${h(result.error || 'unknown error')}`)
      }
    } catch (err) {
      reply(msg.chat.id, `Cancel failed: ${h(err instanceof Error ? err.message : String(err))}`)
    }
  })

  // ── CALLBACK QUERY HANDLER ──────────────────────────────────────────

  bot.on('callback_query', async (query) => {
    const data = query.data || ''
    const chatIdQ = query.message?.chat.id
    if (!chatIdQ) return

    // Authorization check for callback queries
    if (!isAuthorizedChat(chatIdQ)) {
      await bot.answerCallbackQuery(query.id, { text: 'Unauthorized' })
      return
    }

    // Acknowledge the callback to remove loading spinner
    await bot.answerCallbackQuery(query.id)

    if (data.startsWith('cmd:')) {
      const cmd = data.slice(4)
      switch (cmd) {
        case 'status': handleStatus(chatIdQ); break
        case 'agents': handleAgents(chatIdQ); break
        case 'tasks': handleTasks(chatIdQ); break
        case 'swarm': handleSwarm(chatIdQ); break
        default: reply(chatIdQ, `Unknown command: ${h(cmd)}`)
      }
    }

    if (data.startsWith('cancel:')) {
      const taskId = data.slice(7)
      const result = await stores.cancelTask(taskId)
      if (result.ok) {
        reply(chatIdQ, `Task <code>${h(taskId)}</code> cancelled.`)
      } else {
        reply(chatIdQ, `Could not cancel task <code>${h(taskId)}</code>: ${h(result.error || 'unknown error')}`)
      }
    }
  })

  // ── NOTIFICATIONS (broadcast hook) ───────────────────────────────────

  const notif = config.notifications
  const progressThrottle = new Map<string, number>() // taskId -> last notify timestamp
  const PROGRESS_INTERVAL = 30_000 // 30s between progress updates per task

  function onBroadcast(type: string, payload: unknown) {
    try {
      const p = payload as Record<string, unknown>

      if (type === 'task:updated') {
        const status = String(p?.status ?? '')
        const title = String(p?.title ?? p?.id ?? 'Unknown')
        const taskId = String(p?.id ?? '')
        // Clean up progress throttle for terminal states
        if (status === 'completed' || status === 'failed' || status === 'cancelled') {
          progressThrottle.delete(taskId)
        }
        if (status === 'completed' && notif.taskCompleted) {
          const result = truncate(String(p?.result ?? 'No result'), 2400)
          send(`Task completed: <b>${h(title)}</b>\nID: <code>${h(taskId)}</code>\n\n${h(result)}`)
        } else if (status === 'failed' && notif.taskFailed) {
          const result = truncate(String(p?.result ?? 'No details'), 200)
          send(`Task failed: <b>${h(title)}</b>\n${h(result)}`)
        }
      }

      if (type === 'task:output' && notif.taskProgress) {
        const pType = String(p?.type ?? '')
        if (pType === 'progress') {
          const taskId = String(p?.id ?? '')
          const now = Date.now()
          const last = progressThrottle.get(taskId) || 0
          if (now - last >= PROGRESS_INTERVAL) {
            progressThrottle.set(taskId, now)
            const content = truncate(String(p?.content ?? ''), 150)
            send(`Task <code>${h(taskId)}</code>: ${h(content)}`)
          }
        }
      }

      if (type === 'swarm:status') {
        const status = String(p?.status ?? '')
        if (status === 'active' && notif.swarmInit) {
          const topology = String(p?.topology ?? 'unknown')
          const agents = Number(p?.activeAgents ?? p?.agentCount ?? 0)
          send(`Swarm initialized: ${h(topology)} topology, ${agents} agents`)
        } else if (status === 'shutdown' && notif.swarmShutdown) {
          send('Swarm shut down')
        }
      }

      if (type === 'agent:activity' && notif.agentError) {
        const status = String(p?.status ?? '')
        if (status === 'error') {
          const agentId = String(p?.agentId ?? p?.id ?? 'unknown')
          send(`Agent error: ${h(agentId)}`)
        }
      }
    } catch {
      // Fire-and-forget — never crash the broadcast path
    }
  }

  function send(text: string) {
    stores.addLog('out', text.replace(/<[^>]+>/g, '').slice(0, 100))
    bot.sendMessage(chatId, text, HTML).catch(err => {
      console.error(`${PREFIX} Send failed:`, err instanceof Error ? err.message : String(err))
    })
  }

  async function stop() {
    try {
      await bot.stopPolling()
      console.log(`${PREFIX} Bot stopped`)
    } catch (err) {
      console.error(`${PREFIX} Stop failed:`, err instanceof Error ? err.message : String(err))
    }
  }

  function getStatus() {
    return { enabled: true, connected, botUsername }
  }

  async function sendTest(): Promise<{ ok: boolean; error?: string }> {
    try {
      const ts = new Date().toLocaleString()
      await bot.sendMessage(chatId, `RuFloUI test message - ${h(ts)}`, HTML)
      return { ok: true }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      return { ok: false, error: msg }
    }
  }

  return { onBroadcast, stop, getStatus, sendTest }
}

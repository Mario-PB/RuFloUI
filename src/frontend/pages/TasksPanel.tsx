import { useEffect, useState, useCallback, useRef, type CSSProperties } from 'react'
import { useStore } from '@/store'
import { api } from '@/api'
import { Card } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { StatusBadge } from '@/components/ui/StatusBadge'
import type { Task } from '@/types'

const PRIORITY_COLORS: Record<string, string> = {
  critical: 'var(--accent-red)',
  high: 'var(--accent-orange)',
  normal: 'var(--accent-blue)',
  low: 'var(--text-muted)',
}

function getPriorityBadge(priority: string): CSSProperties {
  return {
    fontSize: 11, fontWeight: 600, padding: '2px 8px', borderRadius: 10,
    background: `${PRIORITY_COLORS[priority] || 'var(--text-muted)'}20`, color: PRIORITY_COLORS[priority] || 'var(--text-muted)',
    textTransform: 'uppercase', letterSpacing: '0.04em',
  }
}

const COLUMNS: Array<{ key: string; label: string; statuses: string[] }> = [
  { key: 'pending', label: 'Queued / Pending', statuses: ['pending'] },
  { key: 'in_progress', label: 'In Progress', statuses: ['in_progress'] },
  { key: 'completed', label: 'Completed', statuses: ['completed'] },
  { key: 'failed', label: 'Failed / Cancelled', statuses: ['failed', 'cancelled', 'interrupted'] },
]

interface TaskSummary {
  total: number
  completed: number
  pending: number
  inProgress: number
  failed: number
  interrupted?: number
  completionRate: number
  averageTime: string
  maxInFlight?: number
  inFlight?: number
  queued?: number
  terminal?: number
}

const s: Record<string, CSSProperties> = {
  page: { display: 'flex', flexDirection: 'column', gap: 20 },
  formToggle: { cursor: 'pointer', color: 'var(--accent-blue)', fontSize: 13, userSelect: 'none' },
  formGrid: {
    display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 12,
  },
  formFull: { gridColumn: '1 / -1' },
  label: { fontSize: 12, fontWeight: 500, color: 'var(--text-secondary)', marginBottom: 4, display: 'block' },
  input: {
    width: '100%', padding: '8px 12px', background: 'var(--bg-tertiary)',
    border: '1px solid var(--border)', borderRadius: 'var(--radius)', color: 'var(--text-primary)',
    fontSize: 13, outline: 'none',
  },
  textarea: {
    width: '100%', padding: '8px 12px', background: 'var(--bg-tertiary)',
    border: '1px solid var(--border)', borderRadius: 'var(--radius)', color: 'var(--text-primary)',
    fontSize: 13, outline: 'none', resize: 'vertical' as const, minHeight: 60,
  },
  select: {
    width: '100%', padding: '8px 12px', background: 'var(--bg-tertiary)',
    border: '1px solid var(--border)', borderRadius: 'var(--radius)', color: 'var(--text-primary)',
    fontSize: 13, outline: 'none',
  },
  board: { display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, minHeight: 300 },
  column: {
    background: 'var(--bg-secondary)', borderRadius: 'var(--radius-lg)',
    border: '1px solid var(--border)', display: 'flex', flexDirection: 'column', overflow: 'hidden',
  },
  colHeader: {
    padding: '12px 16px', fontSize: 13, fontWeight: 600,
    color: 'var(--text-primary)', borderBottom: '1px solid var(--border)',
    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
  },
  colCount: {
    fontSize: 11, background: 'var(--bg-tertiary)', padding: '2px 8px',
    borderRadius: 10, color: 'var(--text-muted)',
  },
  colBody: { padding: 8, flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 8 },
  taskCard: {
    padding: 12, background: 'var(--bg-card)', borderRadius: 'var(--radius)',
    border: '1px solid var(--border)', cursor: 'pointer', transition: 'border-color var(--transition)',
  },
  taskTitle: { fontSize: 13, fontWeight: 500, color: 'var(--text-primary)', marginBottom: 6 },
  taskMeta: { display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' },
  priorityBadge: {} as CSSProperties, // use getPriorityBadge() instead
  modeBadge: {
    fontSize: 10, fontWeight: 600, padding: '2px 6px', borderRadius: 8,
    textTransform: 'uppercase', letterSpacing: '0.04em',
  },
  queueBadge: {
    fontSize: 10, padding: '2px 6px', borderRadius: 8,
    background: 'var(--bg-tertiary)', color: 'var(--text-muted)',
  },
  branchTag: {
    fontSize: 11, padding: '2px 6px', borderRadius: 6,
    background: 'rgba(6,182,212,0.12)', color: 'var(--accent-cyan)',
    fontFamily: 'monospace',
  },
  cwdTag: {
    fontSize: 11, padding: '2px 6px', borderRadius: 6,
    background: 'rgba(139,92,246,0.12)', color: 'var(--accent-purple)',
    fontFamily: 'monospace', wordBreak: 'break-all', maxWidth: '100%',
  },
  agentTag: {
    fontSize: 11, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 4,
  },
  timeTag: { fontSize: 11, color: 'var(--text-muted)' },
  detail: {
    marginTop: 10, padding: '10px 0 0', borderTop: '1px solid var(--border)',
    display: 'flex', flexDirection: 'column', gap: 8,
  },
  detailRow: { fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5 },
  detailLabel: { fontWeight: 600, color: 'var(--text-muted)', marginRight: 6 },
  detailActions: { display: 'flex', gap: 6, marginTop: 4 },
  summaryGrid: { display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16 },
  summaryItem: {
    textAlign: 'center', padding: 16, background: 'var(--bg-secondary)',
    borderRadius: 'var(--radius)', border: '1px solid var(--border)',
  },
  summaryValue: { fontSize: 24, fontWeight: 700, color: 'var(--text-primary)' },
  summaryLabel: { fontSize: 12, color: 'var(--text-muted)', marginTop: 4 },
  assignSelect: {
    padding: '4px 8px', background: 'var(--bg-tertiary)', border: '1px solid var(--border)',
    borderRadius: 'var(--radius)', color: 'var(--text-primary)', fontSize: 12,
  },
}

function formatTime(iso: string): string {
  const d = new Date(iso)
  const now = new Date()
  const diff = now.getTime() - d.getTime()
  if (diff < 60000) return 'just now'
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`
  return d.toLocaleDateString()
}

function TaskCard({
  task, agents, expanded, onToggle, swarmActive, taskOutput, onLoadHistory,
}: { task: Task; agents: Array<{ id: string; name: string }>; expanded: boolean; onToggle: () => void; swarmActive: boolean; taskOutput: string[]; onLoadHistory: (id: string) => void }) {
  const [assigning, setAssigning] = useState(false)
  const [loading, setLoading] = useState('')
  const [continueOpen, setContinueOpen] = useState(false)
  const [continueText, setContinueText] = useState('')
  const logRef = useRef<HTMLDivElement>(null)
  const agentName = agents.find((a) => a.id === task.assignedTo)?.name ?? task.assignedTo

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [taskOutput])

  // Load persisted output history when card is expanded
  useEffect(() => {
    if (expanded && taskOutput.length === 0) onLoadHistory(task.id)
  }, [expanded, task.id, taskOutput.length, onLoadHistory])

  const handleAssign = async (agentId: string) => {
    setLoading('assign')
    try { await api.tasks.assign(task.id, agentId) } catch { /* handled by polling */ }
    setAssigning(false)
    setLoading('')
  }
  const handleAssignToSwarm = async () => {
    setLoading('swarm')
    try { await api.tasks.assign(task.id, 'swarm') } catch { /* handled by polling */ }
    setLoading('')
  }
  const handleComplete = async () => {
    setLoading('complete')
    try { await api.tasks.complete(task.id) } catch { /* handled by polling */ }
    setLoading('')
  }
  const handleCancel = async () => {
    setLoading('cancel')
    try { await api.tasks.cancel(task.id) } catch { /* handled by polling */ }
    setLoading('')
  }
  const handleContinue = async () => {
    if (!continueText.trim()) return
    setLoading('continue')
    try {
      await api.tasks.continue(task.id, continueText.trim())
      setContinueText('')
      setContinueOpen(false)
    } catch { /* silent */ }
    setLoading('')
  }

  const isRunning = task.status === 'in_progress'
  const isDone = task.status === 'completed' || task.status === 'failed'

  return (
    <div
      style={{ ...s.taskCard, ...(expanded ? { borderColor: 'var(--accent-blue)' } : {}), ...(isRunning ? { borderColor: 'var(--accent-cyan)', borderWidth: 2 } : {}) }}
      onClick={onToggle}
      onMouseEnter={(e) => { e.currentTarget.style.borderColor = isRunning ? 'var(--accent-cyan)' : 'var(--accent-blue)' }}
      onMouseLeave={(e) => { if (!expanded && !isRunning) e.currentTarget.style.borderColor = 'var(--border)' }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        {isRunning && <span style={{ width: 8, height: 8, borderRadius: '50%', background: 'var(--accent-cyan)', animation: 'pulse-glow 2s ease infinite', flexShrink: 0 }} />}
        <div style={s.taskTitle}>{task.title}</div>
      </div>
      <div style={s.taskMeta}>
        <span style={getPriorityBadge(task.priority)}>{task.priority}</span>
        {task.mode && (
          <span style={{
            ...s.modeBadge,
            background: task.mode === 'READ-ONLY' ? 'rgba(34,197,94,0.12)' : 'rgba(168,85,247,0.12)',
            color: task.mode === 'READ-ONLY' ? 'var(--accent-green)' : 'var(--accent-purple)',
          }}>{task.mode === 'READ-ONLY' ? 'READ' : 'WRITE'}</span>
        )}
        {task.queueState === 'queued' && (
          <span style={s.queueBadge}>queued {task.queuePosition ? `#${task.queuePosition}` : ''}</span>
        )}
        {task.queueState === 'dispatching' && (
          <span style={s.queueBadge}>dispatching…</span>
        )}
        {task.assignedTo && <span style={s.agentTag}>{agentName}</span>}
        <span style={s.timeTag}>{formatTime(task.createdAt)}</span>
      </div>
      {/* Live output for running tasks + persisted output for completed/failed */}
      {taskOutput.length > 0 && (isRunning || task.status === 'failed' || (expanded && isDone)) && (
        <div ref={logRef} onClick={(e) => e.stopPropagation()} style={{
          marginTop: 8, padding: 8, background: 'var(--bg-primary)', borderRadius: 'var(--radius)',
          border: `1px solid ${task.status === 'failed' ? 'var(--accent-red)' : 'var(--border)'}`, maxHeight: 200, overflowY: 'auto',
          fontFamily: 'monospace', fontSize: 11, lineHeight: 1.5, color: 'var(--text-secondary)',
        }}>
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 4, fontWeight: 600 }}>
            OUTPUT LOG ({taskOutput.length} lines)
          </div>
          {taskOutput.map((line, i) => (
            <div key={i} style={{ color: line.startsWith('[tool]') ? 'var(--accent-yellow)' : line.startsWith('[err]') ? 'var(--accent-red)' : line.startsWith('Progress:') ? 'var(--accent-cyan)' : 'var(--text-secondary)' }}>
              {line}
            </div>
          ))}
        </div>
      )}
      {/* Always show error result for failed tasks */}
      {task.status === 'failed' && task.result && (
        <div onClick={(e) => e.stopPropagation()} style={{
          marginTop: 6, padding: 8, background: 'rgba(239,68,68,0.08)', borderRadius: 'var(--radius)',
          border: '1px solid var(--accent-red)', maxHeight: 200, overflowY: 'auto',
        }}>
          <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--accent-red)', marginBottom: 4 }}>Error:</div>
          <pre style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: 11, margin: 0, color: 'var(--text-secondary)' }}>{task.result}</pre>
        </div>
      )}
      {expanded && (
        <div style={s.detail} onClick={(e) => e.stopPropagation()}>
          {task.description && (
            <div style={s.detailRow}><span style={s.detailLabel}>Description:</span>{task.description}</div>
          )}
          {(task as any).cwd && (
            <div style={s.detailRow}><span style={s.detailLabel}>CWD:</span><code style={{ fontFamily: 'monospace', fontSize: '0.85rem' }}>{(task as any).cwd}</code></div>
          )}
          {task.branchName && (
            <div style={s.detailRow}>
              <span style={s.detailLabel}>Branch:</span>
              <span style={s.branchTag}>{task.branchName}</span>
              {task.baseCommit && (
                <span style={{ ...s.queueBadge, marginLeft: 6, fontFamily: 'monospace' }}>
                  base {task.baseCommit.slice(0, 7)}
                </span>
              )}
            </div>
          )}
          {task.executionCwd && task.executionCwd !== task.sourceCwd && (
            <div style={s.detailRow}>
              <span style={s.detailLabel}>Execution CWD:</span>
              <span style={s.cwdTag}>{task.executionCwd}</span>
            </div>
          )}
          {task.sourceCwd && (
            <div style={s.detailRow}>
              <span style={s.detailLabel}>Source CWD:</span>
              <span style={s.cwdTag}>{task.sourceCwd}</span>
            </div>
          )}
          {task.result && task.status !== 'failed' && (
            <div style={{ ...s.detailRow, maxHeight: 200, overflowY: 'auto' }}>
              <span style={s.detailLabel}>Result:</span>
              <pre style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: 12, margin: 0 }}>{task.result}</pre>
            </div>
          )}
          {task.completedAt && (
            <div style={s.detailRow}><span style={s.detailLabel}>Completed:</span>{formatTime(task.completedAt)}</div>
          )}
          <div style={s.detailActions}>
            {assigning ? (
              <select
                style={s.assignSelect}
                onChange={(e) => { if (e.target.value) handleAssign(e.target.value) }}
                defaultValue=""
              >
                <option value="" disabled>Select agent...</option>
                {agents.map((a) => <option key={a.id} value={a.id}>{a.name}</option>)}
              </select>
            ) : (
              <>
                {task.status !== 'completed' && task.status !== 'cancelled' && task.status !== 'failed' && (
                  <>
                    <Button size="sm" variant="secondary" onClick={() => setAssigning(true)}
                      loading={loading === 'assign'}>Assign</Button>
                    {swarmActive && !isRunning && (
                      <Button size="sm" variant="secondary" onClick={handleAssignToSwarm}
                        loading={loading === 'swarm'}
                        style={{ background: 'rgba(6,182,212,0.15)', color: 'var(--accent-cyan)', borderColor: 'var(--accent-cyan)' }}
                      >Assign to Swarm</Button>
                    )}
                    {!isRunning && <Button size="sm" onClick={handleComplete}
                      loading={loading === 'complete'}>Complete</Button>}
                    <Button size="sm" variant="danger" onClick={handleCancel}
                      loading={loading === 'cancel'}>{isRunning ? 'Stop' : 'Cancel'}</Button>
                  </>
                )}
                {isDone && (
                  <Button size="sm" variant="secondary" onClick={() => setContinueOpen(!continueOpen)}
                    style={{ background: 'rgba(139,92,246,0.15)', color: 'var(--accent-purple)', borderColor: 'var(--accent-purple)' }}
                  >Continue Task</Button>
                )}
              </>
            )}
          </div>
          {/* Continue task form */}
          {continueOpen && (
            <div style={{ marginTop: 8, display: 'flex', flexDirection: 'column', gap: 6 }}>
              <textarea
                style={{
                  width: '100%', padding: '8px 10px', background: 'var(--bg-tertiary)',
                  border: '1px solid var(--accent-purple)', borderRadius: 'var(--radius)',
                  color: 'var(--text-primary)', fontSize: 12, outline: 'none',
                  resize: 'vertical', minHeight: 50, fontFamily: 'inherit',
                }}
                placeholder="What should be done next? (e.g. 'Add tests for the new feature', 'Fix the styling issue')"
                value={continueText}
                onChange={(e) => setContinueText(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter' && e.ctrlKey) handleContinue() }}
              />
              <div style={{ display: 'flex', gap: 6 }}>
                <Button size="sm" onClick={handleContinue} loading={loading === 'continue'}
                  disabled={!continueText.trim()}
                  style={{ background: 'var(--accent-purple)', borderColor: 'var(--accent-purple)' }}
                >Launch Follow-up</Button>
                <Button size="sm" variant="secondary" onClick={() => { setContinueOpen(false); setContinueText('') }}>Cancel</Button>
              </div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                Ctrl+Enter to submit. Previous task context will be injected automatically.
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

export default function TasksPanel() {
  const tasks = useStore((st) => st.tasks)
  const setTasks = useStore((st) => st.setTasks)
  const agents = useStore((st) => st.agents)
  const swarm = useStore((st) => st.swarm)
  const swarmActive = swarm != null && swarm.status !== 'shutdown' && swarm.status !== 'inactive'
  const [formOpen, setFormOpen] = useState(false)
  const [title, setTitle] = useState('')
  const [description, setDescription] = useState('')
  const [priority, setPriority] = useState<string>('normal')
  const [assignTo, setAssignTo] = useState('')
  const [cwd, setCwd] = useState('')
  const [creating, setCreating] = useState(false)
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [summary, setSummary] = useState<TaskSummary | null>(null)
  const [taskOutputs, setTaskOutputs] = useState<Record<string, string[]>>({})
  const [cleaning, setCleaning] = useState(false)
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined)
  const wsRef = useRef<WebSocket | null>(null)
  // Track tasks updated via WebSocket to avoid poll overwriting them
  const wsUpdatedRef = useRef<Map<string, { task: Task; ts: number }>>(new Map())
  // Track cleaned task IDs so poll doesn't re-add them
  const cleanedIdsRef = useRef<{ ids: Set<string>; ts: number }>({ ids: new Set(), ts: 0 })

  // Load persisted output history for a task
  const loadOutputHistory = useCallback(async (taskId: string) => {
    // Only load if we don't have live output already
    if ((taskOutputs[taskId] || []).length > 0) return
    try {
      const data = await api.tasks.output(taskId) as { taskId: string; lines: Array<{ content: string }> }
      if (data.lines?.length > 0) {
        setTaskOutputs(prev => ({
          ...prev,
          [taskId]: data.lines.map((l: { content: string }) => l.content),
        }))
      }
    } catch { /* silent */ }
  }, [taskOutputs])

  // WebSocket for live task output
  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws`)
    wsRef.current = ws
    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data)
        if (msg.type === 'task:output' && msg.payload?.id) {
          const { id, type, content, tool, input } = msg.payload
          let line = ''
          if (type === 'tool') line = `[tool] ${tool}: ${input || ''}`
          else if (type === 'stderr') line = `[err] ${content}`
          else if (type === 'text') line = content?.slice(0, 200) || ''
          else if (type === 'raw') line = content?.slice(0, 200) || ''
          else if (type === 'progress') line = content || ''
          else if (type === 'done') line = `--- Done (exit ${msg.payload.code}) ---`
          if (line) {
            setTaskOutputs(prev => ({
              ...prev,
              [id]: [...(prev[id] || []).slice(-100), line],
            }))
          }
        }
        // Track task updates from WebSocket so polling doesn't overwrite them
        if ((msg.type === 'task:updated' || msg.type === 'task:added') && msg.payload) {
          const t = msg.payload as Task
          if (t.id) {
            wsUpdatedRef.current.set(t.id, { task: t, ts: Date.now() })
          }
        }
      } catch { /* ignore */ }
    }
    return () => { ws.close() }
  }, [])

  const setAgents = useStore((st) => st.setAgents)

  const fetchData = useCallback(async () => {
    try {
      const [taskRes, summaryRes, agentRes] = await Promise.allSettled([
        api.tasks.list(),
        api.tasks.summary(),
        api.agents.list(),
      ])
      if (taskRes.status === 'fulfilled') {
        const data = taskRes.value as { tasks?: Task[] } | Task[]
        let polledTasks = Array.isArray(data) ? data : (data.tasks ?? [])
        // Merge: prefer WebSocket-updated tasks over stale poll data (within 10s window)
        const now = Date.now()
        const wsMap = wsUpdatedRef.current
        polledTasks = polledTasks.map(t => {
          const ws = wsMap.get(t.id)
          if (ws && now - ws.ts < 10_000) return ws.task
          return t
        })
        // Add any WS tasks not yet in poll results (newly created)
        for (const [id, ws] of wsMap.entries()) {
          if (now - ws.ts < 10_000 && !polledTasks.find(t => t.id === id)) {
            polledTasks.push(ws.task)
          }
        }
        // Clean up old WS entries
        for (const [id, ws] of wsMap.entries()) {
          if (now - ws.ts > 10_000) wsMap.delete(id)
        }
        // Filter out recently cleaned tasks (prevent poll re-adding them)
        const cleaned = cleanedIdsRef.current
        if (cleaned.ids.size > 0 && now - cleaned.ts < 10_000) {
          polledTasks = polledTasks.filter(t => !cleaned.ids.has(t.id))
        } else if (cleaned.ids.size > 0) {
          cleanedIdsRef.current = { ids: new Set(), ts: 0 }
        }
        setTasks(polledTasks)
      }
      if (summaryRes.status === 'fulfilled') {
        setSummary(summaryRes.value as TaskSummary)
      }
      if (agentRes.status === 'fulfilled') {
        const data = agentRes.value as { agents?: Array<{ id: string; name: string; type: string; status: string }> } | Array<{ id: string; name: string; type: string; status: string }>
        setAgents(Array.isArray(data) ? data : (data.agents ?? []) as any)
      }
    } catch { /* silent */ }
  }, [setTasks, setAgents])

  useEffect(() => {
    fetchData()
    intervalRef.current = setInterval(fetchData, 5000)
    return () => clearInterval(intervalRef.current)
  }, [fetchData])

  const handleCleanCompleted = async () => {
    setCleaning(true)
    // Optimistic: remove from local store immediately and prevent poll re-adding
    const removedIds = new Set(tasks.filter(t => t.status === 'completed' || t.status === 'failed' || t.status === 'cancelled').map(t => t.id))
    cleanedIdsRef.current = { ids: removedIds, ts: Date.now() }
    setTasks(tasks.filter(t => !removedIds.has(t.id)))
    try {
      await api.tasks.cleanCompleted()
    } catch { /* silent */ }
    setCleaning(false)
    fetchData()
  }

  const handleCreate = async () => {
    if (!title.trim()) return
    setCreating(true)
    try {
      await api.tasks.create({
        title: title.trim(),
        description: description.trim(),
        priority,
        ...(assignTo ? { assignTo } : {}),
        ...(cwd.trim() ? { cwd: cwd.trim() } : {}),
      })
      setTitle('')
      setDescription('')
      setPriority('normal')
      setAssignTo('')
      setCwd('')
      fetchData()
    } catch { /* silent */ }
    setCreating(false)
  }

  return (
    <div style={s.page}>
      <Card
        title="Create Task"
        actions={
          <span style={s.formToggle} onClick={() => setFormOpen(!formOpen)}>
            {formOpen ? 'Collapse' : 'Expand'}
          </span>
        }
      >
        {formOpen && (
          <>
            <div style={s.formGrid}>
              <div>
                <label style={s.label}>Title</label>
                <input style={s.input} placeholder="Task title..." value={title}
                  onChange={(e) => setTitle(e.target.value)} />
              </div>
              <div>
                <label style={s.label}>Priority</label>
                <select style={s.select} value={priority} onChange={(e) => setPriority(e.target.value)}>
                  <option value="low">Low</option>
                  <option value="normal">Normal</option>
                  <option value="high">High</option>
                  <option value="critical">Critical</option>
                </select>
              </div>
              <div style={s.formFull}>
                <label style={s.label}>Description</label>
                <textarea style={s.textarea} placeholder="Task description..." value={description}
                  onChange={(e) => setDescription(e.target.value)} />
              </div>
              <div>
                <label style={s.label}>Assign To</label>
                <select style={s.select} value={assignTo} onChange={(e) => setAssignTo(e.target.value)}>
                  <option value="">Unassigned</option>
                  {swarmActive && <option value="swarm">Swarm (Coordinator)</option>}
                  {agents.map((a) => <option key={a.id} value={a.id}>{a.name} ({a.type})</option>)}
                </select>
              </div>
              <div style={s.formFull}>
                <label style={s.label}>Working Directory</label>
                <input style={{ ...s.input, fontFamily: 'monospace', fontSize: '0.85rem' }}
                  placeholder="C:\Projects\my-app (optional — defaults to server CWD)"
                  value={cwd} onChange={(e) => setCwd(e.target.value)} />
              </div>
              <div style={{ display: 'flex', alignItems: 'flex-end' }} data-tour="task-create">
                <Button onClick={handleCreate} loading={creating} disabled={!title.trim()}>
                  Create Task
                </Button>
              </div>
            </div>
          </>
        )}
      </Card>

      <Card title="Task Board">
        <div style={s.board} data-tour="task-board">
          {COLUMNS.map((col) => {
            const colTasks = tasks.filter((t) => col.statuses.includes(t.status))
            return (
              <div key={col.key} style={s.column}>
                <div style={s.colHeader}>
                  {col.label}
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    {(col.key === 'completed' || col.key === 'failed') && colTasks.length > 0 && (
                      <Button size="sm" variant="ghost" loading={cleaning} onClick={(e) => { e.stopPropagation(); handleCleanCompleted() }}
                        style={{ fontSize: 10, padding: '2px 6px', color: 'var(--text-muted)' }}>Clean</Button>
                    )}
                    <span style={s.colCount}>{colTasks.length}</span>
                  </div>
                </div>
                <div style={s.colBody}>
                  {colTasks.length === 0 && (
                    <div style={{ fontSize: 12, color: 'var(--text-muted)', textAlign: 'center', padding: 20 }}>
                      No tasks
                    </div>
                  )}
                  {colTasks.map((task) => (
                    <TaskCard
                      key={task.id}
                      task={task}
                      agents={agents}
                      expanded={expandedId === task.id}
                      onToggle={() => setExpandedId(expandedId === task.id ? null : task.id)}
                      swarmActive={swarmActive}
                      taskOutput={taskOutputs[task.id] || []}
                      onLoadHistory={loadOutputHistory}
                    />
                  ))}
                </div>
              </div>
            )
          })}
        </div>
      </Card>

      {summary && (
        <Card title="Task Summary">
          <div style={s.summaryGrid}>
            <div style={s.summaryItem}>
              <div style={s.summaryValue}>{summary.total}</div>
              <div style={s.summaryLabel}>Total Tasks</div>
            </div>
            <div style={s.summaryItem}>
              <div style={{ ...s.summaryValue, color: 'var(--accent-cyan)' }}>
                {summary.inFlight ?? summary.inProgress}
                {summary.maxInFlight != null && (
                  <span style={{ fontSize: 14, color: 'var(--text-muted)' }}> / {summary.maxInFlight}</span>
                )}
              </div>
              <div style={s.summaryLabel}>In Flight</div>
            </div>
            <div style={s.summaryItem}>
              <div style={{ ...s.summaryValue, color: 'var(--accent-blue)' }}>
                {summary.queued ?? summary.pending}
              </div>
              <div style={s.summaryLabel}>Queued</div>
            </div>
            <div style={s.summaryItem}>
              <div style={{ ...s.summaryValue, color: 'var(--accent-green)' }}>
                {summary.completionRate != null ? `${Math.round(summary.completionRate * 100)}%` : '--'}
              </div>
              <div style={s.summaryLabel}>Completion Rate</div>
            </div>
            <div style={s.summaryItem}>
              <div style={{ ...s.summaryValue, color: 'var(--accent-green)' }}>{summary.completed}</div>
              <div style={s.summaryLabel}>Completed</div>
            </div>
            <div style={s.summaryItem}>
              <div style={{ ...s.summaryValue, color: 'var(--accent-red)' }}>
                {summary.failed + (summary.interrupted || 0)}
              </div>
              <div style={s.summaryLabel}>Failed / Interrupted</div>
            </div>
            <div style={s.summaryItem}>
              <div style={{ ...s.summaryValue, color: 'var(--accent-purple)' }}>{summary.terminal ?? 0}</div>
              <div style={s.summaryLabel}>Terminal</div>
            </div>
            <div style={s.summaryItem}>
              <div style={{ ...s.summaryValue, color: 'var(--accent-yellow)' }}>
                {summary.averageTime ?? '--'}
              </div>
              <div style={s.summaryLabel}>Avg Time</div>
            </div>
          </div>
        </Card>
      )}
    </div>
  )
}

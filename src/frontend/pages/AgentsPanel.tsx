import { useEffect, useState, useCallback, useRef } from 'react'
import { useStore } from '@/store'
import { api } from '@/api'
import { Card } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { StatusBadge } from '@/components/ui/StatusBadge'
import type { Agent } from '@/types'

const AGENT_TYPES = [
  'coder', 'researcher', 'tester', 'reviewer', 'architect',
  'coordinator', 'analyst', 'optimizer',
  'security-architect', 'security-auditor',
  'memory-specialist', 'swarm-specialist', 'performance-engineer',
  'core-architect', 'test-architect',
] as const

const STATUS_FILTERS = ['all', 'idle', 'running', 'completed', 'error', 'terminated'] as const

const styles = {
  page: { padding: 24, display: 'flex', flexDirection: 'column' as const, gap: 24 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
  title: { fontSize: 24, fontWeight: 700, color: 'var(--text-primary)' },
  form: { display: 'flex', gap: 16, alignItems: 'flex-end', flexWrap: 'wrap' as const },
  field: { display: 'flex', flexDirection: 'column' as const, gap: 6 },
  label: { fontSize: 12, fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase' as const, letterSpacing: '0.05em' },
  select: { minWidth: 200, height: 38 },
  input: { minWidth: 180, height: 38 },
  toolbar: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' as const },
  viewToggle: { display: 'flex', gap: 4, background: 'var(--bg-tertiary)', borderRadius: 'var(--radius)', padding: 2 },
  viewBtn: (active: boolean) => ({
    padding: '6px 12px', borderRadius: 'var(--radius)', fontSize: 12, fontWeight: 500, cursor: 'pointer' as const,
    background: active ? 'var(--accent-blue)' : 'transparent',
    color: active ? '#fff' : 'var(--text-secondary)',
    border: 'none', transition: 'all var(--transition)',
  }),
  table: { width: '100%', borderCollapse: 'collapse' as const },
  th: { padding: '10px 14px', textAlign: 'left' as const, fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' as const, letterSpacing: '0.05em', borderBottom: '1px solid var(--border)' },
  td: { padding: '12px 14px', borderBottom: '1px solid var(--border)', fontSize: 13 },
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: 12 },
  gridCard: { padding: 16, background: 'var(--bg-tertiary)', borderRadius: 'var(--radius)', border: '1px solid var(--border)', display: 'flex', flexDirection: 'column' as const, gap: 8, cursor: 'pointer', transition: 'border-color var(--transition)' },
  overlay: { position: 'fixed' as const, inset: 0, background: 'rgba(0,0,0,0.6)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 1000 },
  modal: { background: 'var(--bg-secondary)', borderRadius: 'var(--radius-lg)', border: '1px solid var(--border)', width: 520, maxHeight: '80vh', overflow: 'auto' as const, padding: 24, display: 'flex', flexDirection: 'column' as const, gap: 16 },
  detailRow: { display: 'flex', justifyContent: 'space-between', padding: '8px 0', borderBottom: '1px solid var(--border)', fontSize: 13 },
  detailLabel: { color: 'var(--text-muted)' },
  detailValue: { color: 'var(--text-primary)', fontWeight: 500 },
  empty: { textAlign: 'center' as const, padding: 48, color: 'var(--text-muted)' },
  error: { padding: '10px 14px', background: 'rgba(239,68,68,0.1)', border: '1px solid var(--accent-red)', borderRadius: 'var(--radius)', color: 'var(--accent-red)', fontSize: 13 },
  clickRow: { cursor: 'pointer', transition: 'background var(--transition)' },
}

const statusRowColor = (s: Agent['status']) => {
  if (s === 'running') return 'rgba(16,185,129,0.05)'
  if (s === 'error') return 'rgba(239,68,68,0.05)'
  if (s === 'terminated') return 'rgba(100,116,139,0.05)'
  return 'transparent'
}

export default function AgentsPanel() {
  const { agents, setAgents, addAgent, removeAgent } = useStore()
  const [spawnType, setSpawnType] = useState<string>(AGENT_TYPES[0])
  const [spawnName, setSpawnName] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [view, setView] = useState<'table' | 'grid'>('table')
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const [sortBy, setSortBy] = useState<'name' | 'type' | 'status'>('name')
  const [selectedAgent, setSelectedAgent] = useState<Agent | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [confirmTerminate, setConfirmTerminate] = useState<{ id: string; name: string } | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const fetchAgents = useCallback(async () => {
    try {
      const data = await api.agents.list() as { agents?: Agent[] } | Agent[]
      setAgents(Array.isArray(data) ? data : (data.agents ?? []))
    } catch { /* ignore polling errors */ }
  }, [setAgents])

  useEffect(() => {
    fetchAgents()
    intervalRef.current = setInterval(fetchAgents, 3000)
    return () => { if (intervalRef.current) clearInterval(intervalRef.current) }
  }, [fetchAgents])

  // WebSocket for real-time agent activity
  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws`)
    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data)
        if (msg.type === 'agent:activity' && msg.payload) {
          const { agentId, status, currentTask, currentAction } = msg.payload
          const current = useStore.getState().agents
          setAgents(current.map(a => a.id === agentId ? {
            ...a,
            status: status === 'working' ? 'running' : status === 'idle' ? 'idle' : a.status,
            currentTask, currentAction,
          } : a))
        }
      } catch { /* ignore */ }
    }
    return () => ws.close()
  }, [setAgents])

  const handleSpawn = async () => {
    if (!spawnName.trim()) { setError('Agent name is required'); return }
    setLoading(true)
    setError(null)
    try {
      const agent = await api.agents.spawn({ type: spawnType, name: spawnName.trim() }) as Agent
      addAgent(agent)
      setSpawnName('')
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to spawn agent')
    } finally {
      setLoading(false)
    }
  }

  const handleTerminate = (id: string, name: string) => {
    setConfirmTerminate({ id, name })
  }

  const executeTerminate = async () => {
    if (!confirmTerminate) return
    const { id } = confirmTerminate
    setConfirmTerminate(null)
    try {
      if (id === '__all__') {
        await api.agents.terminateAll()
        setAgents([])
        setSelectedAgent(null)
      } else {
        await api.agents.terminate(id)
        removeAgent(id)
        if (selectedAgent?.id === id) setSelectedAgent(null)
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to terminate agent')
    }
  }

  const handleHealthCheck = async (id: string) => {
    setDetailLoading(true)
    try {
      const health = await api.agents.health(id) as Record<string, unknown>
      alert(`Health check: ${JSON.stringify(health, null, 2)}`)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Health check failed')
    } finally {
      setDetailLoading(false)
    }
  }

  const openDetail = async (agent: Agent) => {
    setSelectedAgent(agent)
    try {
      const detail = await api.agents.status(agent.id) as Agent
      setSelectedAgent(prev => prev?.id === agent.id ? { ...agent, ...detail } : prev)
    } catch { /* use existing data */ }
  }

  const filtered = agents
    .filter(a => statusFilter === 'all' || a.status === statusFilter)
    .sort((a, b) => {
      if (sortBy === 'name') return a.name.localeCompare(b.name)
      if (sortBy === 'type') return a.type.localeCompare(b.type)
      return a.status.localeCompare(b.status)
    })

  return (
    <div style={styles.page}>
      <div style={styles.header}>
        <span style={styles.title}>Agents Management</span>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ fontSize: 13, color: 'var(--text-muted)' }}>{agents.length} total agents</span>
          {agents.length > 0 && (
            <Button size="sm" variant="danger" onClick={() => setConfirmTerminate({ id: '__all__', name: 'ALL agents' })}>
              Terminate All
            </Button>
          )}
        </div>
      </div>

      <Card>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <h3 style={{ fontSize: 16, fontWeight: 600, color: 'var(--text-primary)' }}>Spawn Agent</h3>
          {error && <div style={styles.error}>{error}</div>}
          <div style={styles.form}>
            <div style={styles.field}>
              <label style={styles.label}>Agent Type</label>
              <select style={styles.select} value={spawnType} onChange={e => setSpawnType(e.target.value)}>
                {AGENT_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div style={styles.field}>
              <label style={styles.label}>Name</label>
              <input style={styles.input} placeholder="my-agent" value={spawnName}
                onChange={e => setSpawnName(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleSpawn()} />
            </div>
            <Button onClick={handleSpawn} disabled={loading}>
              {loading ? 'Spawning...' : 'Spawn Agent'}
            </Button>
          </div>
        </div>
      </Card>

      <Card>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={styles.toolbar}>
            <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
              <div style={styles.field}>
                <label style={styles.label}>Filter Status</label>
                <select style={{ ...styles.select, minWidth: 140 }} value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
                  {STATUS_FILTERS.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>
              <div style={styles.field}>
                <label style={styles.label}>Sort By</label>
                <select style={{ ...styles.select, minWidth: 120 }} value={sortBy} onChange={e => setSortBy(e.target.value as typeof sortBy)}>
                  <option value="name">Name</option>
                  <option value="type">Type</option>
                  <option value="status">Status</option>
                </select>
              </div>
            </div>
            <div style={styles.viewToggle}>
              <button style={styles.viewBtn(view === 'table')} onClick={() => setView('table')}>Table</button>
              <button style={styles.viewBtn(view === 'grid')} onClick={() => setView('grid')}>Grid</button>
            </div>
          </div>

          {filtered.length === 0 ? (
            <p style={styles.empty}>No agents found. Spawn one above to get started.</p>
          ) : view === 'table' ? (
            <div style={{ overflowX: 'auto' }}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    <th style={styles.th}>Name</th>
                    <th style={styles.th}>Type</th>
                    <th style={styles.th}>Status</th>
                    <th style={styles.th}>Current Activity</th>
                    <th style={styles.th}>Tasks</th>
                    <th style={styles.th}>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map(agent => (
                    <tr key={agent.id} style={{ ...styles.clickRow, background: statusRowColor(agent.status) }}
                      onClick={() => openDetail(agent)}
                      onMouseEnter={e => (e.currentTarget.style.background = 'var(--bg-hover)')}
                      onMouseLeave={e => (e.currentTarget.style.background = statusRowColor(agent.status))}>
                      <td style={{ ...styles.td, fontWeight: 600, color: 'var(--text-primary)' }}>{agent.name}</td>
                      <td style={{ ...styles.td, fontFamily: 'monospace', color: 'var(--text-secondary)', fontSize: 12 }}>{agent.type}</td>
                      <td style={styles.td}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                          {agent.status === 'running' && <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-green)', animation: 'pulse-glow 2s ease infinite', flexShrink: 0 }} />}
                          <StatusBadge status={agent.status} />
                        </div>
                      </td>
                      <td style={{ ...styles.td, fontSize: 12, maxWidth: 250 }}>
                        {agent.currentAction ? (
                          <span style={{ color: 'var(--accent-cyan)', fontFamily: 'monospace', fontSize: 11 }}>
                            {agent.currentAction.slice(0, 60)}{agent.currentAction.length > 60 ? '...' : ''}
                          </span>
                        ) : (
                          <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}>Idle</span>
                        )}
                      </td>
                      <td style={{ ...styles.td, color: 'var(--text-secondary)' }}>{agent.metrics?.tasksCompleted ?? 0}</td>
                      <td style={styles.td} onClick={e => e.stopPropagation()}>
                        <div style={{ display: 'flex', gap: 8 }}>
                          <Button size="sm" onClick={() => openDetail(agent)}>Status</Button>
                          {agent.status !== 'terminated' && (
                            <Button size="sm" variant="danger" onClick={() => handleTerminate(agent.id, agent.name)}>
                              Terminate
                            </Button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div style={styles.grid}>
              {filtered.map(agent => (
                <div key={agent.id} style={{ ...styles.gridCard, borderColor: agent.status === 'running' ? 'var(--accent-green)' : 'var(--border)' }}
                  onClick={() => openDetail(agent)}
                  onMouseEnter={e => (e.currentTarget.style.borderColor = 'var(--accent-blue)')}
                  onMouseLeave={e => (e.currentTarget.style.borderColor = agent.status === 'running' ? 'var(--accent-green)' : 'var(--border)')}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontWeight: 600, color: 'var(--text-primary)' }}>{agent.name}</span>
                    <StatusBadge status={agent.status} />
                  </div>
                  <span style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--text-muted)' }}>{agent.type}</span>
                  {agent.currentAction && (
                    <div style={{
                      padding: '4px 8px', background: 'rgba(6,182,212,0.1)', borderRadius: 'var(--radius)',
                      fontSize: 11, fontFamily: 'monospace', color: 'var(--accent-cyan)',
                      overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                    }}>
                      {agent.currentAction.slice(0, 50)}
                    </div>
                  )}
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: 'var(--text-secondary)' }}>
                    <span>Tasks: {agent.metrics?.tasksCompleted ?? 0}</span>
                    <span>Err: {agent.metrics?.errorRate != null ? `${(agent.metrics.errorRate * 100).toFixed(1)}%` : '--'}</span>
                  </div>
                  <div style={{ display: 'flex', gap: 8, marginTop: 4 }} onClick={e => e.stopPropagation()}>
                    {agent.status !== 'terminated' && (
                      <Button size="sm" variant="danger" onClick={() => handleTerminate(agent.id, agent.name)}>
                        Terminate
                      </Button>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </Card>

      {selectedAgent && (
        <div style={styles.overlay} onClick={() => setSelectedAgent(null)}>
          <div style={styles.modal} onClick={e => e.stopPropagation()} className="animate-fade-in">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h2 style={{ fontSize: 18, fontWeight: 700, color: 'var(--text-primary)' }}>Agent Detail</h2>
              <button onClick={() => setSelectedAgent(null)}
                style={{ background: 'none', color: 'var(--text-muted)', fontSize: 20, lineHeight: 1, padding: 4 }}>
                x
              </button>
            </div>

            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>ID</span>
              <span style={{ ...styles.detailValue, fontFamily: 'monospace', fontSize: 12 }}>{selectedAgent.id}</span>
            </div>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Name</span>
              <span style={styles.detailValue}>{selectedAgent.name}</span>
            </div>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Type</span>
              <span style={{ ...styles.detailValue, fontFamily: 'monospace' }}>{selectedAgent.type}</span>
            </div>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Status</span>
              <StatusBadge status={selectedAgent.status} />
            </div>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Created</span>
              <span style={styles.detailValue}>
                {selectedAgent.createdAt ? new Date(selectedAgent.createdAt).toLocaleString() : '--'}
              </span>
            </div>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Last Activity</span>
              <span style={styles.detailValue}>
                {selectedAgent.lastActivity ? new Date(selectedAgent.lastActivity).toLocaleString() : '--'}
              </span>
            </div>

            {selectedAgent.metrics && (
              <>
                <h3 style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-secondary)', marginTop: 8 }}>Metrics</h3>
                <div style={styles.detailRow}>
                  <span style={styles.detailLabel}>Tasks Completed</span>
                  <span style={styles.detailValue}>{selectedAgent.metrics.tasksCompleted}</span>
                </div>
                <div style={styles.detailRow}>
                  <span style={styles.detailLabel}>Error Rate</span>
                  <span style={styles.detailValue}>{(selectedAgent.metrics.errorRate * 100).toFixed(1)}%</span>
                </div>
                <div style={styles.detailRow}>
                  <span style={styles.detailLabel}>Avg Response Time</span>
                  <span style={styles.detailValue}>{selectedAgent.metrics.avgResponseTime.toFixed(0)}ms</span>
                </div>
              </>
            )}

            {(selectedAgent.currentTask || selectedAgent.taskId) && (
              <div style={styles.detailRow}>
                <span style={styles.detailLabel}>Current Task</span>
                <span style={{ ...styles.detailValue, fontFamily: 'monospace', fontSize: 12 }}>{selectedAgent.currentTask || selectedAgent.taskId}</span>
              </div>
            )}
            {selectedAgent.currentAction && (
              <div style={styles.detailRow}>
                <span style={styles.detailLabel}>Current Action</span>
                <span style={{ ...styles.detailValue, fontFamily: 'monospace', fontSize: 11, color: 'var(--accent-cyan)' }}>{selectedAgent.currentAction}</span>
              </div>
            )}

            <div style={{ display: 'flex', gap: 12, marginTop: 8 }}>
              <Button onClick={() => handleHealthCheck(selectedAgent.id)} disabled={detailLoading}>
                {detailLoading ? 'Checking...' : 'Health Check'}
              </Button>
              {selectedAgent.status !== 'terminated' && (
                <Button variant="danger" onClick={() => handleTerminate(selectedAgent.id, selectedAgent.name)}>
                  Terminate Agent
                </Button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Terminate Confirmation Modal */}
      {confirmTerminate && (
        <div style={styles.overlay} onClick={() => setConfirmTerminate(null)}>
          <div
            style={{
              background: 'var(--bg-secondary)',
              borderRadius: 'var(--radius-lg)',
              border: '1px solid var(--border)',
              padding: 24,
              width: 400,
              display: 'flex',
              flexDirection: 'column',
              gap: 16,
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ fontSize: 16, fontWeight: 700, color: 'var(--text-primary)' }}>
              Terminate Agent
            </div>
            <div style={{ fontSize: 14, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
              Are you sure you want to terminate <strong style={{ color: 'var(--accent-red)' }}>{confirmTerminate.name}</strong>?
              This action cannot be undone.
            </div>
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 4 }}>
              <Button variant="ghost" onClick={() => setConfirmTerminate(null)}>Cancel</Button>
              <Button variant="danger" onClick={executeTerminate}>Terminate</Button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

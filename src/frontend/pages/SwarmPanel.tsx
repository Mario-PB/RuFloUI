import { useEffect, useState, useCallback, useRef } from 'react'
import { useStore } from '@/store'
import { api } from '@/api'
import { Card } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { StatusBadge } from '@/components/ui/StatusBadge'
import type { Agent, SwarmState } from '@/types'

type Topology = SwarmState['topology']
type Strategy = 'specialized' | 'generalist' | 'adaptive'

const TOPOLOGIES: Topology[] = ['hierarchical', 'mesh', 'star', 'ring', 'hierarchical-mesh']
const STRATEGIES: Strategy[] = ['specialized', 'generalist', 'adaptive']

const styles = {
  page: { padding: 24, display: 'flex', flexDirection: 'column' as const, gap: 24 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
  title: { fontSize: 24, fontWeight: 700, color: 'var(--text-primary)' },
  form: { display: 'flex', gap: 16, alignItems: 'flex-end', flexWrap: 'wrap' as const },
  field: { display: 'flex', flexDirection: 'column' as const, gap: 6 },
  label: { fontSize: 12, fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase' as const, letterSpacing: '0.05em' },
  select: { minWidth: 180, height: 38 },
  input: { width: 100, height: 38 },
  infoRow: { display: 'flex', gap: 16, flexWrap: 'wrap' as const },
  infoBadge: { padding: '6px 14px', background: 'var(--bg-tertiary)', borderRadius: 'var(--radius)', fontSize: 13, color: 'var(--text-secondary)', display: 'flex', gap: 8, alignItems: 'center' },
  infoValue: { color: 'var(--text-primary)', fontWeight: 600 },
  svgContainer: { width: '100%', height: 600, background: 'var(--bg-primary)', borderRadius: 'var(--radius-lg)', border: '1px solid var(--border)', overflow: 'hidden' },
  agentGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 12 },
  agentCard: { padding: 16, background: 'var(--bg-tertiary)', borderRadius: 'var(--radius)', border: '1px solid var(--border)', display: 'flex', flexDirection: 'column' as const, gap: 8 },
  agentName: { fontWeight: 600, fontSize: 14, color: 'var(--text-primary)' },
  agentType: { fontSize: 12, color: 'var(--text-muted)', fontFamily: 'monospace' },
  agentMeta: { fontSize: 12, color: 'var(--text-secondary)' },
  pulseDot: { width: 8, height: 8, borderRadius: '50%', background: 'var(--accent-green)', animation: 'pulse-glow 2s ease infinite', display: 'inline-block' },
  emptyState: { textAlign: 'center' as const, padding: 48, color: 'var(--text-muted)' },
  actions: { display: 'flex', gap: 12 },
}

function TopologyGraph({ topology, agents }: { topology: Topology; agents: Agent[] }) {
  const count = Math.max(agents.length, 3)
  const cx = 350
  const cy = 315
  const r = 180

  const positions = agents.map((_, i) => {
    if (topology === 'star') {
      if (i === 0) return { x: cx, y: cy }
      const angle = ((i - 1) / (count - 1)) * Math.PI * 2 - Math.PI / 2
      return { x: cx + Math.cos(angle) * r, y: cy + Math.sin(angle) * r }
    }
    if (topology === 'hierarchical' || topology === 'hierarchical-mesh') {
      const levels = Math.ceil(Math.log2(count + 1))
      let level = 0
      let idx = i
      let nodesAtLevel = 1
      let cumulative = 0
      while (idx >= cumulative + nodesAtLevel && level < levels) {
        cumulative += nodesAtLevel
        nodesAtLevel *= 2
        level++
      }
      const posInLevel = idx - cumulative
      const spacing = 700 / (nodesAtLevel + 1)
      return { x: spacing * (posInLevel + 1), y: 60 + level * 140 }
    }
    // ring or mesh: circular layout
    const angle = (i / count) * Math.PI * 2 - Math.PI / 2
    return { x: cx + Math.cos(angle) * r, y: cy + Math.sin(angle) * r }
  })

  const edges: Array<[number, number]> = []
  if (topology === 'mesh' || topology === 'hierarchical-mesh') {
    for (let i = 0; i < positions.length; i++)
      for (let j = i + 1; j < positions.length; j++) edges.push([i, j])
  } else if (topology === 'star') {
    for (let i = 1; i < positions.length; i++) edges.push([0, i])
  } else if (topology === 'ring') {
    for (let i = 0; i < positions.length; i++) edges.push([i, (i + 1) % positions.length])
  } else if (topology === 'hierarchical') {
    for (let i = 1; i < positions.length; i++) edges.push([Math.floor((i - 1) / 2), i])
  }

  const statusColor = (s: Agent['status']) =>
    s === 'running' ? 'var(--accent-green)' : s === 'idle' ? 'var(--accent-blue)' : s === 'error' ? 'var(--accent-red)' : 'var(--text-muted)'

  return (
    <svg viewBox="0 0 700 630" style={{ width: '100%', height: '100%' }}>
      <defs>
        <linearGradient id="line-grad" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="var(--accent-blue)" stopOpacity="0.4" />
          <stop offset="100%" stopColor="var(--accent-cyan)" stopOpacity="0.4" />
        </linearGradient>
      </defs>
      {edges.map(([a, b], i) => (
        positions[a] && positions[b] ? (
          <line key={i} x1={positions[a].x} y1={positions[a].y} x2={positions[b].x} y2={positions[b].y}
            stroke="url(#line-grad)" strokeWidth={1.5} strokeDasharray="4 4">
            <animate attributeName="stroke-dashoffset" values="8;0" dur="1.5s" repeatCount="indefinite" />
          </line>
        ) : null
      ))}
      {positions.map((pos, i) => (
        <g key={agents[i]?.id || i}>
          <circle cx={pos.x} cy={pos.y} r={42} fill="var(--bg-card)" stroke={statusColor(agents[i]?.status || 'idle')} strokeWidth={2} />
          <text x={pos.x} y={pos.y - 4} textAnchor="middle" fill="var(--text-primary)" fontSize={12} fontWeight={600}>
            {agents[i]?.name || `Agent ${i}`}
          </text>
          <text x={pos.x} y={pos.y + 12} textAnchor="middle" fill="var(--text-muted)" fontSize={10}>
            {agents[i]?.type || 'agent'}
          </text>
        </g>
      ))}
    </svg>
  )
}

export default function SwarmPanel() {
  const { swarm, setSwarm, agents } = useStore()
  const [topology, setTopology] = useState<Topology>('hierarchical')
  const [maxAgents, setMaxAgents] = useState(10)
  const [strategy, setStrategy] = useState<Strategy>('specialized')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [confirmShutdown, setConfirmShutdown] = useState(false)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const fetchStatus = useCallback(async () => {
    try {
      const data = await api.swarm.status() as SwarmState & { status: string }
      if (data.status === 'inactive' || data.status === 'shutdown') {
        setSwarm(null)
      } else {
        setSwarm(data)
      }
      setError(null)
    } catch {
      /* swarm may not be active */
    }
  }, [setSwarm])

  useEffect(() => {
    fetchStatus()
    intervalRef.current = setInterval(fetchStatus, 5000)
    return () => { if (intervalRef.current) clearInterval(intervalRef.current) }
  }, [fetchStatus])

  const handleInit = async () => {
    setLoading(true)
    setError(null)
    try {
      await api.swarm.init({ topology, maxAgents, strategy })
      await fetchStatus()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to initialize swarm')
    } finally {
      setLoading(false)
    }
  }

  const handleShutdown = async () => {
    setConfirmShutdown(false)
    setLoading(true)
    try {
      await api.swarm.shutdown()
      setSwarm(null)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to shutdown swarm')
    } finally {
      setLoading(false)
    }
  }

  const isActive = swarm && swarm.status !== 'shutdown'
  const swarmAgents = isActive ? (swarm.agents?.length ? swarm.agents : agents) : []

  if (!isActive) {
    return (
      <div style={styles.page}>
        <div style={styles.header}>
          <span style={styles.title}>Swarm Management</span>
        </div>

        <Card>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
            <h3 style={{ fontSize: 16, fontWeight: 600, color: 'var(--text-primary)' }}>Initialize New Swarm</h3>
            {error && (
              <div style={{ padding: '10px 14px', background: 'rgba(239,68,68,0.1)', border: '1px solid var(--accent-red)', borderRadius: 'var(--radius)', color: 'var(--accent-red)', fontSize: 13 }}>
                {error}
              </div>
            )}
            <div style={styles.form}>
              <div style={styles.field} data-tour="swarm-topology">
                <label style={styles.label}>Topology</label>
                <select style={styles.select} value={topology} onChange={e => setTopology(e.target.value as Topology)}>
                  {TOPOLOGIES.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div style={styles.field}>
                <label style={styles.label}>Max Agents</label>
                <input type="number" min={1} max={15} style={styles.input} value={maxAgents}
                  onChange={e => setMaxAgents(Math.min(15, Math.max(1, Number(e.target.value))))} />
              </div>
              <div style={styles.field}>
                <label style={styles.label}>Strategy</label>
                <select style={styles.select} value={strategy} onChange={e => setStrategy(e.target.value as Strategy)}>
                  {STRATEGIES.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>
              <div data-tour="swarm-init">
                <Button onClick={handleInit} disabled={loading}>
                  {loading ? 'Initializing...' : 'Initialize Swarm'}
                </Button>
              </div>
            </div>
          </div>
        </Card>

        <Card>
          <div style={{ ...styles.emptyState, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16, padding: 28 }}>
            <svg viewBox="0 0 120 120" width={100} height={100}>
              {[0, 1, 2, 3, 4].map(i => {
                const angle = (i / 5) * Math.PI * 2 - Math.PI / 2
                const x = 60 + Math.cos(angle) * 40
                const y = 60 + Math.sin(angle) * 40
                return (
                  <g key={i}>
                    <line x1={60} y1={60} x2={x} y2={y} stroke="var(--border)" strokeWidth={1} strokeDasharray="3 3" />
                    <circle cx={x} cy={y} r={10} fill="var(--bg-tertiary)" stroke="var(--border)" strokeWidth={1.5} />
                  </g>
                )
              })}
              <circle cx={60} cy={60} r={12} fill="var(--bg-card)" stroke="var(--accent-blue)" strokeWidth={2} opacity={0.5} />
            </svg>
            <p style={{ fontSize: 14 }}>No active swarm. Configure and initialize one above.</p>
          </div>
        </Card>
      </div>
    )
  }

  return (
    <div style={styles.page}>
      <div style={styles.header}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={styles.title}>Swarm Management</span>
          <span style={styles.pulseDot} />
        </div>
        <div style={styles.actions}>
          <Button variant="danger" onClick={() => setConfirmShutdown(true)} disabled={loading}>
            {loading ? 'Shutting down...' : 'Shutdown Swarm'}
          </Button>
        </div>
      </div>

      {error && (
        <div style={{ padding: '10px 14px', background: 'rgba(239,68,68,0.1)', border: '1px solid var(--accent-red)', borderRadius: 'var(--radius)', color: 'var(--accent-red)', fontSize: 13 }}>
          {error}
        </div>
      )}

      <div style={styles.infoRow}>
        <div style={styles.infoBadge}>ID <span style={styles.infoValue}>{(swarm.id ?? '').slice(0, 8) || '—'}</span></div>
        <div style={styles.infoBadge}>Topology <span style={styles.infoValue}>{swarm.topology}</span></div>
        <div style={styles.infoBadge}>Strategy <span style={styles.infoValue}>{swarm.strategy}</span></div>
        <div style={styles.infoBadge}>Status <StatusBadge status={swarm.status} /></div>
        <div style={styles.infoBadge}>Agents <span style={styles.infoValue}>{swarm.activeAgents}/{swarm.maxAgents}</span></div>
      </div>

      <Card>
        <h3 data-tour="swarm-topology-view" style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 12 }}>Topology View</h3>
        <div style={styles.svgContainer}>
          <TopologyGraph topology={swarm.topology} agents={swarmAgents} />
        </div>
      </Card>

      <Card>
        <h3 style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 12 }}>
          Active Agents ({swarmAgents.length})
        </h3>
        {swarmAgents.length === 0 ? (
          <p style={{ color: 'var(--text-muted)', fontSize: 13, padding: 16, textAlign: 'center' }}>No agents in swarm yet.</p>
        ) : (
          <div style={styles.agentGrid}>
            {swarmAgents.map(agent => (
              <div key={agent.id} style={styles.agentCard}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span style={styles.agentName}>{agent.name}</span>
                  <StatusBadge status={agent.status} />
                </div>
                <span style={styles.agentType}>{agent.type}</span>
                <span style={styles.agentMeta}>
                  Tasks: {agent.metrics?.tasksCompleted ?? 0}
                </span>
              </div>
            ))}
          </div>
        )}
      </Card>

      {/* Shutdown Confirmation Modal */}
      {confirmShutdown && (
        <div onClick={() => setConfirmShutdown(false)} style={{
          position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex',
          alignItems: 'center', justifyContent: 'center', zIndex: 9999,
        }}>
          <div onClick={e => e.stopPropagation()} style={{
            background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 'var(--radius-lg)',
            padding: 24, maxWidth: 400, width: '90%',
          }}>
            <h3 style={{ fontSize: 16, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 8 }}>Shutdown Swarm</h3>
            <p style={{ fontSize: 13, color: 'var(--text-secondary)', marginBottom: 20 }}>
              Are you sure? All agents will be terminated and the swarm will be destroyed.
            </p>
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
              <Button variant="secondary" onClick={() => setConfirmShutdown(false)}>Cancel</Button>
              <Button variant="danger" onClick={handleShutdown}>Shutdown</Button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

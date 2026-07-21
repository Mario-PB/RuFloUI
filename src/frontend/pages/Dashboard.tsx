import { useEffect, useCallback, useState, useRef, type CSSProperties } from 'react'
import { createPortal } from 'react-dom'
import { useStore } from '@/store'
import { api } from '@/api'
import { Card } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { StatusBadge } from '@/components/ui/StatusBadge'
import { Bot, ListTodo, Database, Zap, ShieldAlert } from 'lucide-react'
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import type { SystemHealth, PerformanceMetrics, HealthCheck } from '@/types'

const STATUS_COLORS: Record<string, string> = {
  healthy: 'var(--accent-green)',
  degraded: 'var(--accent-yellow)',
  unhealthy: 'var(--accent-red)',
}

function StatCard({
  label,
  value,
  icon: Icon,
  color,
  delay,
  subtitle,
}: {
  label: string
  value: string | number
  icon: React.ComponentType<{ size?: number; color?: string }>
  color: string
  delay: number
  subtitle?: React.ReactNode
}) {
  return (
    <Card>
      <div
        className="animate-fade-in"
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '20px 24px',
          minHeight: 88,
          animationDelay: `${delay}ms`,
        }}
      >
        <div>
          <div style={{ fontSize: 28, fontWeight: 700, color: 'var(--text-primary)', lineHeight: 1.2 }}>
            {value}
          </div>
          <div style={{ fontSize: 13, color: 'var(--text-secondary)', marginTop: 4 }}>{label}</div>
          {subtitle}
        </div>
        <div
          style={{
            width: 44,
            height: 44,
            borderRadius: 'var(--radius)',
            background: `${color}18`,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <Icon size={22} color={color} />
        </div>
      </div>
    </Card>
  )
}

const CHECK_ICONS: Record<string, string> = { pass: '✓', warn: '⚠', fail: '✗' }
const CHECK_COLORS: Record<string, string> = {
  pass: 'var(--accent-green)',
  warn: 'var(--accent-yellow)',
  fail: 'var(--accent-red)',
}

function HealthStatusCard({
  status, checks, passed, warnings, statusColor,
}: {
  status: string
  checks?: HealthCheck[]
  passed?: number
  warnings?: number
  statusColor: string
}) {
  const [showTooltip, setShowTooltip] = useState(false)
  const [tooltipPos, setTooltipPos] = useState({ top: 0, left: 0, width: 0 })
  const ref = useRef<HTMLDivElement>(null)

  const handleMouseEnter = () => {
    if (ref.current) {
      const rect = ref.current.getBoundingClientRect()
      setTooltipPos({ top: rect.bottom + 8, left: rect.left, width: rect.width })
    }
    setShowTooltip(true)
  }

  return (
    <Card>
      <div
        ref={ref}
        className="animate-fade-in"
        style={{ cursor: 'pointer' }}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={() => setShowTooltip(false)}
      >
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '20px 24px', minHeight: 88,
        }}>
          <div>
            <div style={{ fontSize: 28, fontWeight: 700, color: 'var(--text-primary)', lineHeight: 1.2 }}>
              {status}
            </div>
            <div style={{ fontSize: 13, color: 'var(--text-secondary)', marginTop: 4 }}>
              System Status
              {passed != null && (
                <span style={{ marginLeft: 8, fontSize: 11, color: 'var(--text-muted)' }}>
                  ({passed} passed, {warnings ?? 0} warnings)
                </span>
              )}
            </div>
          </div>
          <div style={{
            width: 44, height: 44, borderRadius: 'var(--radius)',
            background: `${statusColor}18`,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <div style={{
              width: 14, height: 14, borderRadius: '50%',
              background: statusColor, boxShadow: `0 0 10px ${statusColor}`,
            }} />
          </div>
        </div>
        <HealthTooltipPortal checks={checks ?? []} show={showTooltip} top={tooltipPos.top} left={tooltipPos.left} width={tooltipPos.width} />
      </div>
    </Card>
  )
}

function HealthTooltipPortal({
  checks, show, top, left, width,
}: {
  checks: HealthCheck[]
  show: boolean
  top: number
  left: number
  width: number
}) {
  if (!show || checks.length === 0) return null

  return createPortal(
    <div style={{
      position: 'fixed',
      top,
      left,
      width: Math.max(width, 380),
      padding: '12px 14px',
      background: 'var(--bg-secondary)',
      border: '1px solid var(--border)',
      borderRadius: 'var(--radius)',
      boxShadow: '0 8px 24px rgba(0,0,0,0.5)',
      zIndex: 9999,
      fontSize: 12,
      pointerEvents: 'none',
    }}>
      <div style={{ fontWeight: 600, color: 'var(--text-primary)', marginBottom: 8, fontSize: 13 }}>
        Health Checks
      </div>
      {checks.map((c, i) => (
        <div key={i} style={{
          display: 'flex', alignItems: 'flex-start', gap: 8,
          padding: '4px 0',
          borderBottom: i < checks.length - 1 ? '1px solid var(--border)' : 'none',
        }}>
          <span style={{ color: CHECK_COLORS[c.status], fontWeight: 700, flexShrink: 0, width: 14, textAlign: 'center' }}>
            {CHECK_ICONS[c.status]}
          </span>
          <span style={{ color: 'var(--text-secondary)', flexShrink: 0, minWidth: 130 }}>{c.name}</span>
          <span style={{ color: c.status === 'warn' ? 'var(--accent-yellow)' : c.status === 'fail' ? 'var(--accent-red)' : 'var(--text-muted)' }}>
            {c.detail}
          </span>
        </div>
      ))}
    </div>,
    document.body,
  )
}

export default function Dashboard() {
  const {
    systemHealth,
    agents,
    tasks,
    memoryEntries,
    swarm,
    performance,
    setSystemHealth,
    setPerformance,
    addLog,
  } = useStore()
  const [skipPermissions, setSkipPermissions] = useState<boolean | null>(null)

  const fetchInitialData = useCallback(async () => {
    api.config.getServerSettings().then(d => setSkipPermissions(d.skipPermissions)).catch(() => {})
    const results = await Promise.allSettled([
      api.system.health(),
      api.performance.metrics(),
    ])
    if (results[0].status === 'fulfilled') {
      setSystemHealth(results[0].value as SystemHealth)
      addLog({ level: 'info', message: 'System health loaded', source: 'dashboard' })
    } else {
      addLog({ level: 'warn', message: `Health check: ${results[0].reason?.message ?? 'unavailable'}`, source: 'dashboard' })
    }
    if (results[1].status === 'fulfilled') {
      setPerformance(results[1].value as PerformanceMetrics)
    }
  }, [setSystemHealth, setPerformance, addLog])

  useEffect(() => {
    fetchInitialData()
  }, [fetchInitialData])

  const runningTasks = tasks.filter((t) => t.status === 'in_progress').length
  const statusColor = STATUS_COLORS[systemHealth?.status ?? 'unhealthy'] ?? 'var(--text-muted)'

  const handleInitSwarm = async () => {
    try {
      await api.swarm.init({ topology: 'hierarchical', maxAgents: 10, strategy: 'specialized' })
      addLog({ level: 'info', message: 'Swarm initialized with 10-agent team', source: 'dashboard' })
    } catch (err) {
      addLog({ level: 'error', message: `Swarm init failed: ${(err as Error).message}`, source: 'dashboard' })
    }
  }

  const handleShutdownSwarm = async () => {
    try {
      await api.swarm.shutdown()
      addLog({ level: 'info', message: 'Swarm shut down', source: 'dashboard' })
    } catch (err) {
      addLog({ level: 'error', message: `Swarm shutdown failed: ${(err as Error).message}`, source: 'dashboard' })
    }
  }

  const handleBenchmark = async () => {
    try {
      await api.performance.benchmark()
      addLog({ level: 'info', message: 'Benchmark started', source: 'dashboard' })
    } catch (err) {
      addLog({ level: 'error', message: `Benchmark failed: ${(err as Error).message}`, source: 'dashboard' })
    }
  }

  const handleHealthCheck = async () => {
    try {
      const h = (await api.system.health()) as SystemHealth
      setSystemHealth(h)
      addLog({ level: 'info', message: 'Health check completed', source: 'dashboard' })
    } catch (err) {
      addLog({ level: 'error', message: `Health check failed: ${(err as Error).message}`, source: 'dashboard' })
    }
  }

  const handleMemoryStats = async () => {
    try {
      await api.memory.stats()
      addLog({ level: 'info', message: 'Memory stats fetched', source: 'dashboard' })
    } catch (err) {
      addLog({ level: 'error', message: `Memory stats failed: ${(err as Error).message}`, source: 'dashboard' })
    }
  }

  const chartData = performance?.history ?? []
  const agentProgress = swarm ? (swarm.activeAgents / swarm.maxAgents) * 100 : 0

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, padding: 0 }}>
      {/* Top Stats Row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 16 }}>
        <HealthStatusCard
          status={systemHealth?.status ?? 'unknown'}
          checks={systemHealth?.checks}
          passed={systemHealth?.passed}
          warnings={systemHealth?.warnings}
          statusColor={statusColor}
        />
        <StatCard
          label="Active Agents"
          value={agents.length}
          icon={Bot}
          color="var(--accent-cyan)"
          delay={50}
          subtitle={skipPermissions !== null ? (
            <div
              data-tour="stat-agents"
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 4,
                marginTop: 6,
                padding: '3px 8px',
                borderRadius: 4,
                background: skipPermissions ? 'rgba(239, 68, 68, 0.12)' : 'rgba(34, 197, 94, 0.12)',
                fontSize: 11,
                color: skipPermissions ? 'var(--accent-yellow)' : 'var(--accent-green)',
                lineHeight: 1.3,
              }}
              title={skipPermissions
                ? 'Agents run with --dangerously-skip-permissions. Change in Config > Server Settings.'
                : 'Agents require manual approval for each tool use.'}
            >
              <ShieldAlert size={12} style={{ flexShrink: 0 }} />
              <span>{skipPermissions ? 'Auto-permissions ON' : 'Permissions secured'}</span>
            </div>
          ) : undefined}
        />
        <StatCard label="Running Tasks" value={runningTasks} icon={ListTodo} color="var(--accent-purple)" delay={100} />
        <StatCard
          label="Memory Entries"
          value={memoryEntries.length}
          icon={Database}
          color="var(--accent-orange)"
          delay={150}
        />
      </div>

      {/* Swarm Overview */}
      <div>
        <Card>
          <div className="animate-fade-in" style={{ padding: '20px 24px', animationDelay: '200ms' }}>
            <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 16, color: 'var(--text-primary)' }}>
              Swarm Overview
            </div>
            {swarm ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <StatusBadge status={swarm.status} />
                  <span style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
                    Topology: <strong style={{ color: 'var(--text-primary)' }}>{swarm.topology}</strong>
                  </span>
                  <span style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
                    Strategy: <strong style={{ color: 'var(--text-primary)' }}>{swarm.strategy}</strong>
                  </span>
                </div>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>
                    <span>Agents</span>
                    <span>{swarm.activeAgents} / {swarm.maxAgents}</span>
                  </div>
                  <div
                    style={{
                      height: 6,
                      background: 'var(--bg-tertiary)',
                      borderRadius: 3,
                      overflow: 'hidden',
                    }}
                  >
                    <div
                      style={{
                        width: `${agentProgress}%`,
                        height: '100%',
                        background: 'linear-gradient(90deg, var(--accent-blue), var(--accent-cyan))',
                        borderRadius: 3,
                        transition: 'width 0.3s ease',
                      }}
                    />
                  </div>
                </div>
                <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
                  <Button onClick={handleInitSwarm}>Init Swarm</Button>
                  <Button onClick={handleShutdownSwarm}>Shutdown</Button>
                </div>
              </div>
            ) : (
              <div style={{ textAlign: 'center', padding: '20px 0' }}>
                <div style={{ color: 'var(--text-muted)', fontSize: 13, marginBottom: 12 }}>
                  No active swarm
                </div>
                <Button onClick={handleInitSwarm}>Initialize Swarm</Button>
              </div>
            )}
          </div>
        </Card>

      </div>

      {/* Bottom row: System Health Chart + Quick Actions */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 16 }}>
        {/* System Health Chart */}
        <Card>
          <div className="animate-fade-in" style={{ padding: '20px 24px', animationDelay: '300ms' }}>
            <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 16, color: 'var(--text-primary)' }}>
              System Health
            </div>
            {chartData.length > 0 ? (
              <ResponsiveContainer width="100%" height={200}>
                <AreaChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
                  <defs>
                    <linearGradient id="gradLatency" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--accent-blue)" stopOpacity={0.4} />
                      <stop offset="95%" stopColor="var(--accent-blue)" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="gradThroughput" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--accent-cyan)" stopOpacity={0.4} />
                      <stop offset="95%" stopColor="var(--accent-cyan)" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <XAxis
                    dataKey="timestamp"
                    tick={{ fill: 'var(--text-muted)', fontSize: 11 }}
                    tickLine={false}
                    axisLine={{ stroke: 'var(--border)' }}
                    tickFormatter={(v: string) => new Date(v).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  />
                  <YAxis
                    tick={{ fill: 'var(--text-muted)', fontSize: 11 }}
                    tickLine={false}
                    axisLine={{ stroke: 'var(--border)' }}
                  />
                  <Tooltip
                    contentStyle={{
                      background: 'var(--bg-secondary)',
                      border: '1px solid var(--border)',
                      borderRadius: 'var(--radius)',
                      color: 'var(--text-primary)',
                      fontSize: 12,
                    }}
                  />
                  <Area
                    type="monotone"
                    dataKey="latency"
                    stroke="var(--accent-blue)"
                    fill="url(#gradLatency)"
                    strokeWidth={2}
                  />
                  <Area
                    type="monotone"
                    dataKey="throughput"
                    stroke="var(--accent-cyan)"
                    fill="url(#gradThroughput)"
                    strokeWidth={2}
                  />
                </AreaChart>
              </ResponsiveContainer>
            ) : (
              <div
                style={{
                  height: 200,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: 'var(--text-muted)',
                  fontSize: 13,
                }}
              >
                No performance data available
              </div>
            )}
          </div>
        </Card>

        {/* Quick Actions Panel */}
        <Card>
          <div className="animate-fade-in" style={{ padding: '20px 24px', animationDelay: '350ms' }}>
            <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 16, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 8 }}>
              <Zap size={16} color="var(--accent-yellow)" />
              Quick Actions
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <Button onClick={handleInitSwarm}>Init Swarm</Button>
              <Button onClick={handleBenchmark}>Run Benchmark</Button>
              <Button onClick={handleHealthCheck}>System Health</Button>
              <Button onClick={handleMemoryStats}>Memory Stats</Button>
            </div>
          </div>
        </Card>
      </div>
    </div>
  )
}

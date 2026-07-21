import { useEffect, useState, useCallback, useRef, type CSSProperties } from 'react'
import { useStore } from '@/store'
import { api } from '@/api'
import { Card } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { StatusBadge } from '@/components/ui/StatusBadge'
import type { WorkflowDef } from '@/types'

function getProgressFill(pct: number): CSSProperties {
  return {
    height: '100%', width: `${pct}%`, background: 'var(--accent-green)',
    borderRadius: 3, transition: 'width 0.3s ease',
  }
}

interface WorkflowTemplate {
  id: string
  name: string
  description: string
  steps: Array<{ name: string; agent?: string }>
}

const s: Record<string, CSSProperties> = {
  page: { display: 'flex', flexDirection: 'column', gap: 20 },
  table: { width: '100%', borderCollapse: 'collapse' },
  th: {
    textAlign: 'left', padding: '10px 16px', fontSize: 12, fontWeight: 600,
    color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.04em',
    borderBottom: '1px solid var(--border)',
  },
  td: {
    padding: '12px 16px', fontSize: 13, color: 'var(--text-secondary)',
    borderBottom: '1px solid var(--border)',
  },
  trHover: { transition: 'background var(--transition)', cursor: 'pointer' },
  actions: { display: 'flex', gap: 6, flexWrap: 'wrap' },
  formGrid: { display: 'grid', gridTemplateColumns: '1fr', gap: 12 },
  label: { fontSize: 12, fontWeight: 500, color: 'var(--text-secondary)', marginBottom: 4, display: 'block' },
  input: {
    width: '100%', padding: '8px 12px', background: 'var(--bg-tertiary)',
    border: '1px solid var(--border)', borderRadius: 'var(--radius)', color: 'var(--text-primary)',
    fontSize: 13, outline: 'none',
  },
  stepsBuilder: { display: 'flex', flexDirection: 'column', gap: 8 },
  stepRow: {
    display: 'flex', alignItems: 'center', gap: 10, padding: '8px 12px',
    background: 'var(--bg-secondary)', borderRadius: 'var(--radius)', border: '1px solid var(--border)',
  },
  stepNum: {
    width: 24, height: 24, borderRadius: '50%', background: 'var(--accent-blue)',
    color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center',
    fontSize: 12, fontWeight: 600, flexShrink: 0,
  },
  stepArrow: {
    textAlign: 'center', color: 'var(--text-muted)', fontSize: 18, lineHeight: 1, padding: '2px 0',
  },
  stepInput: {
    flex: 1, padding: '6px 10px', background: 'var(--bg-tertiary)',
    border: '1px solid var(--border)', borderRadius: 'var(--radius)', color: 'var(--text-primary)',
    fontSize: 13, outline: 'none',
  },
  stepSelect: {
    width: 160, padding: '6px 10px', background: 'var(--bg-tertiary)',
    border: '1px solid var(--border)', borderRadius: 'var(--radius)', color: 'var(--text-primary)',
    fontSize: 12, outline: 'none',
  },
  removeBtn: {
    background: 'transparent', border: 'none', color: 'var(--accent-red)',
    cursor: 'pointer', fontSize: 16, padding: '0 4px', lineHeight: 1,
  },
  pipeline: {
    display: 'flex', alignItems: 'center', gap: 0, flexWrap: 'wrap', padding: '12px 0',
  },
  pipeNode: {
    padding: '10px 16px', borderRadius: 'var(--radius)', border: '1px solid var(--border)',
    background: 'var(--bg-secondary)', minWidth: 120, textAlign: 'center',
  },
  pipeNodeName: { fontSize: 13, fontWeight: 500, color: 'var(--text-primary)', marginBottom: 4 },
  pipeNodeAgent: { fontSize: 11, color: 'var(--text-muted)' },
  pipeArrow: { color: 'var(--text-muted)', fontSize: 20, padding: '0 8px', flexShrink: 0 },
  progressBar: {
    height: 6, background: 'var(--bg-tertiary)', borderRadius: 3,
    overflow: 'hidden', marginTop: 12,
  },
  progressFill: {} as CSSProperties, // use getProgressFill() instead
  templateGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: 12 },
  templateCard: {
    padding: 16, background: 'var(--bg-secondary)', borderRadius: 'var(--radius)',
    border: '1px solid var(--border)', display: 'flex', flexDirection: 'column', gap: 8,
  },
  templateName: { fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' },
  templateDesc: { fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.5, flex: 1 },
  empty: { textAlign: 'center', padding: 32, color: 'var(--text-muted)', fontSize: 13 },
  detailOverlay: {
    marginTop: 12, padding: 16, background: 'var(--bg-secondary)',
    borderRadius: 'var(--radius-lg)', border: '1px solid var(--border)',
  },
  formActions: { display: 'flex', gap: 8, marginTop: 8 },
  chainPreview: {
    display: 'flex', alignItems: 'center', gap: 4, padding: '8px 0',
    fontSize: 12, color: 'var(--text-muted)', flexWrap: 'wrap',
  },
  chainStep: {
    padding: '4px 10px', background: 'var(--bg-card)', borderRadius: 'var(--radius)',
    border: '1px solid var(--border)', color: 'var(--text-secondary)', fontSize: 12,
  },
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

function WorkflowDetail({ workflow, onClose }: { workflow: WorkflowDef; onClose: () => void }) {
  const [selectedStep, setSelectedStep] = useState<string | null>(null)
  const completedSteps = workflow.steps.filter((st) => st.status === 'completed').length
  const pct = workflow.steps.length > 0 ? (completedSteps / workflow.steps.length) * 100 : 0

  return (
    <div style={s.detailOverlay}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
        <span style={{ fontSize: 15, fontWeight: 600, color: 'var(--text-primary)' }}>{workflow.name}</span>
        <Button size="sm" variant="ghost" onClick={onClose}>Close</Button>
      </div>
      <div style={s.pipeline}>
        {workflow.steps.map((step, i) => (
          <div key={step.id || i} style={{ display: 'flex', alignItems: 'center' }}>
            {i > 0 && <span style={s.pipeArrow}>&#8594;</span>}
            <div
              onClick={() => setSelectedStep(selectedStep === step.id ? null : step.id)}
              style={{
                ...s.pipeNode,
                cursor: 'pointer',
                borderColor: selectedStep === step.id ? 'var(--accent-cyan)'
                  : step.status === 'completed' ? 'var(--accent-green)'
                  : step.status === 'running' ? 'var(--accent-blue)' : 'var(--border)',
                transform: selectedStep === step.id ? 'scale(1.05)' : undefined,
                transition: 'all 0.15s ease',
              }}
            >
              <div style={s.pipeNodeName}>{step.name}</div>
              {step.agent && <div style={s.pipeNodeAgent}>{step.agent}</div>}
              <div style={{ marginTop: 6 }}><StatusBadge status={step.status} size="sm" /></div>
            </div>
          </div>
        ))}
      </div>
      {/* Step detail panel */}
      {selectedStep && (() => {
        const step = workflow.steps.find(st => st.id === selectedStep)
        if (!step) return null
        const stepIdx = workflow.steps.indexOf(step)
        return (
          <div style={{
            marginTop: 12, padding: 12, background: 'var(--bg-primary)', borderRadius: 'var(--radius)',
            border: '1px solid var(--accent-cyan)', fontSize: 13,
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
              <span style={{ fontWeight: 600, color: 'var(--text-primary)' }}>
                Step {stepIdx + 1}: {step.name}
              </span>
              <StatusBadge status={step.status} size="sm" />
            </div>
            {step.agent && (
              <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>
                <span style={{ fontWeight: 600, color: 'var(--text-muted)' }}>Agent: </span>{step.agent}
              </div>
            )}
            {step.detail && (
              <div style={{
                marginTop: 6, padding: 8, background: 'var(--bg-tertiary)', borderRadius: 'var(--radius)',
                fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)',
                wordBreak: 'break-all',
              }}>
                {step.detail}
              </div>
            )}
            {!step.detail && (
              <div style={{ fontSize: 12, color: 'var(--text-muted)', fontStyle: 'italic' }}>
                No additional details available
              </div>
            )}
          </div>
        )
      })()}
      <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 8 }}>
        {completedSteps} of {workflow.steps.length} steps completed
      </div>
      <div style={s.progressBar}>
        <div style={getProgressFill(pct)} />
      </div>
    </div>
  )
}

export default function WorkflowsPanel() {
  const workflows = useStore((st) => st.workflows)
  const setWorkflows = useStore((st) => st.setWorkflows)
  const agents = useStore((st) => st.agents)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [templates, setTemplates] = useState<WorkflowTemplate[]>([])
  const [showCreate, setShowCreate] = useState(false)
  const [wfName, setWfName] = useState('')
  const [steps, setSteps] = useState<Array<{ name: string; agent: string }>>([{ name: '', agent: '' }])
  const [creating, setCreating] = useState(false)
  const [loadingAction, setLoadingAction] = useState('')
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined)

  const fetchData = useCallback(async () => {
    try {
      const [wfRes, tplRes] = await Promise.all([
        api.workflows.list(),
        api.workflows.templates(),
      ])
      const wfData = wfRes as { workflows?: Partial<WorkflowDef>[] } | Partial<WorkflowDef>[]
      const rawList = Array.isArray(wfData) ? wfData : (wfData.workflows ?? [])
      setWorkflows(rawList.map(w => ({
        id: w.id || '',
        name: w.name || (w as Record<string, string>).template || 'Unnamed',
        status: (w.status as WorkflowDef['status']) || 'draft',
        steps: (w.steps ?? []).map(st => ({ id: st.id || '', name: st.name || '', status: st.status || 'pending', agent: st.agent, detail: (st as Record<string, unknown>).detail as string | undefined })),
        createdAt: w.createdAt || new Date().toISOString(),
      })))
      const tplData = tplRes as { templates?: WorkflowTemplate[] } | WorkflowTemplate[]
      setTemplates(Array.isArray(tplData) ? tplData : (tplData.templates ?? []))
    } catch { /* silent */ }
  }, [setWorkflows])

  useEffect(() => {
    fetchData()
    intervalRef.current = setInterval(fetchData, 3000)
    return () => clearInterval(intervalRef.current)
  }, [fetchData])

  // WebSocket for live workflow updates
  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws`)
    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data)
        if (msg.type === 'workflow:updated' && msg.payload) {
          const updated = msg.payload
          const current = useStore.getState().workflows || []
          setWorkflows(current.map(w => w.id === updated.id ? {
            ...w,
            status: updated.status || w.status,
            steps: (updated.steps || []).map((st: { id?: string; name: string; status: string; agent?: string; detail?: string }) => ({
              id: st.id || '', name: st.name, status: st.status, agent: st.agent, detail: st.detail,
            })),
          } : w))
        } else if (msg.type === 'workflow:added' && msg.payload) {
          fetchData()
        }
      } catch { /* ignore */ }
    }
    return () => ws.close()
  }, [fetchData, setWorkflows])

  const handleAction = async (id: string, action: string) => {
    setLoadingAction(`${id}-${action}`)
    try {
      switch (action) {
        case 'execute': await api.workflows.execute(id); break
        case 'pause': await api.workflows.pause(id); break
        case 'resume': await api.workflows.resume(id); break
        case 'cancel': await api.workflows.cancel(id); break
        case 'delete': await api.workflows.delete(id); setSelectedId(null); break
      }
      fetchData()
    } catch { /* silent */ }
    setLoadingAction('')
  }

  const addStep = () => setSteps([...steps, { name: '', agent: '' }])
  const removeStep = (i: number) => setSteps(steps.filter((_, idx) => idx !== i))
  const updateStep = (i: number, field: 'name' | 'agent', value: string) => {
    const next = [...steps]
    next[i] = { ...next[i], [field]: value }
    setSteps(next)
  }

  const handleCreate = async () => {
    if (!wfName.trim() || steps.every((st) => !st.name.trim())) return
    setCreating(true)
    try {
      await api.workflows.create({
        name: wfName.trim(),
        steps: steps.filter((st) => st.name.trim()).map((st) => ({
          name: st.name.trim(), ...(st.agent ? { agent: st.agent } : {}),
        })),
      })
      setWfName('')
      setSteps([{ name: '', agent: '' }])
      setShowCreate(false)
      fetchData()
    } catch { /* silent */ }
    setCreating(false)
  }

  const useTemplate = (tpl: WorkflowTemplate) => {
    setWfName(tpl.name)
    setSteps(tpl.steps.map((st) => ({ name: st.name, agent: st.agent ?? '' })))
    setShowCreate(true)
  }

  const selectedWorkflow = workflows.find((w) => w.id === selectedId)

  return (
    <div style={s.page}>
      <Card
        title="Workflows"
        actions={<Button size="sm" onClick={() => setShowCreate(!showCreate)}>
          {showCreate ? 'Cancel' : 'New Workflow'}
        </Button>}
      >
        {showCreate && (
          <div style={{ ...s.formGrid, marginBottom: 20, paddingBottom: 20, borderBottom: '1px solid var(--border)' }}>
            <div>
              <label style={s.label}>Workflow Name</label>
              <input style={s.input} placeholder="My workflow..." value={wfName}
                onChange={(e) => setWfName(e.target.value)} />
            </div>
            <div>
              <label style={s.label}>Steps</label>
              <div style={s.stepsBuilder}>
                {steps.map((step, i) => (
                  <div key={i}>
                    {i > 0 && <div style={s.stepArrow}>&#8595;</div>}
                    <div style={s.stepRow}>
                      <span style={s.stepNum}>{i + 1}</span>
                      <input style={s.stepInput} placeholder="Step name..." value={step.name}
                        onChange={(e) => updateStep(i, 'name', e.target.value)} />
                      <select style={s.stepSelect} value={step.agent}
                        onChange={(e) => updateStep(i, 'agent', e.target.value)}>
                        <option value="">No agent</option>
                        {agents.map((a) => <option key={a.id} value={a.id}>{a.name}</option>)}
                      </select>
                      {steps.length > 1 && (
                        <button style={s.removeBtn} onClick={() => removeStep(i)} title="Remove step">x</button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
              <div style={s.formActions}>
                <Button size="sm" variant="secondary" onClick={addStep}>Add Step</Button>
                <Button size="sm" onClick={handleCreate} loading={creating}
                  disabled={!wfName.trim() || steps.every((st) => !st.name.trim())}>Create Workflow</Button>
              </div>
            </div>
            {steps.some((st) => st.name.trim()) && (
              <div>
                <label style={s.label}>Preview</label>
                <div style={s.chainPreview}>
                  {steps.filter((st) => st.name.trim()).map((st, i, arr) => (
                    <span key={i} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                      <span style={s.chainStep}>{st.name}</span>
                      {i < arr.length - 1 && <span style={{ color: 'var(--text-muted)' }}>&#8594;</span>}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {workflows.length === 0 ? (
          <div style={s.empty}>No workflows found</div>
        ) : (
          <table style={s.table}>
            <thead>
              <tr>
                <th style={s.th}>Name</th>
                <th style={s.th}>Status</th>
                <th style={s.th}>Steps</th>
                <th style={s.th}>Created</th>
                <th style={s.th}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {workflows.map((wf) => (
                <tr
                  key={wf.id}
                  style={{ ...s.trHover, background: selectedId === wf.id ? 'var(--bg-hover)' : undefined }}
                  onClick={() => setSelectedId(selectedId === wf.id ? null : wf.id)}
                  onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-hover)' }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.background = selectedId === wf.id ? 'var(--bg-hover)' : ''
                  }}
                >
                  <td style={{ ...s.td, color: 'var(--text-primary)', fontWeight: 500 }}>{wf.name}</td>
                  <td style={s.td}><StatusBadge status={wf.status} /></td>
                  <td style={s.td}>{wf.steps.length}</td>
                  <td style={s.td}>{formatDate(wf.createdAt)}</td>
                  <td style={s.td}>
                    <div style={s.actions} onClick={(e) => e.stopPropagation()}>
                      {wf.status === 'draft' && (
                        <Button size="sm" onClick={() => handleAction(wf.id, 'execute')}
                          loading={loadingAction === `${wf.id}-execute`}>Execute</Button>
                      )}
                      {wf.status === 'running' && (
                        <Button size="sm" variant="secondary" onClick={() => handleAction(wf.id, 'pause')}
                          loading={loadingAction === `${wf.id}-pause`}>Pause</Button>
                      )}
                      {wf.status === 'paused' && (
                        <Button size="sm" onClick={() => handleAction(wf.id, 'resume')}
                          loading={loadingAction === `${wf.id}-resume`}>Resume</Button>
                      )}
                      {(wf.status === 'running' || wf.status === 'paused') && (
                        <Button size="sm" variant="danger" onClick={() => handleAction(wf.id, 'cancel')}
                          loading={loadingAction === `${wf.id}-cancel`}>Cancel</Button>
                      )}
                      <Button size="sm" variant="danger" onClick={() => handleAction(wf.id, 'delete')}
                        loading={loadingAction === `${wf.id}-delete`}>Delete</Button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        {selectedWorkflow && (
          <WorkflowDetail workflow={selectedWorkflow} onClose={() => setSelectedId(null)} />
        )}
      </Card>

      {templates.length > 0 && (
        <Card title="Templates">
          <div style={s.templateGrid}>
            {templates.map((tpl) => (
              <div key={tpl.id} style={s.templateCard}>
                <div style={s.templateName}>{tpl.name}</div>
                <div style={s.templateDesc}>{tpl.description}</div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                  {tpl.steps.length} step{tpl.steps.length !== 1 ? 's' : ''}
                </div>
                <Button size="sm" variant="secondary" onClick={() => useTemplate(tpl)}>
                  Use Template
                </Button>
              </div>
            ))}
          </div>
        </Card>
      )}
    </div>
  )
}

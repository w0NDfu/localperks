import { tokenStorage } from '@/utils/token'

export interface AgentEvent {
  type: string
  agent?: string
  model?: string
  step?: number
  tool?: string
  task?: string
  answer?: string
  session_id?: string
  message?: string
  code?: string
  result?: unknown
  requests?: { method: string; path: string; status: number | null }[]
  trace?: AgentEvent[]
  task_id?: string
  state?: string
  intent?: string
  node_id?: string
  strategy?: string
  duration_ms?: number
  status?: string
  order_id?: string
  action?: AgentAction
  constraints?: AgentConstraints
  missing_fields?: string[]
  metrics?: { model_calls: number; tool_calls: number; total_tokens: number; duration_ms: number }
}
export interface AgentConstraints {
  location?: { longitude: number; latitude: number }
  budget_per_person?: number
  people?: number
  visit_time?: string
  radius_m?: number
  cuisine_preferences?: string[]
  excluded_categories?: string[]
  answer_style?: 'concise' | 'detailed' | 'neutral'
}
export interface AgentPreferences {
  budget_per_person?: number | null
  radius_m?: number | null
  cuisine_preferences?: string[]
  excluded_categories?: string[]
  answer_style?: 'concise' | 'detailed' | 'neutral'
}
export interface AgentProfile { preferences: AgentPreferences; revision: number; updated: number | null; source: string }
export interface AgentAction { action_id: string; operation: string; state: string; preview: Record<string, unknown>; confirmation_token?: string; result?: unknown }
export interface AgentTurn { user: string; assistant: string; trace: AgentEvent[]; action?: AgentAction; action_id?: string; task_id?: string; metrics?: AgentEvent['metrics'] }
export interface AgentStatus {
  configured: boolean; model: string; redis: boolean; localperks: boolean; ready: boolean
  async_tasks?: boolean
}
export interface AgentTask { id: string; session_id: string; state: string; request: { message: string }; output?: AgentEvent & { error?: { message: string } } }

function headers(): Record<string, string> {
  const token = tokenStorage.get()
  return { 'Content-Type': 'application/json', ...(token ? { authorization: token } : {}) }
}

async function checked(response: Response) {
  if (response.ok) return response
  const error = await response.json().catch(() => ({}))
  throw new Error(error.detail?.message || `服务请求失败（${response.status}），请确认 Agent 服务已启动`)
}

async function taskFetch(url: string, signal: AbortSignal) {
  const controller = new AbortController()
  const abort = () => controller.abort()
  if (signal.aborted) controller.abort()
  else signal.addEventListener('abort', abort, { once: true })
  const timeout = setTimeout(abort, 8000)
  try { return await checked(await fetch(url, { headers: headers(), signal: controller.signal })) }
  finally { clearTimeout(timeout); signal.removeEventListener('abort', abort) }
}

export const agentApi = {
  async preferences(): Promise<AgentProfile> {
    return (await checked(await fetch('/agent-api/agent/preferences', { headers: headers(), signal: AbortSignal.timeout(8000) }))).json()
  },
  async savePreferences(preferences: AgentPreferences, revision: number): Promise<AgentProfile> {
    return (await checked(await fetch('/agent-api/agent/preferences', {
      method: 'PUT', headers: headers(), signal: AbortSignal.timeout(8000), body: JSON.stringify({ preferences, revision }),
    }))).json()
  },
  async clearPreferences(revision: number): Promise<AgentProfile> {
    return (await checked(await fetch(`/agent-api/agent/preferences?revision=${revision}`, {
      method: 'DELETE', headers: headers(), signal: AbortSignal.timeout(8000),
    }))).json()
  },
  async status(): Promise<AgentStatus> {
    return (await checked(await fetch('/agent-api/agent/status', { signal: AbortSignal.timeout(8000) }))).json()
  },
  async history(session: string): Promise<{ turns: AgentTurn[] }> {
    return (await checked(await fetch(`/agent-api/agent/conversations/${encodeURIComponent(session)}`, {
      headers: headers(), signal: AbortSignal.timeout(8000),
    }))).json()
  },
  async task(id: string, signal?: AbortSignal): Promise<AgentTask> {
    if (signal) return (await taskFetch(`/agent-api/agent/tasks/${encodeURIComponent(id)}`, signal)).json()
    return (await checked(await fetch(`/agent-api/agent/tasks/${encodeURIComponent(id)}`, { headers: headers(), signal: AbortSignal.timeout(8000) }))).json()
  },
  async submit(message: string, requestId: string, session?: string, constraints?: AgentConstraints): Promise<{ task_id: string; session_id: string; state: string }> {
    return (await checked(await fetch('/agent-api/agent/tasks', {
      method: 'POST', headers: headers(), signal: AbortSignal.timeout(20000),
      body: JSON.stringify({ message, request_id: requestId, ...(session ? { session_id: session } : {}), ...(constraints ? { constraints } : {}) }),
    }))).json()
  },
  async followTask(id: string, signal: AbortSignal, onEvent: (event: AgentEvent) => void) {
    let after = 0
    const complete = async (task: Pick<AgentTask, 'state' | 'output'>) => {
      if (task.output?.error || !task.output?.answer) throw new Error(task.output?.error?.message || '任务中断，请重新提交原请求恢复')
      const output = { ...task.output }
      if (output.action?.action_id) output.action = await agentApi.action(output.action.action_id)
      if (signal.aborted) throw new DOMException('Aborted', 'AbortError')
      onEvent({ ...output, type: 'done', state: task.state })
    }
    if (signal.aborted) throw new DOMException('Aborted', 'AbortError')
    // WebSocket is a notification channel; HTTP remains the source of truth after disconnect.
    const snapshot = typeof WebSocket === 'undefined' ? undefined : await new Promise<Pick<AgentTask, 'state' | 'output'> | undefined>((resolve, reject) => {
      const url = new URL(`/agent-api/agent/tasks/${encodeURIComponent(id)}/ws`, location.href)
      url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
      let socket: WebSocket
      try { socket = new WebSocket(url) } catch { resolve(undefined); return }
      let timeout = setTimeout(() => finish(), 3000)
      const abort = () => { cleanup(); socket.close(); reject(new DOMException('Aborted', 'AbortError')) }
      const cleanup = () => { clearTimeout(timeout); signal.removeEventListener('abort', abort); socket.onclose = null; socket.onerror = null; socket.onmessage = null; socket.onopen = null }
      const finish = (result?: Pick<AgentTask, 'state' | 'output'>) => { cleanup(); socket.close(); resolve(result) }
      signal.addEventListener('abort', abort, { once: true })
      socket.onopen = () => { clearTimeout(timeout); timeout = setTimeout(() => finish(), 30000); socket.send(JSON.stringify({ authorization: headers().authorization || '', after })) }
      socket.onerror = () => finish()
      socket.onclose = () => finish()
      socket.onmessage = message => {
        clearTimeout(timeout); timeout = setTimeout(() => finish(), 30000)
        try {
          const event = JSON.parse(message.data) as AgentEvent & { seq?: number; output?: AgentTask['output'] }
          if (event.type === 'snapshot') finish({ state: event.state || 'failed', output: event.output })
          else if (event.type === 'error') finish()
          else { after = Math.max(after, event.seq || 0); onEvent(event) }
        } catch { finish() }
      }
    })
    if (snapshot) { await complete(snapshot); return }
    while (!signal.aborted) {
      const response = await taskFetch(`/agent-api/agent/tasks/${encodeURIComponent(id)}/events?after=${after}`, signal)
      const { events } = await response.json() as { events: (AgentEvent & { seq: number })[] }
      for (const event of events) { after = event.seq; onEvent(event) }
      const task = await agentApi.task(id, signal)
      if (!['accepted', 'running'].includes(task.state)) { await complete(task); return }
      await new Promise<void>((resolve, reject) => {
        const abort = () => { clearTimeout(timeout); reject(new DOMException('Aborted', 'AbortError')) }
        const timeout = setTimeout(() => { signal.removeEventListener('abort', abort); resolve() }, 500)
        signal.addEventListener('abort', abort, { once: true })
      })
    }
    throw new DOMException('Aborted', 'AbortError')
  },
  async confirm(action: AgentAction): Promise<AgentAction> {
    return (await checked(await fetch(`/agent-api/agent/actions/${action.action_id}/confirm`, {
      method: 'POST', headers: headers(), body: JSON.stringify({ confirmed: true, confirmation_token: action.confirmation_token }), signal: AbortSignal.timeout(15000),
    }))).json()
  },
  async action(id: string): Promise<AgentAction> {
    return (await checked(await fetch(`/agent-api/agent/actions/${encodeURIComponent(id)}`, { headers: headers(), signal: AbortSignal.timeout(8000) }))).json()
  },
  async reconcile(action: AgentAction): Promise<AgentAction> {
    return (await checked(await fetch(`/agent-api/agent/actions/${action.action_id}/reconcile`, { method: 'POST', headers: headers(), signal: AbortSignal.timeout(15000) }))).json()
  },
  async chat(message: string, session: string | undefined, signal: AbortSignal, onEvent: (event: AgentEvent) => void, options?: { task_id?: string; constraints?: AgentConstraints }) {
    const response = await checked(await fetch('/agent-api/agent/chat/stream', {
      method: 'POST', headers: headers(), signal,
      body: JSON.stringify({ message, ...(session ? { session_id: session } : {}), ...options }),
    }))
    if (!response.body) throw new Error('浏览器无法读取实时响应')
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    let completed = false
    try {
      while (true) {
        const { done, value } = await reader.read()
        buffer += decoder.decode(value, { stream: !done }).replace(/\r\n/g, '\n')
        let boundary: number
        while ((boundary = buffer.indexOf('\n\n')) >= 0) {
          const frame = buffer.slice(0, boundary)
          buffer = buffer.slice(boundary + 2)
          const data = frame.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5).trimStart()).join('\n')
          if (!data) continue
          const event = JSON.parse(data) as AgentEvent
          if (event.type === 'error') throw new Error(event.message || 'Agent 执行失败')
          onEvent(event)
          if (event.type === 'done') completed = true
        }
        if (done) break
      }
      if (!completed) throw new Error('连接中断，本轮未完成，请重试')
    } finally { await reader.cancel().catch(() => undefined); reader.releaseLock() }
  },
}

import { afterEach, describe, expect, it, vi } from 'vitest'
import { agentApi } from './agent'

afterEach(() => { vi.unstubAllGlobals(); localStorage.clear() })

function response(chunks: string[]) {
  const encoded = new TextEncoder().encode(chunks.join(''))
  return new Response(new ReadableStream({
    start(controller) {
      // Split inside UTF-8 characters and SSE frames to exercise real network boundaries.
      for (let i = 0; i < encoded.length; i += 7) controller.enqueue(encoded.slice(i, i + 7))
      controller.close()
    },
  }))
}

describe('Agent streaming API', () => {
  it('saves and clears owned preferences using revision checks and the login header', async () => {
    localStorage.setItem('hmdp-token', 'java-token')
    const fetch = vi.fn().mockImplementation(async () => Response.json({ preferences: {}, revision: 4 }))
    vi.stubGlobal('fetch', fetch)
    await agentApi.savePreferences({ answer_style: 'concise', excluded_categories: ['火锅'] }, 3)
    expect(JSON.parse(fetch.mock.calls[0]?.[1].body)).toEqual({ preferences: { answer_style: 'concise', excluded_categories: ['火锅'] }, revision: 3 })
    expect(fetch.mock.calls[0]?.[1].headers.authorization).toBe('java-token')
    await agentApi.clearPreferences(4)
    expect(fetch.mock.calls[1]?.[0]).toBe('/agent-api/agent/preferences?revision=4')
    expect(fetch.mock.calls[1]?.[1].method).toBe('DELETE')
  })
  it('recovers a persisted async result over HTTP when WebSocket is unavailable', async () => {
    vi.stubGlobal('WebSocket', undefined)
    const fetch = vi.fn()
      .mockResolvedValueOnce(Response.json({ events: [{ seq: 7, type: 'tool_result', tool: 'find_shops' }] }))
      .mockResolvedValueOnce(Response.json({ id: 'task-1', state: 'completed', output: { answer: '已查到店铺', task_id: 'task-1', trace: [] } }))
    vi.stubGlobal('fetch', fetch)
    const events: string[] = []
    await agentApi.followTask('task-1', new AbortController().signal, event => events.push(event.type))
    expect(events).toEqual(['tool_result', 'done'])
    expect(fetch.mock.calls[0]?.[0]).toContain('/events?after=0')
    expect(fetch.mock.calls[1]?.[0]).toBe('/agent-api/agent/tasks/task-1')
  })

  it('reuses the caller request ID and never puts authentication in the request URL', async () => {
    localStorage.setItem('hmdp-token', 'java-token')
    const fetch = vi.fn().mockResolvedValue(Response.json({ task_id: 'task', state: 'accepted' }, { status: 202 }))
    vi.stubGlobal('fetch', fetch)
    await agentApi.submit('查店', 'stable-request-id')
    expect(JSON.parse(fetch.mock.calls[0]?.[1].body)).toEqual({ message: '查店', request_id: 'stable-request-id' })
    expect(fetch.mock.calls[0]?.[1].headers.authorization).toBe('java-token')
    expect(fetch.mock.calls[0]?.[0]).not.toContain('java-token')
  })
  it('decodes split UTF-8 frames and forwards the Java login token', async () => {
    localStorage.setItem('hmdp-token', 'java-token')
    const fetch = vi.fn().mockResolvedValue(response([
      ': keep-alive\n\n',
      'data: {"type":"tool_result","tool":"find_shops","result":{"name":"咖啡店"}}\n\n',
      'data: {"type":"done","answer":"剩余7张"}\n\n',
    ]))
    vi.stubGlobal('fetch', fetch)
    const events: string[] = []
    await agentApi.chat('查券', undefined, new AbortController().signal, e => events.push(e.type))
    expect(events).toEqual(['tool_result', 'done'])
    expect(fetch.mock.calls[0]?.[1].headers.authorization).toBe('java-token')
    expect(JSON.parse(fetch.mock.calls[0]?.[1].body)).toEqual({ message: '查券' })
  })

  it('reports explicit model errors and does not mark a broken stream complete', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(['data: {"type":"error","message":"模型令牌无效"}\n\n'])))
    await expect(agentApi.chat('查券', undefined, new AbortController().signal, () => undefined)).rejects.toThrow('模型令牌无效')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(['data: {"type":"model_request"}\n\n'])))
    await expect(agentApi.chat('查券', undefined, new AbortController().signal, () => undefined)).rejects.toThrow('连接中断')
  })
})

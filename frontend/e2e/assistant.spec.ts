import { expect, test } from '@playwright/test'

test('loads, saves and clears the authenticated user preferences', async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('hmdp-token', 'test-login'))
  await page.route('**/api/user/me', route => route.fulfill({ json: { success: true, data: { id: 1, nickName: '测试用户' } } }))
  await page.route('**/agent-api/agent/status', route => route.fulfill({ json: { configured: true, model: 'fixture', ready: true, redis: true, localperks: true } }))
  let revision = 0
  let preferences: Record<string, unknown> = {}
  await page.route('**/agent-api/agent/preferences*', route => {
    const request = route.request()
    expect(request.headers().authorization).toBe('test-login')
    if (request.method() === 'PUT') {
      expect(request.postDataJSON().revision).toBe(revision)
      expect(request.postDataJSON().preferences.cuisine_preferences).toEqual(['日料', '粤菜'])
      preferences = request.postDataJSON().preferences
      revision++
    } else if (request.method() === 'DELETE') {
      expect(new URL(request.url()).searchParams.get('revision')).toBe(String(revision))
      preferences = {}; revision++
    }
    return route.fulfill({ json: { preferences, revision, source: 'user', updated: 1 } })
  })
  await page.goto('/assistant')
  await expect(page.getByText('测试用户', { exact: true }).first()).toBeVisible()
  await page.getByText('我的偏好（可选）', { exact: true }).click()
  await page.getByRole('textbox', { name: '偏好口味或分类' }).fill('日料、粤菜')
  await page.getByRole('textbox', { name: '排除餐饮分类' }).fill('火锅')
  await page.getByLabel('回答风格', { exact: true }).selectOption('concise')
  await page.getByRole('button', { name: '保存偏好', exact: true }).click()
  await expect(page.getByRole('status')).toContainText('偏好已更新')
  await page.getByRole('button', { name: '清除偏好', exact: true }).click()
  await expect(page.getByRole('textbox', { name: '偏好口味或分类' })).toHaveValue('')
  expect(revision).toBe(2)
  await page.setViewportSize({ width: 390, height: 844 })
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true)
  await page.screenshot({ path: 'test-results/assistant-preferences-mobile.png', fullPage: true })
})

test('uses an async receipt and HTTP compensation for a disconnected notification channel', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('hmdp-token', 'test-login')
    Object.defineProperty(window, 'WebSocket', { value: undefined })
  })
  const id = '11111111-1111-4111-8111-111111111111'
  await page.route('**/api/user/me', route => route.fulfill({ json: { success: true, data: { id: 1, nickName: '测试用户' } } }))
  await page.route('**/agent-api/agent/status', route => route.fulfill({ json: { configured: true, model: 'fixture', ready: true, redis: true, localperks: true, async_tasks: true } }))
  await page.route('**/agent-api/agent/tasks', route => {
    expect(route.request().postDataJSON().request_id).toMatch(/^[0-9a-f-]{36}$/)
    return route.fulfill({ status: 202, json: { task_id: id, session_id: id, state: 'accepted' } })
  })
  await page.route(`**/agent-api/agent/tasks/${id}/events?after=0`, route => route.fulfill({ json: { events: [] } }))
  await page.route(`**/agent-api/agent/tasks/${id}`, route => route.fulfill({ json: { id, state: 'completed', output: { task_id: id, session_id: id, answer: '通过任务查询恢复了回答。', trace: [] } } }))
  await page.goto('/assistant')
  await page.getByRole('textbox', { name: '输入给助手的问题' }).fill('查询店铺4详情')
  await page.getByRole('button', { name: '发送', exact: true }).click()
  await expect(page.getByText('通过任务查询恢复了回答。', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: '恢复任务', exact: true })).toHaveCount(0)
  await page.screenshot({ path: 'test-results/assistant-async-recovery.png', fullPage: true })
})

test('shows missing configuration and prevents empty model requests', async ({ page }) => {
  await page.route('**/agent-api/agent/status', route => route.fulfill({ json: { configured: false, redis: true, localperks: true, ready: false, model: '' } }))
  await page.goto('/assistant')
  await expect(page.getByText('模型尚未配置。', { exact: false })).toBeVisible()
  await expect(page.getByRole('button', { name: '发送', exact: true })).toBeDisabled()
})

test('sends a question, displays factual tool trace, and resets the conversation', async ({ page }) => {
  await page.route('**/agent-api/agent/status', route => route.fulfill({ json: { configured: true, redis: true, localperks: true, ready: true, model: 'test-model' } }))
  await page.route('**/agent-api/agent/chat/stream', async route => {
    expect(route.request().postDataJSON().message).toBe('咖啡店有什么券')
    const trace = [
      { type: 'model_request', agent: 'coordinator', model: 'test-model', step: 1 },
      { type: 'delegation', agent: 'voucher', task: '查咖啡店优惠' },
      { type: 'tool_result', agent: 'voucher', tool: 'find_vouchers', requests: [{ method: 'GET', path: '/voucher/market', status: 200 }], result: { vouchers: [{ id: 13, stock: 7 }] } },
    ]
    const events = [...trace, { type: 'done', session_id: '11111111-1111-4111-8111-111111111111', answer: '咖啡券13还有7张，售价29.9元。', trace }]
    await route.fulfill({ contentType: 'text/event-stream', body: events.map(e => `data: ${JSON.stringify(e)}\n\n`).join('') })
  })
  await page.goto('/assistant')
  await page.getByRole('textbox', { name: '输入给助手的问题' }).fill('咖啡店有什么券')
  await page.getByRole('button', { name: '发送', exact: true }).click()
  await expect(page.getByText('咖啡券13还有7张，售价29.9元。', { exact: true })).toBeVisible()
  await page.getByText('查看模型与接口调用记录').click()
  await expect(page.getByText('GET /voucher/market → 200')).toBeVisible()
  await page.getByText('接口结果', { exact: true }).click()
  await expect(page.locator('pre')).toContainText('"stock": 7')
  await page.screenshot({ path: 'test-results/assistant-desktop.png', fullPage: true })
  await page.setViewportSize({ width: 390, height: 844 })
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true)
  await page.screenshot({ path: 'test-results/assistant-mobile.png', fullPage: true })
  await page.getByRole('button', { name: '新对话' }).click()
  await expect(page.getByText('从一句话开始')).toBeVisible()
})

test('recovers a waiting task after the user supplies missing constraints', async ({ page }) => {
  await page.route('**/agent-api/agent/status', route => route.fulfill({ json: { configured: true, redis: true, localperks: true, ready: true, model: 'test-model' } }))
  const id = '11111111-1111-4111-8111-111111111111'
  let requests = 0
  await page.route('**/agent-api/agent/chat/stream', async route => {
    const body = route.request().postDataJSON()
    if (requests++) expect(body.task_id).toBe(id)
    const answer = requests === 1 ? '请补充当前位置和具体用餐时间。' : '已按新预算和位置重新查询，当前没有已验证的可用优惠。'
    const frames = [{ type: 'session', session_id: id }, { type: 'task', task_id: id }, { type: 'done', session_id: id, task_id: id, answer, state: requests === 1 ? 'waiting_input' : 'completed', trace: [] }]
    await route.fulfill({ contentType: 'text/event-stream', body: frames.map(e => `data: ${JSON.stringify(e)}\n\n`).join('') })
  })
  await page.goto('/assistant')
  await page.getByRole('textbox', { name: '输入给助手的问题' }).fill('推荐附近两个人吃饭人均100今晚有优惠的店')
  await page.getByRole('button', { name: '发送', exact: true }).click()
  await expect(page.getByRole('button', { name: '恢复任务', exact: true })).toBeVisible()
  await page.getByRole('textbox', { name: '输入给助手的问题' }).fill('120.16,30.25 今晚19:00，预算改成80')
  await page.getByRole('button', { name: '恢复任务', exact: true }).click()
  await expect(page.getByText('已按新预算和位置重新查询，当前没有已验证的可用优惠。', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: '恢复任务', exact: true })).toHaveCount(0)
})

test('never submits a transaction until the user clicks explicit confirmation', async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('hmdp-token', 'test-login'))
  await page.route('**/api/user/me', route => route.fulfill({ json: { success: true, data: { id: 1, nickName: '测试用户' } } }))
  await page.route('**/agent-api/agent/status', route => route.fulfill({ json: { configured: true, redis: true, localperks: true, ready: true, model: 'test-model' } }))
  const action = { action_id: '11111111-1111-4111-8111-111111111111', operation: 'seckill', state: 'prepared', confirmation_token: 'test-confirmation-token-12345', preview: { voucher_id: 8, title: '测试券', pay_cents: 1900, notice: '仅提交请求，不自动支付' } }
  let submitted = 0
  await page.route('**/agent-api/agent/chat/stream', route => route.fulfill({ contentType: 'text/event-stream', body: `data: ${JSON.stringify({ type: 'done', answer: '请核对操作摘要后确认。', action, trace: [] })}\n\n` }))
  await page.route(`**/agent-api/agent/actions/${action.action_id}/confirm`, route => {
    expect(route.request().postDataJSON()).toEqual({ confirmed: true, confirmation_token: action.confirmation_token })
    submitted++
    return route.fulfill({ json: { ...action, state: 'completed', result: { orderId: '123', statusCode: 10 } } })
  })
  await page.goto('/assistant')
  await page.getByRole('textbox', { name: '输入给助手的问题' }).fill('帮我抢8号券')
  await page.getByRole('button', { name: '发送', exact: true }).click()
  await expect(page.getByText('抢券确认', { exact: true })).toBeVisible()
  await expect(page.locator('.action-preview')).toContainText('¥19.00')
  expect(submitted).toBe(0)
  await page.getByRole('button', { name: '确认提交此操作', exact: true }).click()
  await expect(page.getByText('操作状态：已提交')).toBeVisible()
  expect(submitted).toBe(1)
  await expect(page.getByRole('button', { name: '确认提交此操作', exact: true })).toHaveCount(0)
})

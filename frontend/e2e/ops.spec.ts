import { expect, test } from '@playwright/test'
import { readFile, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

test.skip(process.env.VITE_OPS_ENABLED !== 'true', '故障诊断已移出默认主系统；实验需显式启用')

test('ops requires independent administrator access and keeps secrets out of storage', async ({ page }) => {
  await page.route('**/agent-api/agent/ops/access', route => route.fulfill({ status: 403, json: { detail: 'forbidden' } }))
  await page.goto('/ops')
  await expect(page.getByRole('heading', { name: '验证运维权限' })).toBeVisible()
  await page.getByRole('textbox', { name: '管理员运维凭证' }).fill('ordinary-user-token')
  await page.getByRole('button', { name: '验证权限', exact: true }).click()
  await expect(page.getByText('管理员凭证无效或运维入口尚未启用')).toBeVisible()
  await expect(page.getByRole('button', { name: '开始只读调查' })).toHaveCount(0)
  expect(await page.evaluate(() => JSON.stringify({ local: localStorage, session: sessionStorage }))).not.toContain('ordinary-user-token')
})

test('ops renders investigation evidence safely without executing remediation', async ({ page }) => {
  const token = 'ui-only-admin-test-secret-at-least-32-characters'
  const report = { task_id: 'task-demo', state: 'completed', root_code: 'REDIS_STOCK_DRIFT',
    root_title: '库存快照不一致', answer: '两个快照显示库存差额。<script>window.unsafe=true</script>',
    recommendation: '请管理员核对在途订单后制定人工对账方案。',
    route: { path: 'rules_fastpath', reasons: ['证据支持唯一根因类别，已通过程序核验'] },
    trigger_operation: { status: 'unconfirmed', detail: '尚无变更审计。' },
    observations: [{ id: 'e3', tool: 'inventory', raw: { data: { mysql: [{ stock: 10 }], redisStock: '7' } } }] }
  await page.route('**/agent-api/agent/ops/**', async route => {
    expect(route.request().headers()['x-ops-token']).toBe(token)
    const path = new URL(route.request().url()).pathname
    if (path.endsWith('/access')) return route.fulfill({ json: { role: 'ops_admin', read_only: true } })
    if (path.endsWith('/deep-demo')) return route.fulfill({ json: { id: 'deep-demo', state: 'completed',
      output: { ...report, task_id: 'deep-demo', root_title: '证据不足', root_code: 'INSUFFICIENT_EVIDENCE',
        route: { path: 'agent_deep', reasons: ['两次库存读数冲突'], review: 'evidence_gate' },
        history: { strategy: 'rrf', latency_ms: 120, cases: [{ id: 'historical-demo',
          title: '历史库存差额<script>window.unsafe=true</script>', kind: 'isolated_fault_injection',
          similarities: ['库存差额'], differences: ['未观察到持续差额'], hypothesis: '读取第二次库存',
          sources: [{ path: 'docs/AIOPS_P4_REPORT.md', sha256: 'b'.repeat(64) }],
          checks: [{ signal: '库存差额', observation_ids: ['e3'], status: 'observed' }] }] } },
      events: [], model_usage: [], cost: 'UNKNOWN' } })
    if (path.endsWith('/task-demo')) return route.fulfill({ json: { id: 'task-demo', state: 'completed', output: report,
      events: [{ type: 'investigation_state', phase: 'verification' }], model_usage: [{ input_tokens: 600, output_tokens: 100, total_tokens: 700 }], cost: 'UNKNOWN' } })
    if (route.request().method() === 'POST') {
      expect(route.request().postDataJSON()).not.toHaveProperty('shell')
      return route.fulfill({ json: report })
    }
    return route.fulfill({ json: { items: [
      { id: 'task-demo', state: 'completed', request: JSON.stringify({ symptom: '库存异常' }), created: Date.now() / 1000 },
      { id: 'deep-demo', state: 'completed', request: { symptom: '两次读数不同' }, created: Date.now() / 1000 },
    ] } })
  })
  await page.goto('/ops'); await page.getByRole('textbox', { name: '管理员运维凭证' }).fill(token)
  await page.getByRole('button', { name: '验证权限', exact: true }).click()
  await page.getByRole('button', { name: /库存异常/ }).click()
  await expect(page.getByRole('heading', { name: '库存快照不一致' })).toBeVisible()
  await expect(page.getByRole('region', { name: '诊断路径' }).getByText('规则直达')).toBeVisible()
  await expect(page.getByRole('region', { name: '历史相似案例' })).toHaveCount(0)
  await expect(page.getByText('具体触发操作 · 未确认')).toBeVisible()
  await expect(page.getByText('核验根因')).toBeVisible()
  await expect(page.getByRole('region', { name: '关键观测' }).getByText('MySQL 库存')).toBeVisible()
  await page.getByText('e3 · 库存对账').click()
  await expect(page.locator('pre')).toContainText('"redisStock": "7"')
  expect(await page.evaluate(() => (window as Window & { unsafe?: boolean }).unsafe)).toBeUndefined()
  expect(await page.evaluate(() => JSON.stringify({ local: localStorage, session: sessionStorage }))).not.toContain(token)
  await page.screenshot({ path: 'test-results/ops-desktop.png', fullPage: true })
  await page.setViewportSize({ width: 390, height: 844 })
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true)
  await page.screenshot({ path: 'test-results/ops-mobile.png', fullPage: true })
  await page.getByRole('button', { name: /两次读数不同/ }).click()
  await expect(page.getByRole('region', { name: '诊断路径' }).getByText('Agent 深度调查')).toBeVisible()
  await expect(page.getByText('两次库存读数冲突')).toBeVisible()
  const history = page.getByRole('region', { name: '历史相似案例' })
  await expect(history.getByText('未观察到持续差额')).toBeVisible()
  await history.getByText('本次症状核对').click()
  await expect(history.getByText(/对应本次观测 e3/)).toBeVisible()
  await history.getByText(/来源与 SHA-256/).click()
  await expect(history.getByText('b'.repeat(64))).toBeVisible()
  await expect(page.getByText('结论引用', { exact: true })).toHaveCount(0)
  expect(await page.evaluate(() => (window as Window & { unsafe?: boolean }).unsafe)).toBeUndefined()
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true)
  await page.getByRole('button', { name: '锁定运维页' }).click()
  await expect(page.getByRole('heading', { name: '验证运维权限' })).toBeVisible()
})

test('opens a real investigation as a standalone HTML preview', async ({ page }) => {
  const input = process.env.OPS_PREVIEW_RECORD
  test.skip(!input, 'Export requires a recorded investigation')
  const record = JSON.parse(await readFile(input!, 'utf8'))
  const output = record.output ?? record.result ?? record
  const request = record.request ?? { symptom: record.task?.symptom ?? output.symptom, scope: output.scope }
  const p5 = Boolean(output.history)
  const mixed = output.route?.path === 'agent_deep'
  expect(output.root_code).toBe(mixed ? 'INSUFFICIENT_EVIDENCE' : 'MQ_CONSUME_DISABLED')
  expect(output.answer_origin).toBe(mixed ? 'adaptive_verified' : 'llm_grounded')
  const shownEvidence = output.observations.find((item: { tool: string }) => item.tool === (p5 ? 'delivery' : mixed ? 'traffic' : 'mq'))
  const metric = p5 ? '投递与幂等' : mixed ? '请求与限流' : '消息消费'
  const detailText = p5 ? '"type": "order_idempotent"' : mixed ? '"limitScope": "ip"' : '"lag": 3'
  const screenshot = p5 ? 'ops-p5' : 'ops-live-mq'
  const id = String(output.task_id)
  await page.route('**/agent-api/agent/ops/**', route => {
    const path = new URL(route.request().url()).pathname
    if (path.endsWith('/access')) return route.fulfill({ json: { role: 'ops_admin', read_only: true } })
    if (path.endsWith('/' + id)) return route.fulfill({ json: { id, state: output.state, output, events: output.trace, model_usage: output.model_usage, cost: output.cost } })
    return route.fulfill({ json: { items: [{ id, state: output.state, request, created: request.scope.end / 1000 }] } })
  })
  await page.goto('/ops')
  await page.getByRole('textbox', { name: '管理员运维凭证' }).fill('ui-only-preview-credential')
  await page.getByRole('button', { name: '验证权限', exact: true }).click()
  await page.getByRole('button', { name: p5 ? /复投后订单仍只有一条/ : mixed ? /同时有人遇到/ : /订单已接收/ }).click()
  const result = page.getByRole('region', { name: '调查结果' })
  await expect(result.getByRole('heading', { name: metric })).toBeVisible()
  if (mixed) {
    await expect(result.getByRole('heading', { name: 'Agent 深度调查' })).toBeVisible()
    await expect(result.getByText(/多个根因类别同时获得证据/)).toBeVisible()
  } else {
    await expect(result.getByText('消费开关', { exact: true })).toBeVisible()
    await expect(result.getByText('关闭', { exact: true })).toBeVisible()
  }
  if (p5) {
    const history = result.getByRole('region', { name: '历史相似案例' })
    await expect(history.getByText('同一消息重复送达但订单幂等')).toBeVisible()
    await history.getByText(/来源与 SHA-256/).first().click()
    await expect(history.getByText('docs/AIOPS_P4_REPORT.md', { exact: false }).first()).toBeVisible()
  }
  await expect(result.getByText('结论引用', { exact: true }).first()).toBeVisible()
  await page.screenshot({ path: `test-results/${screenshot}-desktop.png`, fullPage: true })

  const html = await page.evaluate((caption) => {
    const styles = Array.from(document.querySelectorAll<HTMLStyleElement>('style[data-vite-dev-id]'))
      .filter(style => /OpsView\.vue|OpsEvidenceMetrics\.vue|\/src\/styles\/main\.css/.test(style.dataset.viteDevId ?? ''))
      .map(style => style.textContent).join('\n')
    const header = document.querySelector('.ops-heading')?.outerHTML
    const result = document.querySelector('.ops-result')?.outerHTML
    return `<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'"><title>LocalPerks · 运维诊断实测预览</title><style>${styles}
      body{margin:0;background:#f4f8f6;font-family:system-ui,sans-serif}.ops-page{padding:clamp(12px,4vw,32px);max-width:1020px}.preview-caption{margin:0 0 14px;font-size:13px;color:#4b6259}.el-icon{display:inline-flex;width:1em;height:1em}.el-icon svg{width:100%;height:100%}
      </style></head><body><main class="ops-page">${header}<p class="preview-caption">实测记录回放 · ${caption} · 数据采集于隔离实验</p>${result}</main></body></html>`
  }, p5 ? 'P5 · 消息复投与库存偏差叠加' : mixed ? 'IP 限流与库存偏差叠加' : 'MQ 消费组停用')
  const preview = resolve(p5 ? 'test-results/ops-p5-preview.html' : 'test-results/ops-preview.html')
  await writeFile(preview, html, 'utf8')
  await page.goto(pathToFileURL(preview).href)
  await page.setViewportSize({ width: 390, height: 844 })
  await expect(page.getByText('实测记录回放', { exact: false })).toBeVisible()
  await expect(page.getByRole('heading', { name: metric })).toBeVisible()
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390)
  const summary = page.getByText(`${shownEvidence.id} · ${metric}`, { exact: false })
  await summary.click()
  await expect(page.locator('details[open] pre')).toContainText(detailText)
  await summary.click()
  await page.screenshot({ path: `test-results/${screenshot}-mobile.png`, fullPage: true })
})

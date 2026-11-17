import { describe, expect, it } from 'vitest'
import { renderAgentMarkdown } from './agentMarkdown'

describe('Agent answer rendering', () => {
  it('renders tables, emphasis and internal business links', () => {
    const html = renderAgentMarkdown('**查询结果**\n\n| 券 | 库存 |\n|---|---|\n| 咖啡券 | 7 |\n\n[去领券](/vouchers?shopId=4)')
    expect(html).toContain('<strong>查询结果</strong>')
    expect(html).toContain('<table>')
    expect(html).toContain('href="/vouchers?shopId=4"')
  })
  it('removes executable HTML, images and non-business links', () => {
    const html = renderAgentMarkdown('<script>alert(1)</script><img src=x onerror=alert(1)><iframe src="https://evil.test"></iframe>\n\n[危险](javascript:alert(1)) [外部](https://evil.test) [伪装](//evil.test)')
    expect(html).not.toMatch(/<script|onerror|<img|<iframe|href=/)
    expect(html).toContain('<a>外部</a>')
  })
})

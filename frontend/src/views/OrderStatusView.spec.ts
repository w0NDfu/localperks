import { flushPromises, mount } from '@vue/test-utils'
import ElementPlus from 'element-plus'
import { expect, it, vi } from 'vitest'
import OrderStatusView from './OrderStatusView.vue'

const { orderId, status } = vi.hoisted(() => ({
  orderId: '700000000000000001',
  status: vi.fn(),
}))

vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { id: orderId } }),
  useRouter: () => ({ push: vi.fn() }),
}))

vi.mock('@/api/voucher', () => ({ voucherApi: { status } }))

it('queries the exact order ID from the route without rounding it', async () => {
  status.mockResolvedValue({ orderId, statusCode: 13, statusText: '已支付' })
  const page = mount(OrderStatusView, { global: { plugins: [ElementPlus], stubs: { RouterLink: true } } })
  try {
    await flushPromises()
    expect(status).toHaveBeenCalledWith(orderId)
    expect(page.get('[data-testid="order-status"]').text()).toContain('已支付')
  } finally {
    page.unmount()
  }
})

it('recovers when the first status request fails', async () => {
  status.mockRejectedValueOnce(new Error('查询超时'))
  status.mockResolvedValueOnce({ orderId, statusCode: 12, statusText: '待支付' })
  const page = mount(OrderStatusView, { global: { plugins: [ElementPlus], stubs: { RouterLink: true } } })
  try {
    await flushPromises()
    expect(page.text()).toContain('查询超时')
    await page.findAll('button').find(button => button.text() === '刷新状态')!.trigger('click')
    await flushPromises()
    expect(page.text()).toContain('待支付')
    expect(page.text()).not.toContain('查询超时')
  } finally {
    page.unmount()
  }
})

it('keeps polling an unknown receipt until the original order becomes visible', async () => {
  vi.useFakeTimers()
  status.mockResolvedValueOnce({ orderId, statusCode: 19, statusText: '受理结果待核对', requestState: 'UNKNOWN' })
  status.mockResolvedValueOnce({ orderId, statusCode: 12, statusText: '待支付', requestState: 'SUCCESS' })
  const page = mount(OrderStatusView, { global: { plugins: [ElementPlus], stubs: { RouterLink: true } } })
  try {
    await flushPromises()
    expect(page.text()).toContain('受理结果待核对')
    expect(page.text()).toContain('保留当前订单号')
    await vi.advanceTimersByTimeAsync(1000)
    await flushPromises()
    expect(page.text()).toContain('待支付')
    expect(status).toHaveBeenLastCalledWith(orderId)
  } finally {
    page.unmount()
    vi.useRealTimers()
  }
})

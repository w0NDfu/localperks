import { onBeforeUnmount, ref } from 'vue'
import { voucherApi } from '@/api/voucher'
import type { VoucherOrderStatusResult } from '@/types/domain'

const terminal = new Set([13, 14, 15, 16, 17, 18])
const MAX_POLL_ATTEMPTS = 30
export const isTerminalOrderStatus = (statusCode: number) => terminal.has(statusCode)

export function useOrderPolling() {
  const order = ref<VoucherOrderStatusResult | null>(null)
  const polling = ref(false)
  const loading = ref(false)
  const error = ref('')
  const paused = ref(false)
  let timer: number | undefined
  let generation = 0

  function stop() {
    generation++
    window.clearTimeout(timer)
    timer = undefined
    polling.value = false
    loading.value = false
  }

  async function start(id: string) {
    stop()
    const active = generation
    if (order.value?.orderId !== id) order.value = null
    error.value = ''
    paused.value = false
    loading.value = true
    polling.value = true
    let attempts = 0

    async function check() {
      try {
        const result = await voucherApi.status(id)
        if (active !== generation) return
        order.value = result
        loading.value = false
        if (isTerminalOrderStatus(result.statusCode) || result.statusCode === 12) {
          stop()
          return
        }
        if (++attempts >= MAX_POLL_ATTEMPTS) {
          paused.value = true
          stop()
          return
        }
        timer = window.setTimeout(() => void check(), 1000)
      } catch (cause) {
        if (active !== generation) return
        error.value = cause instanceof Error ? cause.message : '暂时无法查询订单状态'
        stop()
      }
    }

    await check()
  }

  onBeforeUnmount(stop)
  return { order, polling, loading, error, paused, start, stop }
}

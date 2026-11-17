<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import { voucherApi } from '@/api/voucher'
import { useOrderPolling } from '@/composables/useOrderPolling'
import { formatVoucherTime } from '@/utils/vouchers'

const route = useRoute()
const id = computed(() => String(route.params.id))
const { order, polling, loading, error, paused, start } = useOrderPolling()
const paying = ref(false)
const stage = computed(() => {
  if (order.value?.statusCode === 13) return 3
  if (order.value?.statusCode === 12) return 2
  return 1
})
const closed = computed(() => order.value?.statusCode === 14 || order.value?.statusCode === 15)
const headline = computed(() => order.value?.statusText || (error.value ? '查询暂不可用' : '正在确认订单'))
const descriptions: Record<number, string> = {
  10: '请求已收到，正在等待处理。页面会自动更新。',
  11: '正在确认库存和创建订单，请稍候。',
  12: '已为你预留一张券。完成模拟支付后可在「我的券」查看。',
  13: '优惠券已放入你的券包。',
  14: '这笔订单已取消。你可以返回领券中心浏览其他活动。',
  15: '这笔请求未能完成，请返回领券中心重新查看库存。',
  16: '这张券已核销，可在我的券查看历史记录。',
  17: '这笔订单正在退款，可稍后刷新状态。',
  18: '这笔订单已退款。',
  19: '受理或投递结果正在核对。请保留当前订单号并刷新状态，避免重复提交。',
}
const description = computed(() => descriptions[order.value?.statusCode ?? 0] || '正在读取抢券结果。')

watch(id, value => void start(value), { immediate: true })

async function pay(success: boolean) {
  if (paying.value) return
  paying.value = true
  const orderId = id.value
  try {
    const message = await voucherApi.mockPay(orderId, success)
    ElMessage[success ? 'success' : 'warning'](message)
    if (id.value === orderId) await start(orderId)
  } catch {
    // API errors are shown by the response interceptor.
  } finally {
    paying.value = false
  }
}
</script>

<template>
  <div class="page-head">
    <div>
      <p class="eyebrow">订单追踪</p>
      <h2>秒杀进度</h2>
    </div>
    <router-link class="text-link" to="/vouchers">返回领券中心</router-link>
  </div>
  <section class="surface order-panel" data-testid="order-status" :data-status-code="String(order?.statusCode ?? '')" aria-live="polite">
    <el-steps :active="stage" :process-status="closed ? 'error' : 'process'" finish-status="success" align-center>
      <el-step title="提交请求" />
      <el-step title="确认订单" />
      <el-step title="收入券包" />
    </el-steps>

    <div class="order-summary">
      <span class="pill" :class="{ 'pill-green': order?.statusCode === 13 }">{{ polling ? '自动更新中' : headline }}</span>
      <h3>{{ headline }}</h3>
      <p class="muted">{{ error || description }}</p>
      <dl>
        <div><dt>订单号</dt><dd>{{ id }}</dd></div>
        <div v-if="order?.createTime"><dt>提交时间</dt><dd>{{ formatVoucherTime(order.createTime) }}</dd></div>
      </dl>
    </div>

    <el-alert v-if="paused" title="处理时间较长，可稍后从「我的券」继续查询。" type="info" :closable="false" />
    <div class="order-actions">
      <el-button v-if="order?.statusCode === 12" type="primary" :loading="paying" @click="pay(true)">模拟支付</el-button>
      <el-button v-if="order?.statusCode === 12" :disabled="paying" @click="pay(false)">模拟支付失败</el-button>
      <el-button :loading="loading" :disabled="paying" @click="start(id)">刷新状态</el-button>
      <router-link class="text-link" to="/my-vouchers">打开我的券</router-link>
    </div>
  </section>
</template>

<style scoped>
.order-panel { max-width: 820px; margin: 0 auto; padding: clamp(20px, 4vw, 44px); }
.order-summary { text-align: center; padding: 30px 0 22px; }
.order-summary h3 { font-size: clamp(25px, 4vw, 36px); margin: 12px 0 6px; }
.order-summary dl { display: flex; flex-wrap: wrap; justify-content: center; gap: 12px 24px; margin: 20px 0 0; }
.order-summary dl div { display: flex; gap: 8px; flex-wrap: wrap; justify-content: center; font-size: 13px; }
.order-summary dt { color: var(--ink-muted); }
.order-summary dd { margin: 0; overflow-wrap: anywhere; }
.order-actions { display: flex; justify-content: center; align-items: center; flex-wrap: wrap; gap: 10px 16px; margin-top: 20px; }
.order-actions .el-button { margin: 0; }
</style>

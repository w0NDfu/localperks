<script setup lang="ts">
import { computed } from 'vue'
import type { OpsEvidence } from '@/api/ops'

const props = defineProps<{ evidence: OpsEvidence[]; root?: string }>()

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown> : undefined
}

function number(value: unknown): number | undefined {
  if (typeof value !== 'number' && !(typeof value === 'string' && /^\d+$/.test(value))) return
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : undefined
}

function entries(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.flatMap(row => record(row) ?? []) : []
}

function sample(tool: string) {
  const item = [...props.evidence].reverse().find(e => e.tool === tool && !e.error)
  const raw = record(item?.raw)
  return { id: item?.id, at: number(raw?.observedAt), data: record(raw?.data) }
}

const panels = computed(() => {
  const inventory = sample('inventory')
  const mq = sample('mq')
  const traffic = sample('traffic')
  const delivery = sample('delivery')
  const mysql = number(entries(inventory.data?.mysql)[0]?.stock)
  const redis = number(inventory.data?.redisStock)
  const panels = [
    { key: 'inventory', root: 'REDIS_STOCK_DRIFT', title: '库存对账', ...inventory,
      bars: [{ label: 'MySQL 库存', value: mysql }, { label: 'Redis 库存', value: redis }],
      facts: [{ label: '库存差额', value: mysql != null && redis != null ? mysql - redis : undefined },
        { label: '在途预留', value: number(entries(inventory.data?.pending)[0]?.total) }] },
    { key: 'mq', root: 'MQ_CONSUME_DISABLED', title: '消息消费', ...mq,
      bars: [{ label: 'Broker 位点', value: number(mq.data?.brokerOffset) }, { label: 'Consumer 位点', value: number(mq.data?.consumerOffset) }],
      facts: [{ label: '消息积压', value: number(mq.data?.lag) },
        { label: '消费开关', value: typeof mq.data?.consumeEnabled === 'boolean' ? (mq.data.consumeEnabled ? '开启' : '关闭') : undefined }] },
    { key: 'traffic', root: 'IP_RATE_LIMIT', title: '请求与限流', ...traffic,
      bars: [{ label: '窗口 HTTP 429', value: Array.isArray(traffic.data?.events) ? entries(traffic.data.events).filter(e => e.type === 'http' && e.status === 429).length : undefined },
        { label: '其中 IP 拒绝', value: Array.isArray(traffic.data?.events) ? entries(traffic.data.events).filter(e => e.type === 'http' && e.status === 429 && e.limitScope === 'ip').length : undefined }],
      facts: [{ label: 'IP 窗口读取值', value: number(traffic.data?.ipWindowCount) }, { label: 'IP 阈值', value: number(traffic.data?.ipLimit) }] },
    { key: 'delivery', root: 'DUPLICATE_DELIVERY', title: '投递与幂等', ...delivery,
      bars: [{ label: '窗口接收事件', value: Array.isArray(delivery.data?.events) ? entries(delivery.data.events).filter(e => e.type === 'mq_received').length : undefined },
        { label: '窗口幂等事件', value: Array.isArray(delivery.data?.events) ? entries(delivery.data.events).filter(e => e.type === 'order_idempotent').length : undefined }],
      facts: [{ label: '读取到的订单行', value: Array.isArray(delivery.data?.orders) ? delivery.data.orders.length : undefined }] },
  ]
  return panels.filter(panel => panel.data).sort((a, b) => Number(b.root === props.root) - Number(a.root === props.root))
})
</script>

<template>
  <section v-if="panels.length" aria-label="关键观测" class="metric-grid">
    <article v-for="panel in panels" :key="panel.key" class="metric-panel">
      <header><h3>{{ panel.title }}</h3><span>{{ panel.id }}</span></header>
      <div v-for="bar in panel.bars" :key="bar.label" class="metric-bar">
        <div><span>{{ bar.label }}</span><strong>{{ bar.value?.toLocaleString() ?? '—' }}</strong></div>
        <div class="bar-track" aria-hidden="true">
          <div :style="{ width: `${100 * Math.max(0, bar.value ?? 0) / Math.max(1, ...panel.bars.map(item => item.value ?? 0))}%` }" />
        </div>
      </div>
      <dl><div v-for="fact in panel.facts" :key="fact.label"><dt>{{ fact.label }}</dt><dd>{{ fact.value ?? '—' }}</dd></div></dl>
      <small v-if="panel.at">采样 {{ new Date(panel.at).toLocaleTimeString('zh-CN', { hour12: false }) }}</small>
    </article>
  </section>
</template>

<style scoped>
.metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 240px), 1fr)); gap: 12px; }
.metric-panel { min-width: 0; padding: 16px; background: #fff; border: 1px solid #dce8e5; border-radius: 12px; }
header, .metric-bar > div:first-child { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
h3 { font-size: 15px; margin: 0; }
header span { font-family: monospace; color: #315950; background: #e8f3ef; padding: 2px 7px; border-radius: 5px; }
.metric-bar { margin-top: 12px; font-size: 13px; }
.bar-track { margin-top: 5px; height: 6px; border-radius: 4px; background: #e9efed; overflow: hidden; }
.bar-track div { height: 100%; border-radius: inherit; background: #267a66; }
.metric-bar + .metric-bar .bar-track div { background: #6b88aa; }
dl { display: flex; flex-wrap: wrap; gap: 12px 24px; margin: 13px 0 4px; }
dt, small { color: #546861; font-size: 12px; }
dd { margin: 2px 0 0; font-weight: 700; }
</style>

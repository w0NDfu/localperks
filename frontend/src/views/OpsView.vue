<script setup lang="ts">
import { computed, onBeforeUnmount, ref } from 'vue'
import { Monitor, Lock, Refresh } from '@element-plus/icons-vue'
import { opsRequest, type OpsDetail, type OpsEvidence, type OpsHistory, type OpsReport, type OpsRoute, type OpsScope, type OpsTask } from '@/api/ops'
import OpsEvidenceMetrics from '@/components/OpsEvidenceMetrics.vue'

// Administrator secret exists only in this component's memory, never URL/storage.
const token = ref('')
const admitted = ref(false)
const busy = ref(false)
const error = ref('')
const symptom = ref('')
const voucher = ref('')
const ip = ref('127.0.0.1')
const seconds = ref(60)
const tasks = ref<OpsTask[]>([])
const detail = ref<OpsDetail>()
const report = ref<OpsReport>()
let timer: ReturnType<typeof setInterval> | undefined
let detailRequest = 0
const labels: Record<string, string> = {
  running: '调查中', completed: '已完成', failed: '调查失败', interrupted: '已中断', created: '待开始',
  spring: '服务运行状态', events: '事件记录', traffic: '请求与限流', mq: '消息消费', inventory: '库存对账',
  delivery: '投递与幂等', compensation: '补偿记录',
  IP_RATE_LIMIT: '出口 IP 限流', MQ_CONSUME_DISABLED: '消费组停用', REDIS_STOCK_DRIFT: '库存差额',
  DUPLICATE_DELIVERY: '消息重复投递', NO_FAULT: '未发现故障', INSUFFICIENT_EVIDENCE: '证据不足',
}
const phases = [
  { key: 'symptom', label: '确认范围' }, { key: 'hypotheses', label: '提出假设' },
  { key: 'collection', label: '采集证据' }, { key: 'verification', label: '核验根因' },
  { key: 'report', label: '生成报告' },
]
const events = computed(() => detail.value?.events ?? [])
const route = computed(() => report.value?.route ?? [...events.value].reverse().find(event => event.type === 'ops_route') as OpsRoute | undefined)
const history = computed(() => report.value?.history ?? [...events.value].reverse().find(event => event.type === 'ops_history') as OpsHistory | undefined)
const historyKinds = { isolated_load_test: '隔离压测', isolated_fault_injection: '隔离故障注入', runbook: '运维手册' }
const historyStrategies: Record<string, string> = { rrf: '混合检索', model_api: '混合检索 + 重排',
  empty: '案例库无条目', no_match: '无相关案例', no_query_signal: '缺少可检索症状', unavailable: '检索暂不可用' }
const evidence = computed(() => report.value?.observations ?? events.value.flatMap(event =>
  event.type === 'ops_evidence' && event.evidence ? [event.evidence as OpsEvidence] : []))
const seenPhases = computed(() => events.value.filter(e => e.type === 'investigation_state').map(e => e.phase))
const activePhase = computed(() => seenPhases.value[seenPhases.value.length - 1])
const hypotheses = computed(() => events.value.flatMap(event => {
  if (event.type !== 'ops_hypotheses' || !event.move) return []
  return [(event.move as { hypotheses?: { code: string; reason: string }[] }).hypotheses ?? []]
}))
const verdict = computed(() => report.value?.checks?.supported?.[report.value.root_code ?? '']?.detail)
const excluded = computed(() => report.value?.checks?.excluded ?? {})
const usage = computed(() => {
  const rows = detail.value?.model_usage || []
  return { requests: rows.length, tokens: rows.reduce((n, r) => n + (r.total_tokens || 0), 0),
    unknown: rows.filter(r => r.total_tokens === null).length,
    retrieval: rows.filter(r => r.kind === 'embedding' || r.kind === 'rerank').length }
})

async function refresh() {
  const credential = token.value
  try {
    const { items } = await opsRequest<{ items: OpsTask[] }>(credential, 'investigations')
    if (credential === token.value) tasks.value = items
    return items
  } catch (e) {
    if (credential === token.value) error.value = (e as Error).message
    return []
  }
}
async function unlock() {
  error.value = ''
  try {
    await opsRequest(token.value, 'access'); admitted.value = true; await refresh()
  } catch (e) { error.value = (e as Error).message; admitted.value = false }
}
function lock() {
  detailRequest++
  admitted.value = false; token.value = ''; tasks.value = []; detail.value = undefined; report.value = undefined
  if (timer) clearInterval(timer)
}
async function openTask(id: string) {
  const request = ++detailRequest
  try {
    const result = await opsRequest<OpsDetail>(token.value, 'investigations/' + id)
    if (request !== detailRequest) return
    detail.value = result
    report.value = result.output
  } catch (e) {
    if (request === detailRequest) error.value = (e as Error).message
  }
}
async function investigate() {
  error.value = ''
  if (!symptom.value.trim() || !/^\d+$/.test(voucher.value) || !Number.isSafeInteger(Number(voucher.value)) || Number(voucher.value) <= 0) {
    error.value = '请填写异常症状与有效券 ID'; return
  }
  busy.value = true; report.value = undefined; detail.value = undefined
  const end = Date.now()
  const credential = token.value
  let polling = false
  // Persisted running investigations are visible while the bounded request runs.
  timer = setInterval(async () => {
    if (polling) return
    polling = true
    try {
      const items = await refresh()
      if (!busy.value || token.value !== credential) return
      const running = items.find(t => taskRequest(t)?.scope?.end === end)
      if (running) await openTask(running.id)
    } finally { polling = false }
  }, 2000)
  try {
    const result = await opsRequest<OpsReport>(token.value, 'investigations', { symptom: symptom.value,
      scope: { start: end - seconds.value * 1000, end, voucher_id: Number(voucher.value), ip: ip.value } })
    if (token.value !== credential) return
    if (timer) clearInterval(timer)
    busy.value = false
    await refresh()
    if (token.value === credential) await openTask(result.task_id)
  } catch (e) { error.value = (e as Error).message }
  finally { busy.value = false; if (timer) clearInterval(timer) }
}
function taskRequest(task: OpsTask): { symptom?: string; scope?: OpsScope } | undefined {
  try { return typeof task.request === 'string' ? JSON.parse(task.request) : task.request }
  catch { return undefined }
}
onBeforeUnmount(lock)
</script>

<template>
  <section class="ops-page">
    <header class="ops-heading">
      <div><span class="ops-eyebrow"><el-icon><Monitor /></el-icon> LocalPerks AIOps</span><h1>运维诊断台</h1><p>从业务症状追查根因，逐步核对指标与证据。</p></div>
      <span class="ops-readonly"><el-icon><Lock /></el-icon> 只读调查</span>
    </header>
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <el-card v-if="!admitted" class="ops-access">
      <h2>验证运维权限</h2><p>请输入专用管理员凭证。普通用户登录不授予运维权限。</p>
      <el-input v-model="token" type="password" autocomplete="off" aria-label="管理员运维凭证" placeholder="管理员运维凭证" @keyup.enter="unlock" />
      <el-button type="primary" :disabled="!token" @click="unlock">验证权限</el-button>
    </el-card>
    <template v-else>
      <div class="ops-toolbar"><el-button :disabled="busy" :icon="Refresh" @click="refresh">刷新记录</el-button><el-button :disabled="busy" :icon="Lock" @click="lock">锁定运维页</el-button></div>
      <el-card class="ops-form">
        <h2>发起调查</h2>
        <el-form label-position="top" @submit.prevent="investigate">
          <el-form-item label="异常症状"><el-input v-model="symptom" type="textarea" :rows="3" maxlength="2000" placeholder="描述异常现象，以及需要核查的业务范围" /></el-form-item>
          <div class="ops-inputs">
            <el-form-item label="优惠券 ID"><el-input v-model="voucher" inputmode="numeric" placeholder="本次调查的券 ID" /></el-form-item>
            <el-form-item label="观测事件窗口"><el-select v-model="seconds"><el-option :value="60" label="最近 1 分钟" /><el-option :value="300" label="最近 5 分钟" /><el-option :value="900" label="最近 15 分钟" /></el-select></el-form-item>
            <el-form-item label="请求出口 IP"><el-input v-model="ip" /></el-form-item>
          </div>
          <el-button type="primary" native-type="submit" :loading="busy">开始只读调查</el-button>
          <span class="ops-note">调查最多 300 秒，页面会显示采集进度。</span>
        </el-form>
      </el-card>
      <div class="ops-content">
        <el-card class="ops-history"><h2>调查记录 <span>{{ tasks.length }}</span></h2><p v-if="!tasks.length">暂无调查记录</p>
          <button v-for="task in tasks" :key="task.id" type="button" class="ops-task" :aria-pressed="detail?.id === task.id" :disabled="busy" @click="openTask(task.id)">
            <strong>{{ taskRequest(task)?.symptom || '故障调查' }}</strong>
            <small><span class="status-dot" :data-state="task.state" />{{ labels[task.state] ?? task.state }} · {{ new Date(task.created * 1000).toLocaleString('zh-CN', { hour12: false }) }}</small>
          </button>
        </el-card>
        <section class="ops-result" aria-label="调查结果">
          <template v-if="detail">
            <article class="ops-panel ops-verdict" :class="{ 'ops-caution': report?.root_code === 'INSUFFICIENT_EVIDENCE' }" aria-live="polite">
              <div class="panel-heading"><span>调查结论</span><span class="state-tag" :data-state="detail.state">{{ labels[detail.state] ?? detail.state }}</span></div>
              <h2>{{ report?.root_title || (report ? '根因尚未确认' : '正在收集证据') }}</h2>
              <p v-if="report">{{ verdict ?? report.answer }}</p>
              <p v-if="report?.symptom" class="scope-line">报告现象 · {{ report.symptom }}</p>
              <div v-if="report?.scope" class="scope-line">券 #{{ report.scope.voucher_id }} · {{ new Date(report.scope.start).toLocaleString('zh-CN', { hour12: false }) }} 至 {{ new Date(report.scope.end).toLocaleTimeString('zh-CN', { hour12: false }) }}</div>
              <div v-if="report?.recommendation" class="ops-advice"><strong>人工处置建议</strong><p>{{ report.recommendation }}</p></div>
              <details v-if="verdict && report?.answer"><summary>查看完整诊断报告</summary><p class="ops-answer">{{ report.answer }}</p></details>
            </article>
            <section v-if="route" class="ops-panel" aria-label="诊断路径">
              <div class="panel-heading">
                <h3>{{ route.path === 'rules_fastpath' ? '规则直达' : 'Agent 深度调查' }}</h3>
                <span v-if="route.path === 'rules_fastpath'" class="evidence-tag">0 次模型调用</span>
                <span v-else-if="route.review === 'unverified'" class="evidence-tag failed">等待人工复核</span>
              </div>
              <ul class="ops-route-reasons"><li v-for="reason in route.reasons" :key="reason">{{ reason }}</li></ul>
              <dl v-if="report?.verified_categories?.length || report?.findings?.length || report?.trigger_operation" class="ops-hypotheses">
                <div v-for="item in report?.verified_categories" :key="item.code"><dt>已核验类别 · {{ labels[item.code] ?? item.code }}</dt><dd>{{ item.detail }}（{{ item.evidence_ids.join('、') }}）</dd></div>
                <div v-for="item in report?.findings" :key="item.kind"><dt>观测发现</dt><dd>{{ item.detail }}（{{ item.evidence_ids.join('、') }}）</dd></div>
                <div v-if="report?.trigger_operation"><dt>具体触发操作 · {{ report.trigger_operation.status === 'unconfirmed' ? '未确认' : '已确认' }}</dt><dd>{{ report.trigger_operation.detail }}</dd></div>
              </dl>
            </section>
            <section v-if="history" class="ops-panel ops-history" aria-label="历史相似案例">
              <div class="panel-heading">
                <h3>历史相似案例</h3>
                <small>{{ historyStrategies[history.strategy] ?? '历史检索' }} · {{ Math.round(history.latency_ms) }} ms</small>
              </div>
              <p>案例提供调查线索。根因和触发操作由本次证据独立核验。</p>
              <p v-if="history.warning">{{ history.warning }}</p>
              <p v-else-if="!history.cases.length">本次没有可用历史线索。</p>
              <article v-for="item in history.cases" :key="item.id" class="ops-history-case">
                <h4>{{ item.title }} <span class="evidence-tag">{{ historyKinds[item.kind] }}</span></h4>
                <dl class="ops-hypotheses">
                  <div><dt>相似点</dt><dd>{{ item.similarities.join('、') || '语义匹配，暂无相同的已分类症状' }}</dd></div>
                  <div><dt>差异与待核对项</dt><dd>{{ item.differences.join('。') || '已抽取症状重叠，具体成因仍需核验' }}</dd></div>
                  <div><dt>调查建议</dt><dd>{{ item.hypothesis }}</dd></div>
                </dl>
                <details v-if="item.checks.length">
                  <summary>本次症状核对</summary>
                  <ul><li v-for="check in item.checks" :key="check.signal">{{ check.signal }} ·
                    {{ check.status === 'observed' ? '对应本次观测 ' + check.observation_ids.join('、') : '仅报告症状，尚无对应观测' }}
                  </li></ul>
                </details>
                <details>
                  <summary>来源与 SHA-256（{{ item.sources.length }}）</summary>
                  <ul><li v-for="source in item.sources" :key="source.path">{{ source.path }}<code>{{ source.sha256 }}</code></li></ul>
                </details>
              </article>
            </section>
            <article class="ops-panel">
              <div class="panel-heading"><h3>调查进度</h3><small>{{ evidence.length }} 条观测证据</small></div>
              <ol class="ops-progress">
                <li v-for="(phase, index) in phases" :key="phase.key" :aria-current="detail.state === 'running' && activePhase === phase.key ? 'step' : undefined" :class="{ reached: seenPhases.includes(phase.key) }">
                  <span>{{ index + 1 }}</span>{{ phase.label }}
                </li>
              </ol>
            </article>
            <OpsEvidenceMetrics :evidence="evidence" :root="report?.root_code" />
            <article v-if="hypotheses.length || Object.keys(excluded).length" class="ops-panel">
              <h3>候选假设与核验</h3>
              <details v-for="(group, index) in hypotheses" :key="index">
                <summary>第 {{ index + 1 }} 轮 · {{ group.length }} 个候选假设</summary>
                <dl class="ops-hypotheses"><div v-for="(item, hypothesisIndex) in group" :key="hypothesisIndex"><dt>{{ labels[item.code] ?? item.code }}</dt><dd>{{ item.reason }}</dd></div></dl>
              </details>
              <dl v-if="Object.keys(excluded).length" class="ops-hypotheses"><div v-for="(reason, key) in excluded" :key="key"><dt>已排除 · {{ labels[key] ?? key }}</dt><dd>{{ reason }}</dd></div></dl>
            </article>
            <article class="ops-panel">
              <div class="panel-heading"><h3>证据明细</h3><small>点击展开原始数据</small></div>
              <p v-if="!evidence.length">等待首条观测证据。</p>
              <details v-for="item in evidence" :key="item.id">
                <summary>{{ item.id }} · {{ labels[item.tool] ?? item.tool }}<span v-if="item.error" class="evidence-tag failed">查询失败</span><span v-else-if="report?.evidence_ids?.includes(item.id)" class="evidence-tag">结论引用</span></summary>
                <pre>{{ JSON.stringify(item, null, 2) }}</pre>
              </details>
            </article>
            <div class="ops-usage"><span>模型请求 <b>{{ usage.requests }}</b> 次</span><span v-if="usage.retrieval">其中检索 <b>{{ usage.retrieval }}</b> 次</span><span>已知用量 <b>{{ usage.tokens.toLocaleString() }}</b> Token</span><span>用量缺失 <b>{{ usage.unknown }}</b> 次</span><span v-if="report?.latency_ms != null">耗时 <b>{{ (report.latency_ms / 1000).toFixed(1) }}</b> 秒</span></div>
          </template>
          <div v-else class="ops-panel ops-empty"><el-icon><Monitor /></el-icon><h2>{{ busy ? '正在创建调查' : '选择一条调查记录' }}</h2><p>关键指标、根因和引用证据会在这里呈现。</p></div>
        </section>
      </div>
    </template>
  </section>
</template>

<style scoped>
.ops-page { max-width: 1240px; margin: auto; color: #1b312c; }
.ops-page h2 { margin: 0 0 12px; font-size: 18px; }
.ops-page h3 { margin: 0; font-size: 15px; }
.ops-heading { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; margin-bottom: 16px; padding: clamp(16px, 3vw, 30px); color: #fff; background: #173c34; border-radius: 14px; }
.ops-heading h1 { margin: 9px 0 4px; font-size: clamp(26px, 3vw, 36px); line-height: 1.2; }
.ops-heading p { margin: 0; color: #d6e9e2; font-size: 14px; }
.ops-eyebrow, .ops-readonly { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; color: #d6e9e2; }
.ops-readonly { flex-shrink: 0; padding: 5px 9px; background: #285448; border-radius: 6px; }
.ops-access { max-width: 520px; margin: 24px auto; }
.ops-access .el-button { margin-top: 16px; }
.ops-toolbar { display: flex; justify-content: flex-end; margin: 14px 0; }
.ops-inputs { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
.ops-note { margin-left: 12px; color: #526961; font-size: 13px; }
.ops-form, .ops-history { border-color: #dce8e5; border-radius: 12px; }
.ops-content { display: grid; grid-template-columns: minmax(170px, 24%) minmax(0, 1fr); gap: 16px; margin-top: 16px; align-items: start; }
.ops-history h2 { display: flex; justify-content: space-between; align-items: center; gap: 10px; }
.ops-history h2 span { padding: 2px 8px; font-size: 12px; color: #315950; background: #e8f3ef; border-radius: 20px; }
.ops-task { display: block; width: 100%; text-align: left; background: #f7faf9; border: 1px solid #dce8e5; border-radius: 8px; padding: 11px; margin-top: 9px; cursor: pointer; color: inherit; overflow-wrap: anywhere; }
.ops-task[aria-pressed='true'] { border-color: #267a66; background: #ecf6f1; }
.ops-task small { display: block; color: #4b6259; margin-top: 7px; }
.status-dot { display: inline-block; width: 7px; height: 7px; margin-right: 5px; border-radius: 50%; background: #7b8c85; }
.status-dot[data-state='completed'] { background: #267a66; }
.status-dot[data-state='running'] { background: #ba7f20; }
.status-dot[data-state='failed'] { background: #ad413b; }
.ops-result { min-width: 0; display: grid; gap: 12px; }
.ops-panel { min-width: 0; padding: clamp(12px, 2vw, 20px); background: #fff; border: 1px solid #dce8e5; border-radius: 12px; overflow-wrap: anywhere; }
.ops-verdict { background: #eff8f4; border-color: #bad9cd; }
.ops-caution { background: #fffbef; border-color: #e7d3a1; }
.ops-route-reasons { margin: 8px 0; padding-left: 20px; font-size: 13px; color: #4b6259; }
.ops-verdict h2 { font-size: clamp(18px, 2.4vw, 26px); margin: 6px 0; }
.ops-verdict p, .ops-empty p { margin: 6px 0; line-height: 1.65; }
.panel-heading { display: flex; justify-content: space-between; flex-wrap: wrap; align-items: center; gap: 8px; }
.panel-heading > span:first-child, .panel-heading small, .scope-line { color: #4b6259; font-size: 12px; }
.scope-line { margin-top: 10px; }
.state-tag, .evidence-tag { padding: 3px 7px; font-size: 12px; color: #225747; background: #dcefe6; border-radius: 5px; }
.state-tag[data-state='running'] { color: #654509; background: #fff0cc; }
.state-tag[data-state='failed'], .evidence-tag.failed { color: #852f29; background: #fbe7e3; }
.ops-advice { margin-top: 12px; border-left: 3px solid #5d947f; padding: 8px 10px; background: #fff; font-size: 13px; }
.ops-progress { list-style: none; display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0 0; padding: 0; }
.ops-progress li { display: inline-flex; align-items: center; gap: 6px; padding: 5px 8px; border-radius: 7px; color: #546961; background: #f2f6f5; font-size: 12px; }
.ops-progress li span { font-weight: 700; }
.ops-progress .reached { background: #e1f1ea; color: #205341; }
.ops-progress [aria-current='step'] { outline: 2px solid #267a66; }
details { margin-top: 9px; border-top: 1px solid #dce8e5; padding-top: 8px; }
summary { cursor: pointer; font-size: 13px; font-weight: 650; }
.evidence-tag { display: inline-block; margin-left: 8px; font-weight: 500; }
.ops-hypotheses { margin: 9px 0; font-size: 13px; }
.ops-hypotheses div + div { margin-top: 8px; }
.ops-hypotheses dt { font-weight: 700; }
.ops-hypotheses dd { margin: 2px 0 0; color: #4b6259; }
.ops-history { border-left: 4px solid #6e807b; }
.ops-history-case { margin-top: 12px; padding: 10px 12px; border: 1px solid #dce8e5; border-radius: 8px; }
.ops-history-case h4 { margin: 0; }
.ops-history-case ul { margin: 6px 0; padding-left: 18px; font-size: 12px; overflow-wrap: anywhere; }
.ops-history-case code { display: block; color: #4b6259; }
.ops-answer, pre { white-space: pre-wrap; overflow-wrap: anywhere; }
pre { max-height: 24rem; overflow: auto; font-size: 12px; background: #f6f8f7; padding: 10px; border-radius: 6px; }
.ops-usage { display: flex; flex-wrap: wrap; gap: 6px 16px; padding: 4px; font-size: 12px; color: #4b6259; }
.ops-empty { text-align: center; padding-block: 36px; color: #4b6259; }
.ops-empty .el-icon { font-size: 26px; margin-bottom: 8px; }
@media(max-width: 760px) {
  .ops-content, .ops-inputs { grid-template-columns: minmax(0, 1fr); }
  .ops-inputs { gap: 0; }
  .ops-note { display: block; margin: 10px 0 0; }
  .ops-history :deep(.el-card__body) { max-height: 200px; overflow: auto; }
}
</style>

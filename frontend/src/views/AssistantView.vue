<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { ChatDotRound, Refresh, Promotion } from '@element-plus/icons-vue'
import { agentApi, type AgentConstraints, type AgentEvent, type AgentStatus, type AgentTurn } from '@/api/agent'
import { useAuthStore } from '@/stores/auth'
import AssistantAnswer from '@/components/AssistantAnswer.vue'
import AgentPreferences from '@/components/AgentPreferences.vue'

const auth = useAuthStore()
const status = ref<AgentStatus>()
const checking = ref(false)
const input = ref('')
const turns = ref<AgentTurn[]>([])
const events = ref<AgentEvent[]>([])
const pendingQuestion = ref('')
const error = ref('')
const busy = ref(false)
const historyLoading = ref(false)
const budget = ref<number>()
const people = ref<number>()
const diningTime = ref('')
const location = ref<AgentConstraints['location']>()
const locating = ref(false)
const recoverTask = ref<string>()
const actionBusy = ref(false)
const session = ref<string>()
const transcript = ref<HTMLElement>()
let controller: AbortController | undefined
let generation = 0
const sessionKey = computed(() => `localperks-agent-session:${auth.user?.id || (auth.isLoggedIn ? 'signed-in' : 'guest')}`)
const agentNames: Record<string, string> = { coordinator: '协调 Agent', supervisor: 'Supervisor', decision: '优惠决策引擎', shop: '找店 Agent', voucher: '优惠券 Agent', order: '订单 Agent' }
const samples = ['帮我找附近适合两个人吃饭、人均100元以内、今晚有优惠的店，推荐最划算的三家', '查询店铺4的优惠券', '优惠券购买时间和使用有效期有什么区别？']
const connectionHint = computed(() => {
  if (!status.value) return '请启动 agent-service，并点击检查连接。'
  if (!status.value.configured) return '模型尚未配置。明确的简单查询仍可使用；开放式问答需在 agent-service/.env 填写模型配置。'
  if (!status.value.redis) return 'Redis 未连接。请先启动 Docker 中的 Redis。'
  if (!status.value.localperks) return 'Java 后端未连接。请先启动 LocalPerks（8081）。'
  return ''
})
const progress = computed(() => {
  const last = events.value.at(-1)
  if (!last) return '正在连接助手…'
  const name = agentNames[last.agent || ''] || '助手'
  if (last.type === 'model_request') return `${name}正在请求模型…`
  if (last.type === 'tool_start') return `${name}正在查询业务接口…`
  if (last.type === 'delegation') return `已委派给${name}`
  return '正在整理查询结果…'
})
const traceTypes = ['route', 'plan', 'node_start', 'node_result', 'delegation', 'model_request', 'tool_result', 'decision', 'rag', 'degradation', 'order_status']
const actionStates: Record<string, string> = { prepared: '待本人确认', running: '提交中，等待核对', unknown: '结果待核对', completed: '已提交', failed: '提交失败' }
const visibleEvents = computed(() => events.value.filter(e => traceTypes.includes(e.type)))
function actionMoney(value: unknown) { return typeof value === 'number' ? `¥${(value / 100).toFixed(2)}` : '金额未知' }

async function check() {
  checking.value = true
  try { status.value = await agentApi.status(); error.value = '' }
  catch { status.value = undefined; error.value = '无法连接 Agent 服务。请在 agent-service 中运行 main.py。' }
  finally { checking.value = false }
}
async function scroll() {
  await nextTick()
  transcript.value?.scrollTo({ top: transcript.value.scrollHeight, behavior: 'smooth' })
}
async function restore() {
  const version = ++generation
  controller?.abort()
  busy.value = false
  historyLoading.value = false
  turns.value = []; events.value = []; pendingQuestion.value = ''; error.value = ''
  session.value = localStorage.getItem(sessionKey.value) || undefined
  recoverTask.value = localStorage.getItem(sessionKey.value + ':task') || undefined
  if (!session.value) return
  historyLoading.value = true
  try {
    const history = await agentApi.history(session.value)
    await Promise.all(history.turns.filter(turn => turn.action_id).map(async turn => {
      try { turn.action = await agentApi.action(turn.action_id!) } catch { /* History remains readable if the action expired or is unavailable. */ }
    }))
    if (version === generation) {
      turns.value = history.turns
      if (recoverTask.value) {
        try {
          const task = await agentApi.task(recoverTask.value)
          if (version === generation && ['completed', 'waiting_confirmation'].includes(task.state)) {
            recoverTask.value = undefined; localStorage.removeItem(sessionKey.value + ':task')
          }
        } catch { /* Keep the task identifier for an explicit recovery attempt. */ }
      }
      void scroll()
    }
  } catch (e) {
    if (version === generation) { localStorage.removeItem(sessionKey.value); session.value = undefined; error.value = e instanceof Error ? e.message : '历史对话不可用' }
  } finally { if (version === generation) historyLoading.value = false }
}
function reset() {
  controller?.abort(); ++generation; busy.value = false
  historyLoading.value = false
  localStorage.removeItem(sessionKey.value); session.value = undefined
  turns.value = []; events.value = []; pendingQuestion.value = ''; error.value = ''
  recoverTask.value = undefined; localStorage.removeItem(sessionKey.value + ':task')
  localStorage.removeItem(sessionKey.value + ':submission')
}
async function locate() {
  if (!navigator.geolocation) { error.value = '浏览器不支持定位，请在问题中提供经度,纬度'; return }
  locating.value = true
  navigator.geolocation.getCurrentPosition(position => {
    location.value = { longitude: position.coords.longitude, latitude: position.coords.latitude }
    locating.value = false; error.value = ''
  }, () => { locating.value = false; error.value = '定位未成功，可以在问题中填写经度,纬度。' }, { timeout: 10000, maximumAge: 60000 })
}
async function transact(turn: AgentTurn, reconcile = false) {
  if (!turn.action || actionBusy.value) return
  actionBusy.value = true
  try { turn.action = await (reconcile ? agentApi.reconcile(turn.action) : agentApi.confirm(turn.action)) }
  catch (e) { error.value = e instanceof Error ? e.message : '操作结果不明，请查询核对，勿重复提交' }
  finally { actionBusy.value = false }
}
async function send(text = input.value, resume = false) {
  if (busy.value || historyLoading.value || !status.value?.ready || !text.trim()) return
  const version = generation
  const aborter = new AbortController()
  const asynchronous = Boolean(auth.isLoggedIn && status.value?.async_tasks)
  let question = text.trim()
  controller = aborter
  busy.value = true; error.value = ''; input.value = ''; events.value = []; pendingQuestion.value = text.trim()
  void scroll()
  try {
    const constraints: AgentConstraints = {}
    if (location.value) constraints.location = location.value
    if (budget.value) constraints.budget_per_person = budget.value
    if (people.value) constraints.people = people.value
    if (diningTime.value) constraints.visit_time = new Date(diningTime.value).toISOString()
    const receive = (event: AgentEvent) => {
      if (version !== generation) return
      if (event.type === 'done') {
        localStorage.removeItem(sessionKey.value + ':submission')
        session.value = event.session_id
        if (event.session_id) localStorage.setItem(sessionKey.value, event.session_id)
        turns.value.push({ user: question, assistant: event.answer || '', trace: event.trace || events.value.slice(), action: event.action, task_id: event.task_id, metrics: event.metrics })
        if (event.state === 'waiting_input' || event.state === 'failed') {
          recoverTask.value = event.task_id
        } else { recoverTask.value = undefined; localStorage.removeItem(sessionKey.value + ':task') }
        pendingQuestion.value = ''; events.value = []; void scroll()
      } else if (event.type === 'task' && event.task_id) {
        recoverTask.value = event.task_id; localStorage.setItem(sessionKey.value + ':task', event.task_id)
      } else if (event.type === 'session' && event.session_id) {
        session.value = event.session_id; localStorage.setItem(sessionKey.value, event.session_id)
      } else { events.value.push(event); void scroll() }
    }
    const conditions = Object.keys(constraints).length ? constraints : undefined
    if (asynchronous && resume && recoverTask.value) {
      const task = await agentApi.task(recoverTask.value)
      if (['accepted', 'running', 'completed'].includes(task.state)) {
        pendingQuestion.value = task.request.message
        question = task.request.message
        await agentApi.followTask(recoverTask.value, aborter.signal, receive)
      } else {
        await agentApi.chat(text.trim(), session.value, aborter.signal, receive, { task_id: recoverTask.value, constraints: conditions })
      }
    } else if (asynchronous) {
      const key = sessionKey.value + ':submission'
      const payload = JSON.stringify({ message: text.trim(), session: session.value, constraints: conditions })
      let pending: { payload: string; id: string } | undefined
      try { pending = JSON.parse(localStorage.getItem(key) || 'null') } catch { /* Replace malformed local state. */ }
      const id = pending?.payload === payload ? pending.id : crypto.randomUUID()
      localStorage.setItem(key, JSON.stringify({ payload, id }))
      const receipt = await agentApi.submit(text.trim(), id, session.value, conditions)
      receive({ type: 'session', session_id: receipt.session_id })
      receive({ type: 'task', task_id: receipt.task_id, state: receipt.state })
      await agentApi.followTask(receipt.task_id, aborter.signal, receive)
    } else {
      await agentApi.chat(text.trim(), session.value, aborter.signal, receive, { ...(resume && recoverTask.value ? { task_id: recoverTask.value } : {}), ...(conditions ? { constraints: conditions } : {}) })
    }
  } catch (e) {
    if (version === generation) {
      error.value = aborter.signal.aborted ? (asynchronous ? '已停止接收。已受理任务继续执行，可恢复任务查询结果。' : '已停止。本轮对话未完成，可重新发送。') : e instanceof Error ? e.message : '请求失败，请稍后重试'
      input.value = text
    }
  } finally { if (version === generation) busy.value = false }
}
function eventLabel(e: AgentEvent) {
  const name = agentNames[e.agent || ''] || '助手'
  if (e.type === 'model_request') return `${name} · 请求 ${e.model} · 第 ${e.step} 步`
  if (e.type === 'delegation') return `委派 ${name}：${e.task}`
  if (e.type === 'tool_result') return `${name} · ${e.tool}`
  if (e.type === 'route') return `路由：${e.intent} · ${e.strategy}`
  if (e.type === 'plan') return '已生成任务依赖图'
  if (e.type === 'node_start') return `${name} · 开始 ${e.node_id}`
  if (e.type === 'node_result') return `${name} · ${e.node_id} · ${e.status || e.state || '已返回'}`
  if (e.type === 'decision') return '已完成优惠资格校验与金额计算'
  if (e.type === 'rag') return `知识检索 · ${e.strategy} · ${Math.round(e.duration_ms || 0)}ms`
  if (e.type === 'degradation') return e.message || '部分能力已降级'
  if (e.type === 'order_status') return `订单跟踪 · ${e.order_id} · ${e.status}`
  return ''
}
watch([() => auth.token, sessionKey], () => { void restore() })
onMounted(() => { void check(); void restore() })
onBeforeUnmount(() => { ++generation; controller?.abort() })
</script>

<template>
  <section class="assistant-page">
    <div class="assistant-heading">
      <div><p class="eyebrow">SHOPLINK ASSISTANT</p><h2><el-icon><ChatDotRound /></el-icon> 说说你想找什么</h2><p>找店、比优惠、查券包，交给助手一起办。</p></div>
      <el-button :icon="Refresh" :disabled="busy" @click="reset">新对话</el-button>
    </div>
    <div class="assistant-status surface">
      <div class="service-pills"><span class="pill" :class="{ online: status?.configured }">模型 {{ status?.configured ? status.model : '未配置' }}</span><span class="pill" :class="{ online: status?.redis }">Redis {{ status?.redis ? '已连接' : '未连接' }}</span><span class="pill" :class="{ online: status?.localperks }">业务服务 {{ status?.localperks ? '已连接' : '未连接' }}</span></div>
      <el-button link :loading="checking" @click="check">检查连接</el-button>
      <p v-if="connectionHint" class="connection-hint">{{ connectionHint }}</p>
    </div>
    <AgentPreferences :key="auth.user?.id || 'guest'" :authenticated="auth.isLoggedIn" :disabled="busy" />
    <details class="surface consumer-conditions"><summary>补充消费条件（可选，文字需求优先）</summary><div>
      <el-input-number v-model="people" :min="1" :max="50" placeholder="人数" aria-label="用餐人数" />
      <el-input-number v-model="budget" :min="1" :max="100000" placeholder="人均预算" aria-label="人均预算" />
      <label>用餐时间 <input v-model="diningTime" type="datetime-local" aria-label="用餐时间" /></label>
      <el-button :loading="locating" :disabled="busy" @click="locate">{{ location ? '更新位置' : '使用我的位置' }}</el-button>
      <span v-if="location" class="muted">{{ location.longitude.toFixed(4) }}, {{ location.latitude.toFixed(4) }}</span>
    </div></details>
    <div v-if="recoverTask && !busy" class="task-recovery surface"><span>有可继续的任务，补充条件后可从保存的状态恢复。</span><el-button :disabled="!status?.ready" @click="send(input.trim() || '继续', true)">恢复任务</el-button></div>
    <div ref="transcript" class="chat-transcript surface" aria-live="polite" aria-label="助手对话">
      <div v-if="!turns.length && !pendingQuestion" class="assistant-welcome">
        <span class="welcome-icon"><el-icon><ChatDotRound /></el-icon></span><h3>从一句话开始</h3><p>助手会查询店铺、优惠券和订单接口。展开调用记录，可以核对实际查询。</p>
        <div class="suggestions"><button v-for="sample in samples" :key="sample" :disabled="!status?.ready || historyLoading" @click="send(sample)">{{ sample }} <span>↗</span></button></div>
        <small>{{ auth.isLoggedIn ? '已登录，可以查询自己的券包与订单。' : '找店和查券无需登录，个人券包需先登录。' }}</small>
      </div>
      <p v-if="historyLoading">正在恢复对话…</p>
      <template v-for="(turn, index) in turns" :key="index">
        <div class="chat-message question"><small>你</small><p>{{ turn.user }}</p></div>
        <div class="chat-message answer"><small>LocalPerks 助手</small><AssistantAnswer :text="turn.assistant" />
          <small v-if="turn.metrics">模型 {{ turn.metrics.model_calls }} 次 · 工具 {{ turn.metrics.tool_calls }} 次 · {{ turn.metrics.total_tokens }} Token · {{ Math.round(turn.metrics.duration_ms) }}ms</small>
          <div v-if="turn.action" class="action-preview">
            <b>{{ turn.action.operation === 'seckill' ? '抢券确认' : turn.action.operation === 'cancel' ? '取消订单确认' : '开发模拟支付确认' }}</b>
            <template v-if="turn.action.operation === 'seckill'"><p>{{ turn.action.preview.title }}（券 {{ turn.action.preview.voucher_id }}）</p><p>券售价：<strong>{{ actionMoney(turn.action.preview.pay_cents) }}</strong> · 面值：{{ actionMoney(turn.action.preview.face_cents) }}</p><p v-if="turn.action.preview.rules">使用规则：{{ turn.action.preview.rules }}</p></template>
            <p v-else>订单号：{{ turn.action.preview.order_id }}</p>
            <p>{{ turn.action.preview.notice }}</p>
            <p>操作状态：{{ actionStates[turn.action.state] || turn.action.state }}</p>
            <el-button v-if="turn.action.state === 'prepared'" type="primary" :loading="actionBusy" @click="transact(turn)">确认提交此操作</el-button>
            <el-button v-if="['running','unknown'].includes(turn.action.state)" :loading="actionBusy" @click="transact(turn, true)">查询核对结果</el-button>
            <details v-if="turn.action.result"><summary>查看业务返回结果</summary><pre>{{ JSON.stringify(turn.action.result, null, 2) }}</pre></details>
          </div>
          <details v-if="turn.trace.length" class="call-trace"><summary>查看模型与接口调用记录</summary><div v-for="(event, i) in turn.trace.filter(e => traceTypes.includes(e.type))" :key="i" class="trace-entry"><p>{{ eventLabel(event) }}</p><code v-for="(request, j) in event.requests" :key="j">{{ request.method }} {{ request.path }} → {{ request.status ?? '连接失败' }}</code><details v-if="event.type === 'tool_result'"><summary>接口结果</summary><pre>{{ JSON.stringify(event.result, null, 2) }}</pre></details></div></details>
        </div>
      </template>
      <template v-if="pendingQuestion">
        <div class="chat-message question"><small>你</small><p>{{ pendingQuestion }}</p></div>
        <div class="chat-message answer"><p v-if="busy" class="working"><span />{{ progress }}</p><div v-for="(event, i) in visibleEvents" :key="i" class="trace-entry"><p>{{ eventLabel(event) }}</p><code v-for="(request, j) in event.requests" :key="j">{{ request.method }} {{ request.path }} → {{ request.status ?? '连接失败' }}</code></div></div>
      </template>
    </div>
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <form class="chat-composer surface" @submit.prevent="send()">
      <el-input v-model="input" type="textarea" :autosize="{ minRows: 2, maxRows: 5 }" :maxlength="4000" :disabled="busy || historyLoading" placeholder="例如：找一家咖啡店，看看它还有什么优惠券…" aria-label="输入给助手的问题" @keydown.ctrl.enter.prevent="send()" />
      <div><small>金额估算与库存仅供参考；交易必须由本人核对并确认。</small><el-button v-if="busy" @click="controller?.abort()">停止</el-button><el-button v-else type="primary" native-type="submit" :icon="Promotion" :disabled="!input.trim() || !status?.ready || historyLoading">发送</el-button></div>
    </form>
    <nav class="assistant-shortcuts"><router-link to="/shops">逛店铺</router-link><router-link to="/vouchers">领券中心</router-link><router-link to="/my-vouchers">我的券包</router-link></nav>
  </section>
</template>

<style scoped>
.assistant-page { max-width: 1000px; margin: 0 auto; display: grid; gap: 16px; }
.assistant-heading { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.assistant-heading h2 { display: flex; align-items: center; gap: 10px; margin: 6px 0; font-size: clamp(23px, 4vw, 30px); }
.assistant-heading p { margin: 6px 0; }
.assistant-status { display: flex; align-items: center; flex-wrap: wrap; padding: 12px 16px; gap: 10px; }
.service-pills { display: flex; flex-wrap: wrap; gap: 7px; flex: 1; }
.service-pills .pill { background: #f2f0e9; color: #6c736d; overflow-wrap: anywhere; }
.service-pills .online { background: #e7f1e7; color: #2d6243; }
.connection-hint { flex-basis: 100%; font-size: 13px; margin: 0; color: #916524; }
.chat-transcript { min-height: 340px; max-height: 62vh; overflow-y: auto; padding: 24px; scroll-behavior: smooth; }
.assistant-welcome { max-width: 640px; margin: 14px auto; text-align: center; }
.welcome-icon { display: inline-grid; place-items: center; width: 52px; height: 52px; background: #e7f1e7; color: #37674a; border-radius: 17px; font-size: 27px; }
.assistant-welcome h3 { margin: 12px 0 8px; font-size: 22px; }
.assistant-welcome p, .assistant-welcome small { color: #6b776d; line-height: 1.8; }
.suggestions { display: grid; gap: 8px; margin: 20px 0; }
.suggestions button { display: flex; align-items: center; justify-content: space-between; gap: 12px; text-align: left; padding: 13px 16px; border: 1px solid #dfe6dc; border-radius: 12px; background: #fafbf7; font: inherit; color: #35533e; cursor: pointer; }
.suggestions button:disabled { opacity: .6; cursor: default; }
.suggestions button:hover:enabled { border-color: #709d75; background: #f1f6ed; }
.chat-message { margin-bottom: 22px; max-width: 94%; }
.chat-message small { color: #748174; }
.chat-message > p { margin: 7px 0; white-space: pre-wrap; overflow-wrap: anywhere; line-height: 1.85; }
.question { margin-left: auto; background: #edf3e8; border-radius: 15px 15px 3px 15px; padding: 12px 16px; width: fit-content; }
.answer { padding: 3px 0; }
.call-trace { margin-top: 14px; font-size: 12px; color: #637564; }
summary { cursor: pointer; padding: 5px 0; }
.trace-entry { border-left: 2px solid #dce9d7; margin: 8px 0; padding: 2px 12px; overflow-wrap: anywhere; font-size: 12px; }
.trace-entry p { margin: 3px 0; }
.trace-entry code { display: block; margin: 5px 0; color: #3c6b4b; }
.trace-entry pre { white-space: pre-wrap; overflow-wrap: anywhere; background: #f5f7f2; padding: 12px; border-radius: 8px; max-height: 260px; overflow-y: auto; }
.working { display: flex; align-items: center; gap: 9px; color: #46724e; }
.working span { height: 8px; width: 8px; border-radius: 50%; background: #62905d; animation: pulse 1.4s infinite; }
@keyframes pulse { 50% { opacity: .3; } }
.chat-composer { padding: 14px; }
.chat-composer > div:last-child { display: flex; justify-content: space-between; gap: 12px; align-items: center; margin-top: 12px; }
.chat-composer small { color: #778172; font-size: 12px; }
.assistant-shortcuts { display: flex; justify-content: center; gap: 24px; color: #547554; font-size: 13px; }
.consumer-conditions { padding: 10px 15px; font-size: 13px; }
.consumer-conditions > div { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; padding: 12px 0; }
.consumer-conditions input[type=datetime-local] { max-width: 100%; border: 1px solid #dce3d7; border-radius: 6px; padding: 7px; }
.task-recovery { padding: 12px 16px; display: flex; gap: 12px; align-items: center; justify-content: space-between; font-size: 13px; }
.action-preview { background: #faf5e9; padding: 14px; border: 1px solid #e6d8b8; border-radius: 12px; margin: 12px 0; }
.action-preview pre { white-space: pre-wrap; overflow-wrap: anywhere; font-size: 12px; }
@media (max-width: 640px) { .chat-transcript { padding: 16px; max-height: 57vh; } .assistant-heading { align-items: flex-start; } .assistant-heading p:not(.eyebrow) { font-size: 13px; } .chat-message { max-width: 100%; } .chat-composer small { max-width: 68%; } }
</style>

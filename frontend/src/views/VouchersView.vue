<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { ArrowRight, Clock, Refresh, Ticket } from '@element-plus/icons-vue'
import { voucherApi } from '@/api/voucher'
import { useAuthStore } from '@/stores/auth'
import type { Voucher } from '@/types/domain'
import { formatVoucherPrice, formatVoucherTime, voucherAvailability } from '@/utils/vouchers'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()
const shopId = computed(() => Number(route.query.shopId) || undefined)
const vouchers = ref<Voucher[]>([])
const loading = ref(false)
const loadError = ref(false)
const submitting = ref<number>()
const keyword = ref('')
const filter = ref<'all' | 'seckill' | 'regular'>('all')
const now = ref(Date.now())
let clock: number | undefined
let stockTimer: number | undefined
const dialog = ref(false)
const creating = ref(false)
const seckill = ref(true)
const form = reactive({ shopId: undefined as number | undefined, title: '', rules: '', payValue: 0,
  actualValue: 0, stock: 10, beginTime: '', endTime: '' })

const scopedVouchers = computed(() => vouchers.value.filter(v => !shopId.value || v.shopId === shopId.value))
const shopName = computed(() => scopedVouchers.value[0]?.shopName || '店铺 ' + shopId.value)
const activeCount = computed(() => scopedVouchers.value.filter(v => voucherAvailability(v, now.value).canClaim).length)
const filtered = computed(() => {
  const query = keyword.value.trim().toLowerCase()
  return scopedVouchers.value.filter(voucher => {
    if (filter.value === 'seckill' && voucher.type !== 1) return false
    if (filter.value === 'regular' && voucher.type === 1) return false
    return !query || [voucher.shopName, voucher.title, voucher.subTitle].some(value => value?.toLowerCase().includes(query))
  })
})

async function load() {
  if (loading.value) return
  loading.value = true
  loadError.value = false
  try {
    vouchers.value = await voucherApi.market()
  } catch {
    loadError.value = true
  } finally {
    loading.value = false
  }
}

async function submit(voucher: Voucher) {
  if (!voucher.id || submitting.value || !voucherAvailability(voucher, Date.now()).canClaim) return
  if (!auth.isLoggedIn) {
    await router.push({ name: 'login', query: { redirect: route.fullPath } })
    return
  }
  submitting.value = voucher.id
  try {
    const result = await voucherApi.seckill(voucher.id)
    await router.push('/orders/' + result.orderId)
  } catch {
    await load()
  } finally {
    submitting.value = undefined
  }
}

function openCreate() {
  form.shopId = shopId.value
  dialog.value = true
}

async function create() {
  if (!form.shopId || !form.title.trim() || form.actualValue <= 0) {
    ElMessage.warning('请填写店铺 ID、券名和抵扣金额')
    return
  }
  if (seckill.value && (!form.beginTime || !form.endTime || form.beginTime >= form.endTime || form.stock < 1)) {
    ElMessage.warning('请设置有效的活动时段和库存')
    return
  }
  creating.value = true
  const data: Voucher = {
    ...form,
    title: form.title.trim(),
    payValue: Math.round(form.payValue * (seckill.value ? 100 : 1)),
    actualValue: Math.round(form.actualValue * (seckill.value ? 100 : 1)),
    type: seckill.value ? 1 : 0,
    status: 1,
  }
  if (!seckill.value) {
    delete data.beginTime
    delete data.endTime
    delete data.stock
  }
  try {
    await (seckill.value ? voucherApi.createSeckill(data) : voucherApi.create(data))
    dialog.value = false
    ElMessage.success('优惠券已发布')
    await load()
  } catch {
    // API errors are shown by the response interceptor.
  } finally {
    creating.value = false
  }
}

onMounted(() => {
  void load()
  clock = window.setInterval(() => { now.value = Date.now() }, 1000)
  stockTimer = window.setInterval(() => {
    if (!document.hidden) void load()
  }, 10000)
})
onUnmounted(() => {
  window.clearInterval(clock)
  window.clearInterval(stockTimer)
})
</script>

<template>
  <section class="feature-banner voucher-banner">
    <div>
      <p class="eyebrow">SHOPLINK · 优惠活动</p>
      <h2>{{ shopId ? shopName : '领券中心' }}</h2>
      <p>{{ shopId ? '浏览这家店的优惠和限时秒杀。' : '发现附近商家的优惠，开抢后先到先得。' }}</p>
    </div>
    <router-link class="banner-link" to="/my-vouchers"><el-icon><Ticket /></el-icon> 我的券 <el-icon><ArrowRight /></el-icon></router-link>
  </section>

  <div class="surface voucher-filters">
    <el-radio-group v-model="filter" aria-label="优惠券类型">
      <el-radio-button value="all">全部优惠</el-radio-button>
      <el-radio-button value="seckill">限时秒杀</el-radio-button>
      <el-radio-button value="regular">普通券</el-radio-button>
    </el-radio-group>
    <el-input v-model="keyword" placeholder="搜索商家或券名" clearable aria-label="搜索商家或券名" />
    <el-button :icon="Refresh" :loading="loading" @click="load">刷新库存</el-button>
    <router-link v-if="shopId" class="text-link" to="/vouchers">查看全部商家</router-link>
  </div>

  <div class="section-heading">
    <div><h3>优惠列表</h3><p class="muted">共 {{ filtered.length }} 张优惠券 · {{ activeCount }} 场活动可抢 · 库存每 10 秒刷新</p></div>
    <el-button link @click="openCreate">创建优惠券（开发）</el-button>
  </div>

  <el-alert v-if="loadError" class="list-alert" title="优惠加载失败，请刷新重试。" type="error" :closable="false" />
  <el-skeleton v-if="loading && !vouchers.length" :rows="5" animated />
  <el-empty v-else-if="!filtered.length && !loadError" description="当前条件下暂无优惠券" />
  <div v-else class="deal-grid">
    <article v-for="voucher in filtered" :key="voucher.id" class="surface deal-card">
      <div class="deal-top">
        <span class="pill" :class="{ 'pill-warm': voucher.type === 1 }">{{ voucher.type === 1 ? '限时秒杀' : '普通优惠' }}</span>
        <router-link v-if="voucher.shopId" class="shop-link" :to="'/shops/' + voucher.shopId">{{ voucher.shopName || '店铺详情' }}</router-link>
      </div>
      <h3>{{ voucher.title }}</h3>
      <p class="deal-description">{{ voucher.subTitle || voucher.rules || '到店使用，详情见商家说明' }}</p>
      <div class="deal-amount">
        <strong>{{ formatVoucherPrice(voucher.payValue, voucher.type === 1) }}</strong>
        <span>抵 {{ formatVoucherPrice(voucher.actualValue, voucher.type === 1) }}</span>
      </div>
      <div v-if="voucher.type === 1" class="deal-foot">
        <p class="muted"><el-icon><Clock /></el-icon> {{ formatVoucherTime(voucher.beginTime) }} 至 {{ formatVoucherTime(voucher.endTime) }}</p>
        <p class="muted">每人限领一张，待支付订单请在预留时限内完成</p>
        <div class="deal-action">
          <span>{{ voucher.stock == null ? '库存待同步' : '剩余 ' + voucher.stock + ' 张' }}</span>
          <el-button
            type="primary"
            :disabled="!voucherAvailability(voucher, now).canClaim || submitting != null"
            :loading="submitting === voucher.id"
            @click="submit(voucher)"
          >{{ submitting === voucher.id ? '提交中' : voucherAvailability(voucher, now).label }}</el-button>
        </div>
      </div>
      <div v-else class="deal-foot muted">普通优惠 · 使用规则见商家说明</div>
    </article>
  </div>

  <el-dialog v-model="dialog" title="创建优惠券（开发）" width="min(560px, 94vw)">
    <el-form :model="form" label-width="96px" @submit.prevent>
      <el-form-item label="店铺 ID"><el-input-number v-model="form.shopId" :min="1" /></el-form-item>
      <el-form-item label="秒杀券"><el-switch v-model="seckill" /></el-form-item>
      <el-form-item label="券名"><el-input v-model="form.title" /></el-form-item>
      <el-form-item label="使用规则"><el-input v-model="form.rules" type="textarea" /></el-form-item>
      <el-form-item label="售价（元）"><el-input-number v-model="form.payValue" :min="0" :precision="seckill ? 2 : 0" /></el-form-item>
      <el-form-item label="抵扣（元）"><el-input-number v-model="form.actualValue" :min="0" :precision="seckill ? 2 : 0" /></el-form-item>
      <template v-if="seckill">
        <el-form-item label="库存"><el-input-number v-model="form.stock" :min="1" :precision="0" /></el-form-item>
        <el-form-item label="开始时间"><el-date-picker v-model="form.beginTime" type="datetime" value-format="YYYY-MM-DDTHH:mm:ss" /></el-form-item>
        <el-form-item label="结束时间"><el-date-picker v-model="form.endTime" type="datetime" value-format="YYYY-MM-DDTHH:mm:ss" /></el-form-item>
      </template>
    </el-form>
    <template #footer>
      <el-button @click="dialog = false">取消</el-button>
      <el-button type="primary" :loading="creating" @click="create">发布</el-button>
    </template>
  </el-dialog>
</template>

<style scoped>
.voucher-filters { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; padding: 14px; margin: 18px 0 24px; }
.voucher-filters .el-input { width: min(100%, 240px); margin-left: auto; }
.list-alert { margin-bottom: 14px; }
.deal-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 300px), 1fr)); gap: 16px; }
.deal-card { padding: 20px; display: flex; flex-direction: column; gap: 12px; }
.deal-top, .deal-action { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.shop-link { color: var(--ink-muted); font-size: 13px; text-align: right; }
.shop-link:hover { color: var(--accent); }
.deal-card h3, .deal-card p { margin: 0; }
.deal-card h3 { font-size: 19px; }
.deal-description { color: var(--ink-muted); font-size: 14px; }
.deal-amount { display: flex; align-items: baseline; gap: 9px; }
.deal-amount strong { color: var(--accent); font-size: 28px; letter-spacing: -1px; }
.deal-amount span { color: var(--ink-muted); font-size: 13px; }
.deal-foot { margin-top: auto; border-top: 1px dashed var(--line); padding-top: 12px; }
.deal-foot .el-icon { vertical-align: -2px; }
.deal-action { margin-top: 10px; font-size: 14px; }
@media (max-width: 640px) {
  .voucher-filters .el-input { margin-left: 0; }
}
</style>

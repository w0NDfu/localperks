<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { Refresh, Ticket } from '@element-plus/icons-vue'
import { voucherApi } from '@/api/voucher'
import type { VoucherWallet } from '@/types/domain'
import { formatVoucherPrice, formatVoucherTime } from '@/utils/vouchers'

const filter = ref<'all' | 'owned'>('all')
const page = ref(1)
const wallet = ref<VoucherWallet>()
const loading = ref(false)
const error = ref(false)
let requestVersion = 0
let refreshTimer: number | undefined
const records = computed(() => wallet.value?.records ?? [])
const emptyText = computed(() => filter.value === 'owned' ? '你的券包暂时没有已获得的券' : '你还没有抢券记录')

async function load() {
  const version = ++requestVersion
  loading.value = true
  error.value = false
  try {
    const result = await voucherApi.mine(page.value, filter.value === 'owned')
    if (version === requestVersion) wallet.value = result
  } catch {
    if (version === requestVersion) error.value = true
  } finally {
    if (version === requestVersion) loading.value = false
  }
}

watch([page, filter], () => void load(), { immediate: true })
onMounted(() => {
  refreshTimer = window.setInterval(() => {
    if (!loading.value && !error.value && !document.hidden &&
        records.value.some(item => item.statusCode === 10 || item.statusCode === 11)) void load()
  }, 3000)
})
onUnmounted(() => {
  requestVersion++
  window.clearInterval(refreshTimer)
})

function changeFilter() {
  page.value = 1
  wallet.value = undefined
}
</script>

<template>
  <section class="feature-banner wallet-banner">
    <div>
      <p class="eyebrow">个人券包</p>
      <h2>我的券</h2>
      <p>在这里查看已获得的券，以及排队、待支付和已结束的抢券记录。</p>
    </div>
    <div class="wallet-count">
      <el-icon><Ticket /></el-icon>
      <strong data-testid="owned-voucher-count">{{ wallet?.availableCount ?? '—' }}</strong>
      <span>张已获得</span>
    </div>
  </section>

  <div class="section-heading wallet-heading">
    <el-radio-group v-model="filter" aria-label="券包筛选" @change="changeFilter">
      <el-radio-button value="all">全部记录</el-radio-button>
      <el-radio-button value="owned">已获得</el-radio-button>
    </el-radio-group>
    <el-button :icon="Refresh" :loading="loading" @click="load">刷新券包</el-button>
  </div>

  <el-alert v-if="error" title="券包加载失败，请刷新重试。" type="error" :closable="false" />
  <el-skeleton v-else-if="loading && !records.length" :rows="5" animated />
  <el-empty v-else-if="!records.length" :description="emptyText">
    <router-link class="text-link" to="/vouchers">去领券中心</router-link>
  </el-empty>
  <div v-else class="wallet-list" v-loading="loading">
    <article v-for="item in records" :key="item.orderId" class="surface wallet-card">
      <div>
        <p class="eyebrow">{{ item.shopName || '店铺优惠' }}</p>
        <h3>{{ item.title || '秒杀优惠券' }}</h3>
        <p class="muted">{{ formatVoucherTime(item.createTime) }} · 订单 {{ item.orderId }}</p>
      </div>
      <div class="wallet-card-action">
        <span class="pill" :class="{ 'pill-green': item.statusCode === 13, 'pill-warm': item.statusCode === 12 }">
          {{ item.statusCode === 13 ? '已获得 · 1 张' : item.statusText }}
        </span>
        <strong>{{ formatVoucherPrice(item.actualValue) }}</strong>
        <router-link class="text-link" :to="'/orders/' + item.orderId">{{ item.statusCode === 12 ? '去支付' : '查看订单' }}</router-link>
      </div>
    </article>
    <el-pagination v-if="(wallet?.total ?? 0) > 12" v-model:current-page="page" layout="prev, pager, next" :page-size="12" :total="wallet?.total" />
  </div>
</template>

<style scoped>
.wallet-count { display: flex; align-items: baseline; gap: 8px; font-weight: 600; white-space: nowrap; }
.wallet-count strong { font-size: 38px; }
.wallet-heading { margin-top: 22px; }
.wallet-list { display: grid; gap: 12px; }
.wallet-card { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 18px; padding: 20px; }
.wallet-card h3 { margin: 6px 0; }
.wallet-card p { margin: 0; overflow-wrap: anywhere; }
.wallet-card-action { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; }
.wallet-card-action strong { color: var(--accent); }
</style>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { Location, Search, Shop as ShopIcon } from '@element-plus/icons-vue'
import { shopApi } from '@/api/shop'
import type { Shop, ShopType } from '@/types/domain'
import { splitImages, toImageUrl } from '@/utils/image'

const route = useRoute()
const router = useRouter()
const types = ref<ShopType[]>([])
const shops = ref<Shop[]>([])
const loading = ref(false)
const loadError = ref(false)
const current = ref(1)
const selectedType = ref<number | undefined>(Number(route.query.typeId) || undefined)
const keyword = ref('')
const x = ref<number>()
const y = ref<number>()
const mode = ref<'type' | 'name' | 'nearby'>('type')
const pageSize = computed(() => mode.value !== 'name' && selectedType.value ? 5 : 10)
const categoryName = (id?: number) => types.value.find(type => type.id === id)?.name
let requestVersion = 0

async function load() {
  const version = ++requestVersion
  loading.value = true
  loadError.value = false
  try {
    const result = mode.value !== 'name' && selectedType.value
      ? await shopApi.byType({
        typeId: selectedType.value,
        current: current.value,
        x: mode.value === 'nearby' ? x.value : undefined,
        y: mode.value === 'nearby' ? y.value : undefined,
      })
      : await shopApi.byName(keyword.value.trim(), current.value)
    if (version === requestVersion) shops.value = result
  } catch {
    if (version === requestVersion) loadError.value = true
  } finally {
    if (version === requestVersion) loading.value = false
  }
}

function selectType(id?: number) {
  selectedType.value = id
  keyword.value = ''
  mode.value = 'type'
  current.value = 1
  void load()
}

function search() {
  mode.value = keyword.value.trim() ? 'name' : 'type'
  current.value = 1
  void load()
}

function nearby() {
  if (!selectedType.value || x.value == null || y.value == null) return
  mode.value = 'nearby'
  current.value = 1
  void load()
}

function goTo(page: number) {
  current.value = page
  void load()
}

onMounted(() => {
  void shopApi.types().then(result => { types.value = result }).catch(() => undefined)
  void load()
})
</script>

<template>
  <div class="page-head">
    <div><p class="eyebrow">逛逛附近</p><h2>发现好店</h2><p class="muted">按店名或分类找到感兴趣的商家。</p></div>
    <el-button plain @click="router.push('/shops/new')">新增店铺</el-button>
  </div>
  <div class="toolbar">
    <el-select v-model="selectedType" placeholder="全部分类" clearable aria-label="店铺分类" class="shop-category" @change="selectType">
      <el-option v-for="type in types" :key="type.id" :label="type.name" :value="type.id" />
    </el-select>
    <el-input v-model="keyword" clearable placeholder="按店铺名搜索" aria-label="店铺名称" class="shop-search" @keyup.enter="search" />
    <el-button type="primary" :icon="Search" @click="search">搜索</el-button>
    <details class="nearby">
      <summary>按坐标查附近</summary>
      <div>
        <el-input-number v-model="x" :precision="6" :min="-180" :max="180" placeholder="经度" />
        <el-input-number v-model="y" :precision="6" :min="-85.051128" :max="85.051128" placeholder="纬度" />
        <el-button :disabled="!selectedType || x == null || y == null" @click="nearby">附近查询</el-button>
      </div>
    </details>
  </div>

  <el-alert v-if="loadError" title="店铺加载失败，可重新搜索。" type="error" :closable="false" />
  <el-skeleton v-else-if="loading && !shops.length" :rows="5" animated />
  <el-empty v-else-if="!shops.length" description="当前条件下暂无店铺" />
  <div v-else v-loading="loading" class="grid grid-3">
    <article v-for="shop in shops" :key="shop.id" class="surface shop-card">
      <el-image v-if="splitImages(shop.images).length" :src="toImageUrl(splitImages(shop.images)[0])" class="shop-image" fit="cover" loading="lazy">
        <template #error><div class="shop-placeholder"><el-icon><ShopIcon /></el-icon></div></template>
      </el-image>
      <div v-else class="shop-placeholder shop-image"><el-icon><ShopIcon /></el-icon></div>
      <div class="shop-content">
        <div class="shop-labels">
          <span class="pill">{{ categoryName(shop.typeId) || '本地店铺' }}</span>
          <span v-if="shop.score" class="rating">{{ (shop.score / 10).toFixed(1) }} 分</span>
        </div>
        <h3>{{ shop.name }}</h3>
        <p class="muted address"><el-icon><Location /></el-icon> {{ shop.address || shop.area || '地址待补充' }}</p>
        <p class="muted">{{ shop.avgPrice == null ? '人均待补充' : '人均 ¥' + shop.avgPrice }}<span v-if="shop.distance != null"> · {{ Math.round(shop.distance) }} m</span></p>
        <div class="shop-actions">
          <el-button link type="primary" @click="router.push('/shops/' + shop.id)">查看店铺</el-button>
          <el-button link @click="router.push('/shops/' + shop.id + '/edit')">编辑</el-button>
        </div>
      </div>
    </article>
  </div>
  <div class="pager">
    <el-button :disabled="loading || current === 1" @click="goTo(current - 1)">上一页</el-button>
    <span>第 {{ current }} 页</span>
    <el-button :disabled="loading || shops.length < pageSize" @click="goTo(current + 1)">下一页</el-button>
  </div>
</template>

<style scoped>
.shop-category { width: min(100%, 155px); }
.shop-search { width: min(100%, 275px); }
.nearby { color: var(--ink-muted); font-size: 13px; }
.nearby summary { cursor: pointer; }
.nearby div { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }
.shop-card { overflow: hidden; min-width: 0; }
.shop-image { display: block; width: 100%; height: 150px; }
.shop-placeholder { display: grid; width: 100%; height: 150px; place-items: center; color: #527762; background: #eaf0e7; font-size: 32px; }
.shop-content { padding: 16px; }
.shop-content h3 { margin: 10px 0 8px; font-size: 18px; }
.shop-labels, .shop-actions { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.address .el-icon { vertical-align: -2px; }
.rating { color: var(--accent); font-weight: 700; font-size: 13px; }
.pager { display: flex; justify-content: center; align-items: center; gap: 14px; margin-top: 20px; color: var(--ink-muted); font-size: 13px; }
</style>

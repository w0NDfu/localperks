<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { Location, Shop as ShopIcon } from '@element-plus/icons-vue'
import { shopApi } from '@/api/shop'
import { voucherApi } from '@/api/voucher'
import type { Shop, Voucher } from '@/types/domain'
import { splitImages, toImageUrl } from '@/utils/image'
import { formatVoucherPrice } from '@/utils/vouchers'

const route = useRoute()
const id = computed(() => Number(route.params.id))
const shop = ref<Shop>()
const vouchers = ref<Voucher[]>([])
const images = computed(() => splitImages(shop.value?.images))
const error = ref(false)

async function load() {
  const shopId = id.value
  shop.value = undefined
  vouchers.value = []
  error.value = false
  void voucherApi.list(shopId).then(items => {
    if (shopId === id.value) vouchers.value = items
  }).catch(() => undefined)
  try {
    const result = await shopApi.get(shopId)
    if (shopId === id.value) shop.value = result
  } catch {
    if (shopId === id.value) error.value = true
  }
}

watch(id, () => void load(), { immediate: true })
</script>

<template>
  <el-alert v-if="error" title="店铺加载失败，可返回店铺列表重试。" type="error" :closable="false" />
  <el-skeleton v-else-if="!shop" :rows="8" animated />
  <div v-else>
    <div class="page-head">
      <div>
        <p class="eyebrow">{{ shop.area || '店铺详情' }}</p>
        <h2>{{ shop.name }}</h2>
        <p class="muted"><el-icon><Location /></el-icon> {{ shop.address || '地址待补充' }}</p>
      </div>
      <router-link class="text-link" :to="'/shops/' + id + '/edit'">编辑店铺</router-link>
    </div>
    <div class="grid grid-2">
      <section class="surface shop-details">
        <el-carousel v-if="images.length" height="230px" :autoplay="images.length > 1" :arrow="images.length > 1 ? 'hover' : 'never'">
          <el-carousel-item v-for="image in images" :key="image">
            <el-image :src="toImageUrl(image)" fit="cover" class="detail-image">
              <template #error><div class="shop-placeholder"><el-icon><ShopIcon /></el-icon></div></template>
            </el-image>
          </el-carousel-item>
        </el-carousel>
        <div v-else class="shop-placeholder"><el-icon><ShopIcon /></el-icon></div>
        <dl>
          <div><dt>营业时间</dt><dd>{{ shop.openHours || '待补充' }}</dd></div>
          <div><dt>人均</dt><dd>{{ shop.avgPrice == null ? '—' : '¥' + shop.avgPrice }}</dd></div>
          <div><dt>评分</dt><dd>{{ shop.score ? (shop.score / 10).toFixed(1) + ' 分' : '—' }}</dd></div>
          <div><dt>销量</dt><dd>{{ shop.sold ?? 0 }}</dd></div>
        </dl>
      </section>
      <section class="surface shop-vouchers">
        <div class="section-heading">
          <h3>到店优惠</h3>
          <router-link class="text-link" :to="{ path: '/vouchers', query: { shopId: id } }">查看全部</router-link>
        </div>
        <el-empty v-if="!vouchers.length" description="暂无优惠券" />
        <article v-for="voucher in vouchers" :key="voucher.id" class="shop-voucher">
          <div><b>{{ voucher.title }}</b><p class="muted">{{ voucher.subTitle || voucher.rules }}</p></div>
          <div>
            <strong>{{ formatVoucherPrice(voucher.payValue, voucher.type === 1) }}</strong>
            <span class="muted"> 抵 {{ formatVoucherPrice(voucher.actualValue, voucher.type === 1) }}</span>
          </div>
          <router-link v-if="voucher.type === 1" class="text-link" :to="{ path: '/vouchers', query: { shopId: id } }">
            {{ voucher.stock == null ? '查看秒杀' : '剩余 ' + voucher.stock + ' 张 · 去抢券' }}
          </router-link>
        </article>
      </section>
    </div>
  </div>
</template>

<style scoped>
.shop-details { overflow: hidden; }
.detail-image { width: 100%; height: 230px; }
.shop-placeholder { display: grid; place-items: center; height: 230px; background: #eaf0e7; color: #527762; font-size: 36px; }
.shop-details dl { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; padding: 18px; margin: 0; }
.shop-details dt { color: var(--ink-muted); font-size: 13px; margin-bottom: 5px; }
.shop-details dd { margin: 0; }
.shop-vouchers { padding: 18px; }
.shop-voucher { display: grid; gap: 8px; padding: 14px 0; border-top: 1px dashed var(--line); }
.shop-voucher p { margin: 6px 0 0; }
.shop-voucher strong { color: var(--accent); }
</style>

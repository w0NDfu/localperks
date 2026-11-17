<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ArrowRight, Shop, Ticket } from '@element-plus/icons-vue'
import { shopApi } from '@/api/shop'
import { blogApi } from '@/api/blog'
import { voucherApi } from '@/api/voucher'
import type { Blog, ShopType, Voucher } from '@/types/domain'
import { formatVoucherPrice, voucherAvailability } from '@/utils/vouchers'
import BlogCard from '@/components/BlogCard.vue'
import { useRouter } from 'vue-router'

const router = useRouter()
const types = ref<ShopType[]>([])
const blogs = ref<Blog[]>([])
const deals = ref<Voucher[]>([])
onMounted(() => {
  void shopApi.types().then(items => { types.value = items }).catch(() => undefined)
  void blogApi.hot().then(items => { blogs.value = items }).catch(() => undefined)
  void voucherApi.market().then(items => {
    deals.value = items.filter(item => voucherAvailability(item, Date.now()).canClaim).slice(0, 3)
  }).catch(() => undefined)
})
</script>

<template>
  <section class="feature-banner home-banner">
    <div>
      <p class="eyebrow">探索身边的好店</p>
      <h2 data-testid="home-title">今天去哪儿？</h2>
      <p>从一杯咖啡到一顿聚餐，发现身边的店铺和限时优惠。</p>
      <div class="hero-actions">
        <router-link class="banner-link" to="/vouchers"><el-icon><Ticket /></el-icon> 去领券 <el-icon><ArrowRight /></el-icon></router-link>
        <router-link class="hero-secondary" to="/shops"><el-icon><Shop /></el-icon> 逛店铺</router-link>
        <router-link class="hero-secondary" to="/assistant">让 AI 帮我选</router-link>
      </div>
    </div>
    <img src="/seed-images/localperks-hangzhou.svg" alt="" />
  </section>

  <section class="home-section">
    <div class="section-heading"><h3>限时秒杀</h3><router-link class="text-link" to="/vouchers">全部活动</router-link></div>
    <div v-if="deals.length" class="grid grid-3">
      <router-link v-for="voucher in deals" :key="voucher.id" class="surface offer-preview" :to="{ path: '/vouchers', query: { shopId: voucher.shopId } }">
        <p class="eyebrow">{{ voucher.shopName }}</p>
        <h4>{{ voucher.title }}</h4>
        <div><b>{{ formatVoucherPrice(voucher.payValue) }}</b><span class="pill pill-warm">剩余 {{ voucher.stock }} 张</span></div>
      </router-link>
    </div>
    <div v-else class="surface empty">浏览领券中心查看当前优惠</div>
  </section>

  <section class="home-section">
    <div class="section-heading"><h3>按类别找店</h3><router-link class="text-link" to="/shops">全部店铺</router-link></div>
    <div class="categories">
      <router-link v-for="type in types" :key="type.id" class="surface category" :to="{ name: 'shops', query: { typeId: type.id } }">
        <span>{{ type.name.slice(0, 1) }}</span>{{ type.name }}
      </router-link>
    </div>
  </section>

  <section class="home-section">
    <div class="section-heading"><h3>热门笔记</h3><router-link class="text-link" to="/blogs">查看全部</router-link></div>
    <div v-if="blogs.length" class="grid grid-3">
      <BlogCard v-for="blog in blogs.slice(0, 6)" :key="blog.id" :blog="blog" @open="router.push('/blogs/' + $event)" />
    </div>
    <div v-else class="surface empty">暂时没有热门笔记</div>
  </section>
</template>

<style scoped>
.home-banner { position: relative; overflow: hidden; min-height: 245px; }
.home-banner > div { position: relative; z-index: 1; }
.home-banner img { position: absolute; right: 0; top: 0; width: 51%; height: 100%; object-fit: cover; opacity: .85; mask-image: linear-gradient(to right, transparent, black 34%); }
.hero-actions { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 18px; }
.hero-secondary { display: flex; align-items: center; gap: 6px; padding: 9px 14px; border: 1px solid #a9c2b5; border-radius: 9px; }
.home-section { margin-top: 26px; }
.offer-preview { padding: 15px 18px; }
.offer-preview:hover { border-color: var(--accent); }
.offer-preview h4 { margin: 6px 0 12px; }
.offer-preview div { display: flex; align-items: center; justify-content: space-between; }
.offer-preview b { font-size: 20px; color: var(--accent); }
.categories { display: flex; flex-wrap: wrap; gap: 10px; }
.category { display: flex; align-items: center; gap: 10px; padding: 9px 14px; }
.category:hover { border-color: var(--accent); }
.category span { display: grid; place-items: center; width: 29px; height: 29px; color: #356147; border-radius: 8px; background: #edf3ea; font-weight: 650; }
@media (max-width: 640px) { .home-banner img { opacity: .28; width: 72%; } }
</style>

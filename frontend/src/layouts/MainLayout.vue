<script setup lang="ts">
import { computed } from 'vue'
import { House, Shop, Document, User, Ticket, Plus, SwitchButton, Connection, Wallet, ChatDotRound } from '@element-plus/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

const router = useRouter()
const route = useRoute()
const auth = useAuthStore()
const active = computed(() => {
  if (route.path.startsWith('/orders/')) return '/my-vouchers'
  if (route.path.startsWith('/shops/')) return '/shops'
  if (route.path.startsWith('/blogs/')) return '/blogs'
  return route.path
})
const links = [
  { path: '/', label: '首页', icon: House },
  { path: '/assistant', label: 'AI 助手', icon: ChatDotRound },
  { path: '/shops', label: '店铺', icon: Shop },
  { path: '/vouchers', label: '领券中心', icon: Ticket },
  { path: '/my-vouchers', label: '我的券', icon: Wallet },
  { path: '/blogs', label: '社区笔记', icon: Document },
  { path: '/feed', label: '关注动态', icon: Connection },
  { path: '/profile', label: '个人中心', icon: User },
]

function logout() {
  auth.clear()
  void router.push('/')
}
</script>

<template>
  <el-container class="shell">
    <el-aside width="210px" class="sidebar">
      <router-link to="/" class="brand">
        <span>SL</span><div>LocalPerks<small>本地好店与优惠</small></div>
      </router-link>
      <el-menu :default-active="active" router aria-label="主导航">
        <el-menu-item v-for="item in links" :key="item.path" :index="item.path">
          <el-icon><component :is="item.icon" /></el-icon><span>{{ item.label }}</span>
        </el-menu-item>
      </el-menu>
      <div class="sidebar-bottom">
        <el-button plain :icon="Plus" @click="router.push('/blogs/new')">发布笔记</el-button>
      </div>
    </el-aside>
    <el-container class="workspace">
      <el-header class="header">
        <div class="header-title"><span>LocalPerks</span><b>{{ route.meta.title }}</b></div>
        <div v-if="auth.isLoggedIn" class="user-bar">
          <el-avatar :src="auth.user?.icon" :size="30">{{ auth.user?.nickName?.slice(0, 1) }}</el-avatar>
          <span data-testid="user-name">{{ auth.user?.nickName || '已登录' }}</span>
          <el-button link :icon="SwitchButton" @click="logout">退出</el-button>
        </div>
        <el-button v-else type="primary" @click="router.push({ name: 'login', query: { redirect: route.fullPath } })">登录</el-button>
      </el-header>
      <el-main class="main"><router-view /></el-main>
    </el-container>
  </el-container>
</template>

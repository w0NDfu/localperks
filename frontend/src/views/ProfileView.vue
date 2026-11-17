<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useAuthStore } from '@/stores/auth'
import { userApi } from '@/api/user'
import type { UserInfo } from '@/types/domain'
const auth=useAuthStore();const info=ref<UserInfo|null>(null);const count=ref(0);const signing=ref(false)
onMounted(async()=>{if(!auth.user)await auth.fetchMe();if(auth.user){[info.value,count.value]=await Promise.all([userApi.getInfo(auth.user.id),userApi.signCount()])}})
async function sign(){signing.value=true;try{await userApi.sign();count.value=await userApi.signCount();ElMessage.success('今日签到成功')}finally{signing.value=false}}
</script>
<template><div class="page-head"><div><h2>个人中心</h2><p class="muted">查看个人信息和每日签到记录。</p></div><el-button type="primary" :loading="signing" @click="sign">今日签到</el-button></div><div class="profile-wallet-entry surface"><div><b>我的优惠券</b><p class="muted">查看已获得的券和抢券进度</p></div><router-link to="/my-vouchers" class="text-link">打开券包 →</router-link></div><div class="grid grid-2"><el-card shadow="never"><div class="profile-top"><el-avatar :size="72" :src="auth.user?.icon">{{auth.user?.nickName?.slice(0,1)}}</el-avatar><div><h3>{{auth.user?.nickName}}</h3><p>ID: {{auth.user?.id}}</p></div></div><el-descriptions :column="1" border><el-descriptions-item label="城市">{{info?.city||'-'}}</el-descriptions-item><el-descriptions-item label="简介">{{info?.introduce||'-'}}</el-descriptions-item><el-descriptions-item label="性别">{{info?.gender === undefined ? '-' : info.gender ? '男' : '女'}}</el-descriptions-item><el-descriptions-item label="积分">{{info?.credits??'-'}}</el-descriptions-item></el-descriptions></el-card><el-card shadow="never"><template #header>签到状态</template><div class="sign-card"><b>{{count}}</b><span>连续签到天数</span></div><el-alert type="info" :closable="false" title="每日签到，记录你的连续签到天数。" /></el-card></div></template>
<style scoped>.profile-wallet-entry{display:flex;justify-content:space-between;align-items:center;padding:18px;margin-bottom:18px}.profile-wallet-entry p{margin:5px 0 0}.profile-top{display:flex;align-items:center;gap:16px;margin-bottom:20px}.profile-top h3{margin:0}.profile-top p{color:#64748b;font-size:13px}.sign-card{padding:35px;text-align:center}.sign-card b{font-size:52px;color:#f97316;display:block}.sign-card span{color:#64748b}</style>

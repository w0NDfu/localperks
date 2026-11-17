<script setup lang="ts">
import { ref } from 'vue'
import { agentApi, type AgentProfile, type AgentPreferences } from '@/api/agent'

const props = defineProps<{ authenticated: boolean; disabled?: boolean }>()
const loaded = ref(false)
const loading = ref(false)
const error = ref('')
const saved = ref(false)
const revision = ref(0)
const budget = ref<number>()
const radius = ref<number>()
const cuisines = ref('')
const excluded = ref('')
const style = ref<NonNullable<AgentPreferences['answer_style']>>('neutral')

function apply(profile: AgentProfile) {
  revision.value = profile.revision
  budget.value = profile.preferences.budget_per_person ?? undefined
  radius.value = profile.preferences.radius_m ?? undefined
  cuisines.value = (profile.preferences.cuisine_preferences || []).join('、')
  excluded.value = (profile.preferences.excluded_categories || []).join('、')
  style.value = profile.preferences.answer_style || 'neutral'
  loaded.value = true
}
async function load() {
  if (!props.authenticated || loading.value) return
  loading.value = true; error.value = ''; saved.value = false
  try { apply(await agentApi.preferences()) }
  catch (e) { error.value = e instanceof Error ? e.message : '偏好读取失败，请重试' }
  finally { loading.value = false }
}
function toggle(event: Event) {
  if ((event.target as HTMLDetailsElement).open && !loaded.value) void load()
}
function terms(value: string) { return [...new Set(value.split(/[,，、;；]/).map(v => v.trim()).filter(Boolean))] }
async function save(clear = false) {
  if (!props.authenticated || !loaded.value || loading.value || props.disabled) return
  loading.value = true; error.value = ''; saved.value = false
  try {
    const profile = clear ? await agentApi.clearPreferences(revision.value) : await agentApi.savePreferences({
      budget_per_person: budget.value || null, radius_m: radius.value || null,
      cuisine_preferences: terms(cuisines.value), excluded_categories: terms(excluded.value), answer_style: style.value,
    }, revision.value)
    apply(profile); saved.value = true
  } catch (e) { error.value = e instanceof Error ? e.message : '偏好保存失败，请重试' }
  finally { loading.value = false }
}
</script>

<template>
  <details class="surface preferences" @toggle="toggle">
    <summary>我的偏好（可选）</summary>
    <p v-if="!authenticated">登录后可以保存偏好，在新对话中使用。</p>
    <template v-else>
      <p>作为新对话的默认条件；当前问题中的明确要求优先。不会根据浏览行为自动修改。</p>
      <fieldset :disabled="disabled || loading || !loaded">
        <label>默认人均预算（元）<el-input-number v-model="budget" :min="1" :max="100000" aria-label="偏好人均预算" /></label>
        <label>默认距离（米）<el-input-number v-model="radius" :min="100" :max="5000" :step="100" aria-label="偏好距离" /></label>
        <label>偏好口味或分类<el-input v-model="cuisines" placeholder="例如：日料、粤菜" maxlength="160" aria-label="偏好口味或分类" /></label>
        <label>排除餐饮分类<el-input v-model="excluded" placeholder="例如：火锅、烧烤" maxlength="160" aria-label="排除餐饮分类" /></label>
        <label>回答风格<select v-model="style" aria-label="回答风格"><option value="neutral">适中</option><option value="concise">简洁</option><option value="detailed">详细</option></select></label>
      </fieldset>
      <small>口味仅根据商家名称和分类匹配，具体菜品和过敏原请向商家确认。</small>
      <div class="actions">
        <el-button type="primary" :disabled="disabled || !loaded" :loading="loading" @click="save()">保存偏好</el-button>
        <el-button :disabled="disabled || loading || !loaded" @click="save(true)">清除偏好</el-button>
        <el-button link :disabled="loading" @click="load">重新读取</el-button>
      </div>
      <p v-if="saved" role="status">偏好已更新，下次新对话会使用最新设置。</p>
      <p v-if="error" role="alert">{{ error }}</p>
    </template>
  </details>
</template>

<style scoped>
.preferences { padding: 10px 15px; font-size: 13px; }
summary { cursor: pointer; padding: 5px 0; }
fieldset { display: flex; flex-wrap: wrap; gap: 14px; border: 0; padding: 10px 0; margin: 0; }
label { display: grid; gap: 7px; max-width: 100%; }
select { border: 1px solid #dce3d7; border-radius: 6px; padding: 7px; background: white; }
small { color: #6b776d; }
.actions { margin-top: 12px; display: flex; flex-wrap: wrap; gap: 8px; }
[role=alert] { color: #a33d37; }
</style>

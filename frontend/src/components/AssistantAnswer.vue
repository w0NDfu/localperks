<script setup lang="ts">
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { renderAgentMarkdown } from '@/utils/agentMarkdown'
const props = defineProps<{ text: string }>()
const router = useRouter()
const html = computed(() => renderAgentMarkdown(props.text))
function navigate(event: MouseEvent) {
  const link = (event.target as Element).closest('a[href]')
  if (!link || event.ctrlKey || event.metaKey || event.shiftKey || event.button !== 0) return
  if (link.getAttribute('href')?.startsWith('/agent-api/')) {
    event.preventDefault()
    window.open(link.getAttribute('href')!, '_blank', 'noopener,noreferrer')
    return
  }
  event.preventDefault()
  void router.push(link.getAttribute('href')!)
}
</script>

<template>
  <!-- HTML is sanitized and links are restricted by renderAgentMarkdown. -->
  <!-- eslint-disable-next-line vue/no-v-html -->
  <div class="assistant-markdown" @click="navigate" v-html="html" />
</template>

<style scoped>
.assistant-markdown { overflow-x: auto; overflow-wrap: anywhere; line-height: 1.85; margin: 7px 0; }
.assistant-markdown :deep(p) { margin: 8px 0; }
.assistant-markdown :deep(a[href]) { color: #a14622; text-decoration: underline; text-underline-offset: 3px; }
.assistant-markdown :deep(table) { border-collapse: collapse; font-size: 13px; margin: 12px 0; min-width: 100%; }
.assistant-markdown :deep(th), .assistant-markdown :deep(td) { border: 1px solid #e0e8da; padding: 9px 12px; text-align: left; white-space: nowrap; }
.assistant-markdown :deep(th) { background: #eff4e9; color: #425e3c; }
.assistant-markdown :deep(code) { background: #f0f3eb; border-radius: 4px; padding: 2px 5px; font-size: 12px; }
.assistant-markdown :deep(pre) { background: #f0f3eb; padding: 12px; border-radius: 9px; overflow-x: auto; }
.assistant-markdown :deep(blockquote) { margin: 8px 0; border-left: 3px solid #a0b68e; padding-left: 14px; color: #6b785f; }
</style>

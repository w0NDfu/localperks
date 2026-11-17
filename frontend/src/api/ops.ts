export interface OpsScope { start: number; end: number; voucher_id: number; ip: string }
export interface OpsEvent { type: string; phase?: string; seq?: number; [key: string]: unknown }
export interface OpsEvidence { id: string; tool: string; source?: string; sha256?: string; raw?: unknown; error?: string }
export interface OpsRoute {
  path: 'rules_fastpath' | 'agent_deep'; reasons: string[]; review?: 'verified' | 'evidence_gate' | 'unverified'
}
export interface OpsHistory {
  strategy: string; latency_ms: number; warning?: string | null;
  cases: {
    id: string; title: string; kind: 'isolated_load_test' | 'isolated_fault_injection' | 'runbook';
    similarities: string[]; differences: string[]; hypothesis: string; matched_sections: string[];
    sources: { path: string; sha256: string }[];
    checks: { signal: string; observation_ids: string[]; status: 'observed' | 'reported_only' }[];
  }[];
}
export interface OpsReport {
  task_id: string; state: string; root_code?: string; root_title?: string; answer: string;
  recommendation?: string; evidence_ids?: string[]; observations?: OpsEvidence[]; trace?: OpsEvent[];
  answer_origin?: string; latency_ms?: number; error?: string;
  scope?: OpsScope;
  checks?: { supported?: Record<string, { detail: string }>; excluded?: Record<string, string> };
  route?: OpsRoute;
  history?: OpsHistory;
  symptom?: string;
  verified_categories?: { code: string; detail: string; evidence_ids: string[] }[];
  findings?: { kind: string; detail: string; evidence_ids: string[] }[];
  trigger_operation?: { status: string; detail: string };
}
export interface OpsTask { id: string; state: string; request: string | { symptom: string }; created: number; updated: number }
export interface OpsDetail { id: string; state: string; output?: OpsReport; events: OpsEvent[]; cost: string;
  model_usage: { kind?: string; input_tokens: number | null; output_tokens: number | null; total_tokens: number | null }[] }

export async function opsRequest<T>(token: string, path: string, body?: unknown): Promise<T> {
  const response = await fetch('/agent-api/agent/ops/' + path, {
    method: body === undefined ? 'GET' : 'POST', cache: 'no-store',
    headers: { 'X-Ops-Token': token, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  })
  if (!response.ok) {
    if (response.status === 403) throw new Error('管理员凭证无效或运维入口尚未启用')
    if (response.status === 422) throw new Error('请检查调查窗口、券 ID 和输入格式')
    throw new Error(`运维请求失败（${response.status}），请稍后查看调查记录`)
  }
  return response.json() as Promise<T>
}

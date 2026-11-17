"""Render a report only from retained measurements, never guessed billing."""
import json
from pathlib import Path
from xml.etree import ElementTree


def generate(root=None):
    root = Path(root) if root else Path(__file__).parent
    data = json.loads((root / 'results/platform-evaluation.json').read_text(encoding='utf-8'))
    ingestion = json.loads((root / 'results/ingestion.json').read_text(encoding='utf-8'))
    summaries = data['comparison']['summaries']
    rows = data['comparison']['rows']
    old, new = summaries['original_sequential_v02'], summaries['router_supervisor_dag']
    missing = {mode: sum(r['metrics'].get('usage_missing_calls', 0) for r in rows if r['mode'] == mode) for mode in summaries}
    tests = ElementTree.parse(root / 'results/pytest.xml').find('.//testsuite').attrib
    content = f'''# Agent / RAG 实测与基线对照

日期：{data['timestamp']}。Chat：`{data['model']}`；Embedding：`text-embedding-3-small`；Rerank：`BAAI/bge-reranker-v2-m3`。

## 测量方法与限制

业务对照使用真实 OpenLux、Java、Redis；无订单写入。冻结的v0.2原版在独立子进程执行；新方案使用同一业务服务及相同四个问题。计时覆盖完整SSE响应、进程内ASGI执行，不含解释器/App启动，不是公网HTTP压测。

冻结基线SHA-256：`{data['comparison']['baseline_sha256']}`。原始响应、事件和usage保存在 `results/platform-evaluation.json`。

每方案四个用例、每例一次；P95采用nearest-rank，小样本中等于最大值。通过标准是任务结束、调用必要的真实接口且无执行错误，不能解释为成交成功率、保证三家可核销或生产SLA。

## 原版与新编排

| 指标 | 原v0.2串行ReAct | Router/Supervisor/DAG |
|---|---:|---:|
| 有依据查询完成 | {old['grounded_query_success_rate']:.0%}（3/4） | {new['grounded_query_success_rate']:.0%}（4/4） |
| 平均耗时 | {old['mean_latency_ms']/1000:.3f}s | {new['mean_latency_ms']/1000:.3f}s |
| 小样本P95/最大耗时 | {old['p95_latency_ms_nearest_rank']/1000:.3f}s | {new['p95_latency_ms_nearest_rank']/1000:.3f}s |
| 实际模型请求 | {old['model_calls']} | {new['model_calls']} |
| 工具调用 | {old['tool_calls']} | {new['tool_calls']} |
| Java请求 | {old['http_requests']} | {new['http_requests']} |
| 已返回usage的Token合计 | {old['total_tokens']:,} | {new['total_tokens']:,} |
| 每请求已知Token平均 | {old['mean_tokens_per_request']:,.2f} | {new['mean_tokens_per_request']:,.2f} |
| 缺失usage的模型请求 | {missing['original_sequential_v02']} | {missing['router_supervisor_dag']} |

原版复杂用例触发100秒时限；超时前的调用仍从SSE计数，不能算作零成本。原版有一次未取得usage，Token合计为已知用量下界。没有中转站账单与单价，费用保持未知，不将Token比例冒充账单降幅。

### 逐例数据

| 用例 | 原版：模型/工具/耗时 | 新版：模型/工具/耗时 |
|---|---|---|
'''
    for case_id in ['shop_details', 'shop_vouchers', 'shop_types', 'consumption']:
        def cell(mode):
            row = next(r for r in rows if r['mode'] == mode and r['case']['id'] == case_id)
            return f"{row['metrics']['model_calls']} / {row['metrics']['tool_calls']} / {row['response']['measured_latency_ms']/1000:.3f}s"
        content += f"| {case_id} | {cell('original_sequential_v02')} | {cell('router_supervisor_dag')} |\n"
    content += f'''
三个明确查询均0次模型；复杂决策2次，用于理解条件及受约束的结果解释，Java按店查券并行。真实数据仅支持两家预算内有券的候选，规则都写“以店铺说明为准”，因此没有凑出三家已验证推荐。

## Router 和 RAG

标注回归集{data['router']['cases']}条，意图准确率{data['router']['intent_accuracy']:.1%}，{data['router']['tool_cases']}条明确工具样例选择准确率{data['router']['tool_selection_accuracy']:.1%}。集合包含发现问题后增加的回归样例，不代表开放域准确率。

真实语料{ingestion['documents']}文档、{ingestion['chunks']}块；批量Embedding {ingestion['embedding_calls']}次、{ingestion['embedding_tokens']:,} Token。未导入价格、库存或个人订单。

六查询分别运行BM25和完整混合检索。相关集合按真实文本片段标注；购买时间问题同时认可FAQ和各券规则中的等价证据。Recall@5为前5块命中的相关块占全部相关块的比例，MRR为首个相关块倒数排名。

| 策略 | Recall@5 | MRR | 小样本P95 |
|---|---:|---:|---:|
'''
    for mode, label in [('bm25', 'BM25'), ('hybrid', 'BM25 + Dense + RRF + API Rerank')]:
        entry = data['rag']['summaries'][mode]
        content += f"| {label} | {entry['recall_at_5']:.4f} | {entry['mrr']:.4f} | {entry['p95_latency_ms_nearest_rank']:.1f}ms |\n"
    content += f'''
小语料及六条查询中，混合检索未优于BM25，外部Embedding/Rerank增加了延迟；不宣称检索质量或性能提升。后续应扩大困难语义集、去重、增加查询缓存及重复采样，再判断混合方案收益。

## 验证结果

- Python：{tests['tests']}项，失败{tests['failures']}、错误{tests['errors']}；含真实Redis和pgvector，机器结果见 `results/pytest.xml`。
- Java：35项通过，含6项订单归属/取消状态测试。未执行会大量写业务缓存、ID计数器和HyperLogLog的历史演示测试。
- 前端：构建、ESLint、22项Vitest通过；7项Chrome Playwright通过，涵盖恢复任务及确认前不提交交易。模拟样例均明确标记，不作真实业务证据。
- 真实Chrome：缺信息追问→原任务恢复→预算改30元→RAG引用与来源HTTP200，检查390像素布局；未写真实订单。回执 `results/browser-receipt.json`，持久化原始记录 `results/browser-journal.json`。
- 镜像经DaoCloud Docker Hub代理下载并本机打标签，摘要 `sha256:0dd3149a06e4579a0d70d8fef9945636cd514005e7237deb27f9b67886f8dc4f`。未改Docker全局代理配置，未删除/重建业务数据卷。

## 复现

在agent-service执行：

```powershell
$env:AGENT_TEST_REDIS_URL='redis://127.0.0.1:6379/0'
$env:AGENT_TEST_PGVECTOR='1'
.\\.venv\\Scripts\\python.exe -m pytest
.\\.venv\\Scripts\\python.exe -m evaluations.run --mode all --output evaluations/results/platform-evaluation.json
```

在LocalPerks执行：

```powershell
.\\scripts\\maven.ps1 '-Dtest=*Test,!RedissonTest,!HmDianPingApplicationTests' test
cd frontend
npm run build
npm run lint
npm run test
$env:PLAYWRIGHT_CHANNEL='chrome'
npm run e2e
```

## 可用于简历的技术亮点

1. 将自研ReAct升级为Router/Supervisor/动态DAG，保留冻结基线；本地四场景实测模型请求22→2，有依据查询完成3/4→4/4（小样本，不作生产SLA）。
2. 实现Decimal优惠计算、地理距离、规则白名单和证据排序，区分购买时段与核销资格；未知规则输出待确认候选，避免LLM编造金额和可用性。
3. 实现SQLite WAL检查点、Redis会话、本人确认/CAS幂等提交和写超时只读核对，覆盖权限、进程重启与会话过期恢复测试。
4. 自研共享RAG：真实41文档/43块，1536维Embedding、pgvector HNSW、BM25/RRF/API Rerank和来源引用；提供六查询对照，明确报告该语料上未优于BM25。

P4运营诊断未启用：缺少运营角色授权和受控监控API，不编造根因。开发验收后停止全部项目服务并保留数据卷。
'''
    docker_path = root / 'results/docker-verification.json'
    if docker_path.exists():
        verification = json.loads(docker_path.read_text(encoding='utf-8-sig'))
        content += f"\n## Docker 实际验收\n\n镜像 `{verification['image']}` 构建并启动通过：health={verification['health']}，Java={verification['java']}，Redis={verification['redis']}；真实店铺查询{verification['shop_records']}条，pgvector/BM25检索{verification['knowledge_hits']}条。非root UID1000；镜像不含真实环境文件；验证不调用模型、不写交易。回执：`results/docker-verification.json`。\n\n构建使用参数 `--build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple` 解决下载速度问题，默认仍为官方PyPI，未改全局配置。\n"
    (root / 'REPORT.md').write_text(content, encoding='utf-8')
    return content


if __name__ == '__main__':
    generate()

"""Bounded validation, ASGI fixture load, frozen-baseline protocol comparison and evidence report.

No live Java mutation or business storage access. Real HTTP model calls require the
explicit `real` command, a persistent request/token ledger and injected transport.
"""
import argparse
import asyncio
import csv
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AGENT = ROOT / 'agent-service'
sys.path[:0] = [str(AGENT), str(ROOT / 'tests')]


def save(path, data):
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding='utf-8')


def read(path, default=None):
    return json.loads(Path(path).read_text(encoding='utf-8-sig')) if Path(path).exists() else default


def write_csv(path, rows, fields=None):
    fields = fields or list(dict.fromkeys(key for row in rows for key in row))
    with Path(path).open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)] if values else None


def environment(output):
    from core.config import load_settings
    settings = load_settings()
    versions = {}
    for name in ('fastapi', 'httpx', 'openai', 'pytest', 'redis', 'psycopg', 'pgvector', 'uvicorn'):
        versions[name] = importlib.metadata.version(name)
    data = {'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S%z'), 'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            'git_status_before': subprocess.check_output(['git', 'status', '--short'], cwd=ROOT, text=True),
            'python': sys.version, 'platform': platform.platform(), 'dependencies': versions,
            'model': settings.model, 'embedding_model': settings.embedding_model, 'rerank_model': settings.rerank_model,
            'model_configured': settings.configured, 'wsl_config': None,
            'limits': {'model_requests': int(os.getenv('OPENAI_MAX_REQUESTS', '40')), 'actual_tokens': int(os.getenv('OPENAI_MAX_TOKENS_BUDGET', '24000')),
                       'per_chat_max_output_tokens': 768, 'artifacts_bytes': 1073741824, 'min_available_memory_bytes': 2147483648,
                       'test_container_memory_bytes': 536870912, 'agent_concurrency': [1, 2, 4, 8]}}
    wsl = Path(os.environ['USERPROFILE']) / '.wslconfig'
    if wsl.exists():
        data['wsl_config'] = wsl.read_text(encoding='utf-8-sig')
    data['source_hashes'] = {str(p.relative_to(AGENT)): hashlib.sha256(p.read_bytes()).hexdigest()
                             for p in AGENT.rglob('*.py') if not any(x in p.parts for x in ('.venv', '__pycache__'))}
    archive = AGENT / 'evaluations/baseline_v02.zip'
    data['baseline_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
    data['dataset_sha256'] = hashlib.sha256((ROOT / 'evaluations/datasets/core_v03.jsonl').read_bytes()).hexdigest()
    data['validation_source_hashes'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
        for directory in ('scripts', 'tests', 'evaluations/datasets') for p in (ROOT / directory).rglob('*')
        if p.is_file() and p.suffix in {'.py', '.ps1', '.jsonl'}}
    for key, command in {
        'docker_volumes_before': ['docker', 'volume', 'ls', '--format', '{{json .}}'],
        'docker_containers_before': ['docker', 'ps', '-a', '--format', '{{json .}}'],
        'docker_version': ['docker', 'version', '--format', '{{json .Server.Version}}'],
        'java_version': ['java', '-version'], 'node_version': ['node', '--version'],
    }.items():
        result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=20)
        data[key] = (result.stdout + result.stderr).strip()
    # Metadata only: no .env contents, URLs with embedded passwords, or tokens.
    save(output / 'environment.json', data)


async def seed(output):
    from core.config import load_settings
    from rag.store import PgVectorStore
    from rag.ingestion import chunks
    from redis.asyncio import Redis
    cfg = load_settings()
    store = PgVectorStore(os.environ['PGVECTOR_DSN'], 3)
    last_error = None
    for _ in range(15):
        try:
            await store.initialize()
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(1)
    if last_error:
        raise last_error
    rows = chunks('未支付订单可以取消。没有真实支付通道。购买活动时间不等于核销使用有效期。', 'fixture:isolated', title='隔离测试规则')
    await store.upsert_document(rows, [[1, 0, 0] for _ in rows], cfg.embedding_model)
    async with Redis.from_url(os.environ['AGENT_TEST_REDIS_URL']) as redis:
        assert await redis.ping()
    save(output / 'isolated_connectivity.json', {'redis_ping': True, 'pgvector_initialized': True, 'fixture_chunks': len(rows), 'dimensions': 3, 'business_data_accessed': False})


def mock_model(request):
    from support import completion
    body = json.loads(request.content)
    if body.get('response_format'):
        text = body['messages'][0]['content']
        if 'focus' in text:
            return completion('{"focus":"savings","mentioned_shop_ids":[1,2,3]}')
        return completion('{"keyword":"","explanation":"fixture"}')
    # Scripted tool protocol for the legacy engine, independent for each request.
    messages = body['messages']
    functions = {tool['function']['name'] for tool in body.get('tools', [])}
    has_result = any(message.get('role') == 'tool' for message in messages)
    if has_result:
        user_text = ' '.join(str(m.get('content', '')) for m in messages if m.get('role') == 'user')
        tool_names = {call.get('function', {}).get('name') for m in messages for call in m.get('tool_calls', [])}
        if 'delegate_shop' in functions and '推荐' in user_text and 'delegate_voucher' not in tool_names:
            return completion(name='delegate_voucher', arguments={'task': '查询店铺1优惠券'})
        return completion('已查询隔离接口，结果仅用于测试。')
    if 'delegate_shop' in functions:
        text = str(messages[-1]['content'])
        if '优惠券' in text:
            return completion(name='delegate_voucher', arguments={'task': '查询店铺1优惠券'})
        return completion(name='delegate_shop', arguments={'task': text})
    if 'find_vouchers' in functions:
        return completion(name='find_vouchers', arguments={'shop_id': 1})
    if 'shop_details' in functions:
        text = str(messages[-1]['content'])
        if '分类' in text:
            return completion(name='shop_types', arguments={})
        if '详情' in text:
            return completion(name='shop_details', arguments={'shop_id': 1})
        return completion(name='find_shops', arguments={'keyword': ''})
    raise AssertionError('Unexpected mock model protocol')


async def load(output):
    import httpx
    from core.config import Settings
    from main import create_app
    from support import BusinessFixture, constraints
    from tests_support import TestMemory
    rows = []
    for kind in ('fast_read', 'decision', 'mock_multistep'):
        for concurrency in (1, 2, 4, 8):
            business = BusinessFixture()
            app = create_app(settings=Settings(api_key='fixture' if kind == 'mock_multistep' else '', model='mock',
                                              orchestration_mode='legacy' if kind == 'mock_multistep' else 'supervisor',
                                              state_dsn=os.environ['AGENT_TEST_STATE_DSN']),
                             transport=httpx.MockTransport(business), model_transport=httpx.MockTransport(mock_model), memory=TestMemory())
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://asgi.fixture') as client:
                    async def request_one():
                        started = time.perf_counter()
                        body = {'message': '查询店铺1详情' if kind == 'fast_read' else '帮我综合查店铺信息' if kind == 'mock_multistep' else '推荐附近最划算的三家餐厅'}
                        if kind == 'decision':
                            body['constraints'] = constraints()
                        response = await client.post('/agent/chat', json=body)
                        data = response.json()
                        success = response.status_code == 200 and not data.get('error')
                        if kind == 'decision':
                            success = success and data.get('state') == 'completed' and len(data.get('decision', {}).get('recommendations', [])) == 3
                        elif kind == 'fast_read':
                            success = success and data.get('metrics', {}).get('tool_calls') == 1
                        else:
                            success = success and data.get('metrics', {}).get('tool_calls', 0) >= 1
                        return {'duration_ms': (time.perf_counter() - started) * 1000, 'http_status': response.status_code,
                                'success': success, 'metrics': data.get('metrics', {}), 'trace': data.get('trace', [])}
                    samples = []
                    begin = time.perf_counter()
                    for _ in range(3):  # exactly three bounded waves; no unbounded duration loop
                        samples.extend(await asyncio.gather(*(request_one() for _ in range(concurrency))))
                    elapsed = time.perf_counter() - begin
            times = [s['duration_ms'] for s in samples]
            row = {'kind': kind, 'test_type': 'in_process_ASGI_mock_business_mock_model', 'concurrent_users': concurrency,
                   'requests': len(samples), 'duration_seconds': elapsed, 'rps': len(samples) / elapsed,
                   'p50_ms': percentile(times, .5), 'p95_ms': percentile(times, .95),
                   'http_error_rate': sum(s['http_status'] >= 400 for s in samples) / len(samples),
                   'task_completion_rate': sum(s['success'] for s in samples) / len(samples),
                   'model_requests': sum(s['metrics'].get('model_calls', 0) for s in samples),
                   'mock_reported_tokens': sum(s['metrics'].get('total_tokens', 0) for s in samples), 'actual_provider_tokens': 0,
                   'tool_calls': sum(s['metrics'].get('tool_calls', 0) for s in samples), 'business_orders_created': 0,
                   'dag_node_ms_sum': sum(e.get('duration_ms', 0) for s in samples for e in s['trace'] if e['type'] == 'node_result')}
            rows.append(row)
            write_csv(output / 'performance.csv', rows)
            save(output / f'load-{kind}-{concurrency}.json', samples)
            print(json.dumps(row), flush=True)
            if row['task_completion_rate'] != 1:
                raise RuntimeError('Load stage failed; stopping escalation')


COMPARE_CASES = [
    {'id': 'shop_details', 'message': '查询店铺1详情', 'tool': 'shop_details'},
    {'id': 'shop_vouchers', 'message': '查询店铺1的优惠券', 'tool': 'find_vouchers'},
    {'id': 'shop_types', 'message': '查询店铺分类列表', 'tool': 'shop_types'},
    {'id': 'consumption', 'message': '推荐附近两个人吃饭、人均100元、2090-11-05 19:00最划算的三家餐厅，位置120.16,30.25', 'tool': None}]


class BudgetTransport:
    """Factory returns an HTTP transport whose ledger survives baseline subprocesses.

    Actual tokens only come from API usage. Missing usage stops further live calls.
    Worst-case UTF-8 byte count + reserved output is checked before each chat call.
    """
    @staticmethod
    def create(output):
        import httpx
        class Transport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                ledger_path = output / 'model_budget.json'
                ledger = read(ledger_path, {'requests': 0, 'actual_tokens': 0, 'missing_usage': False, 'calls': []})
                limit = int(os.getenv('OPENAI_MAX_REQUESTS', '40'))
                token_limit = int(os.getenv('OPENAI_MAX_TOKENS_BUDGET', '24000'))
                payload = json.loads(request.content)
                from urllib.parse import urlparse
                if urlparse(str(request.url)).hostname != 'api.openlux.ai':
                    raise RuntimeError('MODEL_DESTINATION_NOT_AUTHORIZED')
                if 'messages' in payload:
                    payload['max_tokens'] = 768
                data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
                observed = max((c.get('actual_tokens') or 0 for c in ledger['calls']), default=0)
                reserve = max(8192, len(data) + 768 + observed)
                if (ledger['requests'] >= limit or ledger['actual_tokens'] + reserve > token_limit or ledger['missing_usage']
                        or ledger.get('accounting_bound_exceeded') or ledger.get('budget_exceeded')):
                    raise RuntimeError('MODEL_BUDGET_EXHAUSTED')
                if ledger.get('pending_request'):
                    raise RuntimeError('PREVIOUS_REQUEST_USAGE_UNKNOWN')
                ledger['requests'] += 1
                ledger['pending_request'] = True
                save(ledger_path, ledger)  # Count attempts even if network fails or the process exits.
                started = time.perf_counter()
                async with httpx.AsyncClient(timeout=35, trust_env=False) as client:
                    response = await client.post(str(request.url), headers={'Authorization': request.headers['authorization'], 'Content-Type': 'application/json'}, content=data)
                usage = response.json().get('usage', {}) if response.is_success else {}
                tokens = usage.get('total_tokens')
                if tokens is None and usage.get('prompt_tokens') is not None:
                    tokens = usage['prompt_tokens'] + usage.get('completion_tokens', 0)
                # Some rerank endpoints provide no usage: retain that fact and stop future paid calls.
                if response.is_success and tokens is None:
                    ledger['missing_usage'] = True
                ledger['actual_tokens'] += tokens or 0
                ledger['pending_request'] = False
                ledger['accounting_bound_exceeded'] = tokens is not None and tokens > reserve
                ledger['budget_exceeded'] = ledger['actual_tokens'] > token_limit
                ledger['calls'].append({'path': request.url.path, 'status': response.status_code,
                                        'duration_ms': (time.perf_counter() - started) * 1000, 'actual_tokens': tokens,
                                        'preflight_reserved_tokens': reserve, 'input_utf8_bytes': len(data)})
                save(ledger_path, ledger)
                return httpx.Response(response.status_code, content=response.content, headers={'content-type': 'application/json'}, request=request)
        return Transport()


async def current_compare(output, case, real=False):
    import httpx
    from core.config import Settings, load_settings
    from main import create_app
    from support import BusinessFixture
    from tests_support import TestMemory
    cfg = load_settings() if real else Settings(api_key='fixture', model='mock')
    cfg = replace(cfg, state_dsn=os.environ['AGENT_TEST_STATE_DSN'])
    business = BusinessFixture()
    app = create_app(settings=cfg, memory=TestMemory(), transport=httpx.MockTransport(business),
                     model_transport=BudgetTransport.create(output) if real else httpx.MockTransport(mock_model))
    started = time.perf_counter()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app, raise_app_exceptions=False), base_url='http://fixture') as client:
            response = await client.post('/agent/chat', json={'message': case['message']})
            try:
                data = response.json()
            except ValueError:
                data = {'error': 'INVALID_RESPONSE'}
    data.update(http_status=response.status_code, measured_latency_ms=(time.perf_counter() - started) * 1000)
    return data


def grade(data, case):
    if data.get('http_status') != 200:
        return False
    events = data.get('trace', [])
    if case['tool']:
        return any(event.get('type') == 'tool_result' and event.get('tool') == case['tool'] and
                   any(r.get('status') == 200 for r in event.get('requests', [])) for event in events)
    # Query-completion only: original free-text recommendations are not certified money/eligibility decisions.
    return any(event.get('type') == 'tool_result' and event.get('tool') in {'shop_vouchers', 'find_vouchers'} and
               any(r.get('status') == 200 for r in event.get('requests', [])) for event in events)


async def compare(output, real=False):
    from core.config import load_settings
    from observability.metrics import summarize
    rows = []
    settings = load_settings()
    if real and not settings.configured:
        save(output / 'real_model_unverified.json', {'status': 'NOT_VERIFIED', 'reason': 'Model credentials missing'})
        return
    for case in COMPARE_CASES:
        for mode in ('original_sequential_v02', 'router_supervisor_dag'):
            if mode == 'original_sequential_v02':
                env = dict(os.environ)
                if real:
                    env.update(OPENAI_API_KEY=settings.api_key, OPENAI_MODEL=settings.model, OPENAI_BASE_URL=settings.base_url)
                started = time.perf_counter()
                process = await asyncio.create_subprocess_exec(sys.executable, str(ROOT / 'tests/load/frozen_worker.py'),
                    '--output', str(output), '--message', case['message'], *(['--real'] if real else []),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
                try:
                    stdout, stderr = await asyncio.wait_for(process.communicate(), 110)
                    data = json.loads(stdout.decode('utf-8-sig').strip().splitlines()[-1])
                    if process.returncode:
                        raise RuntimeError('Frozen subprocess failed')
                except Exception as exc:
                    if process.returncode is None:
                        process.kill()
                        await process.wait()
                    data = {'http_status': 0, 'error': type(exc).__name__, 'measured_latency_ms': (time.perf_counter() - started) * 1000}
            else:
                data = await current_compare(output, case, real)
            metrics = data.get('metrics') or summarize(data.get('trace', []), data.get('measured_latency_ms', 0))
            passed = grade(data, case)
            rows.append({'mode': mode, 'case_id': case['id'], 'test_type': 'real_model_fixture_business' if real else 'scripted_mock_model_fixture_business',
                         'passed': passed, 'latency_ms': data['measured_latency_ms'], 'model_requests': metrics['model_calls'],
                         'tool_calls': metrics['tool_calls'], 'reported_tokens': metrics['total_tokens'],
                         'actual_provider_tokens': metrics['total_tokens'] if real else 0,
                         'ranking_certification': 'NOT_VERIFIED' if mode.startswith('original') else 'programmatic_fixture_tests_separate'})
            save(output / f"{'real' if real else 'mock'}-{mode}-{case['id']}.json", data)
            write_csv(output / ('baseline_real_comparison.csv' if real else 'baseline_comparison.csv'), rows)
            print(json.dumps(rows[-1]), flush=True)
    if not real:
        from evaluations.run import offline
        save(output / 'router_evaluation.json', offline())


async def rag_compare(output):
    from core.config import load_settings
    from rag.ingestion import chunks
    from rag.lexical import BM25, rrf
    from rag.retrieval import EmbeddingClient, Reranker
    from rag.store import PgVectorStore
    cfg = load_settings()
    if not cfg.configured or not os.environ.get('PGVECTOR_DSN'):
        write_csv(output / 'rag_comparison.csv', [{'mode': 'all', 'status': 'NOT_VERIFIED', 'reason': 'Real model or isolated pgvector unavailable'}])
        return
    # Separate schema in the disposable instance; no production corpus is read or modified.
    from dataclasses import replace
    cfg = replace(cfg, embedding_dimensions=1536)
    store = PgVectorStore(cfg.pgvector_dsn + '?options=-csearch_path%3Drag_eval%2Cpublic', 1536)
    base = PgVectorStore(cfg.pgvector_dsn, 3)
    await base.transact(lambda c: c.execute('CREATE SCHEMA rag_eval'), register=False)
    await store.initialize()
    texts = [
        ('取消订单', '只允许当前登录用户取消自己的未支付订单。取消后回补库存，已支付订单不能直接取消。', '未支付订单取消后会怎样？'),
        ('支付说明', '没有真实支付通道。系统只有开发模拟支付，不发生真实扣款。', '系统会真实扣款吗？'),
        ('活动时间', '购买活动时间不等于核销使用有效期。使用日期必须以券的使用规则为准。', '秒杀结束时间就是券的有效期吗？'),
        ('参考价格', 'avgPrice不是菜单报价，只是人均参考价，不能保证实际结账金额。', '人均参考价格是实际结账金额吗？'),
        ('库存', '库存和订单状态是实时业务事实，必须重新查询Java接口，知识库不能保证实时库存。', '知识库能确认实时库存吗？'),
        ('订单归属', '个人订单和券包必须登录后查询，只能访问本人资源。', '可以查询其他用户券包吗？')]
    documents = []
    for title, text, _query in texts:
        documents.extend(chunks(text, 'fixture:' + title, title=title))
    budget = BudgetTransport.create(output)
    vectors, usage = await EmbeddingClient(cfg, budget).embed([doc.text for doc in documents])
    await store.upsert_document(documents, vectors, cfg.embedding_model)
    query_vectors, query_embedding_ms = {}, {}
    # Finish bounded query embeddings before rerank; if rerank omits usage, all
    # remaining paid calls stop while already-computed BM25/Dense/RRF remain usable.
    for _title, _text, query in texts:
        started = time.perf_counter()
        embedded, _ = await EmbeddingClient(cfg, BudgetTransport.create(output)).embed([query])
        query_vectors[query] = embedded[0]
        query_embedding_ms[query] = (time.perf_counter() - started) * 1000
    rows = []
    for title, _text, query in texts:
        start = time.perf_counter()
        sparse = BM25([d.to_dict() for d in documents]).search(query, 20)
        sparse_ms = (time.perf_counter() - start) * 1000
        start = time.perf_counter()
        dense = await store.dense(query_vectors[query], cfg.embedding_model, 20)
        dense_ms = (time.perf_counter() - start) * 1000 + query_embedding_ms[query]
        start = time.perf_counter()
        merged = rrf(sparse, dense)
        merged_ms = (time.perf_counter() - start) * 1000
        start = time.perf_counter()
        ranked, backend, warning = await Reranker(cfg, BudgetTransport.create(output)).rank(query, merged, 5)
        rerank_ms = (time.perf_counter() - start) * 1000
        for mode, hits, elapsed in [('bm25', sparse[:5], sparse_ms), ('dense', dense[:5], dense_ms),
                                    ('hybrid_rrf', merged[:5], sparse_ms + dense_ms + merged_ms),
                                    ('rerank', ranked, sparse_ms + dense_ms + merged_ms + rerank_ms)]:
            relevant = {d.id for d in documents if d.title == title}
            ids = [hit['id'] for hit in hits]
            rank = next((i for i, identifier in enumerate(ids, 1) if identifier in relevant), None)
            rows.append({'query': query, 'mode': mode, 'test_type': 'real_remote_models_isolated_pgvector_fixture_corpus',
                         'status': 'PASS' if mode != 'rerank' or backend == 'model_api' else 'DEGRADED',
                         'recall_at_5': len(set(ids) & relevant) / len(relevant), 'mrr': 1 / rank if rank else 0,
                         'citation_source_accuracy': sum(i in {d.id for d in documents} for i in ids) / len(ids) if ids else None,
                         'latency_ms': elapsed, 'rerank_backend': backend if mode == 'rerank' else '',
                         'warning': warning if mode == 'rerank' else ''})
        write_csv(output / 'rag_comparison.csv', rows)
    save(output / 'rag_dataset.json', {'documents': [d.to_dict() for d in documents], 'queries': [t[2] for t in texts],
                                     'note': '6 document fixture corpus, not production retrieval quality. Citation source accuracy checks existence, not answer semantic entailment.'})


async def rag_fixture(output):
    """Ablation mechanics with real BM25/pgvector/RRF and deliberately synthetic embeddings.

    No learned-embedding quality claim is permitted. Rerank is an injected outage,
    providing a measured degradation comparison without paying for a model.
    """
    import httpx
    from core.config import Settings
    from rag.ingestion import chunks
    from rag.lexical import BM25, rrf
    from rag.retrieval import Reranker
    from rag.store import PgVectorStore
    if not os.getenv('PGVECTOR_DSN'):
        write_csv(output / 'rag_comparison.csv', [{'mode': 'all', 'status': 'NOT_VERIFIED', 'reason': 'Isolated pgvector unavailable'}])
        return
    texts = [
        ('取消订单', '只允许当前登录用户取消自己的未支付订单，库存会回补。', '未支付订单取消后会怎样？'),
        ('支付', '系统没有真实支付通道，模拟支付不会发生真实扣款。', '系统会真实扣款吗？'),
        ('有效期', '购买活动时间不等于核销使用有效期，日期必须以使用规则为准。', '购买活动结束就是核销有效期吗？'),
        ('参考价', 'avgPrice不是菜单报价，是人均参考价，不能保证实际结账金额。', '人均参考价能保证实际结账金额吗？'),
        ('实时事实', '库存和价格必须重新查询Java接口，知识库不能保证实时库存。', '知识库可以保证实时库存吗？'),
        ('权限', '个人券包必须登录，不能查询其他用户的券包。', '可以查询其他用户券包吗？')]
    base = PgVectorStore(os.environ['PGVECTOR_DSN'], 3)
    await base.transact(lambda c: c.execute('CREATE SCHEMA rag_fixture'), register=False)
    store = PgVectorStore(os.environ['PGVECTOR_DSN'] + '?options=-csearch_path%3Drag_fixture%2Cpublic', 3)
    await store.initialize()
    documents = [chunks(text, 'fixture:' + title, title=title)[0] for title, text, _ in texts]
    vectors = [[1,0,0], [0,1,0], [0,0,1], [1,1,0], [1,0,1], [0,1,1]]
    await store.upsert_document(documents, vectors, 'synthetic-three-dimensional')
    rows = []
    raw = []
    for title, _text, query in texts:
        started = time.perf_counter()
        sparse = BM25([d.to_dict() for d in documents]).search(query, 20)
        bm_ms = (time.perf_counter() - started) * 1000
        started = time.perf_counter()
        dense = await store.dense([1, 0, 0], 'synthetic-three-dimensional', 20)
        dense_ms = (time.perf_counter() - started) * 1000
        started = time.perf_counter()
        merged = rrf(sparse, dense)
        rrf_ms = (time.perf_counter() - started) * 1000
        started = time.perf_counter()
        ranked, backend, warning = await Reranker(Settings(api_key='fixture'), httpx.MockTransport(lambda _: httpx.Response(503))).rank(query, merged, 5)
        rerank_ms = (time.perf_counter() - started) * 1000
        assert ranked == merged[:5] and backend == 'rrf' and warning
        relevant = {d.id for d in documents if d.title == title}
        for mode, hits, elapsed in [('bm25', sparse[:5], bm_ms), ('dense', dense[:5], dense_ms),
                                    ('hybrid_rrf', merged[:5], bm_ms + dense_ms + rrf_ms),
                                    ('rerank_outage_rrf_fallback', ranked, bm_ms + dense_ms + rrf_ms + rerank_ms)]:
            ids = [h['id'] for h in hits]
            rank = next((i for i, identifier in enumerate(ids, 1) if identifier in relevant), None)
            rows.append({'query': query, 'mode': mode, 'test_type': 'synthetic_embeddings_real_pgvector_mock_rerank_outage',
                         'status': 'PASS_COMPONENT_ONLY', 'recall_at_5': len(set(ids) & relevant) / len(relevant),
                         'mrr': 1 / rank if rank else 0, 'citation_source_accuracy': sum(i in {d.id for d in documents} for i in ids) / len(ids) if ids else None,
                         'latency_ms': elapsed, 'embedding_model': 'synthetic-3d-fixed-query-vector-not-semantic',
                         'rerank_backend': backend if mode.startswith('rerank') else ''})
        raw.append({'query': query, 'relevant_ids': sorted(relevant), 'sparse': sparse, 'dense': dense, 'rrf': merged, 'fallback': ranked})
    write_csv(output / 'rag_comparison.csv', rows)
    save(output / 'rag_fixture_evidence.json', {'documents': [d.to_dict() for d in documents], 'vectors': vectors, 'rows': raw,
                                              'limitation': 'Synthetic vectors and fixed query vector only test retrieval mechanics, not learned semantic retrieval. No real rerank quality measured.'})


def report(output):
    import xml.etree.ElementTree as ET
    suites = {}
    def junit(name, files):
        counts = dict(passed=0, failed=0, skipped=0, total=0, seconds=0.0)
        for file in files:
            root = ET.parse(file).getroot()
            leaves = [root] if root.tag == 'testsuite' else list(root.iter('testsuite'))
            for suite in leaves:
                total = int(suite.get('tests', 0)); failed = int(suite.get('failures', 0)) + int(suite.get('errors', 0)); skipped = int(suite.get('skipped', 0))
                counts['total'] += total; counts['failed'] += failed; counts['skipped'] += skipped
                counts['passed'] += total - failed - skipped; counts['seconds'] += float(suite.get('time', 0))
        suites[name] = counts
    junit('python', [output / 'pytest.xml'] if (output / 'pytest.xml').exists() else [])
    if (output / 'supplemental.xml').exists():
        junit('python_supplemental', [output / 'supplemental.xml'])
    steps = read(output / 'steps.json', [])
    if isinstance(steps, dict):
        steps = [steps]
    java_dir = output / 'surefire'
    java_dir.mkdir(exist_ok=True)
    import shutil
    if any(step['name'] == 'java' for step in steps):
        for file in (ROOT / 'target/surefire-reports').glob('TEST-*.xml'):
            shutil.copy2(file, java_dir / file.name)
    junit('java', list(java_dir.glob('TEST-*.xml')))
    vitest = read(output / 'vitest.json', {})
    suites['frontend'] = {'passed': vitest.get('numPassedTests', 0), 'failed': vitest.get('numFailedTests', 0), 'skipped': vitest.get('numPendingTests', 0), 'total': vitest.get('numTotalTests', 0)}
    playwright = read(output / 'playwright.json', {})
    stats = playwright.get('stats', {})
    suites['browser'] = {'passed': stats.get('expected', 0), 'failed': stats.get('unexpected', 0), 'skipped': stats.get('skipped', 0), 'flaky': stats.get('flaky', 0), 'seconds': stats.get('duration', 0) / 1000}
    steps = read(output / 'steps.json', [])
    if isinstance(steps, dict):
        steps = [steps]
    cleanup = read(output / 'cleanup_report.json', {})
    usage = read(output / 'resource_usage.json', {})
    env = read(output / 'environment.json', {})
    prior_real = read(output / 'prior_real_evidence.json')
    results = {'run_id': output.name, 'suites': suites, 'steps': steps, 'model_budget': read(output / 'model_budget.json', {'requests': 0, 'actual_tokens': 0}),
               'prior_real_evidence': prior_real,
               'not_verified': ['Java MySQL/Redis/RocketMQ end-to-end seckill concurrency 20/50/100 and MQ backlog; full isolated stack not provisioned',
                                'Real Java business state checks; Java is mocked in Agent tests', 'Original free-text recommendation amount/eligibility correctness',
                                'Production network Agent load, real-model concurrency throughput', 'Production RAG quality; small isolated fixture corpus only',
                                'Hard kill or power loss cannot guarantee finally execution; rerun cleanup with recorded manifest'],
               'cleanup': cleanup, 'remaining_run_bytes_before_final_report': sum(p.stat().st_size for p in output.rglob('*') if p.is_file())}
    save(output / 'test_results.json', results)
    if not (output / 'rag_comparison.csv').exists():
        write_csv(output / 'rag_comparison.csv', [{'mode': 'all', 'status': 'NOT_VERIFIED', 'reason': 'No successful bounded real RAG evaluation'}])
    def table(rows):
        if not rows:
            return '无结果。'
        fields = list(rows[0])
        return '\n'.join(['| ' + ' | '.join(fields) + ' |', '| ' + ' | '.join(['---'] * len(fields)) + ' |'] +
                         ['| ' + ' | '.join(str(r.get(f, '')) for f in fields) + ' |' for r in rows])
    def csv_rows(name):
        path = output / name
        return list(csv.DictReader(path.open(encoding='utf-8-sig'))) if path.exists() else []
    phases = usage.get('snapshots', [])
    phase_rows = [{'phase': s['phase'], 'available_memory_bytes': s.get('available_memory_bytes'),
                   'cpu_percent': s.get('cpu_percent'), 'run_bytes': s['run_bytes'],
                   'D_free_bytes': next((d['free_bytes'] for d in s.get('drives', []) if d['drive'] == 'D'), None)}
                  for s in phases if s['phase'] in ('before', 'end', 'after-cleanup')]
    import re
    def size_bytes(text):
        match = re.match(r'([\d.]+)\s*([KMGT]?i?B)', text.strip())
        if not match:
            return 0
        units = {'B': 1, 'kB': 1000, 'KB': 1000, 'MB': 10**6, 'GB': 10**9,
                 'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3}
        return float(match[1]) * units.get(match[2], 1)
    docker_memory_samples, docker_cpu_samples = [], []
    for snapshot in phases:
        mem, cpu = 0, 0
        for line in snapshot.get('docker_stats', []):
            stat = json.loads(line) if isinstance(line, str) else line
            mem += size_bytes(stat['MemUsage'].split('/')[0])
            cpu += float(stat.get('CPUPerc', '0%').rstrip('%'))
        docker_memory_samples.append(mem)
        docker_cpu_samples.append(cpu)
    summary = {'sampled_peak_docker_memory_bytes': max(docker_memory_samples, default=0),
               'sampled_peak_docker_cpu_percent': max(docker_cpu_samples, default=0),
               'sampled_peak_windows_cpu_percent': max((s.get('cpu_percent') or 0 for s in phases), default=0),
               'minimum_available_windows_memory_bytes': usage.get('minimum_available_memory_bytes'),
               'sampled_peak_run_bytes': usage.get('peak_run_bytes'),
               'note': 'Whole-machine Windows values include unrelated applications; short peaks between samples may be missed.'}
    save(output / 'resource_summary.json', summary)
    prior_budget = (prior_real or {}).get('ledger', {})
    local_pass = all(s.get('status') == 'PASS' for s in steps)
    rag_rows = csv_rows('rag_comparison.csv')
    rag_summary = []
    for mode in dict.fromkeys(row['mode'] for row in rag_rows):
        group = [row for row in rag_rows if row['mode'] == mode and row.get('recall_at_5')]
        if group:
            rag_summary.append({'mode': mode, 'queries': len(group),
                'mean_recall_at_5': round(statistics.mean(float(r['recall_at_5']) for r in group), 4),
                'mrr': round(statistics.mean(float(r['mrr']) for r in group), 4),
                'mean_latency_ms': round(statistics.mean(float(r['latency_ms']) for r in group), 4)})
    fixture_comparison = csv_rows('baseline_comparison.csv')
    baseline_calls = sum(int(row['model_requests']) for row in fixture_comparison if row['mode'] == 'original_sequential_v02')
    upgraded_calls = sum(int(row['model_requests']) for row in fixture_comparison if row['mode'] == 'router_supervisor_dag')
    core_passed = sum(read(p, {}).get('success', False) for p in (output / 'cases').glob('*.json'))
    prior_notice = (f"前一轮 {prior_real['run_id']} 的真实模型证据：4次请求，API实际累计 {prior_budget.get('actual_tokens')} Token；"
                    "超过24000预算1719 Token，已停止后续真实调用。旧实现只按UTF-8字节预留，供应商返回输入Token高于预留，门禁存在缺陷。"
                    "已改为至少8192且结合历史最大用量的保守预留，预留失效/usage缺失/请求结果未知立即阻断后续调用，新增5项Mock预算回归。"
                    "本轮不再发真实请求，不扩大额度。仅首个店铺详情样例完成真实原版对照（原版4次调用，新版0次），单样例不得外推。"
                    "其余原版样例因预算未完成，CSV中false表示未完成验证，不代表架构正确率；新版消费样例退化到确定性路径，未验证真实模型解释。"
                    if prior_real else '本轮无引用的前次真实模型证据。')
    report_text = f'''# LocalPerks AI v0.3 系统验证报告

运行 ID：`{output.name}`。证据目录：`{output}`。结果区分本轮本地验证与明确标注来源的前轮真实模型证据。

本轮本地验证状态：**{'PASS' if local_pass else 'FAIL/INCOMPLETE'}**。真实模型预算异常属于前轮保留问题，不因本轮本地通过而消失；完整系统验收仍有NOT_VERIFIED项。

## 环境、边界与复现

Git HEAD：`{env.get('git_head')}`，工作区已有未提交修改，未提交或推送 Git。源码逐文件 SHA256、数据集和冻结 ZIP 哈希见 environment.json。
Python：{env.get('python')}。依赖：{json.dumps(env.get('dependencies'), ensure_ascii=False)}。
Java 17 / Maven 离线缓存，Node 20 / npm 10；精确版本及命令输出见日志。模型：`{env.get('model')}`，Embedding：`{env.get('embedding_model')}`，Rerank：`{env.get('rerank_model')}`。

复现：在仓库根目录执行 `pwsh -NoProfile -File scripts/run_validation.ps1`（PowerShell 7），显式加 `-RealModel` 才允许联网模型测试。最多40次请求、Token预算24000、每次Chat输出上限768；全程账本按真正出站尝试次数计数，usage缺失/未知请求/预留失效则停止后续付费请求。预算预留取至少8192和历史用量加本次请求大小的较大值，不能保证外部供应商不注入隐藏Token。目的地址限制为用户授权的api.openlux.ai。

{prior_notice}

真实接口审查：FastAPI /agent/chat、/agent/tasks/{{id}}/resume、/agent/actions/prepare/confirm/reconcile；登录头转交 Java /user/me 得到服务端身份。个人工具不能传用户ID。Java /voucher-order/seckill/{{id}} 保留Lua/MQ/MySQL边界。本次Agent测试使用注入业务替身，不能证明真实业务数据库状态。

MySQL/MQ正常数据不访问；Redis/pgvector使用本轮标签容器、动态回环端口、tmpfs、合计512MiB内存上限、各0.5CPU；SQLite/pytest临时文件仅在run/temp。正式 tasks.sqlite3 不写。Java旧的500线程/GEO/HLL/固定锁全上下文测试默认门禁跳过；独立Redis限流测试使用专用端口。正常容器/卷不删除，最终按仓库要求仅stop。

## 实际执行与回归计数

{table([dict(suite=k, **v) for k,v in suites.items()])}

{table([{'name':s['name'],'status':s['status'],'seconds':round(s.get('duration_seconds',0),3),'command':s.get('command',''),'reason':s.get('reason','')} for s in steps])}

核心任务固定40条（evaluations/datasets/core_v03.jsonl），每条预期/实际/轨迹/失败原因/耗时/调用数保存在 cases/。路由与规则单测无工具轨迹，明确为空；HTTP工作流有真实程序轨迹，业务返回是隔离fixture。Token为0表示未调用远程供应商，Mock报告的Token不当作真实消耗。

本轮核心样例实际通过：{core_passed}/40。

## 功能、恢复、故障与安全

金额由Decimal断言，覆盖预算/距离/人数、库存0/未知、未开始/过期、规则未知和不凑数。快速路由断言无Planner和模型请求；复合任务核验DAG依赖、券分支及部分失败警告。多轮和恢复复用旧测试并补充真实async取消后的SQLite状态、恢复重查。结果以JUnit和cases为准，失败不能因本文描述变成通过。

故障注入覆盖Java500、连接/读取超时、业务失败和非法响应；断言读取最多2次、每次请求轨迹均记录。LLM429、超时、非法JSON/数组断言仅一次调用；Rerank异常/非法索引保持RRF顺序；pgvector关闭端口返回明确错误。既有测试覆盖DAG失败、循环、STEP_LIMIT、checkpoint恢复和身份隔离。

并发两次确认以SQLite CAS及替身POST计数断言仅1次写入；写超时后仅GET核对、不重复POST；价格变化、无库存、过期和错误凭证均禁止写。跨用户/失效身份不能读取或恢复任务，Java Mockito测试核验订单归属。真实MySQL订单数、Lua原子扣库存、MQ投递与消费一致性本轮 NOT_VERIFIED。

## Agent轻量并发与DAG

{table(csv_rows('performance.csv'))}

每档3波、1/2/4/8并发用户，三类合计135次请求。计时为同进程ASGI含SQLite和业务替身，非网络服务吞吐、非真实模型性能、非生产容量。无业务订单。P50/P95为小样本nearest-rank。节点耗时和原始轨迹见load-*.json；CPU/内存来自采样，不是每请求归因。升档前有资源/成功率门禁。

实际秒杀Lua组件测试（专用Redis，非Java/MQ/MySQL全链路）：

{table(csv_rows('seckill_component.csv'))}

三独立DAG分支计时（每分支注入20ms等待，非业务性能）：{json.dumps(read(output / 'dag_timing.json', {}), ensure_ascii=False)}。

## 原版/新版对照

{table(csv_rows('baseline_comparison.csv'))}

{table(csv_rows('baseline_real_comparison.csv') or csv_rows('prior-real-evidence/baseline_real_comparison.csv'))}

同一冻结原版ZIP、同一四条任务、同一业务fixture。Mock结果仅验证工具协议/路由调用开销。真实模型结果（若存在）仍为fixture业务，非真实Java端到端。判定固定为HTTP成功且工具实际返回指定业务证据，消费任务必须查券；原版自由文本金额/排名没有确定性认证。每组4条、无统计显著性，不外推生产改进。

本轮脚本化Mock协议对照，原版{baseline_calls}次模型请求，新版{upgraded_calls}次；此差值只描述固定测试协议的路由开销，不当作真实模型效率结论。真实模型仅店铺详情单样例可比（4→0）。

## RAG消融

{table(csv_rows('rag_comparison.csv'))}

{table(rag_summary)}

本轮离线消融：6文档/6固定问题，真实BM25、独立pgvector、RRF；Embedding为手工3维向量且固定查询向量，Rerank为注入503后的真实降级代码。所有行标记synthetic，指标仅验证检索组件和降级，不衡量真实语义检索模型质量。Recall@5按相关源定义，MRR按首个相关块；引用准确性只验证来源存在，不等同语义正确性。正常知识库未读写。真实Dense/Embedding/Rerank对照因前轮模型预算耗尽NOT_VERIFIED，不借历史结果代替。

## 资源与清理

Windows采样最小可用内存：{usage.get('minimum_available_memory_bytes')} bytes；run目录采样峰值：{usage.get('peak_run_bytes')} bytes。完整初始、运行中、结束、清理后Windows/容器CPU内存、镜像/构建缓存/卷占用、C/D/F空间见resource_usage.json与.md。无F盘时明确缺失，不使用D盘数字冒充F盘。WSL配置只读，不设置内存上限；当前主机总内存以实测为准。Docker全局总内存没有配置成6GB硬限，测试自身容器有512MiB硬限。

{table(phase_rows)}

采样峰值摘要：{json.dumps(summary, ensure_ascii=False)}。

最终容器、数据卷和端口复核见final_state.json（存在时）。所有轮次累计保留空间/清理量见session_summary.json。主动异常退出实验v03-20261105-194222-72134f26返回1、finally移除2个本轮容器；仅作为故障注入证据，不计入通过套件。

{table(cleanup.get('actions', []))}

清理文件字节：{cleanup.get('reclaimed_file_bytes')}。保留最终报告、原始JSON/CSV、JUnit和失败证据。`test_cleanup.ps1`默认DryRun，Apply只接受run/temp和标签/完整ID/项目/无持久挂载均吻合的容器；未知资源保留并报错。常规退出和异常在finally清理，原始失败退出码保留。硬终止/断电需按清单重跑清理。Docker内部回收与Windows实际磁盘变化分别读取，虚拟磁盘不保证缩小。未删除镜像、共享构建缓存或原有数据卷。

当前证据目录最终写报告前大小：{results['remaining_run_bytes_before_final_report']} bytes；最终大小见disk_final.json。测试生成frontend/dist和Maven target为可复用构建产物，单独统计，未冒称它们全部回收。

## 已知限制与未验证项

''' + '\n'.join('- ' + x for x in results['not_verified']) + f'''

真实模型账本：{json.dumps(results['model_budget'], ensure_ascii=False)}。

## 可用于简历的实测表述

- 建立40条固定消费任务和程序判定，本轮{core_passed}/40通过，逐条保留输入、输出、工具证据与失败原因。
- 对交易CAS并发确认、未知结果只读核对、身份隔离及中断恢复建立可复现回归断言；结论范围为SQLite与业务替身/Java Mockito。
- 完成1/2/4/8档、三种编排路径共135次受控ASGI请求，保留逐请求轨迹和延迟；准确范围为Mock业务/模型。
- 实现带资源登记、标签验证、路径/链接防护、DryRun/Apply及finally清理的Windows验证入口；资源回收量见本轮清理证据。
'''
    (ROOT / 'docs/TEST_REPORT.md').write_text(report_text, encoding='utf-8')
    (output / 'TEST_REPORT.md').write_text(report_text, encoding='utf-8')
    save(output / 'disk_final.json', {'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
        'run_bytes_before_this_file': sum(p.stat().st_size for p in output.rglob('*') if p.is_file()),
        'frontend_dist_bytes': sum(p.stat().st_size for p in (ROOT / 'frontend/dist').rglob('*') if p.is_file()),
        'maven_target_bytes': sum(p.stat().st_size for p in (ROOT / 'target').rglob('*') if p.is_file()),
        'd_drive_free_bytes': shutil.disk_usage('D:/').free, 'f_drive_present': Path('F:/').exists()})


async def main(args):
    output = Path(args.output).resolve()
    if args.mode == 'environment':
        environment(output)
    elif args.mode == 'seed':
        await seed(output)
    elif args.mode == 'load':
        await load(output)
    elif args.mode == 'compare':
        await compare(output)
    elif args.mode == 'real':
        await compare(output, real=True)
        try:
            await rag_compare(output)
        except Exception as exc:
            save(output / 'rag_unverified.json', {'status': 'NOT_VERIFIED', 'reason': type(exc).__name__})
            raise
    elif args.mode == 'rag-fixture':
        await rag_fixture(output)
    else:
        report(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['environment', 'seed', 'load', 'compare', 'real', 'rag-fixture', 'report'])
    parser.add_argument('--output', required=True)
    asyncio.run(main(parser.parse_args()))

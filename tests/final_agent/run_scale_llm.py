"""Run a new 100-task real-model holdout with an isolated Java and knowledge stack."""
import argparse
import asyncio
import hashlib
import json
import re
import secrets
import sys
import threading
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
AGENT = ROOT / 'agent-service'
sys.path[:0] = [str(HERE), str(ROOT / 'tests/real_e2e'), str(AGENT)]
from run import Run, pct
from model_budget import write
from usage import ActualApiTransport, CASE, export, rows
from scale_holdout import freeze
from scale_capture import FaultCapture, proof, seed_consumer
from core.config import load_settings
from main import create_app
from memory.task_store import TaskStore
from ops.engine import Investigator
from ops.tools import ObservationScope, evidence
from ops.usage import UsageTransport
from rag.ingestion import chunks
from rag.retrieval import EmbeddingClient, RagService, Reranker
from rag.store import PgVectorStore


def hash_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Meter(ActualApiTransport):
    def __init__(self, path, maximum):
        super().__init__(path, output_limit=2400)
        self.limit = asyncio.Semaphore(maximum)

    async def handle_async_request(self, request):
        async with self.limit:
            return await super().handle_async_request(request)


class FrozenObservations:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.count = Counter()

    async def read(self, tool, scope):
        if scope.model_dump() != self.snapshot['scope']:
            raise ValueError('Observation scope mismatch')
        index = self.count[tool]
        self.count[tool] += 1
        return self.snapshot['samples'][tool][index]


class Business(httpx.AsyncHTTPTransport):
    def __init__(self, faults):
        super().__init__(retries=0)
        self.faults = faults

    async def handle_async_request(self, request):
        if request.url.path == self.faults.get(CASE.get()):
            return httpx.Response(503, json={'success': False, 'errorMsg': 'Explicit isolated tool failure'})
        return await super().handle_async_request(request)


def prepare_dataset(path):
    day = (datetime.now(ZoneInfo('Asia/Shanghai')) + timedelta(days=2)).date().isoformat()
    tasks = freeze()
    for task in tasks:
        if not task.get('turns'):
            continue
        for i, turn in enumerate(task['turns']):
            people, budget, radius, hour = turn['slots']
            if i == 0:
                turn['message'] = (f'推荐适合{people}人吃饭的餐厅，人均{budget}元以内，范围{radius / 1000:g}公里。'
                                   f'预计{day} {hour:02d}:00用餐，我的位置是{task["lon"]},{task["lat"]}。'
                                   '请核算一张券的估算实付，区分已验证优惠和仍需核对的候选。')
            else:
                turn['message'] = f'刚才的消费计划人均{budget}元，改成{people}人，地点、时间和范围不变，请重新比较优惠。'
    target = path / 'frozen-holdout.json'
    write(target, {'tasks': tasks, 'counts': dict(Counter(task['group'] for task in tasks)),
                   'protocol': '100 independent fixture IDs or distinct knowledge questions. Freeze before any model call.',
                   'baselines': {'consumer': 'Single real Chat, same constraint turns and complete independently fetched Java facts',
                                 'rag': 'BM25 then the same real grounded generator',
                                 'ops': 'Real Chat log-only Investigator mode'},
                   'scoring': 'First execution only. Grounded answer, actual Chat usage and independent exact facts/citations. No LLM judge.'})
    digest = hash_file(target)
    write(path / 'frozen-holdout.sha256.json', {'sha256': digest})
    return tasks, digest


def matches_decision(output, expected):
    decision = output.get('decision', output)
    constraints = output.get('constraints', {})
    errors = []
    for key, value in [('people', expected['people']), ('budget_per_person', expected['budget']), ('radius_m', expected['radius'])]:
        if constraints.get(key) != value:
            errors.append(key)
    for observed_key, gold_key in [('recommendations', 'verified'), ('unverified_candidates', 'unknown')]:
        actual = decision.get(observed_key, [])
        wanted = expected[gold_key]
        if not isinstance(actual, list) or any(not isinstance(item, dict) for item in actual):
            errors.append(observed_key + ':shape')
            continue
        fields = ['shop_id', 'voucher_id', 'estimated_cash_cost', 'stock']
        if [[item.get(k) for k in fields] for item in actual] != [[item[k] for k in fields] for item in wanted]:
            errors.append(observed_key + ':facts_or_order')
        if any(item.get('source') != '/voucher/list/' + str(item.get('shop_id')) for item in actual):
            errors.append(observed_key + ':source')
    if expected['partial'] and not decision.get('warnings'):
        errors.append('missing_partial_warning')
    return errors


def grounded(output):
    facts = output.get('answer_evidence', {})
    composition = output.get('answer_composition', '')
    if not isinstance(composition, str) or not isinstance(facts, dict):
        return False
    rendered = re.sub(r'\{\{(\w+)\}\}', lambda match: str(facts.get(match[1], 'UNKNOWN')), composition)
    return (output.get('answer_origin') == 'llm_grounded' and bool(composition) and
            rendered == output.get('answer') and not any(row.get('type') == 'degradation' for row in output.get('trace', [])))


async def consumer_main(task, client, token):
    outputs = []
    session = None
    for turn in task['turns']:
        body = {'message': turn['message']}
        if session:
            body['session_id'] = session
        response = await client.post('/agent/chat', json=body, headers={'authorization': token})
        result = response.json()
        result['http_status'] = response.status_code
        outputs.append(result)
        session = result.get('session_id')
    checks = [matches_decision(result, turn['expected']) for result, turn in zip(outputs, task['turns'])]
    evidence_ok = all(not check for check in checks)
    if len(outputs) > 1:
        evidence_ok &= bool(session) and all(result.get('session_id') == session for result in outputs)
    return outputs, evidence_ok, evidence_ok and all(map(grounded, outputs)), checks


async def consumer_baseline(task, transport, cfg, oracle):
    inputs = []
    for entry in task['facts']:
        shop = oracle['shops'][entry['shop']]
        offer = oracle['vouchers'][entry['voucher']]
        inputs.append({'shop': shop, 'voucher': offer if entry['shop'] != task['fault_shop'] else
                       {'http_status': 503, 'source': '/voucher/list/' + str(entry['shop'])}})
    instructions = (
        'You are the single-call LocalPerks baseline. Return one JSON object, no Markdown. '
        'Resolve the turns in order. Use only provided shop and voucher facts. '
        'Give {"turns":[{"constraints":{"people":number,"budget_per_person":number,"radius_m":number},'
        '"recommendations":[{"shop_id":number,"voucher_id":number,"estimated_cash_cost":number,"stock":number,'
        '"source":"/voucher/list/<shop_id>"}],"unverified_candidates":[],"warnings":[]}]} for each turn. '
        'Filter distance and opening, reference budget, active sales, stock and exact party size. '
        'Without explicit redemption terms put eligible data in unverified_candidates. '
        'A 503 must yield a warning and no fabricated offer. '
        'Estimated group cash=max(avg*party-face/100,0)+pay/100. '
        'Order by score=70*(face-pay)/face+20*(1-distance/radius)+10*(1-avg/budget).'
    )
    payload = {'model': cfg.model, 'messages': [{'role': 'system', 'content': instructions},
               {'role': 'user', 'content': json.dumps({'turns': [turn['message'] for turn in task['turns']],
                 'java_facts': inputs}, ensure_ascii=False)}], 'temperature': 0}
    async with httpx.AsyncClient(transport=transport, timeout=90, trust_env=False) as client:
        response = await client.post(cfg.base_url.rstrip('/') + '/chat/completions', json=payload,
                                     headers={'authorization': 'Bearer ' + cfg.api_key})
    response.raise_for_status()
    raw = response.json()['choices'][0]['message']['content']
    parsed = json.loads(re.sub(r'^\s*```(?:json)?\s*|\s*```\s*$', '', raw.strip()))
    results = parsed.get('turns', [])
    checks = [matches_decision(result, turn['expected']) for result, turn in zip(results, task['turns'])]
    valid = len(results) == len(task['turns']) and all(not check for check in checks)
    return {'raw': raw, 'parsed': parsed, 'java_facts': inputs}, valid, valid, checks


async def start_rag(run, cfg, transport):
    run.create('pgvector', 'pgvector/pgvector:pg16', ['--memory', '384m', '--memory-swap', '384m',
        '--tmpfs', '/var/lib/postgresql/data:rw,size=128m', '-p', f'127.0.0.1:{run.pg_port}:5432',
        '-e', 'POSTGRES_PASSWORD=' + run.mysql_password, '-e', 'POSTGRES_DB=scale_rag'],
        ['postgres', '-c', 'shared_buffers=32MB', '-c', 'max_connections=15'])
    dsn = f'postgresql://postgres:{run.mysql_password}@127.0.0.1:{run.pg_port}/scale_rag'
    cfg = replace(cfg, pgvector_dsn=dsn)
    store = PgVectorStore(dsn, cfg.embedding_dimensions)
    for _ in range(20):
        try:
            await store.initialize()
            break
        except Exception:
            await asyncio.sleep(1)
    else:
        raise RuntimeError('Isolated pgvector startup failed')
    source = ROOT / 'docs/AGENT_BUSINESS_KNOWLEDGE.md'
    documents = []
    for section in source.read_text(encoding='utf-8').split('\n## ')[1:]:
        title, text = section.split('\n', 1)
        documents.extend(chunks(text, 'repo:docs/AGENT_BUSINESS_KNOWLEDGE.md#' + title, title=title,
                                size=600, overlap=80))
    write(run.path / 'rag-sources.json', {'sha256': hash_file(source), 'documents': [doc.to_dict() for doc in documents]})
    embed = EmbeddingClient(cfg, transport)
    token = CASE.set('preparation-rag-index')
    try:
        vectors, _ = await embed.embed([doc.text for doc in documents])
        await store.upsert_document(documents, vectors, cfg.embedding_model)
    finally:
        CASE.reset(token)
    return RagService(cfg, store=store, embedding=embed, reranker=Reranker(cfg, transport), model_transport=transport)


async def rag_task(task, rag, mode):
    trace = []
    async def emit(event):
        trace.append(event)
    retrieval = await rag.search(task['message'], emit, baseline=mode == 'baseline')
    result = await rag.answer_from_retrieval(task['message'], retrieval, emit)
    result['trace'] = trace
    titles = {item['title'] for item in result.get('sources', [])}
    hits = {hit['id']: hit for hit in retrieval['hits']}
    citations_valid = all(item['id'] in hits and item['title'] == hits[item['id']]['title']
                          for item in result.get('sources', []))
    expected = set(task['relevant_titles'])
    evidence_ok = bool(result.get('abstained')) == (not expected) and citations_valid
    evidence_ok &= not titles if not expected else bool(titles) and titles <= expected
    return {'result': result, 'retrieved': retrieval['hits']}, evidence_ok, evidence_ok and grounded(result), []


async def ops_task(task, snapshot, cfg, store, transport, mode):
    scope = ObservationScope.model_validate(snapshot['scope'])
    task_id = await store.create('scale-evaluation', str(uuid4()), {'symptom': task['symptom']}, kind='ops')
    investigator = Investigator(cfg, store, FrozenObservations(snapshot), UsageTransport(store.path, transport))
    result = await investigator.run(task_id, scope, task['symptom'], mode='hybrid' if mode == 'primary' else 'logs')
    observed = {item['id']: item for item in result.get('observations', [])}
    references = result.get('evidence_ids', [])
    matched = [observed[ref] for ref in references if ref in observed and 'raw' in observed[ref]]
    authentic = (bool(matched) and len(matched) == len(references) == len(set(references)) and
                 all(item['raw'] in snapshot['samples'][item['tool']] and
                     evidence(item['id'], item['tool'], item['raw'])['sha256'] == item['sha256'] for item in matched))
    root_ok = result.get('root_code') == task['root']
    try:
        valid = root_ok and authentic and proof(task, [item['raw'] for item in matched])
    except (KeyError, TypeError, ValueError, IndexError):
        valid = False
    route_ok = mode == 'baseline' or (result.get('route', {}).get('path') == 'agent_deep' and
                                    result.get('route', {}).get('review') == 'verified')
    return result, valid, valid and route_ok and result.get('state') == 'completed', [{'root_correct': root_ok, 'model_review': route_ok}]


def write_summary(path, results):
    groups = {}
    for row in results:
        for group in (row['group'], 'all'):
            groups.setdefault((row['mode'], group), []).append(row)
    aggregates = []
    for (mode, group), group_rows in sorted(groups.items()):
        aggregates.append({'mode': mode, 'group': group, 'tasks': len(group_rows),
            'real_model_tasks': sum(row['real_chat_successes'] > 0 for row in group_rows),
            'passed': sum(row['pass'] for row in group_rows), 'evidence_correct': sum(row['evidence'] for row in group_rows),
            'p50_ms': pct([row['latency_ms'] for row in group_rows], .5),
            'p95_ms': pct([row['latency_ms'] for row in group_rows], .95),
            'actual_api_requests': sum(row['actual_requests'] for row in group_rows),
            'known_tokens': sum(row['known_tokens'] for row in group_rows),
            'missing_usage': sum(row['missing_usage'] for row in group_rows)})
    write(path / 'scale-llm-results.json', {'rows': results, 'aggregate': aggregates})


async def execute(run, cfg, tasks, transport, snapshots, rag, facts, concurrency):
    artifacts = run.path / 'first-attempts'
    artifacts.mkdir()
    store = TaskStore(run.path / 'tasks-evidence.sqlite3')
    faults = {'primary/' + task['id']: '/voucher/list/' + str(task['fault_shop'])
              for task in tasks if task.get('fault_shop')}
    app = create_app(settings=cfg, transport=Business(faults), model_transport=transport, task_store=store, rag_service=rag)
    all_results = []
    turns = asyncio.Semaphore(concurrency)
    ops_turns = asyncio.Semaphore(2)

    async def one(task, mode, client):
        label = mode + '/' + task['id']
        marker = CASE.set(label)
        started = time.perf_counter()
        output, evidence_ok, valid, checks = None, False, False, []
        try:
            async with asyncio.timeout(250):
                group = task['group']
                if group == 'ops':
                    snapshot = snapshots.get(task['id'])
                    if not snapshot:
                        raise RuntimeError('Frozen source unavailable')
                    output, evidence_ok, valid, checks = await ops_task(task, snapshot, cfg, store, transport, mode)
                elif group == 'rag':
                    if rag is None:
                        raise RuntimeError('Isolated knowledge preparation unavailable')
                    output, evidence_ok, valid, checks = await rag_task(task, rag, mode)
                elif mode == 'baseline':
                    output, evidence_ok, valid, checks = await consumer_baseline(task, transport, cfg, facts)
                else:
                    output, evidence_ok, valid, checks = await consumer_main(task, client, run.tokens[10001])
        except Exception as error:
            checks = [type(error).__name__]
        finally:
            CASE.reset(marker)
        calls = rows(run.path, label)
        successful_chats = sum(call['kind'] == 'chat' and call['status'] == 200 for call in calls)
        row = {'id': task['id'], 'group': task['group'], 'mode': mode, 'attempt': 1,
               'real_chat_successes': successful_chats, 'actual_requests': len(calls),
               'known_tokens': sum(call['total_tokens'] or 0 for call in calls),
               'missing_usage': sum(call['total_tokens'] is None for call in calls),
               'latency_ms': round((time.perf_counter() - started) * 1000, 2),
               'evidence': bool(evidence_ok), 'pass': bool(valid and successful_chats), 'checks': checks}
        write(artifacts / f'{mode}-{task["id"]}.json', {'result': row, 'output': output, 'request_ids': [call['id'] for call in calls]})
        all_results.append(row)
        write_summary(run.path, all_results)
        if len(all_results) % 10 == 0:
            run.stage(f'first-attempts-{len(all_results)}-of-200')

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app, raise_app_exceptions=False),
                                     base_url='http://isolated-agent', timeout=250) as client:
            async def schedule(task, mode):
                async with ops_turns if task['group'] == 'ops' else turns:
                    await one(task, mode, client)
            await asyncio.gather(*(schedule(task, mode) for task in tasks for mode in ('primary', 'baseline')))
    return all_results


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--parallel', type=int, choices=(2, 4, 6, 8), default=6)
    args = parser.parse_args()
    run = Run()
    tasks, digest = prepare_dataset(run.path)
    run.stage('frozen-100-' + digest[:12])
    print('SCALE_LLM ' + str(run.path), flush=True)
    cfg = load_settings()
    endpoint = urlsplit(cfg.base_url)
    assert cfg.configured and endpoint.scheme == 'https' and endpoint.hostname == 'api.openlux.ai'
    write(run.path / 'authorization.json', {'model': cfg.model, 'provider_host': endpoint.hostname,
          'scope': '100 synthetic first attempts and matched real baselines', 'parallel_provider': args.parallel,
          'per_case_limit': 18, 'chat_output_limit': 2400,
          'historical_ledgers_untouched': True, 'dataset_sha256': digest})
    ops_token = secrets.token_urlsafe(32)
    run.env.update(HMDP_OPS_ADMIN_TOKEN=ops_token, HMDP_RATE_LIMIT_TRUST_FORWARDED_HEADERS='true')
    try:
        run.sample('before', guard=True)
        run.build()
        await run.infrastructure()
        run.monitor_thread = threading.Thread(target=run.monitor, daemon=True)
        run.monitor_thread.start()
        cfg = replace(cfg, shoplink_base_url=run.java_url, redis_url=run.redis_url,
                      task_db=str(run.path / 'tasks-evidence.sqlite3'), pgvector_dsn='', turn_timeout=200, max_rounds=5)
        oracle = await seed_consumer(run, [task for task in tasks if 'facts' in task])
        write(run.path / 'java-fixture-oracle.json', oracle)
        facts = {name: {item['id']: item for item in items} for name, items in oracle.items()}
        assert len(facts['shops']) == len(facts['vouchers']) == 168
        snapshots = {}
        capture = FaultCapture(run, ops_token)
        await run.coupon(44999, 10)
        assert (await capture.submit(44999, 10099))['body']['success']
        await run.wait_orders(44999, 1)
        for task in tasks:
            if task['group'] != 'ops':
                continue
            try:
                value = await capture.capture(task)
                write(run.path / f'snapshot-{task["id"]}.json', value)
                snapshots[task['id']] = value
            except Exception as error:
                write(run.path / f'capture-failure-{task["id"]}.json', {'type': type(error).__name__, 'detail': str(error)})
            if int(task['id'][4:]) % 5 == 0:
                run.stage('captured-' + task['id'])
        write(run.path / 'snapshot-hashes.json', {name: hash_file(run.path / f'snapshot-{name}.json') for name in snapshots})
        assert hash_file(run.path / 'frozen-holdout.json') == digest
        transport = Meter(run.path, args.parallel)
        rag = None
        try:
            rag = await start_rag(run, cfg, transport)
        except Exception as error:
            write(run.path / 'rag-preparation-failure.json', {'type': type(error).__name__})
        await execute(run, cfg, tasks, transport, snapshots, rag, facts, args.parallel)
    except BaseException as error:
        run.failure = f'{type(error).__name__}: {error}'
        write(run.path / 'scale-llm-failure.json', {'type': type(error).__name__})
        raise
    finally:
        export(run.path)
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())

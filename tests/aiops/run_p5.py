"""Index verified historical cases and evaluate P5 on new isolated Java incidents."""
import asyncio
import argparse
import hashlib
import json
import os
import secrets
import sys
import threading
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from redis.asyncio import Redis

ROOT = Path(__file__).resolve().parents[2]
AGENT = ROOT / 'agent-service'
sys.path[:0] = [str(ROOT / 'tests/real_e2e'), str(ROOT / 'tests/final_agent'), str(AGENT)]
from run import Run, pct
from model_budget import write
from scale_capture import FaultCapture, now_ms
from usage import ActualApiTransport, CASE, export, rows
from core.config import load_settings
from main import create_app
from memory.task_store import TaskStore
from ops.case_history import CaseHistory, CaseStore
from ops.case_index import index_cases, load_cases
from ops.engine import Investigator
from ops.tools import LiveProvider, ObservationScope, TOOLS, evidence
from ops.usage import UsageTransport

UNKNOWN = 'INSUFFICIENT_EVIDENCE'
DRIFT = 'REDIS_STOCK_DRIFT'
PAUSED = 'MQ_CONSUME_DISABLED'
IP = 'IP_RATE_LIMIT'
DUP = 'DUPLICATE_DELIVERY'
MODES = ('p41', 'p5_rrf', 'p5_rerank')
# Labels, relevant incident IDs and scenarios are serialized before starting the stack.
FIXTURES = [
    ('live-canary', UNKNOWN, [DUP, DRIFT], ['mq-repeated-delivery', 'redis-persistent-stock-drift'],
     '复投后订单仍只有一条，但 Redis 库存与数据库有差异。多个故障是否有同一主因？'),
    ('checkout-delay', PAUSED, [PAUSED], ['mq-paused-consumption'],
     '接口受理后没有成单。请核对消费与库存差额的时序，排查 HTTP 超时是否真是这次的原因。'),
    ('ip-missing-cache', IP, [IP], ['ip-window-limiter', 'cache-key-absent', 'compound-ip-and-stock'],
     '共享出口 HTTP 429，同时 Redis 库存无值。请检查两个现象是否可以独立核验。'),
    ('user-missing-cache', UNKNOWN, [], ['ip-window-limiter', 'cache-key-absent'],
     '同一账号请求返回 429，库存键又查不到。请核对是否可以归为共享出口故障。'),
    ('duplicate-and-higher-stock', UNKNOWN, [DUP, DRIFT], ['mq-repeated-delivery', 'redis-persistent-stock-drift'],
     '订单有复投的幂等记录，同时缓存库存多于数据库。请检查多个故障是否来自同一操作。'),
    ('after-window-drift', UNKNOWN, [], ['historical-inventory-window'],
     '复盘历史事件窗口中的库存差异，区分当时库存与后来发生的变化。'),
    ('recovered-sample', UNKNOWN, [], ['redis-persistent-stock-drift'],
     'Redis 和 MySQL 的库存采样矛盾。请判断差额是否稳定，核对两次采样。'),
    ('compensation-residual', DRIFT, [DRIFT], ['broker-outage-timeout-fallback', 'redis-persistent-stock-drift'],
     '有订单失败与预留补偿，库存仍然不一致。请核对时序和真实库存，能否确认是哪次操作造成的？'),
    ('fastpath-drift', DRIFT, [DRIFT], [],
     '核对当前券的 Redis 库存差额。'),
]


def freeze(run):
    # A distinct isolated stack and freshly allocated voucher IDs for every run.
    base = 710_000 + secrets.randbelow(1000) * 20
    cases = [dict(id=name, root=root, categories=categories, relevant=related, symptom=symptom,
                  voucher=base + index, stock=17 + index * 3)
             for index, (name, root, categories, related, symptom) in enumerate(FIXTURES)]
    path = run.path / 'frozen-holdout.json'
    write(path, cases)
    return cases, sha(path)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def index(run, cfg, transport):
    run.create('pgvector', 'pgvector/pgvector:pg16', ['--memory', '384m', '--memory-swap', '384m',
        '--tmpfs', '/var/lib/postgresql/data:rw,size=128m', '-p', f'127.0.0.1:{run.pg_port}:5432',
        '-e', 'POSTGRES_PASSWORD=' + run.mysql_password, '-e', 'POSTGRES_DB=ops_p5_eval'],
        ['postgres', '-c', 'shared_buffers=32MB', '-c', 'max_connections=12'])
    dsn = f'postgresql://postgres:{run.mysql_password}@127.0.0.1:{run.pg_port}/ops_p5_eval'
    cfg = replace(cfg, pgvector_dsn=dsn)
    store = CaseStore(dsn, cfg.embedding_dimensions)
    for _ in range(20):
        try:
            await store.initialize()
            break
        except Exception:
            await asyncio.sleep(1)
    else:
        raise RuntimeError('Isolated pgvector startup failed')
    history = CaseHistory(cfg, store=store, transport=transport)
    token = CASE.set('indexing')
    try:
        first = await index_cases(ROOT, store, cfg.embedding_model, history.embed.embed)
        repeat = await index_cases(ROOT, store, cfg.embedding_model, history.embed.embed)
        write(run.path / 'index-proof.json', {'initial': first, 'unchanged_repeat': repeat,
              'storage': 'isolated pgvector', 'knowledge_sha256': sha(ROOT / 'docs/ops_knowledge/cases.json')})
        assert first['chunks'] == first['vectors'] and first['updated_chunks'] == first['chunks']
        assert repeat['updated_chunks'] == 0 and not repeat['embedding_usage']
    finally:
        CASE.reset(token)
    token = CASE.set('index-probe')
    try:
        hits, strategy, _ = await history.search('HTTP 读超时 RocketMQ 积压 MySQL 行锁 Hikari 等待 同步发送延迟关单',
                                                  service='mysql', code='HTTP_TIMEOUT')
        write(run.path / 'index-search-proof.json', {'strategy': strategy,
              'cases': [{'id': item['doc_id'], 'sections': item['matched_sections']} for item in hits]})
        assert len({item['doc_id'] for item in hits}) == len(hits) and any(
            item['doc_id'] == 'perf-hot-row-outbox' for item in hits)
    finally:
        CASE.reset(token)
    return cfg, store


async def capture(run, fault, provider, case, canary=None):
    name, voucher, stock = (case[key] for key in ('id', 'voucher', 'stock'))
    redis = Redis.from_url(run.redis_url, decode_responses=True)
    key = f'seckill:stock:{voucher}'
    start = now_ms()
    inputs, details = [], {}
    paused = False
    ip = '127.0.0.1'
    try:
        await run.clear_stage_window()
        await run.coupon(voucher, 2 if name == 'compensation-residual' else stock,
                         mysql_stock=0 if name == 'compensation-residual' else None,
                         future=name in ('ip-missing-cache', 'user-missing-cache'))
        start = now_ms()
        end = None
        if name in ('live-canary', 'duplicate-and-higher-stock'):
            reply = await fault.submit(voucher, 10012)
            inputs.append(reply)
            assert reply['body']['success']
            await run.wait_orders(voucher, 1)
            details['replay_accepted'] = 'VALIDATION_SEND SEND_OK' in await asyncio.to_thread(
                fault.replay, reply['body']['data']['orderId'])
            assert details['replay_accepted']
            await asyncio.sleep(2.5)
            await redis.set(key, stock + 3)
        elif name == 'checkout-delay':
            fault.consume(False, name)
            paused = True
            inputs = [await fault.submit(voucher, uid) for uid in (10014, 10015, 10016)]
            assert all(reply['body']['success'] for reply in inputs)
            await asyncio.sleep(3)
            await redis.set(key, stock - 5)
        elif name in ('ip-missing-cache', 'user-missing-cache'):
            users = range(10020, 10051) if name.startswith('ip-') else [10011] * 6
            inputs = [await fault.submit(voucher, user, ip) for user in users]
            assert [reply['http'] for reply in inputs] == [200] * (len(users) - 1) + [429]
            assert await redis.delete(key) == 1
        elif name == 'after-window-drift':
            await asyncio.sleep(.05)
            end = now_ms()
            original_scope = ObservationScope(start=start, end=end, voucher_id=voucher, ip=ip)
            details['before'] = await provider.read('inventory', original_scope)
            assert int(details['before']['data']['redisStock']) == stock
            await asyncio.sleep(31)
            await redis.set(key, stock - 6)
            details['later_write_at'] = now_ms()
        elif name == 'recovered-sample':
            await redis.set(key, stock + 8)
        elif name == 'compensation-residual':
            inputs.append(await fault.submit(voucher, 10061))
            assert inputs[0]['body']['success']
            details['compensated'] = await run.wait_orders(voucher, 0, failed=True)
            assert details['compensated']['redis_stock'] == 2
        elif name == 'fastpath-drift':
            await redis.set(key, stock - 7)
        scope = ObservationScope(start=start, end=end or max(start + 1, now_ms()), voucher_id=voucher, ip=ip)
        samples = {tool: [await provider.read(tool, scope)] for tool in TOOLS}
        if name == 'recovered-sample':
            await redis.set(key, stock)
        await asyncio.sleep(.4)
        samples['inventory'].append(await provider.read('inventory', scope))
        snapshot = {'scope': scope.model_dump(), 'samples': samples, 'inputs': inputs, 'injection_details': details,
                    'origin': 'Isolated Java read-only HTTP observations', 'run_id': run.id}
        write(run.path / f'snapshot-{name}.json', snapshot)
        assert requirements(case, [raw for samples_by_tool in samples.values() for raw in samples_by_tool]), name
        if canary:
            await canary(case, scope, snapshot)
        return snapshot
    finally:
        await redis.aclose()
        if paused:
            fault.consume(True, name)
            write(run.path / f'restored-{name}.json', await run.wait_orders(voucher, 3))


def requirements(case, cited):
    """Independent evidence rubric over actual scoped samples, including refusal reasons."""
    name = case['id']
    inventories = [raw for raw in cited if raw['tool'] == 'inventory']
    data = {raw['tool']: raw['data'] for raw in cited}
    events = [event for raw in cited for event in raw['data'].get('events', [])]
    stocks = [(int(d['mysql'][0]['stock']), None if d.get('redisStock') is None else int(d['redisStock']),
               int(d['pending'][0]['total'])) for raw in inventories if (d := raw['data']).get('mysql')]
    stable = (len(stocks) == 2 and stocks[0] == stocks[1] and stocks[0][0] != stocks[0][1] and stocks[0][2] == 0
              and abs(inventories[1]['observedAt'] - inventories[0]['observedAt']) >= 300)
    if name in ('live-canary', 'duplicate-and-higher-stock'):
        delivery = data.get('delivery', {})
        received = Counter(e['orderId'] for e in delivery.get('events', []) if e['type'] == 'mq_received')
        completed = Counter(e['orderId'] for e in delivery.get('events', []) if e['type'] == 'mq_handler_returned')
        idempotent = {e['orderId'] for e in delivery.get('events', []) if e['type'] == 'order_idempotent'}
        orders = Counter(str(o['orderId']) for o in delivery.get('orders', []))
        return stable and any(received[order] >= 2 and completed[order] >= 2 and orders[order] == 1 for order in idempotent)
    if name == 'checkout-delay':
        mq = data.get('mq', {})
        return (mq.get('consumeEnabled') is False and mq.get('lag', 0) >= 3 and
                any(pending == 3 and db - redis == 5 for db, redis, pending in stocks if redis is not None))
    if name in ('ip-missing-cache', 'user-missing-cache'):
        dimension = 'ip' if name.startswith('ip-') else 'user'
        return (any(e.get('status') == 429 and e.get('limitScope') == dimension for e in events) and
                (dimension == 'ip' or any(redis is None for _, redis, _ in stocks)))
    if name == 'after-window-drift':
        return any(raw['observedAt'] - raw['windowEnd'] >= 30_000 for raw in inventories)
    if name == 'recovered-sample':
        return len(stocks) == 2 and stocks[0] != stocks[1]
    if name == 'compensation-residual':
        return stable and any(e.get('type') == 'reservation_compensated' for e in events)
    return stable


class ReplayObservations:
    def __init__(self, snapshot):
        self.samples = snapshot['samples']
        self.counts = Counter()

    async def read(self, tool, scope):
        index = self.counts[tool]
        self.counts[tool] += 1
        return self.samples[tool][index]


def grade(case, result, requests, snapshot=None):
    observed = {item['id']: item for item in result.get('observations', [])}
    references = result.get('evidence_ids', [])
    matched = [observed[ref] for ref in references if ref in observed and 'raw' in observed[ref]]
    authentic = (bool(matched) and len(matched) == len(references) == len(set(references)) and all(
        (snapshot is None or item['raw'] in snapshot['samples'][item['tool']]) and
        evidence(item['id'], item['tool'], item['raw'])['sha256'] == item['sha256'] for item in matched))
    root_ok = result.get('root_code') == case['root'] and result.get('state') == 'completed'
    categories = {item['code'] for item in result.get('verified_categories', [])}
    valid = bool(root_ok and authentic and categories == set(case['categories']) and requirements(
        case, [item['raw'] for item in matched]))
    history = result.get('history', {})
    ranked = [item['id'] for item in history.get('cases', [])]
    return {'root': result.get('root_code'), 'top1': root_ok, 'verified': valid,
            'correct_refusal': root_ok and case['root'] == UNKNOWN, 'reads': len(observed),
            'hit3': bool(set(ranked) & set(case['relevant'])) if case['relevant'] else None,
            'history_ids': ranked, 'history_strategy': history.get('strategy'),
            'route': result.get('route', {}), 'latency_ms': result['latency_ms'],
            'history_latency_ms': history.get('latency_ms'), 'error': result.get('error'),
            'requests': len(requests), 'kinds': dict(Counter(row['kind'] for row in requests)),
            'tokens': sum(row['total_tokens'] or 0 for row in requests),
            'input_tokens': sum(row['input_tokens'] or 0 for row in requests),
            'output_tokens': sum(row['output_tokens'] or 0 for row in requests),
            'unknown_usage': sum(row['total_tokens'] is None for row in requests)}


async def compare(run, cfg, store, transport, cases, snapshots):
    database = TaskStore(cfg.task_db)
    gate = asyncio.Semaphore(2)
    async def evaluate(case, mode):
        async with gate:
            label = f"{case['id']}-{mode}"
            token = CASE.set(label)
            try:
                snapshot = snapshots[case['id']]
                scope = ObservationScope.model_validate(snapshot['scope'])
                task = await database.create('p5-holdout', str(uuid4()), {'symptom': case['symptom']}, kind='ops')
                wire = UsageTransport(database.path, transport)
                history = None if mode == 'p41' else CaseHistory(cfg, store=store, transport=wire, rerank=mode == 'p5_rerank')
                result = await Investigator(cfg, database, ReplayObservations(snapshot), wire, history=history).run(
                    task, scope, case['symptom'], mode='hybrid')
                write(run.path / f'diagnosis-{label}.json', result)
                return {'case': case['id'], 'mode': mode, **grade(case, result, rows(run.path, label), snapshot)}
            finally:
                CASE.reset(token)
    # Rotate arm order by incident to balance wall-clock/provider variation.
    pending = []
    for index, case in enumerate(cases):
        if case['id'] not in snapshots:
            continue
        pending.extend(evaluate(case, MODES[(index + offset) % len(MODES)]) for offset in range(len(MODES)))
    completed = []
    for task in asyncio.as_completed(pending):
        result = await task
        completed.append(result)
        write(run.path / 'results.json', completed)
        run.stage(f"compared-{result['case']}-{result['mode']}")
    summary = {}
    for mode in MODES:
        group = [row for row in completed if row['mode'] == mode]
        relevant = [row for row in group if row['hit3'] is not None]
        timings = [row['latency_ms'] for row in group]
        summary[mode] = {key: sum(row[key] for row in group) for key in (
            'top1', 'verified', 'correct_refusal', 'reads', 'requests', 'tokens', 'input_tokens', 'output_tokens', 'unknown_usage')}
        summary[mode].update(tasks=len(group), refusal_cases=sum(case['root'] == UNKNOWN and case['id'] in snapshots for case in cases),
            hit3=None if mode == 'p41' else [sum(row['hit3'] for row in relevant), len(relevant)],
            p50_ms=pct(timings, .5), p95_ms=pct(timings, .95),
            kinds=dict(sum((Counter(row['kinds']) for row in group), Counter())),
            strategies=dict(Counter(row['history_strategy'] or 'none' for row in group)))
    write(run.path / 'summary.json', summary)


def authorized_config():
    cfg = load_settings()
    endpoint = urlsplit(cfg.base_url)
    if not cfg.configured or endpoint.scheme != 'https' or endpoint.hostname != 'api.openlux.ai':
        raise RuntimeError('The authorized real model provider is not configured')
    return cfg, endpoint.hostname


def guard(run):
    write(run.path / 'versions.json', {'git_head': run.command(['git', 'rev-parse', 'HEAD']).stdout.strip(),
        'normal_volumes_before': run.command(['docker', 'volume', 'ls', '--format', '{{.Name}}']).stdout.splitlines()})
    run.sample('before', guard=True)


async def regression(source):
    source = source.resolve()
    if not source.is_relative_to(ROOT / 'artifacts/test-runs'):
        raise ValueError('Regression source must be an existing isolated run')
    frozen = source / 'frozen-holdout.json'
    config = json.loads((source / 'authorization.json').read_text(encoding='utf-8'))
    assert sha(frozen) == config['frozen_holdout_sha256']
    case = next(row for row in json.loads(frozen.read_text(encoding='utf-8')) if row['id'] == 'user-missing-cache')
    snapshot_path = source / 'snapshot-user-missing-cache.json'
    snapshot = json.loads(snapshot_path.read_text(encoding='utf-8'))
    assert sha(snapshot_path) == json.loads((source / 'snapshot-hashes.json').read_text(encoding='utf-8'))[case['id']]
    cfg, host = authorized_config()
    run = Run()
    write(run.path / 'authorization.json', {'scope': 'Targeted citation completeness regression on one unchanged snapshot',
        'host': host, 'model': cfg.model, 'max_parallel': 2, 'parent_run': source.name,
        'snapshot_sha256': sha(snapshot_path), 'frozen_holdout_sha256': sha(frozen), 'cost': 'UNKNOWN'})
    run.stage('p5-targeted-regression')
    transport = ActualApiTransport(run.path, output_limit=2400)
    try:
        guard(run)
        from run import free_port
        run.pg_port = free_port()
        run.network = run.command(['docker', 'network', 'create', '--label', 'localperks.validation.run=' + run.id,
                                   '--label', 'com.docker.compose.project=' + run.project, run.project]).stdout.strip()
        run.register('network', run.network, 'Dedicated regression network')
        cfg = replace(cfg, task_db=str(run.path / 'tasks-evidence.sqlite3'), turn_timeout=160)
        cfg, store = await index(run, cfg, transport)
        run.monitor_thread = threading.Thread(target=run.monitor, daemon=True)
        run.monitor_thread.start()
        await compare(run, cfg, store, transport, [case], {case['id']: snapshot})
    finally:
        export(run.path)
        run.cleanup()


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--regression-source', type=Path)
    args = parser.parse_args()
    if args.regression_source:
        return await regression(args.regression_source)
    cfg, host = authorized_config()
    run = Run()
    cases, frozen_hash = freeze(run)
    historical = load_cases(ROOT)
    assert all(run.id not in source.path for case in historical for source in case.sources)
    write(run.path / 'authorization.json', {'scope': 'P5 curated summaries and synthetic isolated Java experiments',
        'model': cfg.model, 'embedding': cfg.embedding_model, 'rerank': cfg.rerank_model, 'host': host,
        'frozen_holdout_sha256': frozen_hash, 'max_parallel': 2, 'max_investigation_calls': 8,
        'cost': 'UNKNOWN', 'history_ledgers_preserved': True})
    run.stage('p5-holdout-frozen')
    java_token, ops_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    run.env.update(HMDP_OPS_ADMIN_TOKEN=java_token, HMDP_RATE_LIMIT_TRUST_FORWARDED_HEADERS='true')
    os.environ.update(OPS_ADMIN_TOKEN=ops_token, OPS_JAVA_TOKEN=java_token, OPS_CASE_RAG='rrf')
    transport = ActualApiTransport(run.path, output_limit=2400)
    try:
        guard(run)
        jar = ROOT / 'target/localperks-backend-0.0.1-SNAPSHOT.jar'
        if jar.is_file() and jar.stat().st_mtime >= max(path.stat().st_mtime for path in (ROOT / 'src/main').rglob('*') if path.is_file()):
            run.jar = jar
        else:
            run.build()
        await run.infrastructure()
        run.monitor_thread = threading.Thread(target=run.monitor, daemon=True)
        run.monitor_thread.start()
        cfg = replace(cfg, shoplink_base_url=run.java_url, redis_url=run.redis_url,
                      task_db=str(run.path / 'tasks-evidence.sqlite3'), turn_timeout=160)
        cfg, store = await index(run, cfg, transport)
        run.stage('p5-real-index-ready')
        fault = FaultCapture(run, java_token)
        await run.coupon(709999, 10)
        assert (await fault.submit(709999, 10099))['body']['success']
        await run.wait_orders(709999, 1)
        app = create_app(settings=cfg, model_transport=transport)
        snapshots = {}
        async with app.router.lifespan_context(app), httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                base_url='http://agent.local', timeout=300) as client, httpx.AsyncClient(base_url=run.java_url,
                timeout=10, trust_env=False) as java:
            provider = LiveProvider(java, java_token)
            async def canary(case, scope, snapshot):
                label = 'live-canary'
                token = CASE.set(label)
                try:
                    body = {'scope': scope.model_dump(), 'symptom': case['symptom']}
                    path = '/agent/ops/investigations'
                    unauthorized = await client.post(path, json=body)
                    assert unauthorized.status_code == 403
                    headers = {'X-Ops-Token': ops_token}
                    response = await client.post(path, json=body, headers=headers)
                    response.raise_for_status()
                    result = response.json()
                    detail = await client.get(path + '/' + result['task_id'], headers=headers)
                    detail.raise_for_status()
                    assert detail.json()['output']['history'] == result['history']
                    assert (await client.get(path + '/' + result['task_id'])).status_code == 403
                    write(run.path / 'live-admin-diagnosis.json', result)
                    live = grade(case, result, rows(run.path, label))
                    write(run.path / 'live-admin-check.json', {'unauthorized': 403, 'authorized': 200,
                          'checkpoint_matches': True, **live})
                    assert live['verified'] and live['kinds'].get('chat') and live['history_ids']
                finally:
                    CASE.reset(token)
            for case in cases:
                try:
                    snapshots[case['id']] = await capture(run, fault, provider, case, canary if case['id'] == 'live-canary' else None)
                    run.stage('p5-captured-' + case['id'])
                except Exception as exc:
                    write(run.path / f"capture-failure-{case['id']}.json", {'error': type(exc).__name__, 'detail': str(exc)})
        assert sha(run.path / 'frozen-holdout.json') == frozen_hash
        write(run.path / 'snapshot-hashes.json', {key: sha(run.path / f'snapshot-{key}.json') for key in snapshots})
        await compare(run, cfg, store, transport, cases[1:], snapshots)
    finally:
        export(run.path)
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())

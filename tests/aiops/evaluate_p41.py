"""Evaluate frozen P4 cases and the independent P4.1 holdout without changing labels."""
import argparse
import asyncio
import hashlib
import json
import math
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from evaluate import AGENT, ROOT, FrozenProvider, Investigator, ObservationScope, TaskStore, UsageTransport
from evaluate import cited_proof, digest, load, load_settings, usage_rows, write


UNKNOWN = 'INSUFFICIENT_EVIDENCE'
BASELINES = ('rules', 'logs', 'agent')
MODES = (*BASELINES, 'hybrid')


def check_holdout(task, cited):
    """Independent checks over referenced source facts, without calling the policy."""
    raw = [entry['raw'] for entry in cited if entry.get('raw')]
    inventory = [row for row in raw if row['tool'] == 'inventory']
    events = [event for row in raw for event in row['data'].get('events', [])]
    case = task['id']
    if case == 'ip-and-drift':
        return cited_proof('IP_RATE_LIMIT', cited) and cited_proof('REDIS_STOCK_DRIFT', cited)
    if case == 'queue-and-residual':
        for row in inventory:
            stock = row['data']
            if stock['pending'][0]['total'] > 0 and int(stock['mysql'][0]['stock']) - int(stock['redisStock']) > stock['pending'][0]['total']:
                return cited_proof('MQ_CONSUME_DISABLED', cited)
        return False
    if case == 'changing-stock':
        return len(inventory) == 2 and inventory[0]['data']['redisStock'] != inventory[1]['data']['redisStock']
    if case == 'missing-cache-key':
        return any(row['data']['mysql'] and row['data'].get('redisStock') is None for row in inventory)
    if case == 'historical-stock':
        return any(row['observedAt'] - row['windowEnd'] >= 30_000 for row in inventory)
    if case == 'user-dimension':
        return any(event.get('status') == 429 and event.get('limitScope') == 'user' for event in events)
    if case == 'partial-redelivery':
        return any(row['tool'] == 'delivery' and
            sum(event['type'] == 'mq_received' for event in row['data']['events']) == 1 and
            any(event['type'] == 'order_idempotent' for event in row['data']['events']) for row in raw)
    if case == 'compensation-and-drift':
        return cited_proof('REDIS_STOCK_DRIFT', cited) and any(event['type'] == 'reservation_compensated' for event in events)
    raise ValueError('Unknown holdout case: ' + case)


def grade(task, snapshot, result, usage, mode, suite):
    observations = {item['id']: item for item in result['observations']}
    refs = result.get('evidence_ids', [])
    cited = [observations[key] for key in refs if key in observations]
    authentic = bool(refs) and len(set(refs)) == len(refs) == len(cited)
    for item in cited:
        samples = snapshot['samples'][item['tool']]
        if 'raw' in item:
            authentic &= item['raw'] in samples and digest(item['raw']) == item['sha256']
        else:
            authentic &= any('_error' in sample for sample in samples)
    root = result.get('root_code')
    completed = result['state'] == 'completed'
    match = completed and root == task['expected']
    evidence = match and authentic and (check_holdout(task, cited) if suite == 'holdout' else cited_proof(root, cited))
    return {'task': task['id'], 'suite': suite, 'mode': mode, 'expected': task['expected'], 'root': root,
            'state': result['state'], 'error': result.get('error'), 'top1_correct': match, 'evidence_accurate': evidence,
            'correct_refusal': match and root == UNKNOWN, 'latency_ms': result['latency_ms'], 'reads': len(observations),
            'model_requests': len(usage), 'input_tokens': sum(row['input_tokens'] or 0 for row in usage),
            'output_tokens': sum(row['output_tokens'] or 0 for row in usage), 'total_tokens': sum(row['total_tokens'] or 0 for row in usage),
            'unknown_usage': sum(row['total_tokens'] is None for row in usage), 'route': result.get('route')}


def aggregates(rows):
    groups = {}
    for suite in ('original', 'holdout'):
        groups[suite] = {}
        for mode in MODES:
            group = [row for row in rows if row['suite'] == suite and row['mode'] == mode]
            if not group:
                continue
            times = sorted(row['latency_ms'] for row in group)
            counters = ('top1_correct', 'evidence_accurate', 'correct_refusal', 'reads', 'model_requests',
                        'input_tokens', 'output_tokens', 'total_tokens', 'unknown_usage')
            groups[suite][mode] = {
                **{key: sum(row[key] for row in group) for key in counters}, 'tasks': len(group),
                'refusal_cases': sum(row['expected'] == UNKNOWN for row in group),
                'p50_ms': times[math.ceil(.50 * len(times)) - 1], 'p95_ms': times[math.ceil(.95 * len(times)) - 1],
                'paths': dict(Counter((row.get('route') or {}).get('path', 'baseline') for row in group)),
            }
    return groups


async def main(holdout, historical, output, reuse_baselines=None):
    if not output.is_relative_to(ROOT / 'artifacts'):
        raise ValueError('Output must stay inside project artifacts')
    manifest = load(holdout / 'capture-complete.json')
    assert manifest['cases'] >= 8 and manifest['model_calls_during_capture'] == 0
    gold_hash = hashlib.sha256((holdout / 'gold.json').read_bytes()).hexdigest()
    assert gold_hash == load(holdout / 'gold-sha256.json')['sha256']
    gold = load(holdout / 'gold.json')['tasks']
    original = load(historical / 'dataset.json')
    settings = load_settings()
    endpoint = urlsplit(settings.base_url)
    assert settings.configured and endpoint.scheme == 'https' and endpoint.hostname == 'api.openlux.ai'
    assert settings.model == original['model'], 'Keep the comparison model consistent with P4'

    sources = {}
    for task in original['tasks']:
        directory = Path(original['stock'] if task['snapshot'] == 'stock' else original['capture'])
        snap = load(directory / f"snapshot-{task['snapshot']}.json")
        assert digest(snap) == task['snapshot_sha256']
        sources[('original', task['id'])] = snap
    for task in gold:
        snap = load(holdout / f"snapshot-{task['id']}.json")
        assert snap['scope']['voucher_id'] == task['voucher_id']
        sources[('holdout', task['id'])] = snap
        complete_evidence = [{'raw': sample} for samples in snap['samples'].values() for sample in samples]
        assert check_holdout(task, complete_evidence), f"Holdout source failed its independent check: {task['id']}"
    assert len({sources[('holdout', task['id'])]['scope']['voucher_id'] for task in gold}) == len(gold)

    output.mkdir(parents=True, exist_ok=False)
    snapshot_hashes = {f'{suite}/{case}': digest(snap) for (suite, case), snap in sources.items()}
    write(output / 'dataset.json', {
        'original_source': str(historical), 'holdout_source': str(holdout), 'gold_sha256': gold_hash,
        'original_tasks': original['tasks'], 'holdout_tasks': gold, 'model': settings.model,
        'snapshots': snapshot_hashes,
        'source_hashes': {file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in (AGENT / 'ops').glob('*.py')},
        'protocol': 'Reuse 36 archived P4 baseline outputs. Run hybrid on original tasks and all four methods on holdout.' if not reuse_baselines else
                    'One bounded hybrid-only regression after correcting the tool ranking. Reuse completed baseline traces from the first trial.',
        'reuse_baselines': str(reuse_baselines) if reuse_baselines else None,
    })
    rows = []
    for task in original['tasks']:
        for mode in BASELINES:
            path = historical / f"{task['id']}-{mode}.json"
            archived = load(path)
            assert archived['task'] == task
            row = grade(task, sources[('original', task['id'])], archived['result'], archived['usage'], mode, 'original')
            rows.append({**row, 'archived_source': str(path)})
    if reuse_baselines:
        earlier = load(reuse_baselines / 'dataset.json')
        assert earlier['gold_sha256'] == gold_hash and earlier['snapshots'] == snapshot_hashes and earlier['model'] == settings.model
        for task in gold:
            for mode in BASELINES:
                path = reuse_baselines / f"holdout-{task['id']}-{mode}.json"
                archived = load(path)
                assert archived['task'] == task
                row = grade(task, sources[('holdout', task['id'])], archived['result'], archived['usage'], mode, 'holdout')
                rows.append({**row, 'archived_source': str(path)})

    db = output / 'investigations.sqlite3'
    store = TaskStore(db)
    transport = UsageTransport(db)
    semaphore = asyncio.Semaphore(2)

    def save_summary():
        write(output / 'summary.json', {'aggregate': aggregates(rows), 'rows': rows})

    async def run_case(task, mode, suite):
        async with semaphore:
            snap = sources[(suite, task['id'])]
            scope = ObservationScope.model_validate(snap['scope'])
            owner = 'p41-evaluation'
            task_id = await store.create(owner, str(uuid4()), {'scope': scope.model_dump(), 'symptom': task['symptom']}, kind='ops')
            await store.claim(owner, task_id, lease_seconds=325)
            result = await Investigator(settings, store, FrozenProvider(snap), transport).run(task_id, scope, task['symptom'], mode=mode)
            usage = usage_rows(db, task_id)
            row = grade(task, snap, result, usage, mode, suite)
            write(output / f"{suite}-{task['id']}-{mode}.json", {'task': task, 'result': result, 'usage': usage, 'grade': row})
            rows.append(row)
            save_summary()
            print(json.dumps(row, ensure_ascii=False), flush=True)

    save_summary()
    try:
        await asyncio.gather(*(run_case(task, 'hybrid', 'original') for task in original['tasks']))
        for mode in (('hybrid',) if reuse_baselines else MODES):
            await asyncio.gather(*(run_case(task, mode, 'holdout') for task in gold))
    finally:
        await transport.inner.aclose()
    assert len(rows) == (len(original['tasks']) + len(gold)) * len(MODES)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for name in ('holdout', 'historical', 'output'):
        parser.add_argument(f'--{name}', type=lambda value: Path(value).resolve(), required=True)
    parser.add_argument('--reuse-baselines', type=lambda value: Path(value).resolve())
    args = parser.parse_args()
    asyncio.run(main(args.holdout, args.historical, args.output, args.reuse_baselines))

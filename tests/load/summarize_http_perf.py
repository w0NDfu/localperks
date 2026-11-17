"""Join P6 JTL, servlet timings, queue samples, pools and thread stacks."""
import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path

from summarize_perf import summarize, percentiles, jsonl


def precise_join(path, samples):
    """Keep inclusive spans separate; percentiles of different spans must not be added."""
    context_file = path / 'benchmark-context.json'
    batched_read = context_file.exists() and json.loads(context_file.read_text(encoding='utf-8')).get('eligibility_batch_enabled', False)
    with (path / 'stages-timing.csv').open(encoding='utf-8') as stream:
        spans = list(csv.DictReader(stream))
    if not spans or 'started_ns' not in spans[0]:
        return None
    with (path / 'http-requests.csv').open(encoding='utf-8') as stream:
        requests = list(csv.DictReader(stream))
    by_uid = {row['uid']: row for row in samples}
    first = min(int(row['timeStamp']) for row in samples)
    last = max(int(row['timeStamp']) + int(row['elapsed']) for row in samples)
    requests = {row['uid']: row for row in requests
                if row['uid'] in by_uid and first <= int(row['entered_ms']) <= last}
    grouped = defaultdict(list)
    acks = defaultdict(list)
    background = defaultdict(list)
    for row in spans:
        request = requests.get(row['uid'])
        if request and row['voucher_id'] == request['voucher_id'] and row['stage'] == 'seckill_producer_ack':
            acks[row['uid']].append(int(row['ended_ns']))
        if request and row['voucher_id'] == request['voucher_id'] and row['thread'].startswith('http-nio'):
            if int(request['entered_ns']) <= int(row['started_ns']) <= int(request['ended_ns']) or row['stage'] == 'tomcat_request_queue':
                grouped[row['uid']].append(row)
        elif first <= int(row['at_epoch_ms']) <= last:
            background[row['stage']].append(int(row['duration_us']) / 1000)
    totals = defaultdict(list)
    per_span = defaultdict(list)
    joined = []
    for uid, request in requests.items():
        own = grouped[uid]
        values = defaultdict(float)
        for row in own:
            milliseconds = int(row['duration_us']) / 1000
            values[row['stage']] += milliseconds
            per_span[row['stage']].append(milliseconds)
        # SQL spans include pool acquisition. Subtract only acquisitions contained in that span.
        sql_parents = ('http_existing_order_sql', 'http_activity_sql', 'http_outbox_insert')
        if not batched_read:
            sql_parents += ('http_precheck_sql',)
        for parent in sql_parents:
            for row in own:
                if row['stage'] != parent:
                    continue
                waits = sum(int(child['duration_us']) / 1000 for child in own
                            if child['stage'].startswith('db_connection_acquire_')
                            and int(row['started_ns']) <= int(child['started_ns'])
                            and int(child['ended_ns']) <= int(row['ended_ns']))
                values[parent + '_pool_wait'] += waits
                values[parent + '_excluding_pool'] += int(row['duration_us']) / 1000 - waits
        for name, value in values.items():
            totals[name].append(value)
        sample = by_uid[uid]
        service = int(request['service_us']) / 1000
        queue = ((int(request['queue_started_ns']) - int(request['queue_submitted_ns'])) / 1e6
                 if int(request['queue_submitted_ns']) else None)
        item = {'uid': uid, 'voucher': request['voucher_id'], 'jtl_start_ms': int(sample['timeStamp']),
                'jtl_ms': int(sample['elapsed']), 'server_filter_ms': service, 'tomcat_queue_ms': queue,
                'ingress_to_filter_ms': int(request['entered_ms']) - int(sample['timeStamp']),
                'unattributed_client_transport_ms': int(sample['elapsed']) - service - (queue or 0), **dict(values)}
        joined.append(item)
    keys = list(dict.fromkeys(key for row in joined for key in row))
    with (path / 'request-phase-join.csv').open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=keys)
        writer.writeheader()
        writer.writerows(joined)
    ordered = sorted(joined, key=lambda row: row['jtl_ms'])
    modern = any(row['stage'] == 'http_precheck_sql' for own in grouped.values() for row in own)
    commit_order = []
    for own in grouped.values():
        commits = [int(row['ended_ns']) for row in own if row['stage'] == 'http_outbox_insert']
        returned = [int(row['ended_ns']) for row in own if row['stage'] == 'http_business']
        if commits and returned:
            commit_order.append(max(commits) <= max(returned))
    return {'matched_requests': len(joined), 'jtl_requests': len(samples),
            'exact_executor_queue_coverage': sum(row['tomcat_queue_ms'] is not None for row in joined),
            'path_invariants': {'modern': modern,
                'eligibility_read_mode': 'batched fresh SQL on workers' if batched_read else 'per-request SQL',
                'http_mq_api_calls': len(per_span['mq_send_entry']),
                'http_business_pool_borrows': len(per_span['db_connection_acquire_primary']),
                'http_admission_pool_borrows': len(per_span['db_connection_acquire_admission']),
                'background_admission_pool_borrows_during_http': len(background['db_connection_acquire_admission']),
                'commit_before_controller_return': {'checked': len(commit_order), 'passed': sum(commit_order)},
                'acknowledged_orders_matched': len(acks),
                'first_ack_after_server_filter_return': sum(min(ends) > int(requests[uid]['ended_ns']) for uid, ends in acks.items())},
            'http_inclusive_per_call_ms': {key: percentiles(value) for key, value in per_span.items()},
            'http_per_request_ms': {key: percentiles(value) for key, value in totals.items()},
            'background_during_http_ms': {key: percentiles(value) for key, value in background.items()},
            'client_transport_residual_ms': percentiles([row['unattributed_client_transport_ms'] for row in joined]),
            'representative_requests': {str(p): ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)]
                                        for p in (50, 95, 99)} if ordered else {},
            'scope': 'Exact executor queue is submitted-to-worker; residual includes injector/network/parser/response and clock granularity. SQL excluding pool still includes JDBC/network. All named spans are inclusive.'}


def summarize_http(path):
    result = json.loads((path / 'perf-result.json').read_text(encoding='utf-8'))
    jtl = next(path.glob(result['group'] + '-*.jtl'))
    with jtl.open(encoding='utf-8-sig') as stream:
        samples = [row for row in csv.DictReader(stream) if row['label'].startswith('seckill.')]
    first = min(int(row['timeStamp']) for row in samples)
    last = max(int(row['timeStamp']) + int(row['elapsed']) for row in samples)
    metrics = defaultdict(list)
    with (path / 'stages-timing.csv').open(encoding='utf-8') as stream:
        for row in csv.DictReader(stream):
            stamp = int(row['at_epoch_ms'])
            if stamp < first:
                continue
            if row['stage'].startswith(('http_', 'identity_', 'rate_', 'reserve_')) and stamp > last:
                continue
            metrics[row['stage']].append(int(row['duration_us']) / 1000)
    jvm = jsonl(path / 'jvm-metrics.jsonl')
    queue = jsonl(path / 'admission-queue.jsonl')
    queue_in_load = [row for row in queue if first <= row['at'] <= last]
    by_id = {row['uid']: row for row in samples}
    ingress = []
    http = path / 'http-requests.csv'
    if http.exists():
        with http.open(encoding='utf-8') as stream:
            for row in csv.DictReader(stream):
                request = by_id.get(row['uid'])
                if request and first <= int(row['entered_ms']) <= last:
                    ingress.append(int(row['entered_ms']) - int(request['timeStamp']))
    blocks = {}
    for dump in sorted(path.glob('app-threads-*.txt')):
        counts = Counter()
        for block in re.split(r'\r?\n\r?\n', dump.read_text(encoding='utf-8', errors='replace')):
            if not re.match(r'^"http-nio.*-exec-', block):
                continue
            if 'HikariPool.getConnection' in block:
                counts['hikari_connection_wait'] += 1
            elif 'Lettuce' in block or 'RedisFuture' in block:
                counts['redis_command_wait'] += 1
            elif 'rocketmq' in block.lower() and ('Semaphore' in block or 'sendDefaultImpl' in block):
                counts['mq_send_entry'] += 1
            elif 'TaskQueue.take' in block or 'TaskQueue.poll' in block:
                counts['idle_worker'] += 1
            else:
                counts['other'] += 1
        blocks[dump.name] = dict(counts)
    summary = {
        'result': result, 'stage_ms': {name: percentiles(values) for name, values in metrics.items()},
        'jmeter_reported_active_threads_peak': max(int(row['allThreads']) for row in samples),
        'ingress_transport_and_executor_ms': percentiles(ingress),
        'ingress_scope': 'JTL start to servlet filter entry on the same Windows clock, includes network/parser/executor scheduling',
        'admission_pool': {key: max((row.get(key, 0) for row in jvm), default=0)
                           for key in ('admissionActive', 'admissionPending', 'admissionTotal')},
        'pending_peak': max((row['reserved_pending'] for row in queue), default=None),
        'pending_final': queue[-1]['reserved_pending'] if queue else None,
        'pending_early_load': queue_in_load[:3], 'pending_late_load': queue_in_load[-3:],
        'thread_stacks': blocks, 'injector_thread_ceiling_triggered': 'P6_INJECTOR_THREAD_LIMIT' in
            next(path.glob('*-jmeter.log')).read_text(encoding='utf-8', errors='replace'),
        'artifact_sha256': {item.name: hashlib.sha256(item.read_bytes()).hexdigest()
                           for item in (jtl, path / 'stages-timing.csv')},
    }
    summary['precise'] = precise_join(path, samples)
    general = summarize(path)
    (path / 'perf-evidence-summary.json').write_text(json.dumps(general, ensure_ascii=False, indent=2), encoding='utf-8')
    (path / 'http-evidence-summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    args = parser.parse_args()
    summarize_http(args.run.resolve())

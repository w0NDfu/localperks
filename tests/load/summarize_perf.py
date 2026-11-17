"""Summarize the timestamped evidence without re-running a workload."""
import argparse
import csv
import json
import math
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def percentiles(values):
    values = sorted(values)
    if not values:
        return {}
    return {'count': len(values), 'p50': values[math.ceil(len(values) * .50) - 1],
            'p95': values[math.ceil(len(values) * .95) - 1],
            'p99': values[math.ceil(len(values) * .99) - 1], 'max': values[-1]}


def jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]


def max_values(rows, fields):
    return {field: max((item[field] for item in rows if field in item), default=None) for field in fields}


def gc(run, role):
    path = run / f'{role}.jfr'
    if not path.exists():
        return {'recording_missing': True}
    result = subprocess.run(['jfr', 'print', '--json', '--events', 'jdk.GCPhasePause', path],
                            capture_output=True, text=True, encoding='utf-8', check=False, timeout=30)
    if result.returncode:
        return {'recording_error': result.returncode}
    durations = []
    for event in json.loads(result.stdout)['recording']['events']:
        value = event['values']['duration']
        match = re.fullmatch(r'PT([\d.]+)S', value)
        if match:
            durations.append(float(match.group(1)) * 1000)
    return {**percentiles(durations), 'total_pause_ms': round(sum(durations), 2)}


def summarize(run):
    result = json.loads((run / 'perf-result.json').read_text(encoding='utf-8'))
    samples = jsonl(run / 'process-network.jsonl')
    os_roles = {}
    for role in ('app', 'jmeter'):
        points = [(row['at'], item) for row in samples for item in row['processes'] if item['role'] == role]
        cpu = [1000 * (b['cpu_s'] - a['cpu_s']) / (t2 - t1)
               for (t1, a), (t2, b) in zip(points, points[1:]) if t2 > t1 and a['pid'] == b['pid']]
        states = {name: max(((item['tcp'] or {}).get(name, 0) for _, item in points), default=0)
                  for name in ('ESTABLISHED', 'SYN_SENT', 'CLOSE_WAIT', 'LISTENING')}
        os_roles[role] = {'samples': len(points), 'peak_cores': round(max(cpu), 2) if cpu else None,
                          **max_values([item for _, item in points], ('rss_bytes', 'threads')),
                          'tcp_peak': states, 'gc_ms': gc(run, 'java' if role == 'app' else role)}
    jvm = jsonl(run / 'jvm-metrics.jsonl')
    jmx = {'hikari': max_values(jvm, ('hikariActive', 'hikariPending', 'hikariTotal')),
           'tomcat': max_values([item for row in jvm for item in row['tomcat']],
                                ('currentThreadsBusy', 'currentThreadCount', 'connectionCount',
                                 'keepAliveCount', 'maxThreads', 'acceptCount', 'maxConnections')),
           'http': max_values([item for row in jvm for item in row['requests']],
                              ('requestCount', 'errorCount', 'processingTime', 'maxTime'))}
    mysql = jsonl(run / 'mysql-live.jsonl')
    metrics = defaultdict(list)
    locks, oldest = [], []
    for sample in mysql:
        locks.append(sum(len(row) > 2 and row[2] == 'LOCK WAIT' for row in sample['rows']))
        for row in sample['rows']:
            if row[0] == 'trx':
                oldest.append(int(row[3]))
            elif len(row) == 2 and row[1].isdigit():
                metrics[row[0]].append(int(row[1]))
    db = {'peak_waiting_trx': max(locks, default=None), 'oldest_trx_seconds': max(oldest, default=None),
          'status': {name: {'peak': max(v), 'delta': v[-1] - v[0]} for name, v in metrics.items()}}
    redis = {}
    for label in ('before', 'after'):
        redis[label] = json.loads((run / f'redis-{label}.json').read_text(encoding='utf-8'))
    commands = {}
    for name in ('cmdstat_evalsha', 'cmdstat_eval', 'cmdstat_get'):
        a = redis['before']['commandstats'].get(name, {})
        b = redis['after']['commandstats'].get(name, {})
        calls = b.get('calls', 0) - a.get('calls', 0)
        if calls:
            commands[name] = {'calls': calls, 'server_mean_us': round((b['usec'] - a.get('usec', 0)) / calls, 2)}
    timings = defaultdict(list)
    with (run / 'stages-timing.csv').open(encoding='utf-8', newline='') as stream:
        for row in csv.DictReader(stream):
            timings[row['stage']].append(int(row['duration_us']) / 1000)
    stacks = {}
    for path in sorted(run.glob('app-threads-*.txt')):
        blocks = path.read_text(encoding='utf-8').split('\n\n')
        stacks[path.name] = {name: sum(frame in stack for stack in blocks) for name, frame in
            [('close_sync', 'sendOrderTimeoutMessage'), ('hikari', 'HikariPool.getConnection'),
             ('seckill_ack_sql', 'SeckillOrderMessageService.markSent'), ('consumer', 'SeckillOrderConsumer.onMessage'),
             ('jdbc_update', 'ClientPreparedStatement.execute')]}
    jtl = next(run.glob('*.jtl'))
    with jtl.open(encoding='utf-8-sig', newline='') as stream:
        rows = [row for row in csv.DictReader(stream) if row['label'].startswith('seckill.')]
    log = next(run.glob('*-jmeter.log')).read_text(encoding='utf-8', errors='replace')
    starts = [datetime.fromisoformat(stamp.replace(',', '.')) for stamp in
              re.findall(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d+) INFO[^\n]*Thread started:', log, re.M)]
    java_log = (run / 'java.log').read_text(encoding='utf-8', errors='replace')
    return {'result': result, 'os': os_roles, 'jmx': jmx, 'mysql': db, 'redis_commands': commands,
            'redis_connected_clients': redis['after']['clients'].get('connected_clients'),
            'redis_slowlog_sample': len(redis['after']['slowlog']),
            'stage_ms': {name: percentiles(values) for name, values in timings.items()}, 'stacks': stacks,
            'jmeter': {'statuses': dict(Counter(row['responseCode'] for row in rows)),
                       'connect_ms': percentiles([int(row['Connect']) for row in rows]),
                       'thread_starts_logged': len(starts),
                       'thread_start_spread_ms': (max(starts) - min(starts)).total_seconds() * 1000 if starts else None},
            'java_log_matches': {pattern: java_log.count(pattern) for pattern in
                ('sendDefaultImpl call timeout', 'Connection is not available', 'reservation_compensated',
                 '消息消费失败', '订单超时任务等待重试')}}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    run = parser.parse_args().run.resolve()
    data = summarize(run)
    (run / 'perf-evidence-summary.json').write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

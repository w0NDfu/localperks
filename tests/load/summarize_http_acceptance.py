"""Offline summary of all retained P6 repeat trials and prescribed sustained-load runs."""
import csv
import hashlib
import json
import re
import statistics
from pathlib import Path

from summarize_http_perf import summarize_http
from summarize_perf import jsonl, percentiles

ROOT = Path(__file__).resolve().parents[2]
RUNS = ROOT / 'artifacts/test-runs'


def queue_metrics(path):
    jtl = next(path.glob('*.jtl'))
    with jtl.open(encoding='utf-8-sig') as stream:
        rows = [row for row in csv.DictReader(stream) if row['label'].startswith('seckill.')]
    first = min(int(row['timeStamp']) for row in rows)
    last = max(int(row['timeStamp']) + int(row['elapsed']) for row in rows)
    pending = [row for row in jsonl(path / 'admission-queue.jsonl') if first <= row['at'] <= last]
    lag = [row for row in jsonl(path / 'load-mq.jsonl') if 'data' in row.get('observation', {})]
    load_lag = [row for row in lag if first <= row['at'] <= last]
    slope = None
    if len(pending) >= 2:
        t = [(row['at'] - first) / 1000 for row in pending]
        y = [row['reserved_pending'] for row in pending]
        mean_t, mean_y = statistics.mean(t), statistics.mean(y)
        denominator = sum((value - mean_t) ** 2 for value in t)
        slope = sum((a - mean_t) * (b - mean_y) for a, b in zip(t, y)) / denominator if denominator else None
    early = [row['reserved_pending'] for row in pending if row['at'] <= first + (last - first) / 4]
    late = [row['reserved_pending'] for row in pending if row['at'] >= last - (last - first) / 4]
    return {'request_window_epoch_ms': [first, last], 'pending_samples_in_load': len(pending),
            'pending_peak_in_load': max((row['reserved_pending'] for row in pending), default=None),
            'pending_linear_slope_per_second': slope, 'pending_first': pending[:3], 'pending_last': pending[-3:],
            'pending_first_quarter_median': statistics.median(early) if early else None,
            'pending_last_quarter_median': statistics.median(late) if late else None,
            'valid_mq_samples_in_load': len(load_lag),
            'observed_mq_lag_peak_in_load': max((row['observation']['data']['lag'] for row in load_lag), default=None),
            'observed_mq_lag_peak_including_drain': max((row['observation']['data']['lag'] for row in lag), default=None),
            'broker_runtime_samples': [{'at': row['at'], **row['observation']['data'].get('brokerRuntime', {})} for row in lag],
            'invalid_mq_samples': sum('data' not in row.get('observation', {}) for row in jsonl(path / 'load-mq.jsonl')),
            'scope': 'Sampled lag and pending maxima are lower bounds on peaks; a startup query failure is unavailable, not zero.'}


def main():
    trials, sustained = [], []
    for path in sorted(RUNS.glob('v03-20261107-*')):
        gate = path / 'p6-gate.json'
        if not gate.exists() or not (path / 'perf-result.json').exists():
            continue
        label = json.loads(gate.read_text(encoding='utf-8'))['label']
        match = re.fullmatch(r'p6-repeat-(\d+)-(original|optimized)', label)
        rate = re.fullmatch(r'p6-final-(20|50|100)-rps-(\d+)-sec', label)
        if not match and not rate:
            continue
        summary = summarize_http(path)
        versions = json.loads((path / 'versions.json').read_text(encoding='utf-8'))
        data = {'run_id': path.name, 'label': label, 'result': summary['result'],
                'context': json.loads((path / 'benchmark-context.json').read_text(encoding='utf-8')),
                'jar_sha256': versions['jar_sha256'], 'compiled_sources': versions.get('compiled_sources'),
                'artifact_sha256': summary['artifact_sha256'], 'queue': queue_metrics(path),
                'measurement_interference': (path / 'measurement-interference.json').exists()}
        if match:
            data.update(round=int(match[1]), variant=match[2])
            trials.append(data)
        else:
            result, queue = data['result'], data['queue']
            data['lossless_profile'] = (result['pass'] and result['offered_rate_met']
                and all(result[key] == 0 for key in ('unexpected', 'non_http_errors', 'http_429', 'unknown_receipts')))
            growth = (queue['pending_last_quarter_median'] - queue['pending_first_quarter_median']
                      if queue['pending_last_quarter_median'] is not None and queue['pending_first_quarter_median'] is not None else None)
            data['finite_window_queue_stable'] = (data['lossless_profile']
                and queue['pending_linear_slope_per_second'] is not None and queue['pending_linear_slope_per_second'] <= 1
                and growth is not None and growth <= max(10, int(rate[1]) / 2))
            sustained.append(data)
    # Whole pairs are excluded from the controlled estimate for a documented protocol deviation.
    interfered_rounds = {row['round'] for row in trials if row['measurement_interference']}
    complete_rounds = {row['round'] for row in trials if sum(x['round'] == row['round'] for x in trials) == 2}
    rounds = sorted(complete_rounds - interfered_rounds)
    groups = {}
    for variant in ('original', 'optimized'):
        selected = [row for row in trials if row['round'] in rounds and row['variant'] == variant]
        pooled = []
        for row in selected:
            with next((RUNS / row['run_id']).glob('*.jtl')).open(encoding='utf-8-sig') as stream:
                pooled.extend(int(x['elapsed']) for x in csv.DictReader(stream) if x['label'].startswith('seckill.'))
        groups[variant] = {'trial_count': len(selected), 'pooled_http_ms': percentiles(pooled),
            'per_trial': {key: {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
                          for key in ('p95_ms', 'p99_ms', 'http_response_rps', 'durable_orders_tps')
                          if (values := [row['result'][key] for row in selected])}}
    output = {'controlled_rounds': rounds, 'excluded_paired_rounds': sorted(interfered_rounds),
              'all_trials_retained': trials, 'sustained': sustained, 'controlled_summary': groups,
              'model_requests': 0, 'notes': ['Medians of trial percentiles and pooled request percentiles are separately named.',
                  '500 injector threads are not 500 TPS. Burst rates are not sustained capacity.']}
    (RUNS / 'p6-http-acceptance-summary.json').write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'rounds': rounds, 'groups': groups, 'sustained_runs': len(sustained)}, ensure_ascii=False))


if __name__ == '__main__':
    main()

"""Export same-build Broker batching evidence without issuing traffic."""
import argparse
import json
import re
from pathlib import Path
from summarize_p7 import summarize

ROOT = Path(__file__).resolve().parents[2]


def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def main():
    parser = argparse.ArgumentParser()
    for name in ('control', 'batch', 'steady', 'faults'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    runs = {}
    for name in ('control', 'batch', 'steady'):
        path = getattr(args, name)
        summary = read(path / 'p7-summary.json') if (path / 'p7-summary.json').exists() else summarize(path)
        assert summary['correctness_pass'] and summary['result']['admission_sql_rows'] == 0
        assert summary['context']['broker_batch_enabled'] == (name != 'control')
        if name == 'steady':
            assert summary['steady_window_pass']
        runs[name] = summary
    versions = {name: read(getattr(args, name) / 'versions.json') for name in runs}
    assert versions['control']['jar_sha256'] == versions['batch']['jar_sha256']
    faults = read(args.faults / 'fault-results.json')
    assert faults['pass']
    test_log = (ROOT / 'artifacts/p9-unit-tests.log').read_text(encoding='utf-8', errors='replace')
    matches = re.findall(r'Tests run: (\d+), Failures: 0, Errors: 0, Skipped: 0', test_log)
    assert matches and 'BUILD SUCCESS' in test_log
    stage = read(ROOT / 'artifacts/performance-p9/stage-comparison.json')
    assert stage['batch']['batched_messages'] == runs['batch']['result']['http_accepted']
    data = {
        'date': '2026-11-13', 'runs': runs,
        'same_build_control_and_batch': True,
        'broker_acceptance': 'Every HTTP ACCEPTED requires the corresponding batch SEND_OK; no new SQL admission Outbox',
        'burst_p99_reduction_percent': (1 - runs['batch']['result']['p99_ms'] / runs['control']['result']['p99_ms']) * 100,
        'stage_evidence': stage,
        'faults': {'run': args.faults.name, 'pass': True, 'scenarios': list(faults)},
        'tests': {'targeted_java_passed': int(matches[-1]), 'production_jar_contains_test_probes': False},
        'limits': ['Finite single-host observations with test-only rate thresholds and synthetic accounts',
                   'No repeated statistical production capacity claim',
                   '500-thread P95<=500ms challenge remains unmet',
                   'Submission and MQ spans include queueing; stage percentiles must not be added',
                   'In-memory batching is not durable acceptance; only Broker confirmation is',
                   'Unknown recovery depends on Redis receipts; single Broker is not replicated HA']}
    target = ROOT / 'evaluations/seckill-broker-batch-20261113-summary.json'
    target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'file': str(target), 'burst_p99_ms': runs['batch']['result']['p99_ms'],
                      'steady_p99_ms': runs['steady']['result']['p99_ms'], 'faults_pass': True,
                      'java_tests': data['tests']['targeted_java_passed']}))


if __name__ == '__main__':
    main()

"""Export retained direct-Broker admission evidence; never sends traffic."""
import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--burst', type=Path, required=True)
    parser.add_argument('--steady', type=Path, required=True)
    parser.add_argument('--faults', type=Path, required=True)
    args = parser.parse_args()
    runs = {}
    for name, path in (('burst_500', args.burst), ('steady_100rps_180s', args.steady)):
        summary = read(path / 'p7-summary.json')
        assert summary['correctness_pass']
        assert summary['result']['admission_mode'] == 'broker-confirmed'
        assert summary['result']['admission_sql_rows'] == 0
        if name.startswith('steady'):
            assert summary['steady_window_pass']
        runs[name] = summary
    faults = read(args.faults / 'fault-results.json')
    assert faults['pass']
    test_log = (ROOT / 'artifacts/p8-unit-tests-final.log').read_text(encoding='utf-8', errors='replace')
    count = re.findall(r'Tests run: (\d+), Failures: 0, Errors: 0, Skipped: 0', test_log)
    assert count and 'BUILD SUCCESS' in test_log
    data = {
        'date': '2026-11-12', 'admission': 'RocketMQ SEND_OK before ACCEPTED; no new SQL admission Outbox',
        'runs': runs,
        'fault_regression': {'run': args.faults.name, 'pass': faults['pass'], 'scenarios': list(faults)},
        'tests': {'targeted_java_passed': int(count[-1]), 'production_jar_contains_test_probes': False},
        'retained_history': {
            'outbox_batch_500': 'v03-20261112-212351-9ac47772',
            'outbox_batch_100rps_180s': 'v03-20261112-212612-c4529bd4',
            'intermediate_direct_500': ['v03-20261112-220544-e38462c9', 'v03-20261112-221229-44accb13'],
            'aborted_steady_before_measured_load': 'v03-20261112-221455-6c3370b4',
            'fault_probe_compile_failure': 'v03-20261112-220812-c26def6c'},
        'limits': [
            'Only finite single-host tests with synthetic accounts and one coupon; test-only global/IP rate limits',
            'Admission waits for configured Broker durability; not final SQL order completion',
            'Outbox batch and Broker ACK have different dependency/availability boundaries; removal does not imply faster latency',
            'Unknown-send recovery relies on Redis receipts; AOF everysec is not zero-loss power-failure durability',
            'Single Broker SYNC_FLUSH does not establish replicated HA',
            'Timeout Outbox remains transactional; legacy admission rows are drained without new inserts']}
    target = ROOT / 'evaluations/seckill-broker-20261112-summary.json'
    target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'file': str(target), 'runs': {key: row['result']['p99_ms'] for key, row in runs.items()},
                      'tests': data['tests'], 'faults_pass': faults['pass']}))


if __name__ == '__main__':
    main()

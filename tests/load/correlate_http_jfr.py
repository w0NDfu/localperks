"""Offline correlation: retained monotonic JFR events against server wall-clock stage windows."""
import argparse
import csv
import json
import re
import subprocess
from datetime import datetime
from pathlib import Path


def seconds(value):
    match = re.fullmatch(r'PT([\d.]+)S', value)
    return float(match.group(1)) if match else 0


def correlate(path):
    with (path / 'stages-timing.csv').open(encoding='utf-8') as stream:
        rows = list(csv.DictReader(stream))
    windows = []
    for stage in ('identity_redis_read', 'http_dispatch_call', 'mq_send_entry', 'redis_sha_check'):
        selected = sorted([row for row in rows if row['stage'] == stage], key=lambda row: int(row['duration_us']))
        if selected:
            for index in sorted(set((int(len(selected) * .95) - 1, len(selected) - 1))):
                row = selected[index]
                end = int(row['at_epoch_ms'])
                windows.append({**row, 'start_ms': end - int(row['duration_us']) / 1000,
                                'end_ms': end, 'rank': index + 1, 'stage_count': len(selected), 'events': []})
    raw = subprocess.run(['jfr', 'print', '--json', '--stack-depth', '24', '--events',
                          'jdk.JavaMonitorEnter,jdk.ThreadPark,jdk.SocketRead,jdk.ExecutionSample',
                          str(path / 'java.jfr')], capture_output=True, text=True, encoding='utf-8', timeout=60, check=True)
    for event in json.loads(raw.stdout)['recording']['events']:
        value = event['values']
        thread = value.get('eventThread') or value.get('sampledThread') or {}
        start = datetime.fromisoformat(value['startTime'].replace('Z', '+00:00')).timestamp() * 1000
        end = start + seconds(value.get('duration', 'PT0S')) * 1000
        for window in windows:
            if thread.get('javaName') != window['thread'] or end < window['start_ms'] or start > window['end_ms']:
                continue
            frames = (value.get('stackTrace') or {}).get('frames') or []
            stack = [frame['method']['type']['name'] + '.' + frame['method']['name'] for frame in frames]
            window['events'].append({'type': event['type'], 'start_ms': start, 'duration_ms': end - start,
                'overlap_ms': max(0, min(end, window['end_ms']) - max(start, window['start_ms'])),
                'monitor': (value.get('monitorClass') or value.get('parkedClass') or {}).get('name'), 'stack': stack})
    for window in windows:
        window['events'].sort(key=lambda event: event['overlap_ms'], reverse=True)
    result = {'windows': windows, 'scope': 'JFR thresholded events and periodic samples are incomplete. End-of-stage wall-clock rounding tolerance is not exact packet tracing.'}
    (path / 'stage-jfr-correlation.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps([{**{key: row[key] for key in ('stage', 'duration_us', 'thread')},
                      'top': [{key: event[key] for key in ('type', 'overlap_ms', 'monitor', 'stack')}
                              for event in row['events'][:2]]} for row in windows]))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    correlate(parser.parse_args().run.resolve())

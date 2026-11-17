"""Compare three diagnosers on frozen observations captured by the real Java API."""
import argparse
import asyncio
import hashlib
import json
import math
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from urllib.parse import urlsplit

import httpx

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
AGENT = ROOT / 'agent-service'
sys.path[:0] = [str(AGENT), str(ROOT / 'tests/real_e2e')]
from core.config import load_settings
from memory.task_store import TaskStore
from model_budget import load, write
from ops.engine import Investigator
from ops.tools import ObservationScope
from ops.usage import UsageTransport, usage_rows


TASKS = {
    'stock': ('REDIS_STOCK_DRIFT', [
        '请核对这个券的库存差额。是正常预扣，还是需要人工对账？',
        '监控提示库存数字不一致。检查两侧库存、订单在途与补偿，并指出能够确认的范围。']),
    'mq': ('MQ_CONSUME_DISABLED', [
        '下单接口返回已受理，订单却没有落库。请验证卡在哪个阶段。',
        '有反馈称订单长时间排队。检查生产、消费进度和库存快照，再给出最可能的原因。']),
    'duplicate': ('DUPLICATE_DELIVERY', [
        '同一笔订单看起来处理了不止一次。请核对消息投递和订单落库结果。',
        '消费日志频繁出现同一个订单。请验证是否存在重复扣库存或重复生成订单。']),
    'ip': ('IP_RATE_LIMIT', [
        '几个不同账号从同一出口提交请求时遇到429。请定位被哪个维度拒绝。',
        '抢券接口出现HTTP 429，请区分系统异常、用户额度与出口额度，并提供观测依据。']),
    'healthy': ('NO_FAULT', [
        '有值班同学怀疑订单链路异常。核查当前范围内是否有证据支持这些故障。',
        '请检查本券的受理、消费、库存和重复处理情况，给出当前证据能支持的结论。']),
    'missing-history': ('INSUFFICIENT_EVIDENCE', [
        '用户反馈较早的窗口出现异常。请判断现有日志是否覆盖，并区分实时快照与历史事实。',
        '请对指定时段的订单处理异常给出结论，数据不足时指出缺少哪些可核查的证据。']),
}
MODES = ('rules', 'logs', 'agent')
FAULT_CODES = {TASKS[name][0] for name in ('stock', 'mq', 'duplicate', 'ip')}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class FrozenProvider:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.read_counts = Counter()

    async def read(self, tool, scope):
        if scope.model_dump() != self.snapshot['scope']:
            raise ValueError('The frozen observation scope cannot be changed')
        index = self.read_counts[tool]
        self.read_counts[tool] += 1
        raw = self.snapshot['samples'][tool][index]
        if '_error' in raw:
            request = httpx.Request('GET', 'http://recorded.invalid/internal/ops/' + tool)
            response = httpx.Response(raw.get('status') or 503, request=request)
            response.raise_for_status()
        return raw


def cited_proof(root, cited):
    """Judge the cited evidence directly, independently of the diagnostic rules."""
    usable = [entry['raw'] for entry in cited if 'raw' in entry]
    by_tool = {row['tool']: row['data'] for row in usable}
    events = [event for row in usable for event in row['data'].get('events', [])]
    if root == 'IP_RATE_LIMIT':
        return any(event.get('status') == 429 and event.get('limitScope') == 'ip' for event in events)
    if root == 'MQ_CONSUME_DISABLED':
        mq = by_tool.get('mq', {})
        return mq.get('consumeEnabled') is False and mq['lag'] > 0 and mq['brokerOffset'] - mq['consumerOffset'] == mq['lag']
    if root == 'REDIS_STOCK_DRIFT':
        inventory = sorted((row for row in usable if row['tool'] == 'inventory'), key=lambda row: row['observedAt'])
        if len(inventory) != 2 or inventory[1]['observedAt'] - inventory[0]['observedAt'] < 300:
            return False
        values = [(int(row['data']['mysql'][0]['stock']), int(row['data']['redisStock']), row['data']['pending'][0]['total']) for row in inventory]
        return values[0] == values[1] and values[0][0] != values[0][1] and values[0][2] == 0
    if root == 'DUPLICATE_DELIVERY':
        delivery = by_tool.get('delivery', {})
        for order in delivery.get('orders', []):
            order_id = str(order['orderId'])
            matching = Counter(event['type'] for event in delivery.get('events', []) if str(event.get('orderId')) == order_id)
            if matching['mq_received'] >= 2 and matching['mq_handler_returned'] >= 2 and matching['order_idempotent'] >= 1:
                return sum(str(row['orderId']) == order_id for row in delivery['orders']) == 1
        return False
    if root == 'NO_FAULT':
        required = {'traffic', 'mq', 'inventory', 'delivery', 'compensation'}
        if not required <= by_tool.keys():
            return False
        inv = by_tool['inventory']
        return (by_tool['traffic']['coverageComplete'] and by_tool['delivery']['coverageComplete']
                and not any(e.get('status') == 429 and e.get('limitScope') == 'ip' for e in events)
                and not any(e['type'] == 'order_idempotent' for e in events)
                and by_tool['mq']['consumeEnabled'] and by_tool['mq']['lag'] == 0
                and int(inv['mysql'][0]['stock']) == int(inv['redisStock']) and inv['pending'][0]['total'] == 0)
    if root == 'INSUFFICIENT_EVIDENCE':
        # Evidence of a historical gap is verifiable. Simply guessing "unknown"
        # because the baseline did not query additional tools gets no proof credit.
        return any(row['data'].get('coverageComplete') is False for row in usable)
    return False


def grade(task, snapshot, output, usage):
    observations = {entry['id']: entry for entry in output.get('observations', [])}
    refs = output.get('evidence_ids', [])
    cited = [observations[ref] for ref in refs if ref in observations]
    authentic = bool(refs) and len(cited) == len(refs) and len(set(refs)) == len(refs)
    for entry in cited:
        raw = entry.get('raw')
        if raw is not None:
            authentic &= raw in snapshot['samples'][entry['tool']] and digest(raw) == entry['sha256']
            authentic &= raw['windowStart'] == snapshot['scope']['start'] and raw['windowEnd'] == snapshot['scope']['end']
        else:
            authentic &= any('_error' in sample for sample in snapshot['samples'][entry['tool']])
    root = output.get('root_code')
    top1 = output['state'] == 'completed' and root == task['expected']
    grounded = output.get('mode') != 'agent' or output.get('answer_origin') == 'llm_grounded'
    return {
        'root': root, 'expected': task['expected'], 'state': output['state'], 'error': output.get('error'),
        'top1_correct': top1, 'evidence_valid': bool(authentic),
        'evidence_accurate': bool(top1 and authentic and grounded and cited_proof(root, cited)),
        'false_alarm': task['expected'] not in FAULT_CODES and root in FAULT_CODES,
        'latency_ms': output['latency_ms'], 'reads': len(observations), 'model_requests': len(usage),
        'input_tokens': sum(row['input_tokens'] or 0 for row in usage),
        'output_tokens': sum(row['output_tokens'] or 0 for row in usage),
        'total_tokens': sum(row['total_tokens'] or 0 for row in usage),
        'unknown_usage': sum(row['total_tokens'] is None for row in usage),
    }


def summary(rows):
    result = {}
    for mode in MODES:
        group = [row for row in rows if row['mode'] == mode]
        if not group:
            continue
        times = sorted(row['latency_ms'] for row in group)
        totals = {key: sum(row[key] for row in group) for key in
                  ('top1_correct','evidence_valid','evidence_accurate','false_alarm','reads','model_requests',
                   'input_tokens','output_tokens','total_tokens','unknown_usage')}
        result[mode] = dict(totals, tasks=len(group), negative_controls=sum(row['expected'] not in FAULT_CODES for row in group),
                            completed=sum(row['state']=='completed' for row in group),
                            p50_ms=times[math.ceil(.50*len(times))-1], p95_ms=times[math.ceil(.95*len(times))-1])
    return result


async def main(capture, stock, evidence_dir):
    if not evidence_dir.is_relative_to(ROOT / 'artifacts'):
        raise ValueError('Evidence directory must be in this project')
    snapshots = {name: load((stock if name == 'stock' else capture) / f'snapshot-{name}.json') for name in TASKS}
    tasks = [{'id': f'{name}-{index}', 'snapshot': name, 'expected': truth, 'symptom': symptom,
              'snapshot_sha256': digest(snapshots[name])} for name, (truth, symptoms) in TASKS.items()
             for index, symptom in enumerate(symptoms, 1)]
    evidence_dir.mkdir(parents=True, exist_ok=False)
    settings = load_settings()
    if not settings.configured:
        raise RuntimeError('The configured real model is required')
    endpoint = urlsplit(settings.base_url)
    if endpoint.scheme != 'https' or endpoint.hostname != 'api.openlux.ai':
        raise RuntimeError('The evaluation must use the authorized model endpoint')
    write(evidence_dir / 'dataset.json', {'created': datetime.now().isoformat(), 'tasks': tasks,
          'capture': str(capture), 'stock': str(stock), 'model': settings.model,
          'source_hashes': {file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in (AGENT / 'ops').glob('*.py')},
          'mode': 'real model against frozen real Java reads', 'cost': 'UNKNOWN'})
    db_path = evidence_dir / 'investigations.sqlite3'
    task_store = TaskStore(db_path)
    transport = UsageTransport(db_path)
    rows = []
    concurrency = asyncio.Semaphore(2)

    async def case(task, mode):
        async with concurrency:
            snapshot = snapshots[task['snapshot']]
            scope = ObservationScope.model_validate(snapshot['scope'])
            owner = 'p4-evaluation'
            task_id = await task_store.create(owner, str(uuid4()), {'scope': scope.model_dump(), 'symptom': task['symptom']}, kind='ops')
            await task_store.claim(owner, task_id, lease_seconds=325)
            result = await Investigator(settings, task_store, FrozenProvider(snapshot), transport).run(task_id, scope, task['symptom'], mode=mode)
            usage = usage_rows(db_path, task_id)
            scored = grade(task, snapshot, result, usage)
            row = {'task': task['id'], 'mode': mode, **scored}
            write(evidence_dir / f"{task['id']}-{mode}.json", {'task': task, 'result': result, 'usage': usage, 'grade': row})
            rows.append(row)
            write(evidence_dir / 'summary.json', {'results': sorted(rows, key=lambda r: (r['mode'],r['task'])), 'aggregate': summary(rows)})
            print(json.dumps(row, ensure_ascii=False), flush=True)

    try:
        for mode in MODES:
            await asyncio.gather(*(case(task, mode) for task in tasks))
    finally:
        await transport.inner.aclose()
    if len(rows) != len(tasks) * len(MODES):
        raise RuntimeError('Incomplete comparison')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for argument in ('capture', 'stock', 'output'):
        parser.add_argument(f'--{argument}', type=lambda p: Path(p).resolve(), required=True)
    args = parser.parse_args()
    asyncio.run(main(args.capture, args.stock, args.output))

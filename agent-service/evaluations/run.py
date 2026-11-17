import argparse
import asyncio
import hashlib
import json
import math
import os
import statistics
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from core.config import load_settings
from main import create_app
from observability.metrics import summarize
from orchestration.router import Router
from rag.retrieval import RagService

ROOT = Path(__file__).resolve().parent


def percentile(values, fraction=.95):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)] if ordered else None


def offline():
    rows, correct, tool_correct, tool_count = [], 0, 0, 0
    for line in (ROOT / 'intents.jsonl').read_text(encoding='utf-8-sig').splitlines():
        case = json.loads(line)
        started = time.perf_counter()
        result = Router().route(case['text'], {'people': 2} if case.get('previous') else None)
        ok = result.kind == case['intent']
        correct += ok
        if case.get('tool'):
            tool_count += 1
            tool_correct += result.tool == case['tool']
        rows.append({**case, 'actual': result.model_dump(), 'passed': ok,
                     'duration_ms': (time.perf_counter() - started) * 1000})
    return {'dataset': 'hand-labelled router regression, no network and no business fixtures presented as real data',
            'cases': len(rows), 'intent_accuracy': correct / len(rows),
            'tool_selection_accuracy': tool_correct / tool_count,
            'tool_cases': tool_count, 'rows': rows}


def grade(data, case):
    if data.get('http_status') != 200 or data.get('error'):
        return False
    trace = data.get('trace', [])
    if not any(e.get('type') == 'tool_result' and any(r.get('status') == 200 for r in e.get('requests', [])) for e in trace):
        return False
    if case['id'] == 'shop_details':
        return any(e.get('tool') == 'shop_details' and e.get('result', {}).get('id') == 4 for e in trace)
    if case['id'] == 'shop_vouchers':
        return any(e.get('tool') in {'find_vouchers', 'shop_vouchers'} and e.get('result', {}).get('vouchers') for e in trace)
    if case['id'] == 'shop_types':
        return any(e.get('tool') == 'shop_types' for e in trace)
    # This grade measures grounded query completion, not correctness of unstructured LLM ranking.
    return any(e.get('tool') in {'shop_vouchers', 'find_vouchers'} for e in trace)


async def compare_live(settings):
    archive = ROOT / 'baseline_v02.zip'
    if not archive.exists():
        raise RuntimeError('Frozen baseline archive missing')
    local_day = datetime.now(ZoneInfo(settings.business_timezone)).date()
    visit = f'{local_day.isoformat()} 19:00'
    cases = [
        {'id': 'shop_details', 'message': '查询店铺4详情'},
        {'id': 'shop_vouchers', 'message': '查询店铺4的优惠券'},
        {'id': 'shop_types', 'message': '查询店铺分类列表'},
        {'id': 'consumption', 'message': f'帮我找附近适合两个人吃饭、人均100元以内、{visit}有优惠的店，推荐最划算的三家。当前位置经度120.16477,纬度30.25492（120.16477,30.25492）。请使用真实接口，规则无法确认时不要保证当晚可用。'},
    ]
    rows = []
    for case in cases:
        env = {**os.environ, 'OPENAI_API_KEY': settings.api_key, 'OPENAI_BASE_URL': settings.base_url,
               'OPENAI_MODEL': settings.model, 'SHOPLINK_BASE_URL': settings.shoplink_base_url, 'REDIS_URL': settings.redis_url,
               'PYTHONIOENCODING': 'utf-8'}
        process = await asyncio.create_subprocess_exec(sys.executable, str(ROOT / 'baseline_worker.py'),
                                                     stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                                                     stderr=asyncio.subprocess.PIPE, env=env)
        try:
            stdout, _stderr = await asyncio.wait_for(process.communicate(json.dumps({'archive': str(archive), 'message': case['message']}).encode()), 120)
            baseline = json.loads(stdout.decode('utf-8-sig').strip().splitlines()[-1])
        except Exception:
            if process.returncode is None:
                process.kill()
            await process.wait()
            baseline = {'http_status': 0, 'error': 'BASELINE_FAILED'}
        rows.append({'mode': 'original_sequential_v02', 'case': case, 'passed': grade(baseline, case),
                     'response': baseline, 'metrics': summarize(baseline.get('trace', []), baseline.get('measured_latency_ms', 0))})
        print(json.dumps({'progress': case['id'], 'mode': 'baseline', 'passed': rows[-1]['passed']}), flush=True)
        app = create_app(settings=settings)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://upgraded') as client:
                started = time.perf_counter()
                response = await client.post('/agent/chat/stream', json={'message': case['message']})
                if 'text/event-stream' in response.headers.get('content-type', ''):
                    events = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith('data: ')]
                    upgraded = next((event for event in reversed(events) if event.get('type') == 'done'), None)
                    if upgraded is None:
                        upgraded = {'trace': events, 'error': next((event for event in reversed(events) if event.get('type') == 'error'), {'code': 'INCOMPLETE_RESPONSE'})}
                else:
                    upgraded = response.json()
                upgraded.update(http_status=response.status_code, measured_latency_ms=(time.perf_counter() - started) * 1000)
        rows.append({'mode': 'router_supervisor_dag', 'case': case, 'passed': grade(upgraded, case),
                     'response': upgraded, 'metrics': upgraded.get('metrics', summarize([], upgraded['measured_latency_ms']))})
        print(json.dumps({'progress': case['id'], 'mode': 'upgraded', 'passed': rows[-1]['passed']}), flush=True)
    summaries = {}
    for mode in {'original_sequential_v02', 'router_supervisor_dag'}:
        group = [row for row in rows if row['mode'] == mode]
        times = [row['response'].get('measured_latency_ms', 0) for row in group]
        summaries[mode] = {'requests': len(group), 'grounded_query_success_rate': sum(r['passed'] for r in group) / len(group),
                           'p95_latency_ms_nearest_rank': percentile(times), 'mean_latency_ms': statistics.mean(times),
                           'model_calls': sum(r['metrics']['model_calls'] for r in group),
                           'tool_calls': sum(r['metrics']['tool_calls'] for r in group),
                           'http_requests': sum(r['metrics']['http_requests'] for r in group),
                           'total_tokens': sum(r['metrics']['total_tokens'] for r in group),
                           'usage_missing_calls': sum(r['metrics'].get('usage_missing_calls', 0) for r in group),
                           'mean_tokens_per_request': statistics.mean(r['metrics']['total_tokens'] for r in group)}
    return {'scope': 'real model + real Java + real Redis; in-process ASGI timing, excludes interpreter/app startup',
            'baseline_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
            'grading': 'HTTP success + actual successful business calls; does not equate free-text baseline ranking with verified decision output',
            'limitations': '4 smoke cases per mode, no repetitions, not a load test or statistically stable P95; no live mutations executed',
            'billing': 'Actual provider tokens only; no relay billing rates or invoice available', 'summaries': summaries, 'rows': rows}


async def rag_evaluation(settings):
    service = RagService(settings)
    documents = await service.store.documents(settings.embedding_model)
    cases = [
        {'query': '未支付订单取消后会怎样？', 'phrase': '只允许当前登录用户取消自己的未支付订单'},
        {'query': '这个项目有真实支付和扣款吗？', 'phrase': '没有真实支付通道'},
        {'query': '订单排队和处理中分别是什么状态码？', 'phrase': '10排队中'},
        {'query': '购买活动结束时间是否就是券的使用有效期？', 'phrase': '不代表到店核销有效期', 'alternative_phrases': ['购买活动时间不等于核销使用有效期']},
        {'query': '为什么人均参考价不能当实际结账金额？', 'phrase': '不是菜单报价'},
        {'query': '优惠券8今晚能否使用，规则是什么？', 'phrase': '使用规则', 'voucher_id': 8},
    ]
    rows = []
    for case in cases:
        eligible = [row for row in documents if any(phrase in row['text'] for phrase in [case['phrase']] + case.get('alternative_phrases', [])) and
                    (not case.get('voucher_id') or row['metadata'].get('voucher_id') == case['voucher_id'])]
        relevant = {row['id'] for row in eligible}
        if not relevant:
            raise RuntimeError('Evaluation relevance annotation missing from actual corpus')
        for baseline in (True, False):
            result = await service.search(case['query'], baseline=baseline, voucher_id=case.get('voucher_id'))
            ids = [hit['id'] for hit in result['hits'][:5]]
            rank = next((i for i, id in enumerate(ids, 1) if id in relevant), None)
            rows.append({'case': case, 'mode': 'bm25' if baseline else 'hybrid', 'recall_at_5': len(set(ids) & relevant) / len(relevant),
                         'reciprocal_rank': 1 / rank if rank else 0, 'relevant_ids': sorted(relevant), 'retrieval': result})
    summaries = {mode: {'queries': len([r for r in rows if r['mode'] == mode]),
                        'recall_at_5': statistics.mean(r['recall_at_5'] for r in rows if r['mode'] == mode),
                        'mrr': statistics.mean(r['reciprocal_rank'] for r in rows if r['mode'] == mode),
                        'p95_latency_ms_nearest_rank': percentile([r['retrieval']['duration_ms'] for r in rows if r['mode'] == mode])}
                 for mode in ('bm25', 'hybrid')}
    return {'scope': 'Real current corpus, real pgvector/Embedding/Rerank; relevant chunks annotated by source text spans',
            'summaries': summaries, 'rows': rows}


async def main(args):
    settings = load_settings()
    report = {'timestamp': datetime.now(ZoneInfo(settings.business_timezone)).isoformat(), 'model': settings.model}
    if args.mode in ('offline', 'all'):
        report['router'] = offline()
    if args.mode in ('live', 'all'):
        report['comparison'] = await compare_live(settings)
    if args.mode in ('rag', 'all'):
        report['rag'] = await rag_evaluation(settings)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'saved': str(args.output), 'sections': list(report)}, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['offline', 'live', 'rag', 'all'], default='offline')
    parser.add_argument('--output', type=Path, default=Path('evaluations/results/report.json'))
    asyncio.run(main(parser.parse_args()))

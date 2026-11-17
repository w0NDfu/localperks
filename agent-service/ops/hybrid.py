"""P4.1 routing and evidence gates for the existing read-only investigator."""
from collections import Counter
import asyncio
import re
import time

from core.state import AgentError
from ops.reasoning import ADVICE, ROOTS, analyze

UNKNOWN = 'INSUFFICIENT_EVIDENCE'
HEALTHY = 'NO_FAULT'
CURRENT_ONLY = {'REDIS_STOCK_DRIFT', 'MQ_CONSUME_DISABLED', HEALTHY}
MAX_SNAPSHOT_GAP_MS = 30_000
REASONS = {
    'multiple_roots': '多个根因类别同时获得证据，单一主因需要进一步判断',
    'conflicting_samples': '采样结果变化或库存差额与在途预留不一致',
    'missing_evidence': '关键观测缺失或查询失败',
    'historical_gap': '事件窗口覆盖不完整，或当前快照与历史窗口相隔过远',
    'partial_delivery': '出现幂等处理线索，但窗口内缺少两次投递的证据',
    'other_limit': '观察到其他限流维度，需要核对是否属于当前故障目录',
    'dynamic_scope': '请求需要核对时序、多个因素或补偿过程',
    'no_unique_proof': '现有证据无法唯一支持一个根因类别',
    'unique_proof': '证据支持唯一根因类别，已通过程序核验',
}


def _data(item):
    return item.get('raw', {}).get('data', {})


def _stock(item):
    data = _data(item)
    try:
        return int(data['mysql'][0]['stock']), int(data['redisStock']), int(data['pending'][0]['total'])
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def inspect(observations, symptom):
    checks = analyze(observations)
    supported = {code: proof for code, proof in checks['supported'].items() if code != UNKNOWN}
    usable = [item for item in observations if not item.get('error')]
    by_id = {item['id']: item for item in observations}
    last = {item['tool']: item for item in usable}
    stock = [_stock(item) for item in usable if item['tool'] == 'inventory']
    mq = _data(last.get('mq', {}))
    stale = {
        item['id'] for item in usable if item['tool'] in ('mq', 'inventory') and
        item['raw']['observedAt'] - item['raw']['windowEnd'] > MAX_SNAPSHOT_GAP_MS
    }
    for code in CURRENT_ONLY & supported.keys():
        if stale.intersection(supported[code]['evidence_ids']):
            del supported[code]

    # Group-wide lag needs a scoped in-flight reservation and a single broker
    # before the observations can support a diagnosis for this voucher.
    if 'MQ_CONSUME_DISABLED' in supported:
        inventory = stock[-1] if stock else None
        if mq.get('brokers') != 1 or not inventory or inventory[2] <= 0 or last['inventory']['id'] in stale:
            del supported['MQ_CONSUME_DISABLED']
        else:
            supported['MQ_CONSUME_DISABLED'] = {
                'evidence_ids': [*supported['MQ_CONSUME_DISABLED']['evidence_ids'], last['inventory']['id']],
                'detail': supported['MQ_CONSUME_DISABLED']['detail'] + f' 券范围内另观测到 {inventory[2]} 条在途预留。',
            }

    events = _data(last.get('events', {})).get('events', [])
    compensated = any(event.get('type') == 'reservation_compensated' for event in events)
    other_limits = sorted({event['limitScope'] for event in events if event.get('status') == 429 and event.get('limitScope') in ('user', 'global')})
    partial_delivery = any(event.get('type') == 'order_idempotent' for event in events) and 'DUPLICATE_DELIVERY' not in supported
    reasons = []
    if len(supported) > 1:
        reasons.append('multiple_roots')
    if stock and stock[-1] is not None:
        mysql, redis, pending = stock[-1]
        changed = len(stock) == 2 and stock[0] is not None and stock[0] != stock[1]
        if changed or (pending > 0 and mysql - redis != pending):
            reasons.append('conflicting_samples')
    if checks['errors'] or any(sample is None for sample in stock):
        reasons.append('missing_evidence')
    if not checks['coverage'] or stale:
        reasons.append('historical_gap')
    if compensated or re.search(r'时序|复盘|叠加|矛盾|多个故障|先后|并存|历史|过去窗口', symptom):
        reasons.append('dynamic_scope')
    if other_limits:
        reasons.append('other_limit')
    if partial_delivery:
        reasons.append('partial_delivery')
    if any(reason in reasons for reason in ('conflicting_samples', 'other_limit', 'partial_delivery', 'historical_gap', 'missing_evidence')) or compensated:
        supported.pop(HEALTHY, None)
    if len(supported) != 1:
        reasons.append('no_unique_proof')

    # A compensation event and an unresolved residual are useful observations,
    # independent of whether the investigation can prove a single primary cause.
    findings = []
    if compensated:
        findings.append({'kind': 'compensation', 'detail': '窗口内观察到预留补偿事件。', 'evidence_ids': [last['events']['id']]})
    if 'conflicting_samples' in reasons:
        findings.append({'kind': 'conflict', 'detail': '库存采样存在变化或无法由在途预留完全解释的差额。',
                         'evidence_ids': [item['id'] for item in usable if item['tool'] == 'inventory']})
    incomplete_stock = [item['id'] for item in usable if item['tool'] == 'inventory' and _stock(item) is None]
    if incomplete_stock:
        findings.append({'kind': 'incomplete_inventory', 'detail': '库存观测缺少完整读数，无法核验两侧库存差额。',
                         'evidence_ids': incomplete_stock})
    if stale:
        findings.append({'kind': 'temporal_gap', 'detail': '当前采样只说明读取时状态，无法证明历史窗口内的状态。',
                         'evidence_ids': sorted(stale)})
    if other_limits:
        findings.append({'kind': 'other_limit', 'detail': '窗口 429 的拒绝维度为 ' + '、'.join(other_limits) + '。',
                         'evidence_ids': [last['events']['id']]})
    if partial_delivery:
        findings.append({'kind': 'partial_delivery', 'detail': REASONS['partial_delivery'],
                         'evidence_ids': [last['events']['id'], *([last['delivery']['id']] if 'delivery' in last else [])]})
    proofs = [{'code': code, **proof} for code, proof in supported.items()]
    if len(supported) != 1:
        proof_ids = {identifier for proof in proofs + findings for identifier in proof['evidence_ids']}
        proof_ids.update(checks['errors'])
        proof_ids.update(item['id'] for item in usable if _data(item).get('coverageComplete') is False)
        if not proof_ids:
            proof_ids = set(by_id)
        supported[UNKNOWN] = {'evidence_ids': sorted(proof_ids), 'detail': '当前证据无法确认唯一的主根因类别。'}
    return {**checks, 'supported': supported, 'verified_categories': proofs, 'findings': findings, 'route_reasons': reasons}


def rank_reads(observations, checks):
    """Rank checks by the open questions they can resolve, bounded by existing tools."""
    counts = Counter(item['tool'] for item in observations)
    inventory = next((item for item in reversed(observations) if item['tool'] == 'inventory'), {})
    stock = _stock(inventory)
    events = next((_data(item).get('events', []) for item in observations if item['tool'] == 'events'), [])
    candidates = {}

    def offer(tool, score, question, limit=1):
        if counts[tool] < limit and score > candidates.get(tool, {}).get('score', 0):
            candidates[tool] = {'tool': tool, 'score': score, 'question': question}

    if stock and stock[0] - stock[1] != stock[2]:
        offer('inventory', 9, '差额是否在第二次库存采样中持续存在', limit=2)
    if any(e.get('type') == 'reservation_compensated' for e in events):
        offer('compensation', 10, '补偿是否与本券同一窗口相关')
    elif checks['route_reasons'] and ('REDIS_STOCK_DRIFT' in checks['supported'] or 'conflicting_samples' in checks['route_reasons']):
        offer('compensation', 8, '补偿或重试记录是否能解释仍未归因的库存差额')
    if any(e.get('status') == 429 and e.get('limitScope') == 'ip' for e in events):
        offer('traffic', 10, '是否有同一窗口的出口维度拒绝证据')
    if checks['route_reasons'] == ['no_unique_proof']:
        offer('compensation', 3, '是否有未核对的补偿或重试记录')
        offer('traffic', 3, '是否能排除窗口内的 IP 限流')
    return sorted(candidates.values(), key=lambda item: -item['score'])


def _report(root, symptom, route, checks):
    proof = checks['supported'][root]
    citations = list(dict.fromkeys(proof['evidence_ids'] + [identifier for finding in checks['findings'] for identifier in finding['evidence_ids']]))
    return {
        'root_code': root, 'root_title': ROOTS[root], 'answer': proof['detail'],
        'answer_origin': 'deterministic_fastpath' if route['path'] == 'rules_fastpath' else 'adaptive_verified',
        'evidence_ids': citations, 'recommendation': ADVICE[root], 'actions_executed': False,
        'route': route, 'symptom': symptom, 'verified_categories': checks['verified_categories'],
        'findings': checks['findings'], 'trigger_operation': {'status': 'unconfirmed', 'detail': '尚无变更审计或操作记录证实具体触发操作。'},
    }


async def investigate(investigator, scope, symptom, observations, collect, emit, phase):
    await phase('collection')
    await collect(['events', 'inventory', 'mq', 'delivery'], 0)
    initial = inspect(observations, symptom)
    proposed = rank_reads(observations, initial)
    focused = [candidate['tool'] for candidate in proposed if candidate['score'] >= 9]
    # A queue with unexplained residual needs the Agent to decide whether to
    # resample stock or inspect compensation, rather than spending both reads.
    if 'conflicting_samples' in initial['route_reasons'] and initial['verified_categories']:
        focused = []
    if focused:
        await collect(focused, 1)
    checks = inspect(observations, symptom)
    # To claim a healthy result, cover the same tool families as the rules baseline.
    if checks['route_reasons'] == ['no_unique_proof']:
        missing = [item['tool'] for item in rank_reads(observations, checks)]
        if missing:
            await collect(missing, 2)
            checks = inspect(observations, symptom)

    await phase('verification')
    if not checks['route_reasons']:
        route = {'path': 'rules_fastpath', 'reasons': [REASONS['unique_proof']]}
        await emit({'type': 'ops_route', **route})
        return _report(next(iter(checks['supported'])), symptom, route, checks), checks

    route = {'path': 'agent_deep', 'reasons': [REASONS[reason] for reason in checks['route_reasons']]}
    await emit({'type': 'ops_route', **route})
    history = None
    if investigator.history is not None:
        started = time.perf_counter()
        try:
            async with asyncio.timeout(36):
                history = await investigator.history.retrieve(symptom, observations, scope)
        except (AgentError, ValueError, RuntimeError, TimeoutError) as exc:
            history = {'strategy': 'unavailable', 'cases': [], 'warning': '历史检索不可用，继续用本次只读证据调查。',
                       'error': getattr(exc, 'code', type(exc).__name__),
                       'latency_ms': round((time.perf_counter() - started) * 1000, 3)}
        await emit({'type': 'ops_history', **history})
    await phase('hypotheses')
    root = None
    for round_number in range(2 if investigator.settings.configured else 0):
        ranked = rank_reads(observations, checks)[:max(0, 8 - len(observations))] if round_number == 0 else []
        await emit({'type': 'ops_information_gain', 'round': round_number, 'candidates': ranked})
        try:
            leads = [{key: case[key] for key in ('title', 'kind', 'similarities', 'differences', 'hypothesis')}
                     for case in history['cases']] if history and round_number == 0 else None
            move = await investigator._move(symptom, scope, observations, checks, emit,
                'Adaptive investigation. The ranked allowed next reads are ' + str(ranked) +
                '. Choose up to two listed tools only if useful. If none, choose one supported root and cite its evidence IDs. '
                'For multiple verified categories with no uniquely attributable primary, choose INSUFFICIENT_EVIDENCE.',
                allowed_tools=[item['tool'] for item in ranked], history_leads=leads)
        except (AgentError, ValueError) as exc:
            await emit({'type': 'ops_adaptive_model_error', 'code': getattr(exc, 'code', type(exc).__name__)})
            break
        await emit({'type': 'ops_hypotheses', 'step': round_number, 'move': move.model_dump()})
        selected = [item['tool'] for item in ranked if item['tool'] in move.tools][:2]
        if selected:
            await phase('collection')
            await collect(selected, 3 + round_number)
            checks = inspect(observations, symptom)
            continue
        if move.root_code in checks['supported']:
            proof = checks['supported'][move.root_code]
            known = {item['id'] for item in observations}
            if set(proof['evidence_ids']) <= set(move.evidence_ids) <= known:
                if len(checks['verified_categories']) <= 1 or move.root_code == UNKNOWN:
                    root = move.root_code
                    break
        await emit({'type': 'ops_hypothesis_rejected', 'root': move.root_code, 'evidence_ids': move.evidence_ids})

    if root is None and UNKNOWN in checks['supported']:
        root = UNKNOWN
        route['review'] = 'evidence_gate'
    elif root is None:
        # A bounded failed review must not silently become a successful diagnosis.
        root = UNKNOWN
        checks['supported'][UNKNOWN] = {
            'evidence_ids': [item['id'] for item in observations],
            'detail': '升级调查未能在有限步骤内完成主根因核验。保留已验证类别和观测，等待人工复核。',
        }
        route['review'] = 'unverified'
    else:
        route['review'] = 'verified'
    await phase('verification')
    await emit({'type': 'ops_verification', 'checks': checks})
    report = _report(root, symptom, route, checks)
    if history is not None:
        report['history'] = history
    return report, checks

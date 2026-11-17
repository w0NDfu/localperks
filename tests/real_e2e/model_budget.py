"""Real HTTP only. Persisted shared budget for one sequential validation run."""
import hashlib
import json
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx


def load(path, default=None):
    return json.loads(Path(path).read_text(encoding='utf-8-sig')) if Path(path).exists() else default


def write(path, value):
    # Atomic replacement prevents a partial ledger from permitting another call.
    target = Path(path)
    temporary = target.with_suffix('.pending.json')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(target)


class RealBudgetTransport(httpx.AsyncBaseTransport):
    def __init__(self, run):
        self.run = Path(run)
        self.path = self.run / 'api_usage.json'

    async def handle_async_request(self, request):
        if urlparse(str(request.url)).hostname != 'api.openlux.ai':
            raise RuntimeError('UNAUTHORIZED_MODEL_DESTINATION')
        kind = ('chat' if request.url.path.endswith('/chat/completions') else
                'embedding' if request.url.path.endswith('/embeddings') else
                'rerank' if request.url.path.endswith('/rerank') else None)
        if kind is None:
            raise RuntimeError('UNAUTHORIZED_API_PATH')
        ledger = load(self.path)
        if ledger is None:
            raise RuntimeError('Missing budget ledger')
        payload = json.loads(request.content)
        if kind == 'chat':
            payload['max_tokens'] = 512
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        previous = max((c.get('total_tokens') or 0 for c in ledger['calls'] if c['kind'] == kind), default=0)
        reserve = max(8192 if kind == 'chat' else 512, len(body) + (512 if kind == 'chat' else 0), previous + 1024)
        counts = {k: sum(c['kind'] == k for c in ledger['calls']) for k in ('chat', 'embedding', 'rerank')}
        limit = ledger['limits'][kind + '_requests']
        # Keep 10k tokens available for the semantic retrieval phase.
        token_cap = min(ledger['limits']['total_tokens'], ledger.get('chat_soft_token_cap',40000)) if kind == 'chat' else ledger['limits']['total_tokens']
        reason = None
        if ledger.get('closed') or ledger.get('pending'):
            reason = 'USAGE_UNKNOWN_OR_BUDGET_CLOSED'
        elif counts[kind] >= limit:
            reason = 'REQUEST_LIMIT'
        elif ledger['known_tokens'] + reserve > token_cap:
            reason = 'TOKEN_RESERVATION_LIMIT'
        if reason:
            ledger['blocked'].append({'kind': kind, 'reason': reason, 'reserved_tokens': reserve, 'timestamp': time.time()})
            write(self.path, ledger)
            raise RuntimeError(reason)
        entry = {'kind': kind, 'model': payload.get('model'), 'path': request.url.path,
                 'timestamp': time.time(), 'input_utf8_bytes': len(body), 'request_sha256': hashlib.sha256(body).hexdigest(),
                 'reserved_tokens': reserve, 'status': None, 'usage': None, 'total_tokens': None,
                 'cost': None, 'cost_reason': 'No provider invoice or verified billing rate'}
        ledger['calls'].append(entry)
        ledger['pending'] = True
        write(self.path, ledger)
        start = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(50, connect=10), trust_env=False) as client:
                response = await client.post(str(request.url), headers={
                    'Authorization': request.headers['authorization'], 'Content-Type': 'application/json'}, content=body)
            result = response.json()
            usage = result.get('usage') or {}
            tokens = usage.get('total_tokens')
            if tokens is None and usage.get('prompt_tokens') is not None:
                tokens = usage['prompt_tokens'] + usage.get('completion_tokens', 0)
            entry.update(status=response.status_code, usage=usage or None, total_tokens=tokens,
                         duration_ms=(time.perf_counter() - start) * 1000)
            for field in ('cost', 'billed_units', 'meta'):
                if field in result:
                    entry['provider_' + field] = result[field]
            if tokens is None:
                ledger['closed'] = 'PROVIDER_USAGE_UNKNOWN'
            else:
                ledger['known_tokens'] += tokens
                if ledger['known_tokens'] >= ledger['limits']['total_tokens']:
                    ledger['closed'] = 'TOKEN_LIMIT_REACHED_OR_EXCEEDED'
                elif tokens > reserve:
                    ledger['closed'] = 'PROVIDER_ACCOUNTING_EXCEEDED_RESERVATION'
            ledger['pending'] = False
            write(self.path, ledger)
            return httpx.Response(response.status_code, content=response.content,
                                  headers={'content-type': 'application/json'}, request=request)
        except Exception as exc:
            entry.update(error=type(exc).__name__, duration_ms=(time.perf_counter() - start) * 1000)
            ledger['closed'] = 'REQUEST_RESULT_OR_USAGE_UNKNOWN'
            ledger['pending'] = False
            write(self.path, ledger)
            raise

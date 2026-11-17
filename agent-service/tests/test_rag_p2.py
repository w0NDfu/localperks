import asyncio

import httpx
import pytest

from core.config import Settings
from rag.ingestion import chunks
from rag.lexical import BM25, rrf
from rag.retrieval import EmbeddingClient, RagService, Reranker


class CorpusStore:
    def __init__(self):
        self.rows = [c.to_dict() for c in chunks('优惠券使用有效期以商家规则为准，秒杀购买时间不等于使用日期。', 'fixture:rules', title='优惠券使用规则')]
        self.rows += [c.to_dict() for c in chunks('未支付订单可以取消，已支付订单不能直接取消。', 'fixture:cancel', title='取消订单说明')]
        self.rows += [c.to_dict() for c in chunks('当前只有模拟支付，没有真实支付通道。', 'fixture:payment', title='支付说明')]
        for row in self.rows:
            row['metadata'] = {}
    async def documents(self, _model, voucher_id=None):
        return self.rows
    async def dense(self, _vector, _model, _top_k, voucher_id=None):
        return [{**row, 'distance': .2 + i / 10} for i, row in enumerate(self.rows)]


def test_chunking_is_bounded_deterministic_and_citations_keep_source():
    text = ('优惠规则说明\n' * 200)
    first = chunks(text, 'source:rules', size=180, overlap=30)
    assert len(first) > 1 and max(len(c.text) for c in first) <= 180
    assert [c.id for c in first] == [c.id for c in chunks(text, 'source:rules', size=180, overlap=30)]
    assert len({c.id for c in first}) == len(first)
    assert all(c.source == 'source:rules' for c in first)


def test_bm25_and_rrf_have_reproducible_rankings():
    rows = CorpusStore().rows
    sparse = BM25(rows).search('取消未支付订单')
    assert sparse[0]['source'] == 'fixture:cancel'
    merged = rrf(sparse, list(reversed(rows)))
    assert len({r['id'] for r in merged}) == len(merged)
    assert merged == rrf(sparse, list(reversed(rows)))


def test_real_sdk_embedding_protocol_and_rerank_protocol():
    async def check():
        settings = Settings(api_key='test-api', embedding_dimensions=3)
        def respond(request):
            import json
            payload = json.loads(request.content)
            assert request.headers['authorization'] == 'Bearer test-api'
            if request.url.path.endswith('/embeddings'):
                assert payload['input'] == ['规则'] and payload['dimensions'] == 3
                return httpx.Response(200, json={'object': 'list', 'model': settings.embedding_model,
                    'data': [{'index': 0, 'object': 'embedding', 'embedding': [.1, .2, .3]}],
                    'usage': {'prompt_tokens': 2, 'total_tokens': 2}})
            assert request.url.path.endswith('/rerank')
            return httpx.Response(200, json={'results': [{'index': 1, 'relevance_score': .9}, {'index': 0, 'relevance_score': .3}]})
        vectors, usage = await EmbeddingClient(settings, httpx.MockTransport(respond)).embed(['规则'])
        assert vectors == [[.1, .2, .3]] and usage['total_tokens'] == 2
        ranked, mode, warning = await Reranker(settings, httpx.MockTransport(respond)).rank('取消订单', CorpusStore().rows, 2)
        assert mode == 'model_api' and warning is None and ranked[0]['source'] == 'fixture:cancel'
    asyncio.run(check())


def test_shared_retrieval_baseline_hybrid_sources_and_explicit_rerank_degradation():
    async def check():
        class Embeddings:
            async def embed(self, _texts):
                return [[1, 0, 0]], {'total_tokens': 3}
        class Rerank:
            async def rank(self, _query, rows, top_k):
                return sorted(rows, key=lambda r: r['source'] != 'fixture:cancel')[:top_k], 'fixture', None
        service = RagService(Settings(api_key='test'), store=CorpusStore(), embedding=Embeddings(), reranker=Rerank())
        baseline = await service.search('取消未支付订单', baseline=True)
        hybrid = await service.search('取消未支付订单')
        assert baseline['strategy'] == 'bm25' and 'dense' in hybrid['strategy']
        events = []
        async def emit(event):
            events.append(event)
        answer = await service.answer('取消订单规则是什么', emit)
        assert answer['sources'] and '来源' in answer['answer']
        assert all(source['url'].endswith(source['id']) for source in answer['sources'])
        assert any(e['type'] == 'rag' for e in events)
        def failed(_request):
            return httpx.Response(503)
        ranked, mode, warning = await Reranker(Settings(api_key='test'), httpx.MockTransport(failed)).rank('取消', CorpusStore().rows)
        assert ranked and mode == 'rrf' and warning
    asyncio.run(check())


def test_no_evidence_returns_uncertainty_and_no_fabricated_sources():
    async def check():
        store = CorpusStore()
        store.rows = []
        service = RagService(Settings(api_key=''), store=store)
        async def emit(_event):
            pass
        result = await service.answer('火星餐厅退款规则', emit)
        assert not result['sources'] and '无法确认' in result['answer']
    asyncio.run(check())

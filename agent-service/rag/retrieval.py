import time

from core.state import AgentError
from models.llm_client import open_model
from rag.lexical import BM25, rrf
from rag.store import PgVectorStore


class EmbeddingClient:
    def __init__(self, settings, transport=None):
        self.settings, self.transport = settings, transport

    async def embed(self, texts):
        if not texts or any(not text.strip() or len(text) > 12000 for text in texts):
            raise ValueError('Embedding input empty or too large')
        if not self.settings.api_key:
            raise AgentError('EMBEDDING_NOT_CONFIGURED', '知识检索需要配置Embedding API令牌', 503)
        try:
            async with open_model(self.settings, self.transport) as client:
                result = await client.embeddings.create(model=self.settings.embedding_model, input=texts,
                                                         dimensions=self.settings.embedding_dimensions)
            indexed = sorted(result.data, key=lambda row: row.index)
            if len(indexed) != len(texts) or any(len(row.embedding) != self.settings.embedding_dimensions for row in indexed):
                raise ValueError('Invalid embedding response')
            return [row.embedding for row in indexed], result.usage.model_dump() if result.usage else {}
        except AgentError:
            raise
        except Exception as exc:
            raise AgentError('EMBEDDING_UNAVAILABLE', 'Embedding接口调用失败，请检查模型权限或网络', 503) from exc


class Reranker:
    def __init__(self, settings, transport=None):
        self.settings, self.transport = settings, transport

    async def rank(self, query, rows, top_k=5):
        if not rows:
            return [], 'none', None
        if not self.settings.rerank_model:
            return rows[:top_k], 'rrf', '未配置Rerank模型'
        import httpx
        from models.runtime import model_http
        try:
            async with model_http(self.settings,self.transport) as client:
                response = await client.post(self.settings.base_url.rstrip('/') + self.settings.rerank_path,
                    headers={'Authorization': 'Bearer ' + self.settings.api_key}, timeout=12,
                    json={'model': self.settings.rerank_model, 'query': query,
                          'documents': [row['title'] + '\n' + row['text'] for row in rows], 'top_n': top_k, 'return_documents': False})
                response.raise_for_status()
                result = response.json()['results']
            seen, ranked = set(), []
            for entry in result:
                index = entry['index']
                if not isinstance(index, int) or index in seen or not 0 <= index < len(rows):
                    raise ValueError('Invalid rerank index')
                seen.add(index)
                ranked.append({**rows[index], 'rerank_score': float(entry['relevance_score'])})
            if not ranked:
                raise ValueError('Empty rerank response')
            return ranked[:top_k], 'model_api', None
        except Exception:
            return rows[:top_k], 'rrf', 'Rerank接口不可用，已明确降级为RRF顺序'


class RagService:
    def __init__(self, settings, *, store=None, embedding=None, reranker=None, model_transport=None):
        self.settings = settings
        self.model_transport = model_transport
        self.store = store or PgVectorStore(settings.pgvector_dsn, settings.embedding_dimensions)
        self.embedding = embedding or EmbeddingClient(settings)
        self.reranker = reranker or Reranker(settings)

    async def search(self, query, emit=None, *, baseline=False, voucher_id=None):
        started = time.perf_counter()
        documents = await self.store.documents(self.settings.embedding_model, voucher_id=voucher_id)
        if not documents:
            result = {'hits': [], 'strategy': 'empty', 'warning': '知识库尚无对应证据，请先导入文档', 'embedding_usage': {}}
        else:
            sparse = BM25(documents).search(query, 20)
            if baseline:
                result = {'hits': sparse[:5], 'strategy': 'bm25', 'embedding_usage': {}}
            else:
                vector, usage = await self.embedding.embed([query])
                dense = await self.store.dense(vector[0], self.settings.embedding_model, 20, voucher_id=voucher_id)
                # Do not turn unrelated nearest neighbours into evidence.
                if not sparse and (not dense or dense[0]['distance'] > .45):
                    hits, backend, warning = [], 'none', '未找到足够相关的知识证据'
                else:
                    hits, backend, warning = await self.reranker.rank(query, rrf(sparse, dense), 5)
                result = {'hits': hits, 'strategy': 'bm25+dense+rrf+' + backend,
                          'warning': warning, 'embedding_usage': usage}
        result['duration_ms'] = round((time.perf_counter() - started) * 1000, 3)
        if emit:
            await emit({'type': 'rag', 'duration_ms': result['duration_ms'], 'strategy': result['strategy'],
                        'usage': result['embedding_usage'], 'sources': [hit['id'] for hit in result['hits']], 'warning': result.get('warning')})
        return result

    async def answer(self, query, emit):
        from rag.evidence import assess
        from models.grounded import compose
        from core.state import AgentError
        result = await self.search(query, emit)
        return await self.answer_from_retrieval(query,result,emit)

    async def answer_from_retrieval(self,query,result,emit):
        from rag.evidence import assess
        from models.grounded import compose
        from core.state import AgentError
        sufficiency = assess(query,result['hits'])
        await emit({'type':'evidence_sufficiency',**sufficiency})
        citations=[]
        if sufficiency['sufficient']:
            ids={identifier for claim in sufficiency['claims'] for identifier in claim['source_ids']}
            facts={claim['id']:claim['claim'] for claim in sufficiency['claims']}
            for hit in result['hits']:
                if hit['id'] not in ids:continue
                url='/agent-api/agent/knowledge/sources/'+hit['id']
                citations.append({'id':hit['id'],'title':hit['title'],'source':hit['source'],'url':url})
                facts['source_'+hit['id']]='来源原文：'+hit['text']+' [查看来源]('+url+')'
        else:
            facts={'abstention':'无法确认用户所问的具体事实。'+sufficiency['reason']+'。不能补出金额、库存或确定资格。'}
        facts['scope']='文档说明不代表当前价格、库存或本人订单；实时事实必须查询Java接口。'
        if self.settings.configured:
            try:
                authored=await compose(self.settings,query,facts,emit,transport=self.model_transport,purpose='rag_answer')
            except (AgentError,ValueError):
                await emit({'type':'degradation','code':'RAG_ANSWER_UNAVAILABLE'})
                authored={'answer':'无法确认。模型回答未通过证据校验。','answer_origin':'deterministic_fallback'}
        else:
            authored={'answer':'\n'.join(facts.values()),'answer_origin':'deterministic'}
        return {**authored,'intent':'knowledge','state':'completed','sources':citations,
                'abstained':not sufficiency['sufficient'],'sufficiency':sufficiency,
                'retrieval':{k:v for k,v in result.items() if k!='hits'}}

    async def evidence_excerpt(self, query, emit):
        import re
        match = re.search(r'(?:券|优惠券)\s*(\d+)|([0-9]+)号券', query)
        voucher_id = int(match[1] or match[2]) if match else None
        result = await self.search(query, emit, voucher_id=voucher_id)
        if not result['hits']:
            return {'answer': '知识库没有足够证据，无法确认这项规则。请查看券详情或联系商家。',
                    'intent': 'knowledge', 'state': 'completed', 'sources': [], 'retrieval': {k: v for k, v in result.items() if k != 'hits'}}
        parts, citations = ['以下是知识库中与问题相关的原文证据：'], []
        if voucher_id and re.search(r'今晚|能用|可用|能否|有效', query) and any('以店铺说明为准' in hit['text'] or '未提供使用规则' in hit['text'] for hit in result['hits']):
            parts.insert(0, '现有规则无法确认你指定的日期一定可用，需要向商家核实。')
        for index, hit in enumerate(result['hits'][:3], 1):
            url = '/agent-api/agent/knowledge/sources/' + hit['id']
            parts.append(f"\n[{index}] {hit['title']}\n\n> " + hit['text'].replace('\n', '\n> '))
            parts.append(f"\n[查看来源{index}]({url})")
            citations.append({'id': hit['id'], 'title': hit['title'], 'source': hit['source'], 'url': url})
        parts.append('\n以上是文档说明，不代表实时库存、当前价格或个人订单状态；这些必须重新查询业务接口。')
        if result.get('warning'):
            parts.append(result['warning'])
        return {'answer': '\n'.join(parts), 'intent': 'knowledge', 'state': 'completed', 'sources': citations,
                'retrieval': {k: v for k, v in result.items() if k != 'hits'}}

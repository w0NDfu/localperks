"""Read-only retrieval of historical incident hypotheses for administrator investigations."""
import asyncio
import re
import time

from rag.lexical import BM25, rrf
from rag.retrieval import EmbeddingClient, Reranker
from rag.store import PgVectorStore


class CaseStore(PgVectorStore):
    async def initialize(self):
        def setup(conn):
            conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
            conn.execute(f'''CREATE TABLE IF NOT EXISTS ops_case_chunks (
                id TEXT NOT NULL, doc_id TEXT NOT NULL, content TEXT NOT NULL, source TEXT NOT NULL,
                title TEXT NOT NULL, metadata JSONB NOT NULL, version TEXT NOT NULL,
                embedding vector({self.dimensions}) NOT NULL, embedding_model TEXT NOT NULL,
                PRIMARY KEY (embedding_model, id))''')
            conn.execute('CREATE INDEX IF NOT EXISTS ops_case_document ON ops_case_chunks (embedding_model, doc_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS ops_case_vector_hnsw ON ops_case_chunks USING hnsw (embedding vector_cosine_ops)')
        await self.transact(setup, register=False)

    async def versions(self, model):
        return await self.transact(lambda conn: dict(conn.execute(
            'SELECT id,version FROM ops_case_chunks WHERE embedding_model=%s', (model,)).fetchall()))

    async def upsert_case(self, model, case, changed, vectors):
        from pgvector import Vector
        from psycopg.types.json import Jsonb

        def save(conn):
            for row in changed:
                conn.execute('''INSERT INTO ops_case_chunks VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(embedding_model,id) DO UPDATE SET content=excluded.content, source=excluded.source,
                    title=excluded.title,metadata=excluded.metadata,version=excluded.version,embedding=excluded.embedding''',
                    (row['id'], row['doc_id'], row['text'], row['source'], row['title'], Jsonb(row['metadata']),
                     row['version'], Vector(vectors[row['id']]), model))
            conn.execute('DELETE FROM ops_case_chunks WHERE embedding_model=%s AND doc_id=%s AND NOT (id=ANY(%s))',
                         (model, case[0]['doc_id'], [row['id'] for row in case]))
        await self.transact(save)

    async def counts(self, model):
        def query(conn):
            incidents, runbooks, chunks, vectors = conn.execute('''SELECT
                count(DISTINCT doc_id) FILTER (WHERE metadata->>'kind'<>'runbook'),
                count(DISTINCT doc_id) FILTER (WHERE metadata->>'kind'='runbook'),
                count(*),count(embedding) FROM ops_case_chunks WHERE embedding_model=%s''', (model,)).fetchone()
            return dict(incidents=incidents, runbooks=runbooks, chunks=chunks, vectors=vectors)
        return await self.transact(query)

    @staticmethod
    def filters(model, service, code):
        sql = """embedding_model=%s AND (%s::text IS NULL OR (metadata->'services') ? %s)
            AND (%s::text IS NULL OR (metadata->'codes') ? %s)"""
        return sql, (model, service, service, code, code)

    async def documents(self, model, *, service=None, code=None):
        where, params = self.filters(model, service, code)
        def query(conn):
            rows = conn.execute('SELECT id,doc_id,content,source,title,metadata FROM ops_case_chunks WHERE '
                                + where + ' ORDER BY id LIMIT 5000', params)
            return [self.row(row) for row in rows]
        return await self.transact(query)

    async def dense(self, vector, model, top_k=30, *, service=None, code=None):
        from pgvector import Vector
        where, params = self.filters(model, service, code)
        value = Vector(vector)
        def query(conn):
            rows = conn.execute('SELECT id,doc_id,content,source,title,metadata,embedding <=> %s AS distance '
                                'FROM ops_case_chunks WHERE ' + where + ' ORDER BY embedding <=> %s LIMIT %s',
                                (value, *params, value, top_k))
            return [{**self.row(row), 'distance': float(row[6])} for row in rows]
        return await self.transact(query)


SIGNALS = {
    'timeout': ('HTTP 请求或读超时', r'超时|timeout|timed out|504'),
    'mq_lag': ('RocketMQ 消费位点落后和积压', r'积压|排队|backlog|\blag\b'),
    'mq_paused': ('RocketMQ 消费开关关闭', r'停消费|暂停消费|关闭消费'),
    'stock_gap': ('Redis 与 MySQL 库存差额', r'库存差|库存不一致|库存偏|库存矛盾|预扣|stock.drift'),
    'lock_wait': ('MySQL 行锁等待和 Hikari 连接池阻塞', r'行锁|锁等待|连接池|hikari|innodb'),
    'outbox': ('订单 Outbox 与延迟关单消息', r'outbox|关单'),
    'duplicate': ('同一订单消息重复投递和幂等', r'幂等|复投|重复|redeliver'),
    'compensation': ('订单库存回补和预留补偿', r'补偿|回补|compensat'),
    'ip_429': ('HTTP 429 共享出口 IP 限流', r'\bip\b|共享出口'),
    'user_429': ('HTTP 429 用户维度限流', r'单用户|用户维度'),
    'http_429': ('HTTP 429 限流维度待核对', r'429|限流'),
    'cache_missing': ('Redis 库存键缺失无读数', r'键缺失|查不到|键不在|无读数|无值|空值'),
    'historical': ('历史事件窗口与当前采样时间差', r'历史|旧窗口|过去窗口|时间线|时序|事后'),
}
SERVICES = {'spring': r'spring|tomcat|http|接口', 'rocketmq': r'rocketmq|\bmq\b|broker|队列|消息',
            'mysql': r'mysql|\bsql\b|数据库', 'redis': r'redis|缓存'}


def live_signals(observations, scope):
    signals = {}
    def add(signal, identifier):
        signals.setdefault(signal, set()).add(identifier)
    for item in observations:
        raw = item.get('raw', {})
        data, identifier, tool = raw.get('data', {}), item['id'], item['tool']
        if tool in ('inventory', 'mq') and raw.get('observedAt', 0) - scope.end > 30_000:
            add('historical', identifier)
        if tool == 'mq':
            if (data.get('lag') or 0) > 0:
                add('mq_lag', identifier)
            if data.get('consumeEnabled') is False:
                add('mq_paused', identifier)
            elif data.get('consumeEnabled') is True:
                add('mq_enabled', identifier)
        if tool == 'inventory' and data.get('mysql'):
            redis = data.get('redisStock')
            if redis is None:
                add('cache_missing', identifier)
            elif int(data['mysql'][0]['stock']) != int(redis):
                add('stock_gap', identifier)
        for event in data.get('events', []):
            if event.get('type') == 'order_idempotent':
                add('duplicate', identifier)
            if event.get('type') == 'reservation_compensated':
                add('compensation', identifier)
            if event.get('status') == 429 and event.get('limitScope') in ('ip', 'user'):
                add(event['limitScope'] + '_429', identifier)
    return signals


def consolidate(rows):
    cases = {}
    for row in rows:
        case = cases.setdefault(row['doc_id'], {**row, 'matched_sections': []})
        case['matched_sections'].append(row['metadata']['section'])
    return list(cases.values())


class CaseHistory:
    def __init__(self, settings, *, store=None, transport=None, rerank=False):
        self.settings = settings
        self.store = store or CaseStore(settings.pgvector_dsn, settings.embedding_dimensions)
        self.embed = EmbeddingClient(settings, transport)
        self.reranker = Reranker(settings, transport)
        self.rerank = rerank

    async def search(self, query, *, service=None, code=None):
        model = self.settings.embedding_model
        filters = {'service': service, 'code': code}
        documents = await self.store.documents(model, **filters)
        if not documents:
            return [], 'empty', None
        sparse = BM25(documents).search(query, 30)
        (vector,), _ = await self.embed.embed([query])
        dense = await self.store.dense(vector, model, **filters)
        if not sparse and (not dense or dense[0]['distance'] > .45):
            return [], 'no_match', None
        candidates = consolidate(rrf(sparse, dense, top_k=60))[:8]
        if not self.rerank:
            return candidates[:3], 'rrf', None
        rerank_input = [{**row, 'text': ' '.join(row['metadata'][key] for key in ('symptoms', 'metrics', 'verified', 'unknown'))}
                        for row in candidates]
        return await self.reranker.rank(query, rerank_input, 3)

    async def retrieve(self, symptom, observations, scope):
        # Only normalized terms cross the model boundary. User text, IPs, IDs,
        # live measurements, logs and paths never go to embedding or reranking.
        started = time.perf_counter()
        live = live_signals(observations, scope)
        mentioned = {key for key, (_, pattern) in SIGNALS.items() if re.search(pattern, symptom, re.I)}
        signals = (live.keys() | mentioned) & SIGNALS.keys()
        services = [service for service, pattern in SERVICES.items() if re.search(pattern, symptom, re.I)]
        if not signals:
            return {'strategy': 'no_query_signal', 'cases': [], 'latency_ms': 0}
        query = ' '.join([*services, *(SIGNALS[key][0] for key in sorted(signals))])
        # Structured filters are available to callers. An investigation keeps
        # cross-service candidates because the failure may be downstream.
        async with asyncio.timeout(35):
            hits, backend, warning = await self.search(query)
        cases = []
        for hit in hits:
            doc = hit['metadata']
            matched = sorted(signals & set(doc['signals']))
            differences = ['本次尚未观察到：' + SIGNALS[key][0]
                           for key in doc['signals'] if key in SIGNALS and key not in live]
            if 'mq_paused' in doc['signals'] and 'mq_enabled' in live and 'mq_paused' not in live:
                differences.insert(0, '本次消费开关为开启，与案例中的停消费条件不同')
            cases.append({'id': hit['doc_id'], 'kind': doc['kind'], 'title': hit['title'],
                          'similarities': [SIGNALS[key][0] for key in matched],
                          'differences': differences, 'hypothesis': doc['investigate'],
                          'sources': doc['sources'], 'matched_sections': hit['matched_sections'],
                          'checks': [{'signal': SIGNALS[key][0], 'observation_ids': sorted(live.get(key, ())),
                                      'status': 'observed' if key in live else 'reported_only'} for key in matched]})
        return {'strategy': backend, 'warning': warning, 'cases': cases,
                'latency_ms': round((time.perf_counter() - started) * 1000, 3)}

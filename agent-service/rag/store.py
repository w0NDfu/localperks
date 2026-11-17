"""pgvector transactions offloaded to threads, compatible with Windows Proactor."""
import asyncio
from core.state import AgentError


class PgVectorStore:
    def __init__(self, dsn, dimensions=1536):
        if not 1 <= dimensions <= 2000:
            raise ValueError('HNSW vector维度需在1..2000之间')
        self.dsn, self.dimensions = dsn, dimensions

    async def transact(self, operation, *, register=True):
        import psycopg
        from pgvector.psycopg import register_vector
        if not self.dsn:
            raise AgentError('RAG_NOT_CONFIGURED', '尚未配置PGVECTOR_DSN，请先初始化知识服务', 503)
        def work():
            conn = None
            try:
                conn = psycopg.connect(self.dsn, connect_timeout=3)
                with conn:
                    if register:
                        register_vector(conn)
                    return operation(conn)
            except psycopg.Error as exc:
                raise AgentError('RAG_STORE_UNAVAILABLE', '知识向量存储不可用或尚未初始化', 503) from exc
            finally:
                if conn is not None:
                    conn.close()
        return await asyncio.to_thread(work)

    async def initialize(self):
        def setup(conn):
            conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
            conn.execute(f'''CREATE TABLE IF NOT EXISTS knowledge_chunks (
                id TEXT PRIMARY KEY, doc_id TEXT NOT NULL, content TEXT NOT NULL,
                source TEXT NOT NULL, title TEXT NOT NULL, metadata JSONB NOT NULL,
                embedding vector({self.dimensions}) NOT NULL, embedding_model TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT true,
                indexed_at TIMESTAMPTZ NOT NULL DEFAULT now())''')
            conn.execute('CREATE INDEX IF NOT EXISTS knowledge_vector_hnsw ON knowledge_chunks USING hnsw (embedding vector_cosine_ops)')
        await self.transact(setup, register=False)

    async def upsert_document(self, chunks, vectors, model):
        from pgvector import Vector
        from psycopg.types.json import Jsonb
        if len(chunks) != len(vectors) or not chunks:
            raise ValueError('Chunks and embeddings must align')
        if any(len(vector) != self.dimensions for vector in vectors):
            raise ValueError('Embedding dimension mismatch')
        def save(conn):
            conn.execute('UPDATE knowledge_chunks SET active=false WHERE doc_id=%s', (chunks[0].doc_id,))
            for chunk, vector in zip(chunks, vectors):
                conn.execute('INSERT INTO knowledge_chunks(id,doc_id,content,source,title,metadata,embedding,embedding_model) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET active=true,embedding=excluded.embedding,embedding_model=excluded.embedding_model,metadata=excluded.metadata,indexed_at=now()',
                             (chunk.id, chunk.doc_id, chunk.text, chunk.source, chunk.title, Jsonb(chunk.metadata), Vector(vector), model))
        await self.transact(save)

    @staticmethod
    def row(row):
        return {'id': row[0], 'doc_id': row[1], 'text': row[2], 'source': row[3], 'title': row[4], 'metadata': row[5]}

    async def documents(self, model, *, voucher_id=None):
        def query(conn):
            rows = conn.execute("SELECT id,doc_id,content,source,title,metadata FROM knowledge_chunks WHERE active=true AND embedding_model=%s AND (%s::text IS NULL OR metadata->>'voucher_id'=%s) ORDER BY id LIMIT 5000",
                                (model, str(voucher_id) if voucher_id is not None else None, str(voucher_id))).fetchall()
            return [self.row(row) for row in rows]
        return await self.transact(query)

    async def dense(self, vector, model, top_k=20, *, voucher_id=None):
        from pgvector import Vector
        def query(conn):
            rows = conn.execute("SELECT id,doc_id,content,source,title,metadata,embedding <=> %s AS distance FROM knowledge_chunks WHERE active=true AND embedding_model=%s AND (%s::text IS NULL OR metadata->>'voucher_id'=%s) ORDER BY embedding <=> %s LIMIT %s",
                                (Vector(vector), model, str(voucher_id) if voucher_id is not None else None, str(voucher_id), Vector(vector), top_k)).fetchall()
            return [{**self.row(row), 'distance': float(row[6])} for row in rows]
        return await self.transact(query)

    async def source(self, chunk_id):
        row = await self.transact(lambda conn: conn.execute('SELECT id,doc_id,content,source,title,metadata FROM knowledge_chunks WHERE id=%s', (chunk_id,)).fetchone())
        if row is None:
            raise AgentError('SOURCE_NOT_FOUND', '知识来源不存在', 404)
        return self.row(row)


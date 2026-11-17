import asyncio
import os

import pytest

from core.config import load_settings
from rag.store import PgVectorStore


def test_real_pgvector_can_retrieve_current_vectors_and_evidence_readonly():
    if os.environ.get('AGENT_TEST_PGVECTOR') != '1':
        pytest.skip('set AGENT_TEST_PGVECTOR=1 after rag.cli ingestion')
    async def check():
        cfg = load_settings()
        store = PgVectorStore(cfg.pgvector_dsn, cfg.embedding_dimensions)
        model = cfg.embedding_model
        row = await store.transact(lambda c: c.execute('SELECT id,embedding FROM knowledge_chunks WHERE active=true AND embedding_model=%s LIMIT 1', (model,)).fetchone())
        assert row is not None
        hits = await store.dense(row[1].to_list(), model, 5)
        assert hits and hits[0]['distance'] < .00001
        evidence = await store.source(row[0])
        assert evidence['source'] and evidence['text']
        assert len(await store.documents(model)) >= 1
    asyncio.run(check())

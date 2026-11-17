"""Index curated, source-verified AIOps case summaries. Raw artifacts stay local."""
import argparse
import asyncio
import hashlib
import json
import re
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Source(BaseModel):
    model_config = ConfigDict(extra='forbid')
    path: str
    sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class Case(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id: str = Field(pattern=r'^[a-z][a-z0-9-]{2,63}$')
    kind: Literal['isolated_load_test', 'isolated_fault_injection', 'runbook']
    title: str = Field(min_length=3, max_length=120)
    services: list[str] = Field(min_length=1, max_length=6)
    codes: list[str] = Field(max_length=8)
    signals: list[str] = Field(min_length=1, max_length=8)
    tools: list[Literal['spring', 'events', 'traffic', 'mq', 'inventory', 'delivery', 'compensation']] = Field(max_length=4)
    symptoms: str = Field(min_length=3, max_length=650)
    timeline: str = Field(min_length=3, max_length=1000)
    metrics: str = Field(min_length=3, max_length=1000)
    verified: str = Field(min_length=3, max_length=650)
    unknown: str = Field(min_length=3, max_length=650)
    investigate: str = Field(min_length=3, max_length=650)
    fix: str = Field(min_length=3, max_length=1000)
    validation: str = Field(min_length=3, max_length=1000)
    sources: list[Source] = Field(min_length=1, max_length=14)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def load_cases(repo):
    repo = Path(repo).resolve()
    entries = json.loads((repo / 'docs/ops_knowledge/cases.json').read_text(encoding='utf-8'))
    cases = [Case.model_validate(entry) for entry in entries]
    if len({case.id for case in cases}) != len(cases):
        raise ValueError('Duplicate case ID')
    evidence_sets = set()
    for case in cases:
        raw = []
        for source in case.sources:
            path = (repo / source.path).resolve()
            if not path.is_relative_to(repo) or not source.path.startswith(('docs/', 'artifacts/test-runs/')):
                raise ValueError(f'Outside approved evidence roots: {source.path}')
            if digest(path.read_bytes()) != source.sha256:
                raise ValueError(f'Source checksum changed: {source.path}')
            if source.path.startswith('artifacts/test-runs/'):
                raw.append(source.sha256)
        if case.kind != 'runbook':
            signature = frozenset(raw)
            if not signature or signature in evidence_sets:
                raise ValueError(f'Missing or duplicate incident evidence: {case.id}')
            evidence_sets.add(signature)
    return cases


def redact(text):
    # This is a second gate on human-curated summaries, not a raw log sanitizer.
    for pattern in (
        r'(?i)(?:authorization\s*:\s*|bearer\s+|(?:api[_-]?key|password|token)\s*[=:]\s*)[^\s，；]+',
        r'(?i)(?:https?|redis|postgresql|jdbc:mysql)://[^\s，；]+',
        r'(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])',
        r'(?<!\d)(?:1[3-9]\d{9}|\d{15,20})(?!\d)',
        r'[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}',
    ):
        text = re.sub(pattern, '[REDACTED]', text)
    return text


def case_chunks(case):
    summary = case.model_dump(exclude={'id', 'sources'})
    summary.update({key: redact(value) for key, value in summary.items() if isinstance(value, str)})
    # Only controlled vocabulary crosses into model input from metadata.
    if any(not re.fullmatch(r'[a-zA-Z0-9_-]{2,64}', value)
           for field in ('services', 'signals', 'codes') for value in summary[field]):
        raise ValueError(f'Invalid case tag: {case.id}')
    origin = [source.model_dump() for source in case.sources]
    sections = {
        'symptoms': ('symptoms', 'timeline', 'metrics'),
        'diagnosis': ('verified', 'unknown', 'investigate'),
        'recovery': ('fix', 'validation'),
    }
    labels = {'symptoms': '症状', 'timeline': '时间线', 'metrics': '指标', 'verified': '已核验',
              'unknown': '未知项', 'investigate': '排查', 'fix': '处置', 'validation': '验证'}
    chunks = []
    for section, keys in sections.items():
        body = '\n'.join(f'{labels[key]}：{summary[key]}' for key in keys)
        content = summary['title'] + '\n' + body + '\n' + ' '.join(summary['services'] + summary['codes'])
        metadata = {**summary, 'section': section, 'sources': origin}
        version = digest(json.dumps([content, metadata], ensure_ascii=False, sort_keys=True).encode())
        chunks.append({'id': f'{case.id}:{section}', 'doc_id': case.id, 'title': summary['title'],
                       'text': content, 'version': version, 'source': origin[0]['path'], 'metadata': metadata})
    return chunks


async def index_cases(repo, store, model, embed):
    cases = load_cases(repo)
    documents = [case_chunks(case) for case in cases]
    await store.initialize()
    existing = await store.versions(model)
    changed = [(case, [row for row in case if existing.get(row['id']) != row['version']])
               for case in documents]
    pending = [row for _, updates in changed for row in updates]
    vectors = {}
    usage = []
    for start in range(0, len(pending), 24):
        batch = pending[start:start + 24]
        encoded, billed = await embed([row['text'] for row in batch])
        vectors.update((row['id'], vector) for row, vector in zip(batch, encoded, strict=True))
        usage.append(billed)
    for case, updates in changed:
        await store.upsert_case(model, case, updates, vectors)
    return {**await store.counts(model), 'updated_chunks': len(pending), 'embedding_usage': usage}


async def main():
    from core.config import load_settings
    from ops.case_history import CaseStore
    from ops.usage import INVESTIGATION, UsageTransport, usage_rows
    from rag.retrieval import EmbeddingClient
    from uuid import uuid4
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True, type=Path)
    parser.add_argument('--apply', action='store_true', help='Write only the AIOps case index to configured PostgreSQL')
    args = parser.parse_args()
    if not args.apply:
        cases = load_cases(args.repo)
        print(json.dumps({'verified_cases': len(cases), 'chunks': sum(len(case_chunks(case)) for case in cases)}))
        return
    settings = load_settings()
    from memory.task_store import TaskStore
    journal = await asyncio.to_thread(TaskStore, settings.state_dsn, schema=settings.state_schema)
    index_id = 'ops-index-' + str(uuid4())
    transport = UsageTransport(journal)
    token = INVESTIGATION.set({'task_id': index_id, 'requests': 0})
    try:
        result = await index_cases(args.repo, CaseStore(settings.pgvector_dsn, settings.embedding_dimensions),
                                   settings.embedding_model, EmbeddingClient(settings, transport).embed)
        print(json.dumps(result, ensure_ascii=False))
    finally:
        INVESTIGATION.reset(token)
        await transport.inner.aclose()
        print(json.dumps({'index_id': index_id, 'cost': 'UNKNOWN', 'requests': await asyncio.to_thread(usage_rows, journal, index_id)},
                         ensure_ascii=False))
        await journal.aclose()


if __name__ == '__main__':
    asyncio.run(main())

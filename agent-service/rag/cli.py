"""Explicit operator ingestion; no public upload or arbitrary file tool."""
import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx

from core.config import load_settings
from rag.ingestion import chunks, parse_file
from rag.retrieval import RagService
from tools.java_api import JavaAPI


async def ingest(repo: Path, output: Path | None = None):
    settings = load_settings()
    service = RagService(settings)
    await service.store.initialize()
    documents = [parse_file(repo / 'docs' / 'AGENT_BUSINESS_KNOWLEDGE.md', 'localperks:docs/AGENT_BUSINESS_KNOWLEDGE.md')]
    async with httpx.AsyncClient(base_url=settings.shoplink_base_url, trust_env=False, timeout=8) as client:
        java = JavaAPI(client)
        market = await java.get('/voucher/market')
        shops = []
        for page in range(1, 21):
            batch = await java.get('/shop/of/name', params={'current': page})
            shops.extend(batch)
            if len(batch) < 10:
                break
        for shop in shops:
            # No stock, prices or per-user information enters the knowledge corpus.
            text = '\n'.join([f"店铺：{shop['name']}", f"区域：{shop.get('area', '未提供')}",
                              f"地址说明：{shop.get('address', '未提供')}",
                              '座位、停车、预约、包间等服务条件未由当前店铺接口提供，不能承诺。'])
            documents.append(chunks(text, f"localperks-api:/shop/{shop['id']}", title=shop['name'],
                                    metadata={'kind': 'shop', 'shop_id': shop['id']}))
        for voucher in market:
            text = f"优惠券{voucher['id']}（店铺{voucher['shopId']}）使用规则：\n{voucher.get('rules') or '接口未提供使用规则，无法确认。'}\n购买活动时间不等于核销使用有效期。实际资格需查最新券详情并由Java验证。"
            documents.append(chunks(text, f"localperks-api:/voucher/{voucher['id']}/rules",
                                    title=f"优惠券{voucher['id']}使用规则", metadata={'kind': 'voucher', 'voucher_id': voucher['id'], 'shop_id': voucher['shopId']}))
    flat = [chunk for document in documents for chunk in document]
    vectors, tokens, calls = [], 0, 0
    for start in range(0, len(flat), 32):
        embedded, usage = await service.embedding.embed([c.text for c in flat[start:start + 32]])
        vectors.extend(embedded)
        tokens += usage.get('total_tokens', usage.get('prompt_tokens', 0))
        calls += 1
    offset = 0
    for document in documents:
        await service.store.upsert_document(document, vectors[offset:offset + len(document)], settings.embedding_model)
        offset += len(document)
    counts = len(flat)
    report = {'timestamp': datetime.now(timezone.utc).isoformat(), 'documents': len(documents), 'chunks': counts,
              'embedding_model': settings.embedding_model, 'embedding_tokens': tokens, 'embedding_calls': calls,
              'sources': [doc[0].source for doc in documents if doc]}
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in report.items() if k != 'sources'}, ensure_ascii=False))
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--output', type=Path, default=Path('evaluations/results/ingestion.json'))
    args = parser.parse_args()
    asyncio.run(ingest(args.repo, args.output))

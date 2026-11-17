"""Frozen Multi-CPR reduced-corpus benchmark; real embeddings and real rerank API.

No documents are written to the application's knowledge store. Qrels are used
only for constructing a documented reduced corpus and scoring, never as model input.
"""
import argparse
import asyncio
import hashlib
import heapq
import json
import math
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "agent-service"))
from core.config import load_settings
from rag.lexical import BM25, rrf
from rag.retrieval import Reranker
import httpx

DATA = ROOT / "artifacts/customer-assistant/datasets/multicpr-ecom"
RUN = ROOT / "artifacts/customer-assistant/retrieval"
SEED = "localperks-multicpr-20261112-v1"


def hashed(value):
    return hashlib.sha256((SEED + value).encode()).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def freeze():
    queries = dict(line.rstrip("\n").split("\t", 1) for line in (DATA / "dev.query.txt").read_text(encoding="utf-8").splitlines())
    qrels = {}
    for line in (DATA / "qrels.dev.tsv").read_text(encoding="utf-8").splitlines():
        qid, _, pid, relevance = line.split()
        if int(relevance) > 0:
            qrels.setdefault(qid, []).append(pid)
    positives = {pid for values in qrels.values() for pid in values}
    documents, candidates = {}, []
    corpus_count = 0
    # Fixed hash sampling: retain all judged dev positives and fill with 9k+ distractors.
    with (DATA / "corpus.tsv").open(encoding="utf-8") as source:
        for line in source:
            pid, text = line.rstrip("\n").split("\t", 1)
            corpus_count += 1
            if pid in positives:
                documents[pid] = text
            else:
                rank = -int(hashed(pid), 16)
                item = (rank, pid, text)
                if len(candidates) < 10000:
                    heapq.heappush(candidates, item)
                elif rank > candidates[0][0]:
                    heapq.heapreplace(candidates, item)
    assert positives <= documents.keys()
    for _, pid, text in sorted(candidates, reverse=True)[:10000 - len(documents)]:
        documents[pid] = text
    ordered_queries = sorted(queries, key=hashed)
    cases = [{"id": qid, "query": queries[qid], "relevant": qrels[qid],
              "split": "development" if index < 200 else "holdout"}
             for index, qid in enumerate(ordered_queries)]
    corpus = [{"id": pid, "title": "", "text": text} for pid, text in sorted(documents.items())]
    manifest = {"dataset": "Multi-CPR E-commerce official dev, reduced corpus", "seed": SEED,
                "upstream": "https://github.com/Alibaba-NLP/Multi-CPR", "original_corpus_count": corpus_count,
                "corpus_count": len(corpus), "query_count": len(cases), "development": 200, "holdout": len(cases) - 200,
                "sampling": "All official dev positives plus hash-sampled distractors; NOT full-corpus leaderboard evaluation",
                "parameters": {"sparse_candidates": 20, "dense_candidates": 20, "rrf_k": 60, "rerank_candidates": 20},
                "download": json.loads((DATA / "download-manifest.json").read_text()),
                "documents": corpus, "cases": cases}
    RUN.mkdir(parents=True, exist_ok=True)
    write(RUN / "frozen.json", manifest)
    print(json.dumps({k: v for k, v in manifest.items() if k not in {"documents", "cases", "download"}}, ensure_ascii=False), flush=True)


class AuditTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.inner = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request):
        payload = json.loads(request.content)
        started = time.perf_counter()
        row = {"model": payload.get("model"), "path": request.url.path,
               "request_sha256": hashlib.sha256(request.content).hexdigest(), "started": time.time()}
        try:
            response = await self.inner.handle_async_request(request)
            await response.aread()
            row["status"] = response.status_code
            try:
                row["usage"] = response.json().get("usage")
            except ValueError:
                pass
            return response
        except Exception as exc:
            row["error_type"] = type(exc).__name__
            raise
        finally:
            row["duration_ms"] = (time.perf_counter() - started) * 1000
            with (RUN / "api-requests.jsonl").open("a", encoding="utf-8") as out:
                out.write(json.dumps(row) + "\n")

    async def aclose(self):
        await self.inner.aclose()


async def embed(settings, documents, name):
    import numpy as np
    path = RUN / (name + ".npy")
    fingerprint = hashed(json.dumps({"texts": [d["text"] for d in documents], "model": settings.embedding_model,
                                     "dimensions": settings.embedding_dimensions}, ensure_ascii=False))
    metadata = path.with_suffix(".json")
    if path.exists() and metadata.exists() and json.loads(metadata.read_text())["fingerprint"] == fingerprint:
        return np.load(path)
    batches = RUN / (name + "-batches")
    batches.mkdir(exist_ok=True)
    gate = asyncio.Semaphore(4)
    async def batch(index):
        texts = [row["text"] for row in documents[index:index + 100]]
        cache = batches / (str(index) + "-" + hashed(fingerprint + str(index))[:12] + ".json")
        if cache.exists():
            return json.loads(cache.read_text())["vectors"]
        async with gate, httpx.AsyncClient(timeout=60, trust_env=False, transport=AuditTransport()) as client:
            response = await client.post(settings.base_url.rstrip("/") + "/embeddings",
                headers={"Authorization": "Bearer " + settings.api_key},
                json={"model": settings.embedding_model, "input": texts, "dimensions": settings.embedding_dimensions})
            if response.status_code != 200:
                raise RuntimeError(f"Embedding HTTP {response.status_code}; sensitive provider text omitted")
            indexed = sorted(response.json()["data"], key=lambda row: row["index"])
            assert len(indexed) == len(texts)
            vectors = [row["embedding"] for row in indexed]
            assert all(len(vector) == settings.embedding_dimensions for vector in vectors)
            write(cache, {"vectors": vectors})
            print(f"embedded {name} {index + len(texts)}/{len(documents)}", flush=True)
            return vectors
    groups = await asyncio.gather(*(batch(i) for i in range(0, len(documents), 100)))
    matrix = np.asarray([vector for group in groups for vector in group], dtype=np.float32)
    matrix /= np.maximum(np.linalg.norm(matrix, axis=1, keepdims=True), 1e-12)
    np.save(path, matrix)
    write(metadata, {"fingerprint": fingerprint, "model": settings.embedding_model, "dimensions": settings.embedding_dimensions})
    return matrix


def score(ranking, relevant):
    relevant = set(relevant)
    ids = [row["id"] for row in ranking]
    first = next((i for i, pid in enumerate(ids[:10], 1) if pid in relevant), None)
    return {"recall_at_5": len(set(ids[:5]) & relevant) / len(relevant),
            "recall_at_10": len(set(ids[:10]) & relevant) / len(relevant),
            "mrr_at_10": 1 / first if first else 0,
            "ndcg_at_10": sum(1 / math.log2(i + 1) for i, pid in enumerate(ids[:10], 1) if pid in relevant) /
                          sum(1 / math.log2(i + 1) for i in range(1, min(10, len(relevant)) + 1))}


async def evaluate():
    import numpy as np
    cfg = load_settings()
    manifest = json.loads((RUN / "frozen.json").read_text(encoding="utf-8"))
    docs = manifest["documents"]
    cases = [case for case in manifest["cases"] if case["split"] == "holdout"]
    # Persist configuration BEFORE scoring, exclude every secret and DSN.
    configuration = {"embedding_model": cfg.embedding_model, "dimensions": cfg.embedding_dimensions,
                     "rerank_model": cfg.rerank_model, "chat_model": cfg.model, "chat_requests": 0,
                     "frozen_sha256": hashlib.sha256((RUN / "frozen.json").read_bytes()).hexdigest(),
                     "dense_backend": "NumPy exact cosine, shared cached vectors; not pgvector ANN latency",
                     "query_embedding": "batched and cached, excluded from per-query retrieval timing",
                     "rerank_candidates": 20, "outbound_concurrency": 4}
    config_path = RUN / "configuration.json"
    if config_path.exists() and json.loads(config_path.read_text()) != configuration:
        raise RuntimeError("Frozen evaluation configuration changed; use a new run directory")
    write(config_path, configuration)
    matrix = await embed(cfg, docs, "corpus")
    vectors = await embed(cfg, [{"text": case["query"]} for case in cases], "queries")
    bm25 = BM25(docs)
    gate = asyncio.Semaphore(4)
    results_dir = RUN / "queries"
    results_dir.mkdir(exist_ok=True)

    async def one(case, vector):
        destination = results_dir / (case["id"] + ".json")
        if destination.exists():
            return json.loads(destination.read_text(encoding="utf-8"))
        async with gate:
            started = time.perf_counter()
            sparse = bm25.search(case["query"], 20)
            sparse_ms = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            similarity = matrix @ vector
            indices = np.argsort(-similarity, kind="stable")[:20]
            dense = [{**docs[int(i)], "distance": float(1 - similarity[i])} for i in indices]
            dense_ms = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            fused = rrf(sparse, dense, k=60, top_k=20)
            fused_ms = (time.perf_counter() - started) * 1000 + sparse_ms + dense_ms
            started = time.perf_counter()
            ranked, backend, warning = await Reranker(cfg, AuditTransport()).rank(case["query"], fused, 10)
            rerank_ms = (time.perf_counter() - started) * 1000 + fused_ms
            rows = {mode: {**score(hits, case["relevant"]), "ids": [r["id"] for r in hits], "duration_ms": elapsed}
                    for mode, hits, elapsed in (("bm25", sparse, sparse_ms), ("dense", dense, dense_ms),
                                               ("hybrid", fused, fused_ms), ("hybrid_rerank", ranked, rerank_ms))}
            result = {"id": case["id"], "query": case["query"], "relevant": case["relevant"],
                      "rerank_backend": backend, "warning": warning, "strategies": rows}
            write(destination, result)
            if len(list(results_dir.glob("*.json"))) % 50 == 0:
                print("scored", len(list(results_dir.glob("*.json"))), "/", len(cases), flush=True)
            return result
    probe = await one(cases[0], vectors[0])
    if probe["rerank_backend"] != "model_api":
        raise RuntimeError("Rerank probe failed; no bulk API run was started")
    results = [probe, *await asyncio.gather(*(one(case, vector) for case, vector in zip(cases[1:], vectors[1:])))]
    summary = {"queries": len(results), "corpus": len(docs), "configuration": configuration,
               "rerank_model_api_success": sum(r["rerank_backend"] == "model_api" for r in results), "strategies": {}}
    for mode in ("bm25", "dense", "hybrid", "hybrid_rerank"):
        values = [r["strategies"][mode] for r in results]
        summary["strategies"][mode] = {key: float(np.mean([v[key] for v in values]))
            for key in ("recall_at_5", "recall_at_10", "mrr_at_10", "ndcg_at_10")}
        summary["strategies"][mode].update({"p95_ms": float(np.percentile([v["duration_ms"] for v in values], 95)),
                                           "p99_ms": float(np.percentile([v["duration_ms"] for v in values], 99))})
    deltas = np.array([r["strategies"]["hybrid_rerank"]["recall_at_5"] - r["strategies"]["dense"]["recall_at_5"] for r in results])
    rng = np.random.default_rng(20261112)
    bootstrap = np.mean(rng.choice(deltas, size=(2000, len(deltas)), replace=True), axis=1)
    summary["recall_at_5_delta_pp"] = float(np.mean(deltas) * 100)
    summary["paired_bootstrap_95ci_pp"] = (np.percentile(bootstrap, [2.5, 97.5]) * 100).tolist()
    baseline = summary["strategies"]["dense"]["recall_at_5"]
    summary["recall_at_5_relative_improvement_percent"] = float(np.mean(deltas) / baseline * 100) if baseline else None
    write(RUN / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["freeze", "evaluate"])
    args = parser.parse_args()
    if args.mode == "freeze":
        freeze()
    else:
        # Keep a small CPU budget on the developer machine.
        os.environ.setdefault("OPENBLAS_NUM_THREADS", "2")
        asyncio.run(evaluate())

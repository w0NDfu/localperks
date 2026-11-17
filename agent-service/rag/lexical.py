import math
import re
from collections import Counter


def tokenize(text):
    tokens = re.findall(r'[a-z0-9]+', text.casefold())
    for run in re.findall(r'[\u4e00-\u9fff]+', text):
        tokens.extend(run[i:i + 2] for i in range(len(run) - 1))
    return tokens


class BM25:
    def __init__(self, documents):
        self.docs = documents
        self.counts = [Counter(tokenize(d['title'] + ' ' + d['text'])) for d in documents]
        self.lengths = [sum(c.values()) for c in self.counts]
        self.average = sum(self.lengths) / max(1, len(documents))
        self.df = Counter(token for counts in self.counts for token in counts)

    def search(self, query, top_k=20):
        terms, n, scored = set(tokenize(query)), len(self.docs), []
        for doc, counts, length in zip(self.docs, self.counts, self.lengths):
            score = 0
            for term in terms:
                freq = counts.get(term, 0)
                if not freq:
                    continue
                idf = math.log(1 + (n - self.df[term] + .5) / (self.df[term] + .5))
                score += idf * freq * 2.5 / (freq + 1.5 * (.25 + .75 * length / max(1, self.average)))
            if score > 0:
                scored.append({**doc, 'bm25_score': score})
        return sorted(scored, key=lambda row: (-row['bm25_score'], row['id']))[:top_k]


def rrf(*rankings, k=60, top_k=20):
    combined = {}
    for ranking in rankings:
        for rank, row in enumerate(ranking, 1):
            entry = combined.setdefault(row['id'], {**row, 'rrf_score': 0})
            entry['rrf_score'] += 1 / (k + rank)
    return sorted(combined.values(), key=lambda row: (-row['rrf_score'], row['id']))[:top_k]

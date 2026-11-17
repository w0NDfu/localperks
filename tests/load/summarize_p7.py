"""Recompute HTTP and post-transaction handler latency, plus pending-queue trends.

All inputs are retained local evidence; this does not issue load or call a model.
"""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import statistics


def percentiles(values):
    values = sorted(values)
    return {"count": len(values), **{name: values[max(0, math.ceil(len(values) * p) - 1)] if values else None
            for name, p in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}}


def summarize(path):
    result = json.loads((path / "perf-result.json").read_text(encoding="utf-8"))
    context = json.loads((path / "benchmark-context.json").read_text(encoding="utf-8"))
    jtl = next(path.glob("throughput-*.jtl"), None) or next(path.glob("arrival-*.jtl"))
    with jtl.open(encoding="utf-8-sig", newline="") as stream:
        samples = [r for r in csv.DictReader(stream) if r["label"].startswith("seckill.")]
    accepted = [r for r in samples if r["outcome"] == "accepted"]
    first = min(int(r["timeStamp"]) for r in samples)
    last = max(int(r["timeStamp"]) + int(r["elapsed"]) for r in samples)
    markers = {}
    for match in re.finditer(r"OPS_EVENT \{[^\n]*?at=(\d+), type=mq_handler_returned, orderId=(\d+), voucherId=(\d+)",
                             (path / "java.log").read_text(encoding="utf-8", errors="replace")):
        stamp, identifier = int(match[1]), match[2]
        markers[identifier] = min(stamp, markers.get(identifier, stamp))
    processing = [markers[r["orderId"]] - int(r["timeStamp"]) for r in accepted if r["orderId"] in markers]
    assert all(value >= 0 for value in processing)
    queue = [json.loads(line) for line in (path / "admission-queue.jsonl").read_text().splitlines()]
    in_load = [r for r in queue if first <= r["at"] <= last]
    x = [(r["at"] - first) / 1000 for r in in_load]
    y = [r["reserved_pending"] for r in in_load]
    mean_x, mean_y = statistics.mean(x), statistics.mean(y)
    denominator = sum((v - mean_x) ** 2 for v in x)
    slope = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y)) / denominator if denominator else None
    quarter = max(1, len(y) // 4)
    growth = statistics.median(y[-quarter:]) - statistics.median(y[:quarter])
    safe = result["pass"] and result["unknown_receipts"] == 0 and result["non_http_errors"] == 0
    steady = None
    if result.get("group") == "arrival":
        steady = (safe and result["http_429"] == 0 and result["offered_rate_met"] and
                  slope is not None and slope <= 1 and growth <= max(10, result["target_rps"] / 2))
    outboxes = {}
    queue_path = path / "outbox-queue.jsonl"
    if queue_path.exists():
        for raw in queue_path.read_text().splitlines():
            sample = json.loads(raw)
            if not first <= sample['at'] <= last:
                continue
            for topic, total, pending, sent, age in sample['topics']:
                outboxes.setdefault(topic, []).append({'at': sample['at'], 'pending': int(pending), 'oldest_seconds': int(age)})
        for topic, values in list(outboxes.items()):
            offsets = [(v['at'] - first) / 1000 for v in values]
            pending = [v['pending'] for v in values]
            mx, my = statistics.mean(offsets), statistics.mean(pending)
            den = sum((v - mx) ** 2 for v in offsets)
            delta = sum((a - mx) * (b - my) for a, b in zip(offsets, pending)) / den if den else 0
            q = max(1, len(pending) // 4)
            difference = statistics.median(pending[-q:]) - statistics.median(pending[:q])
            outboxes[topic] = {'samples': len(values), 'peak_pending': max(pending), 'oldest_pending_seconds': max(v['oldest_seconds'] for v in values),
                              'ols_growth_per_second': delta, 'quartile_median_growth': difference}
            if steady is not None:
                steady = steady and delta <= 1 and difference <= max(10, result['target_rps'] / 2)
    data = {"run": path.name, "context": context, "result": result,
            "http_ms_recomputed": percentiles([int(r["elapsed"]) for r in samples]),
            "accepted_http_ms": percentiles([int(r["elapsed"]) for r in accepted]),
            "request_to_consumer_return_ms": percentiles(processing),
            "completion_marker_coverage": {"matched": len(processing), "accepted": len(accepted)},
            "consumer_marker_scope": "Native Java wall-clock marker after transactional service returns and receipt handling; not client-poll latency or pure SQL commit timing",
            "pending_queue": {"samples_in_load": len(in_load), "peak_in_load": max(y), "last_observed": queue[-1],
                              "ols_growth_per_second": slope, "last_minus_first_quartile_median": growth},
            "correctness_pass": safe, "steady_window_pass": steady,
            "outbox_queues_during_load": outboxes,
            "storage": json.loads((path / "storage-settings.json").read_text()),
            "jtl_sha256": hashlib.sha256(jtl.read_bytes()).hexdigest(),
            "duration_limit": "Only this finite single-host run; no production capacity extrapolation"}
    (path / "p7-summary.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: data[k] for k in ("run", "http_ms_recomputed", "request_to_consumer_return_ms", "steady_window_pass", "pending_queue")}, ensure_ascii=False))
    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("run", type=Path)
    summarize(parser.parse_args().run.resolve())

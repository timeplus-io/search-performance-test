#!/usr/bin/env python3
"""Generate a deterministic, mixed English/Chinese application-log dataset as NDJSON.gz files.

Both loaders read the same files, so Timeplus and Elasticsearch index identical rows.

  python gen_logs.py --rows 100000000 --files 32 --out data/

Writes data/part-XXXX.ndjson.gz plus data/manifest.json (row count, time range,
planted needles such as known trace ids, for the query set).
"""
import argparse, gzip, json, os, random
from datetime import datetime, timedelta, timezone
from multiprocessing import Pool

SERVICES = ["auth-service", "api-gateway", "payment-service", "order-service", "trade-service",
            "quote-service", "risk-service", "account-service", "settlement-service", "market-data",
            "notification-service", "report-service", "clearing-service", "margin-service",
            "position-service", "user-service"]
LEVELS, LEVEL_W = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"], [0.30, 0.45, 0.15, 0.08, 0.02]
HOSTS = [f"prod-node-{i:03d}" for i in range(200)]
ERRORS_EN = ["Connection refused", "Connection timeout", "Read timeout exceeded", "Disk full",
             "Permission denied", "Service unavailable", "Database locked", "SSL handshake failed",
             "DNS resolution failed", "TLS certificate expired"]
ERRORS_ZH = ["交易失败", "委托被拒绝", "行情延迟", "资金不足", "风控拦截", "账户冻结", "清算异常", "连接超时"]
STOCKS = ["600030", "601688", "000001", "600519", "300750", "002594", "601318", "000858"]
# Rare needles with fixed per-row probabilities, so selectivity is known up front.
RARE = [("OutOfMemoryError", 2e-5), ("StackOverflowError", 1e-5), ("DeadlockDetected", 5e-6),
        ("NullPointerException", 1e-4)]

def hexs(r, n):
    return "".join(r.choice("0123456789abcdef") for _ in range(n))

def message(r, level, trace):
    t = r.random()
    if t < 0.25:
        return (f"GET /api/v1/orders/{r.randint(1, 10**7)} status={r.choice([200, 200, 200, 404, 500, 503])} "
                f"latency_ms={r.randint(1, 3000)} trace={trace}")
    if t < 0.40:
        return f"user {r.randint(1, 10**6)} login from 10.{r.randint(0,255)}.{r.randint(0,255)}.{r.randint(1,254)} succeeded"
    if t < 0.55:
        return f"order {hexs(r, 12)} for stock {r.choice(STOCKS)} qty={r.randint(1, 100)*100} accepted"
    if t < 0.65:
        return f"委托 {hexs(r, 10)} 股票 {r.choice(STOCKS)} 数量 {r.randint(1, 100)*100} 已成交"
    if t < 0.75:
        return f"cache hit ratio {r.random():.3f} on region {r.randint(1, 64)} after refresh"
    if level in ("ERROR", "FATAL") or t < 0.85:
        if r.random() < 0.5:
            return f"{r.choice(ERRORS_EN)} while calling upstream host {r.choice(HOSTS)}:{r.choice([5432, 6379, 9092])} trace={trace}"
        return f"{r.choice(ERRORS_ZH)}: 股票 {r.choice(STOCKS)} 订单 {hexs(r, 10)} trace={trace}"
    return f"heartbeat ok seq={r.randint(1, 10**9)} queue_depth={r.randint(0, 500)}"

def gen_file(args):
    idx, n, start_ms, span_ms, seed, out = args
    r = random.Random(seed * 1000003 + idx)
    path = os.path.join(out, f"part-{idx:04d}.ndjson.gz")
    sample_traces = []
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=3) as f:
        for i in range(n):
            ts = datetime.fromtimestamp((start_ms + r.randrange(span_ms)) / 1000, tz=timezone.utc)
            level = r.choices(LEVELS, LEVEL_W)[0]
            trace = hexs(r, 32)
            msg = message(r, level, trace)
            for tok, p in RARE:
                if r.random() < p:
                    msg += f" caused by {tok}"
            if len(sample_traces) < 5 and "trace=" in msg and r.random() < 0.01:
                sample_traces.append(trace)  # a trace id that is searchable in message
            f.write(json.dumps({"log_time": ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z",
                                "level": level, "service": r.choice(SERVICES), "host": r.choice(HOSTS),
                                "trace_id": trace, "message": msg}, ensure_ascii=False) + "\n")
    return sample_traces

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--files", type=int, default=16)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    end = datetime(2026, 11, 1, tzinfo=timezone.utc)
    start_ms = int((end - timedelta(days=a.days)).timestamp() * 1000)
    span_ms = a.days * 86_400_000
    per = [a.rows // a.files + (1 if i < a.rows % a.files else 0) for i in range(a.files)]
    with Pool(min(a.files, os.cpu_count() or 4)) as p:
        traces = p.map(gen_file, [(i, per[i], start_ms, span_ms, a.seed, a.out) for i in range(a.files)])
    manifest = {"rows": a.rows, "start": start_ms, "end": start_ms + span_ms,
                "sample_trace_ids": [t for ts in traces for t in ts][:20]}
    json.dump(manifest, open(os.path.join(a.out, "manifest.json"), "w"), indent=2)
    print(json.dumps(manifest, indent=2))

if __name__ == "__main__":
    main()

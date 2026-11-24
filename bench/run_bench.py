#!/usr/bin/env python3
"""Run the query workload against one engine, or compare two result files.

  python run_bench.py --target tp --data data/ --runs 10
  python run_bench.py --target es --data data/ --runs 10
  python run_bench.py --target tp --concurrency 16 --duration 60     # throughput mode
  python run_bench.py report        # merge results/*.json into results/report.md

Latency = client wall clock per query (Timeplus native TCP, Elasticsearch HTTP).
The first run is reported separately as "first"; p50/p95 come from the remaining runs.
Elasticsearch's request cache is disabled per request so repeated runs are not served from a result cache.
"""
import argparse, json, math, os, statistics, sys, threading, time
from datetime import datetime, timedelta, timezone

from queries import cases

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.getenv("RESULTS_DIR", "results")

def params(data):
    m = json.load(open(os.path.join(data, "manifest.json")))
    end = datetime.fromtimestamp(m["end"] / 1000, tz=timezone.utc)
    since, day, hour = end - timedelta(hours=6), end - timedelta(days=1), end - timedelta(hours=1)
    f = lambda d: d.strftime("%Y-%m-%d %H:%M:%S")
    iso = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"trace": m["sample_trace_ids"][0], "since": f(since), "since_iso": iso(since),
            "day_start": f(day), "day_iso": iso(day), "hour_start": f(hour), "hour_iso": iso(hour)}

class TP:
    def __init__(self, a):
        from proton_driver import client
        self.c = client.Client(host=a.tp_host, port=a.tp_port, user=a.tp_user, password=a.tp_password,
                               send_receive_timeout=600)
        self.settings = json.loads(os.getenv("TP_SETTINGS") or "{}")  # e.g. {"max_threads": 1}
    def run(self, case):
        rows = self.c.execute(case["tp"], settings=self.settings)
        if case["kind"] == "count":
            return rows[0][0]
        if case["kind"] == "groups":
            return {str(k): v for k, v in rows}
        return len(rows)

class ES:
    def __init__(self, a):
        from elasticsearch import Elasticsearch
        self.es = Elasticsearch(a.es_url, request_timeout=600)
    def run(self, case):
        body = dict(case["es"])
        if case["kind"] == "count":
            body.update(size=0, track_total_hits=True)
        r = self.es.search(index="app_logs", request_cache=False, **body)
        if case["kind"] == "count":
            return r["hits"]["total"]["value"]
        if case["kind"] == "groups":
            return {(b["key_as_string"] if isinstance(b["key"], list) else str(b["key"])): b["doc_count"]
                    for b in r["aggregations"]["g"]["buckets"]}
        return len(r["hits"]["hits"])

def pct(xs, q):
    """Nearest-rank percentile."""
    xs = sorted(xs)
    return xs[max(0, math.ceil(q * len(xs)) - 1)]

def latency_mode(eng, cs, runs):
    out = []
    for c in cs:
        times, result = [], None
        for i in range(runs):
            t0 = time.perf_counter()
            try:
                result = eng.run(c)
            except Exception as e:  # keep going; record the failure
                result = f"ERROR: {e}"
                break
            times.append((time.perf_counter() - t0) * 1000)
        warm = times[1:] or times
        rec = {"id": c["id"], "group": c["group"], "result": result,
               "first_ms": round(times[0], 1) if times else None,
               "p50_ms": round(statistics.median(warm), 1) if warm else None,
               "p95_ms": round(pct(warm, 0.95), 1) if warm else None}
        print(f"{rec['id']:28} first={rec['first_ms']} p50={rec['p50_ms']} p95={rec['p95_ms']} "
              f"result={str(result)[:60]}", flush=True)
        out.append(rec)
    return out

def throughput_mode(make_eng, cs, conc, duration):
    cs = [c for c in cs if c["group"] in ("token", "combo")]
    lat, errors, stop = [], [0], time.time() + duration
    lock = threading.Lock()
    def worker(i):
        eng = make_eng()
        k = i
        while time.time() < stop:
            c = cs[k % len(cs)]; k += 1
            t0 = time.perf_counter()
            try:
                eng.run(c)
                with lock: lat.append((time.perf_counter() - t0) * 1000)
            except Exception:
                with lock: errors[0] += 1
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(conc)]
    [t.start() for t in ts]; [t.join() for t in ts]
    lat.sort()
    res = {"concurrency": conc, "duration_s": duration, "queries": len(lat), "errors": errors[0],
           "qps": round(len(lat) / duration, 1),
           "p50_ms": round(lat[len(lat) // 2], 1) if lat else None,
           "p99_ms": round(pct(lat, 0.99), 1) if lat else None}
    print(json.dumps(res, indent=2))
    return res

def _load(name):
    p = os.path.join(RESULTS, name)
    return json.load(open(p)) if os.path.exists(p) else None

def _mb(b):
    return f"{b / 2**20:,.0f} MB" if b else "n/a"

def report():
    """Merge results/*.json for tp and es into results/report.md."""
    lt, le = _load("load_tp.json") or {}, _load("load_es.json") or {}
    bt, be = _load("bench_tp.json"), _load("bench_es.json")
    out = ["# Timeplus text index vs Elasticsearch: results", "",
           "## Ingest and storage", "",
           "| Metric | Timeplus | Elasticsearch |", "| --- | --- | --- |",
           f"| Rows loaded | {lt.get('visible_rows', 'n/a')} | {le.get('visible_rows', 'n/a')} |",
           f"| Ingest rows/s | {lt.get('rows_per_s', 'n/a')} | {le.get('rows_per_s', 'n/a')} |",
           f"| Ingest time (s) | {lt.get('ingest_s', 'n/a')} | {le.get('ingest_s', 'n/a')} |",
           f"| Wait until all rows queryable (s) | {lt.get('commit_wait_s', 'n/a')} | {le.get('commit_wait_s', 'n/a')} |",
           f"| Bytes on disk | {_mb(lt.get('bytes_on_disk'))} | {_mb(le.get('bytes_on_disk'))} |",
           f"| Parts / segments | {lt.get('parts', 'n/a')} | {le.get('segments', 'n/a')} |", ""]
    if bt and be:
        ra, rb = {r["id"]: r for r in bt["latency"]}, {r["id"]: r for r in be["latency"]}
        out += ["## Query latency (ms, client wall clock)", "",
                "| Case | TP p50 | ES p50 | TP p95 | ES p95 | TP first | ES first | Faster | Results match |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
        for k, x in ra.items():
            y = rb.get(k, {})
            px, py = x.get("p50_ms"), y.get("p50_ms")
            faster = "" if not (px and py) else (f"TP {py / px:.1f}x" if px < py else f"ES {px / py:.1f}x")
            same = "yes" if x.get("result") == y.get("result") else "NO"
            out.append(f"| {k} | {px} | {py} | {x.get('p95_ms')} | {y.get('p95_ms')} | "
                       f"{x.get('first_ms')} | {y.get('first_ms')} | {faster} | {same} |")
        out.append("")
    tps = sorted(f for f in os.listdir(RESULTS) if f.startswith("throughput_")) if os.path.isdir(RESULTS) else []
    if tps:
        out += ["## Throughput (token and combo queries, mixed)", "",
                "| Run | Concurrency | QPS | p50 ms | p99 ms | Errors |", "| --- | --- | --- | --- | --- | --- |"]
        for f in tps:
            t = _load(f)
            out.append(f"| {f[11:-5]} | {t['concurrency']} | {t['qps']} | {t['p50_ms']} | {t['p99_ms']} | {t['errors']} |")
    md = "\n".join(out) + "\n"
    open(os.path.join(RESULTS, "report.md"), "w").write(md)
    print(md)

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        return report()
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=["tp", "es"], required=True)
    ap.add_argument("--data", default=os.getenv("DATA_DIR", "data"))
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--only", help="comma-separated case ids")
    ap.add_argument("--concurrency", type=int, default=0)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--tp-host", default=os.getenv("TP_HOST", "localhost")); ap.add_argument("--tp-port", type=int, default=8463)
    ap.add_argument("--tp-user", default=os.getenv("TP_USER", "proton"))
    ap.add_argument("--tp-password", default=os.getenv("TP_PASSWORD", "timeplus@t+"))
    ap.add_argument("--es-url", default=os.getenv("ES_URL", "http://localhost:9200"))
    a = ap.parse_args()
    cs = cases(params(a.data))
    if a.only:
        keep = set(a.only.split(","))
        cs = [c for c in cs if c["id"] in keep]
    make = (lambda: TP(a)) if a.target == "tp" else (lambda: ES(a))
    os.makedirs(os.path.join(RESULTS), exist_ok=True)
    if a.concurrency:
        res = throughput_mode(make, cs, a.concurrency, a.duration)
        path = os.path.join(RESULTS, f"throughput_{a.target}_c{a.concurrency}.json")
    else:
        res = {"target": a.target, "runs": a.runs, "latency": latency_mode(make(), cs, a.runs)}
        path = os.path.join(RESULTS, f"bench_{a.target}.json")
    json.dump(res, open(path, "w"), indent=2, ensure_ascii=False)
    print("saved", path)

if __name__ == "__main__":
    main()

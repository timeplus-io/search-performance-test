#!/usr/bin/env python3
"""Create the schema and load the generated NDJSON files into Timeplus or Elasticsearch.

  python load.py --target tp --data data/          # Timeplus (native TCP 8463)
  python load.py --target es --data data/          # Elasticsearch (HTTP 9200)

Prints and saves results/load_<target>.json: rows, seconds, rows/s, on-disk bytes.
"""
import argparse, glob, gzip, json, os, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.getenv("RESULTS_DIR", "results")
COLS = ["log_time", "level", "service", "host", "trace_id", "message"]

def read_rows(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

# ---------------------------------------------------------------- Timeplus
def tp_client(a):
    from proton_driver import client
    return client.Client(host=a.tp_host, port=a.tp_port, user=a.tp_user, password=a.tp_password,
                         send_receive_timeout=3600)

def wait_ready(check, what, timeout=600):
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        try:
            check(); print(f"{what} is ready"); return
        except Exception as e:
            last = e; time.sleep(5)
    raise SystemExit(f"{what} not ready after {timeout}s: {last}")

def tp_setup(a):
    wait_ready(lambda: tp_client(a).execute("SELECT 1"), "Timeplus")
    c = tp_client(a)
    ddl = open(os.path.join(HERE, "tp_schema.sql")).read()
    for attempt in range(24):  # right after start-up the server can accept queries before it can create streams
        try:
            c.execute("DROP STREAM IF EXISTS app_logs")
            c.execute(ddl, settings={"enable_full_text_index": 1})
            break
        except Exception as e:
            if attempt == 23:
                raise
            print(f"CREATE STREAM not possible yet ({str(e).splitlines()[0][:80]}), retrying in 5s")
            time.sleep(5)
            c = tp_client(a)
    for _ in range(60):  # stream creation is asynchronous
        if c.execute("SELECT count() FROM system.tables WHERE database = current_database() AND name = 'app_logs'")[0][0]:
            return
        time.sleep(1)
    raise SystemExit("app_logs was not created")

def tp_load_file(a, path):
    c = tp_client(a)
    batch, n = [], 0
    def flush():
        c.execute(f"INSERT INTO app_logs ({', '.join(COLS)}) VALUES", batch)
    for d in read_rows(path):
        d["log_time"] = datetime.fromisoformat(d["log_time"].replace("Z", "+00:00"))
        batch.append([d[k] for k in COLS])
        if len(batch) >= a.batch:
            flush(); n += len(batch); batch = []
    if batch:
        flush(); n += len(batch)
    return n

def tp_wait_and_size(a, expected):
    c = tp_client(a)
    t0 = time.time()
    while True:  # wait until every row is committed to the historical store
        got = c.execute("SELECT count() FROM table(app_logs)")[0][0]
        if got >= expected or time.time() - t0 > 1800:
            break
        time.sleep(2)
    parts = c.execute("""SELECT count(), sum(rows), sum(bytes_on_disk), sum(data_compressed_bytes),
                                sum(data_uncompressed_bytes), sum(secondary_indices_compressed_bytes)
                         FROM system.parts WHERE active AND database = current_database() AND table = 'app_logs'""")[0]
    cols = c.execute("""SELECT column, sum(column_data_compressed_bytes) FROM system.parts_columns
                        WHERE active AND database = current_database() AND table = 'app_logs' GROUP BY column""")
    return {"visible_rows": got, "commit_wait_s": round(time.time() - t0, 1), "parts": parts[0],
            "bytes_on_disk": parts[2], "column_bytes_compressed": parts[3], "column_bytes_uncompressed": parts[4],
            "index_bytes": parts[5], "bytes_per_column": dict(cols)}

# ---------------------------------------------------------------- Elasticsearch
def es_client(a):
    from elasticsearch import Elasticsearch
    return Elasticsearch(a.es_url, request_timeout=600)

def es_setup(a):
    es = es_client(a)
    wait_ready(lambda: es.cluster.health(wait_for_status="yellow", timeout="5s"), "Elasticsearch")
    es.indices.delete(index="app_logs", ignore_unavailable=True)
    body = json.load(open(os.path.join(HERE, "es_index.json")))
    body["settings"]["number_of_shards"] = a.es_shards
    es.indices.create(index="app_logs", **body)

def es_load_file(a, path):
    from elasticsearch import helpers
    es = es_client(a)
    ok = 0
    for success, _ in helpers.streaming_bulk(es, ({"_index": "app_logs", "_source": d} for d in read_rows(path)),
                                             chunk_size=a.batch // 10, raise_on_error=True):
        ok += success
    return ok

def es_wait_and_size(a, expected):
    es = es_client(a)
    t0 = time.time()
    es.indices.put_settings(index="app_logs", settings={"refresh_interval": "1s"})
    es.indices.refresh(index="app_logs")
    if a.es_forcemerge:
        es.indices.forcemerge(index="app_logs", max_num_segments=1, request_timeout=7200)
    got = es.count(index="app_logs")["count"]
    st = es.indices.stats(index="app_logs", metric="store,segments")["_all"]["primaries"]
    return {"visible_rows": got, "commit_wait_s": round(time.time() - t0, 1),
            "segments": st["segments"]["count"], "bytes_on_disk": st["store"]["size_in_bytes"]}

# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=["tp", "es"], required=True)
    ap.add_argument("--data", default=os.getenv("DATA_DIR", "data"))
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--batch", type=int, default=100_000)
    ap.add_argument("--tp-host", default=os.getenv("TP_HOST", "localhost")); ap.add_argument("--tp-port", type=int, default=8463)
    ap.add_argument("--tp-user", default=os.getenv("TP_USER", "proton"))
    ap.add_argument("--tp-password", default=os.getenv("TP_PASSWORD", "timeplus@t+"))
    ap.add_argument("--es-url", default=os.getenv("ES_URL", "http://localhost:9200"))
    ap.add_argument("--es-shards", type=int, default=1)
    ap.add_argument("--es-forcemerge", action="store_true", help="force-merge to 1 segment after load")
    a = ap.parse_args()

    files = sorted(glob.glob(os.path.join(a.data, "part-*.ndjson.gz")))
    expected = json.load(open(os.path.join(a.data, "manifest.json")))["rows"]
    setup, load_file, finish = (tp_setup, tp_load_file, tp_wait_and_size) if a.target == "tp" \
        else (es_setup, es_load_file, es_wait_and_size)

    setup(a)
    t0 = time.time()
    with ThreadPoolExecutor(a.workers) as ex:
        loaded = sum(ex.map(lambda p: load_file(a, p), files))
    ingest_s = time.time() - t0
    res = {"target": a.target, "rows_sent": loaded, "ingest_s": round(ingest_s, 1),
           "rows_per_s": round(loaded / ingest_s), **finish(a, expected)}
    os.makedirs(os.path.join(RESULTS), exist_ok=True)
    json.dump(res, open(os.path.join(RESULTS, f"load_{a.target}.json"), "w"), indent=2)
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    main()

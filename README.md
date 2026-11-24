# Log search performance test: Timeplus text index vs Elasticsearch

A single-host, Docker-based benchmark that loads the **same log dataset** into
[Timeplus Enterprise](https://docs.timeplus.com) (stream with an inverted `text` index)
and Elasticsearch, then compares ingest speed, storage size, query latency, concurrent
throughput, and whether both engines return the same answers.

```bash
git clone https://github.com/timeplus-io/search-performance-test.git
cd search-performance-test
./run.sh                      # 10M rows, both engines, report in results/report.md
```

## What it does

1. Builds a small Python client image (`bench/`).
2. Generates a deterministic NDJSON log dataset into `./data` (English and Chinese messages,
   trace ids, rare exception names with known frequencies).
3. For each engine, **one at a time and with the same CPU/memory limits**:
   start the container, create the schema, bulk-load, wait until every row is queryable,
   run 13 queries 10 times each, run a 60-second concurrency test, stop and delete the container.
4. Writes `results/report.md`.

## Requirements

- Docker 24+ with Compose v2 (`docker compose`), Linux or macOS.
- Access to Docker Hub (`timeplus/timeplus-enterprise`, `elasticsearch` official image).
- Free resources for one engine at a time: 8 vCPU and 16 GB RAM by default (`BENCH_CPUS`, `BENCH_MEM`).
- Disk: keep at least 30 GB free for 10M rows and 100 GB for 100M rows. Timeplus pre-allocates its
  streaming log, and both engines refuse writes when the disk is nearly full.

## Configuration

All settings are environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `ROWS` | `10000000` | Rows to generate and load (use `100000000` for the full test) |
| `FILES` | `16` | Data files; also the number of parallel loaders |
| `TARGETS` | `tp es` | Engines to run, in order |
| `RUNS` | `10` | Timed runs per query (first run reported separately) |
| `CONCURRENCY` / `DURATION` | `16` / `60` | Throughput test clients and seconds |
| `BENCH_CPUS` / `BENCH_MEM` | `8` / `16g` | Container limits, identical for both engines |
| `TP_VERSION` | `3.4.1` | Timeplus Enterprise image tag (use the newest 3.4.x / 3.5 available) |
| `ES_VERSION` / `ES_HEAP` | `8.19.3` / `8g` | Elasticsearch image tag and JVM heap |
| `TP_USER` / `TP_PASSWORD` | `proton` / `timeplus@t+` | Timeplus account used by the client |
| `TP_SETTINGS` | empty | JSON of per-query Timeplus settings, e.g. `{"max_threads": 1}` (see Tuning) |
| `KEEP_VOLUMES` | `0` | `1` keeps engine data volumes after each run |
| `SKIP_BUILD` | `0` | `1` reuses an already built bench client image |

Example: `ROWS=100000000 BENCH_CPUS=16 BENCH_MEM=32g ES_HEAP=16g ./run.sh`

## Schemas

**Timeplus** ([`bench/tp_schema.sql`](bench/tp_schema.sql)): an append stream partitioned by day,
ordered by `(service, level, log_time)`, with
`INDEX idx_msg message TYPE text(tokenizer = 'split_by_non_alpha')` and a bloom filter on `trace_id`.
`split_by_non_alpha` splits on ASCII spaces and punctuation, so a run of Chinese characters between
separators (for example `交易失败` in `交易失败: 股票 600519`) is a single token. It is currently the
only tokenizer for which `has_token`, `has_all_tokens`, `has_any_tokens` and `LIKE` use the index at
query time; with `ascii_cjk` or `ngrams` the index is built but queries fall back to a full scan.

**Elasticsearch** ([`bench/es_index.json`](bench/es_index.json)): `message` as `text` with a `pattern`
tokenizer that splits on the same ASCII non-alphanumeric characters and **no lowercase filter**, so
both engines see identical, case-sensitive tokens and hit counts can be compared exactly. A
`wildcard` sub-field serves substring search. 1 shard, 0 replicas, `best_compression`.

## Queries

| Case | Timeplus | Elasticsearch |
| --- | --- | --- |
| q01 rare token | `has_token(message, 'DeadlockDetected')` | `term` |
| q02 common token | `has_token(message, 'timeout')` | `term` |
| q03 two tokens AND | `has_all_tokens(message, ['Connection', 'refused'])` | `bool` filter |
| q04 two tokens OR | `has_any_tokens(...)` | `terms` |
| q05 trace id lookup | `has_token(message, '<trace id>')` | `term` |
| q06 token with no hits | `has_token(...)` | `term` |
| q07 token + level + last 6 h | `has_token` + `level =` + time range | `bool` + `range` |
| q08 newest 100 matches | `ORDER BY log_time DESC LIMIT 100` | `sort` + `size` |
| q09 hits per service | `GROUP BY service` | `terms` aggregation |
| q10 hourly histogram, last day | `GROUP BY to_start_of_hour(log_time)` | `date_histogram` |
| q11 Chinese word | `has_token(message, '交易失败')` | `term` |
| q12 Chinese word + stock code | `has_all_tokens(message, ['风控拦截', '600519'])` | 2 x `term` |
| q13 Chinese substring | `message LIKE '%失败%'` | `wildcard` on `message.wc` |
| q14 English substring | `message LIKE '%imeout%'` | `wildcard` on `message.wc` |
| q15 errors per host and service | `GROUP BY concat(host, service)` over ERROR/FATAL | `multi_terms` aggregation |
| q16 per-minute volume, whole range | `GROUP BY to_start_of_minute(log_time)` | `date_histogram` 1m |
| q17 incident drill-down | one service, ERROR, last hour, `has_token`, newest 50 | `bool` + `sort` |
| q18 one service, keyword, 5-minute buckets, last day | `GROUP BY to_start_of_interval(log_time, 5m)` | `date_histogram` 5m |
| q19 errors per host, last day | `GROUP BY host` | `terms` aggregation |
| q20 level per service, all data | `GROUP BY concat(service, level)` | `multi_terms` aggregation |
| q21 services by error keywords, last day | `has_any_tokens` + `GROUP BY service` | `terms` + `terms` agg |

q15 to q21 are the incident and dashboard queries people run on logs (similar to the published
ClickHouse vs Elasticsearch log benchmark); q15, q16, q19 and q20 use no text index.
All queries are in [`bench/queries.py`](bench/queries.py); add your own pairs there.

## Reading the report

- **Latency** is client wall-clock time (Timeplus over native TCP, ES over HTTP), p50/p95 over
  runs 2..N, plus the first run.
- **Results match = NO** means the engines returned different counts. With the tokenizer parity
  above all cases match on the generated data; a mismatch is a finding to investigate.
- ES's request cache is disabled for every query. Timeplus has no result cache. The OS page cache is
  not dropped, so warm runs measure an in-memory working set; for cold numbers run with `RUNS=1`.

## Using your own logs

Put gzipped NDJSON files named `data/part-*.ndjson.gz` with the fields `log_time` (ISO-8601 UTC),
`level`, `service`, `host`, `trace_id`, `message`, plus a `data/manifest.json` like
`{"rows": N, "start": <epoch ms>, "end": <epoch ms>, "sample_trace_ids": ["..."]}`, then adapt the
search terms in `bench/queries.py`.

## Running pieces by hand

```bash
docker compose --profile bench build bench
docker compose --profile tp up -d
docker compose --profile bench run --rm bench load.py --target tp
docker compose --profile bench run --rm bench run_bench.py --target tp --runs 10 --only q01_rare_token,q11_chinese_word
docker compose --profile tp down -v
docker compose --profile bench run --rm bench run_bench.py report
```

The Timeplus web console is at http://localhost:8000 while the Timeplus container runs.

## Validation status

`./run.sh` was run end to end with `ROWS=1000000` and `ROWS=10000000` on a 4 vCPU cloud sandbox
(Timeplus Enterprise 3.4.1, Elasticsearch 8.19.3, 4 CPUs / 8 GB per engine): every stage completed
and all cases returned identical results on both engines. Use dedicated hardware for conclusions.

What to expect (10M rows, warm, p50):

| Workload | Timeplus | Elasticsearch |
| --- | --- | --- |
| Ingest | ~95k rows/s | ~38k rows/s |
| Disk (data + index) | ~1.0 GB | ~2.8 GB |
| Single token count (q01-q06) | 4-9 ms | 1-4 ms |
| Newest 100 matches (q08) | ~110 ms (59 ms rewritten, see Tuning) | ~40 ms |
| Substring LIKE (q13, q14) | 130-370 ms | 15-36 ms |
| Errors per host and service (q15) | 23 ms | 305 ms |
| Per-minute volume (q16) | 57 ms | 126 ms |

At 50M rows (same limits) Timeplus is 15x to 24x faster on the whole-data aggregations (q15, q20),
1.2x to 2.8x faster on scoped incident queries (q17 to q19), and uses 3.1x less disk; Elasticsearch
stays 2.5x to 7x faster on bare keyword counts and much faster on newest-N and substring search.
With cold caches (engine restarted, OS cache dropped before each query, 10M rows) Timeplus is
faster on 20 of 21 queries.

Elasticsearch leads on warm needle-in-haystack lookups, where Lucene answers counts straight from
posting lists. Timeplus leads on ingest, storage and anything that aggregates many rows.

## Tuning Timeplus

- **Many concurrent searches:** `TP_SETTINGS='{"max_threads": 1}'`. Index lookups are tiny, so
  one thread per query halves CPU per query and improves p50 under load (28 ms to 12 ms at 8 clients).
- **Short or high-cardinality substrings** (`LIKE '%imeout%'`): the dictionary scan can cost more
  than a plain scan when the index holds many unique ids. Compare with
  `{"use_text_index_like_evaluation_by_dictionary_scan": 0}` (q14: 400 ms to 130 ms here).
  Prefer `has_token` whenever users search whole words.
- **Newest N matches** (q08): Timeplus reads the message column for every match before the
  LIMIT. Finding the cut-off time first reads far less:
  ```sql
  SELECT ... FROM table(app_logs)
  WHERE has_token(message, 'timeout')
    AND log_time >= (SELECT min(log_time) FROM (SELECT log_time FROM table(app_logs)
                     WHERE has_token(message, 'timeout') ORDER BY log_time DESC LIMIT 100))
  ORDER BY log_time DESC LIMIT 100
  ```
- **Fewer parts:** each query opens the index of every part (about 1.5 ms for ~60 parts at 10M
  rows). Larger insert batches and daily partitions keep the part count low.

## Known limitations

- The Timeplus text index is **experimental** in Enterprise 3.4. Matching is case-sensitive and there
  is no relevance scoring.
- Chinese text is only searchable through the index when words are separated by ASCII spaces or
  punctuation; free-running Chinese text needs `LIKE`, which scans.
- Starting Timeplus Enterprise begins its 30-day trial.
- This is a single-node test. Cluster behaviour (replication, sharding) is out of scope.

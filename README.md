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

All queries are in [`bench/queries.py`](bench/queries.py); add your own pairs there.

## Reading the report

- **Latency** is client wall-clock time (Timeplus over native TCP, ES over HTTP), p50/p95 over
  runs 2..N, plus the first run.
- **Results match = NO** means the engines returned different counts. With the tokenizer parity
  above all 14 cases match on the generated data; a mismatch is a finding to investigate.
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

`./run.sh` was run end to end with `ROWS=1000000` on a 4 vCPU / 16 GB cloud sandbox
(Timeplus Enterprise 3.4.1, Elasticsearch 8.19.3): every stage completed and all 14 cases returned
identical results on both engines. Numbers from such a small run are dominated by fixed per-query
overhead and are not representative; use 10M+ rows on dedicated hardware for conclusions.

## Known limitations

- The Timeplus text index is **experimental** in Enterprise 3.4. Matching is case-sensitive and there
  is no relevance scoring.
- Chinese text is only searchable through the index when words are separated by ASCII spaces or
  punctuation; free-running Chinese text needs `LIKE`, which scans.
- Starting Timeplus Enterprise begins its 30-day trial.
- This is a single-node test. Cluster behaviour (replication, sharding) is out of scope.

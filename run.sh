#!/usr/bin/env bash
# One-command benchmark: generate data, then for each engine start it, load, query, stop.
#
#   ./run.sh                    # 10M rows (about 30-60 min end to end)
#   ROWS=100000000 ./run.sh     # 100M rows (several hours)
#   TARGETS=tp ./run.sh         # one engine only
#
# Results: results/report.md
set -euo pipefail
cd "$(dirname "$0")"

ROWS=${ROWS:-10000000}
FILES=${FILES:-16}
RUNS=${RUNS:-10}
CONCURRENCY=${CONCURRENCY:-16}
DURATION=${DURATION:-60}
TARGETS=${TARGETS:-"tp es"}
KEEP_VOLUMES=${KEEP_VOLUMES:-0}

dc() { docker compose "$@"; }
bench() { dc --profile bench run --rm bench "$@"; }

mkdir -p data results
if [[ "${SKIP_BUILD:-0}" != 1 ]]; then
  echo "==> building bench client"
  dc --profile bench build bench
fi

if ! grep -q "\"rows\": $ROWS," data/manifest.json 2>/dev/null; then
  echo "==> generating $ROWS rows into ./data"
  rm -f data/part-*.ndjson.gz data/manifest.json
  bench gen_logs.py --rows "$ROWS" --files "$FILES" --out /data
fi

run_target() {
  local t=$1 svc=$2
  echo "==> [$t] starting $svc"
  dc --profile "$t" up -d
  echo "==> [$t] loading"
  bench load.py --target "$t"
  echo "==> [$t] latency ($RUNS runs per query)"
  bench run_bench.py --target "$t" --runs "$RUNS"
  echo "==> [$t] throughput ($CONCURRENCY clients, ${DURATION}s)"
  bench run_bench.py --target "$t" --concurrency "$CONCURRENCY" --duration "$DURATION"
  echo "==> [$t] stopping $svc"
  if [[ "$KEEP_VOLUMES" == 1 ]]; then dc --profile "$t" down; else dc --profile "$t" down -v; fi
}

for t in $TARGETS; do
  case $t in
    tp) run_target tp timeplus ;;
    es) run_target es elasticsearch ;;
    *) echo "unknown target $t"; exit 1 ;;
  esac
done

bench run_bench.py report
echo "==> done: results/report.md"

#!/bin/bash
# Benchmark for the my_cache filter. Runs the same scenarios every time so
# "before" and "after" numbers are comparable.
#
# Usage:
#   ./bench.sh <label> [--perf]
#   e.g.  ./bench.sh before-C --perf
#         ONLY_SLOW=1 ./bench.sh before-C   (only the slow-client memory scenario)
#         ./bench.sh after-C  --perf
#
# Needs: ~/bin/envoy-opt, ~/envoy.yaml, origin.py running on :8000 serving ~/origin
#        (small.bin 1K, mid.bin 1M, large.bin 30M), wrk, h2load, curl.
# --perf also records CPU flame graphs (needs perf + ~/FlameGraph, asks for sudo).
#
# Results go to ~/bench-results/<label>-<time>/ (summary.txt + raw outputs + flame graphs).

LABEL="${1:?usage: ./bench.sh <label> [--perf]}"
PERF=0; [[ "${2:-}" == "--perf" ]] && PERF=1

ENVOY="${ENVOY:-$HOME/bin/envoy-opt}"
CONFIG="${CONFIG:-$HOME/envoy_bench.yaml}"
WORKERS="${WORKERS:-4}"
DURATION="${DURATION:-20s}"
SLOW_CLIENTS="${SLOW_CLIENTS:-50}"
SLOW_RATE="${SLOW_RATE:-1M}"          # per slow client (curl --limit-rate)
FAST_CLIENTS="${FAST_CLIENTS:-5}"
URL="http://127.0.0.1:10000"

OUT="$HOME/bench-results/${LABEL}-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
SUMMARY="$OUT/summary.txt"

die() { echo "ERROR: $*" >&2; stop_envoy; exit 1; }
log() { echo "$*" | tee -a "$SUMMARY"; }

# ---------------------------------------------------------------------------
start_envoy() {
  pkill -x envoy-opt 2>/dev/null; pkill -x envoy 2>/dev/null; sleep 0.5
  "$ENVOY" -c "$CONFIG" --concurrency "$WORKERS" -l warn > "$OUT/envoy.log" 2>&1 &
  ENVOY_PID=$!
  for _ in $(seq 50); do
    curl -s -o /dev/null "$URL/small.bin" && return 0
    sleep 0.2
  done
  die "Envoy did not come up (see $OUT/envoy.log)"
}

stop_envoy() { [[ -n "${ENVOY_PID:-}" ]] && kill "$ENVOY_PID" 2>/dev/null; wait "$ENVOY_PID" 2>/dev/null; }

rss_kb()  { awk '/VmRSS/ {print $2}' "/proc/$ENVOY_PID/status"; }
peak_kb() { awk '/VmHWM/ {print $2}' "/proc/$ENVOY_PID/status"; }

check_alive() { kill -0 "$ENVOY_PID" 2>/dev/null || die "Envoy died during $1 (see $OUT/envoy.log)"; }

# Records a flame graph while "$@" runs (only with --perf).
with_perf() {
  local name="$1"; shift
  if [[ $PERF -eq 1 ]]; then
    sudo perf record -F 99 -g -p "$ENVOY_PID" -o "$OUT/$name.perf.data" -- sleep 15 \
      > /dev/null 2>&1 &
    local perf_pid=$!
    "$@"
    wait "$perf_pid"
    sudo perf script -i "$OUT/$name.perf.data" 2>/dev/null \
      | ~/FlameGraph/stackcollapse-perf.pl \
      | ~/FlameGraph/flamegraph.pl --title "$LABEL: $name" > "$OUT/$name-flame.svg"
    sudo rm -f "$OUT/$name.perf.data"
  else
    "$@"
  fi
}

wrk_summary() {  # file -> "req/s | transfer/s | p50 | p99"
  local f="$1"
  local rps=$(awk '/Requests\/sec/ {print $2}' "$f")
  local tps=$(awk '/Transfer\/sec/ {print $2}' "$f")
  local p50=$(awk '$1=="50%" {print $2}' "$f")
  local p99=$(awk '$1=="99%" {print $2}' "$f")
  local errs=$(grep -E "Non-2xx|Socket errors" "$f" | tr '\n' ' ')
  echo "req/s=$rps  transfer/s=$tps  p50=$p50  p99=$p99  ${errs}"
}

# ---------------------------------------------------------------------------
command -v wrk >/dev/null || die "wrk not installed"
[[ -x "$ENVOY" ]] || die "$ENVOY not found"
curl -s -o /dev/null "http://127.0.0.1:8000/small.bin" || die "origin.py not running on :8000"
for f in small.bin mid.bin large.bin; do
  curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:8000/$f" | grep -q 200 \
    || die "origin does not serve $f (create it in ~/origin)"
done

if [[ $PERF -eq 1 ]]; then
  sudo -v || die "sudo needed for perf"   # ask for the password now, not from a background job
fi

log "=== $LABEL   $(date)   commit $(git -C ~/envoy rev-parse --short HEAD 2>/dev/null)"
log "workers=$WORKERS duration=$DURATION slow_clients=$SLOW_CLIENTS@$SLOW_RATE fast_clients=$FAST_CLIENTS"
log ""

# --- Scenario 1+2+3: hits ---------------------------------------------------
if [[ "${ONLY_SLOW:-0}" != 1 ]]; then
start_envoy
log "idle RSS: $(( $(rss_kb) / 1024 )) MiB"
for f in small.bin mid.bin large.bin; do curl -s -o /dev/null "$URL/$f"; done   # warm cache
curl -s -D - -o /dev/null "$URL/large.bin" | grep -qi "my-cache: HIT" \
  || log "WARNING: large.bin is not a HIT - check max-age and the object size limit"

log "[1] HTTP/1.1 hits, 1 KiB, 64 connections"
with_perf "hits-small" wrk -t4 -c64 -d"$DURATION" --latency "$URL/small.bin" > "$OUT/1-small.txt"
check_alive "scenario 1"; log "    $(wrk_summary "$OUT/1-small.txt")"

log "[2] HTTP/1.1 hits, 30 MiB, 16 connections"
with_perf "hits-large" wrk -t4 -c16 -d"$DURATION" --latency "$URL/large.bin" > "$OUT/2-large.txt"
check_alive "scenario 2"; log "    $(wrk_summary "$OUT/2-large.txt")"
log "    peak RSS so far: $(( $(peak_kb) / 1024 )) MiB"

if command -v h2load >/dev/null; then
  log "[3] HTTP/2 hits, 1 KiB, 64 connections x 10 streams"
  h2load -n 200000 -c 64 -m 10 -t 4 "$URL/small.bin" > "$OUT/3-h2-small.txt" 2>&1
  check_alive "scenario 3"
  log "    $(grep -E '^finished in' "$OUT/3-h2-small.txt")"
  log "    $(grep -E '^requests:' "$OUT/3-h2-small.txt")"
else
  log "[3] skipped (h2load not installed)"
fi
stop_envoy
fi

# --- Scenario 4: slow + fast clients on one coalesced miss (memory) -----------
start_envoy   # fresh process -> VmHWM measures only this scenario
base=$(rss_kb)
key="large.bin?run=$RANDOM$RANDOM"     # fresh cache key -> a coalesced miss
log ""
log "[4] $SLOW_CLIENTS slow clients (@$SLOW_RATE/s) + $FAST_CLIENTS fast on one 30 MiB miss"
expected=$(stat -c %s ~/origin/large.bin)
pids=()
for i in $(seq "$SLOW_CLIENTS"); do
  curl -s -o /dev/null -w "%{size_download}\n" --limit-rate "$SLOW_RATE" "$URL/$key" \
    >> "$OUT/4-sizes.txt" & pids+=($!)
done
for i in $(seq "$FAST_CLIENTS"); do
  curl -s -o /dev/null -w "%{size_download} %{time_total}\n" "$URL/$key" \
    >> "$OUT/4-fast.txt" & pids+=($!)
done
any_alive() { for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && return 0; done; return 1; }
max_rss=0
while any_alive; do
  r=$(rss_kb); (( r > max_rss )) && max_rss=$r
  sleep 0.5
  check_alive "scenario 4"
done
wait "${pids[@]}" 2>/dev/null   # only the curls - a plain 'wait' would also wait for Envoy
ok=$(awk -v e="$expected" '$1==e' "$OUT/4-sizes.txt" "$OUT/4-fast.txt" | wc -l)
total=$(( SLOW_CLIENTS + FAST_CLIENTS ))
log "    complete downloads: $ok / $total"
log "    fast clients' time: $(awk '{printf "%.1fs ", $2}' "$OUT/4-fast.txt")"
log "    RSS before: $(( base / 1024 )) MiB   max sampled: $(( max_rss / 1024 )) MiB   peak (VmHWM): $(( $(peak_kb) / 1024 )) MiB"
stop_envoy

log ""
log "Results: $OUT"
[[ $PERF -eq 1 ]] && log "Flame graphs: $OUT/*-flame.svg (open in a browser)"

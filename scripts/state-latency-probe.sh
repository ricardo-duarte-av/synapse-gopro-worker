#!/usr/bin/env bash
# state-latency-probe.sh -- measure /state latency against the process's own
# memory state, at a fixed hour.
#
# Written for one question: why the same room, at the same size, costs 2.4s at
# midday and 31s at 03:00, when Synapse costs 13s at both. See CLAUDE.md,
# "/state latency varies 13x on identical work, and it is ours".
#
# The design follows from what that investigation could and could not settle.
#
#   - **Both sides, every run.** Synapse being flat night and day is the
#     measurement that made this ours rather than the database's. A probe that
#     recorded only our timings could not have established that, and cannot
#     detect it changing.
#   - **The heap counters, side by side with the timings.** heap_released
#     collapsing at each request is the strongest correlate found, and it is
#     also the thing GOGC/GOMEMLIMIT would move. Recording it every run is what
#     makes a later tuned run comparable to this baseline.
#   - **GOGC and GOMEMLIMIT read from the process, not assumed.** A run is
#     labelled by what the worker actually had, so a tuned run cannot be
#     mistaken for a baseline one months later.
#   - **Cold and warm separately.** The first request after a quiet period is
#     the slow one; averaging it with the warm ones would hide exactly the
#     effect being measured.
#
# The room is discovered by size rather than named. Rooms get purged here, and
# a probe pinned to a room ID would one day measure nothing and say so quietly.
#
# Usage:  state-latency-probe.sh [label]
# Output: one JSON object per run appended to $RESULTS.

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

PROM="${PROM:-https://claude:claude1234claude@prometheus.aguiarvieira.pt/api/v1/query}"
KEY="${KEY:-aguiarvieira.pt.signing.key}"
OURS="${OURS:-/var/sockets/nginx/xx-gopro-worker-1.sock}"
SYNAPSE="${SYNAPSE:-/var/sockets/nginx/av-synapse-main.sock}"
RESULTS="${RESULTS:-$PWD/probe-results.jsonl}"
SWEEP="${SWEEP:-$PWD/.probe-statesweep}"

# Bracket the room the investigation used: large enough to take real time,
# small enough not to be a 97MB outlier.
MIN_STATE="${MIN_STATE:-12600}"
MAX_STATE="${MAX_STATE:-12800}"

LABEL="${1:-baseline}"

# Query one instant value out of Prometheus, or "null".
prom() {
  curl -sfG "$PROM" --data-urlencode "query=$1" 2>/dev/null | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)["data"]["result"]
    print(d[0]["value"][1] if d else "null")
except Exception:
    print("null")'
}

# statesweep prints "9.3s" or "87ms"; normalise to seconds.
probe_seconds() {
  local socket="$1"
  local line
  line=$("$SWEEP" -key "$KEY" -socket "$socket" \
    -min-state "$MIN_STATE" -max-state "$MAX_STATE" \
    -limit 1 -interval 1s -progress /dev/null 2>/dev/null | grep -E '^\[' | head -1)
  [ -z "$line" ] && { echo "null"; return; }
  python3 - "$line" <<'PY'
import re, sys
line = sys.argv[1]
m = re.search(r'\s([0-9.]+)(ms|s)\s+\(', line)
if not m:
    print("null")
else:
    v = float(m.group(1))
    print("%.3f" % (v / 1000 if m.group(2) == "ms" else v))
PY
}

# statesweep needs building once; kept out of the repo tree.
if [ ! -x "$SWEEP" ]; then
  GOEXPERIMENT=jsonv2 go build -o "$SWEEP" ./cmd/statesweep || exit 1
fi

BEFORE_RELEASED=$(prom 'go_memstats_heap_released_bytes{job="gopro-worker"}')
BEFORE_INUSE=$(prom 'go_memstats_heap_inuse_bytes{job="gopro-worker"}')
GOGC=$(prom 'go_gc_gogc_percent{job="gopro-worker"}')
GOMEMLIMIT=$(prom 'go_gc_gomemlimit_bytes{job="gopro-worker"}')
UPTIME=$(prom 'time() - process_start_time_seconds{job="gopro-worker"}')
REQ_RATE=$(prom 'sum(rate(gopro_requests_total[5m]))*60')
RSS=$(prom 'process_resident_memory_bytes{job="gopro-worker"}')

# Cold first: the request after a quiet period is the one under investigation.
COLD=$(probe_seconds "$OURS")
WARM1=$(probe_seconds "$OURS")
WARM2=$(probe_seconds "$OURS")

AFTER_RELEASED=$(prom 'go_memstats_heap_released_bytes{job="gopro-worker"}')
AFTER_INUSE=$(prom 'go_memstats_heap_inuse_bytes{job="gopro-worker"}')

# The control. Synapse being flat is what makes a change in our numbers
# meaningful; without it a slow night looks the same as a regression.
#
# Only the cold figure is used. Synapse's _state_resp_cache memoises the
# response for 30s, so a second reading measures its cache rather than its
# work -- 189ms against 13.4s, which would flatter us enormously if averaged in.
SYNAPSE_COLD=$(probe_seconds "$SYNAPSE")

python3 - <<PY >> "$RESULTS"
import json, datetime
def n(x):
    try:
        return float(x)
    except Exception:
        return None
print(json.dumps({
    "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    "label": "$LABEL",
    "ours_cold_s": n("$COLD"),
    "ours_warm1_s": n("$WARM1"),
    "ours_warm2_s": n("$WARM2"),
    "synapse_cold_s": n("$SYNAPSE_COLD"),
    "heap_released_before_mb": (n("$BEFORE_RELEASED") or 0) / 1048576,
    "heap_released_after_mb": (n("$AFTER_RELEASED") or 0) / 1048576,
    "heap_inuse_before_mb": (n("$BEFORE_INUSE") or 0) / 1048576,
    "heap_inuse_after_mb": (n("$AFTER_INUSE") or 0) / 1048576,
    "rss_mb": (n("$RSS") or 0) / 1048576,
    "gogc": n("$GOGC"),
    "gomemlimit": n("$GOMEMLIMIT"),
    "worker_uptime_s": n("$UPTIME"),
    "req_per_min": n("$REQ_RATE"),
}))
PY

tail -1 "$RESULTS"

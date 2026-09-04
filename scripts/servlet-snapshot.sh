#!/usr/bin/env bash
# servlet-snapshot.sh -- record one full day of per-servlet request counts.
#
# Written because a week of analysis was built on numbers that turned out to be
# three overlapping transitions, and Prometheus will not retain the window long
# enough to re-check it later. See CLAUDE.md, "Reading traffic: what went wrong
# on 2026-09-04".
#
# Three design choices, each from a specific mistake:
#
#   - **Split by `instance`.** This Prometheus scrapes two homeservers,
#     `aguiarvieira.pt` and `matrixbridg.es`, and the `job` labels are shared
#     between them. Summing without this label reported 1,483 /day of
#     /_matrix/client/versions as ours when 1,440 of it belonged to the other
#     server. One label, and it is the difference between a plan and a fiction.
#   - **Split by `job`.** Which worker answers an endpoint is a routing fact
#     that changes without warning -- sync moved to gosync mid-analysis -- and
#     a count with no worker attached cannot show that it moved.
#   - **A health marker on every row.** A scrape gap and a quiet night both
#     read as zero. `targets_up` and `total_requests` make them distinguishable
#     after the fact, which is the whole point of writing this down.
#
# The user-agent section exists because the giveaway was never the total. When
# /messages fell from 3,649/day to 130 it was one user agent going to zero, and
# no aggregate could have shown that.
#
# Usage:  servlet-snapshot.sh [YYYY-MM-DD]   (default: yesterday, UTC)
# Output: one JSON object per day appended to $RESULTS.

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

PROM="${PROM:-https://claude:claude1234claude@prometheus.aguiarvieira.pt/api/v1/query}"
RESULTS="${RESULTS:-$PWD/servlet-snapshots.jsonl}"
LOGDIR="${LOGDIR:-/opt/npm/data/logs}"

# Files that carry client reads. Deliberately a short list: the catch-all vhost
# log is 600MB+ and the sync logs are gigabytes, and neither answers the
# question this section exists for.
UA_LOGS="${UA_LOGS:-av-messages.log proxy-host-324_access.log av-gopro-worker.log}"

DAY="${1:-$(date -u -d 'yesterday' +%F)}"
# increase(...[24h]) evaluated at midnight covers the whole of the day before.
AT=$(date -u -d "$DAY 00:00:00 +1 day" +%s)
LOGDAY=$(date -u -d "$DAY" +'%d/%b/%Y')

prom() {
  curl -sfG "$PROM" --data-urlencode "query=$1" --data-urlencode "time=$AT" 2>/dev/null
}

export SERVLETS TARGETS TOTAL
SERVLETS=$(prom 'sum by (servlet,instance,job) (increase(synapse_http_server_response_count_total[24h]))')
TARGETS=$(prom 'sum(up)')
TOTAL=$(prom 'sum(increase(synapse_http_server_response_count_total[24h]))')

# Top user agents per log, for the day. Fixed-string grep on the date prefix so
# a 600MB file is one linear pass.
ua_section() {
  local first=1
  printf '{'
  for f in $UA_LOGS; do
    [ -r "$LOGDIR/$f" ] || continue
    local body
    body=$(timeout 120 grep -aF "[$LOGDAY:" "$LOGDIR/$f" 2>/dev/null \
      | sed -E 's/.*"([^"]*)"[[:space:]]*$/\1/; s/.*" "([^"]*)".*/\1/' \
      | cut -c1-40 | sort | uniq -c | sort -rn | head -8 \
      | awk 'BEGIN{ORS=""} {c=$1; $1=""; sub(/^ /,""); gsub(/"/,"\\\""); gsub(/\\$/,""); printf "%s{\"ua\":\"%s\",\"n\":%d}", (NR>1?",":""), $0, c}')
    [ -z "$body" ] && continue
    [ $first -eq 0 ] && printf ','
    printf '"%s":[%s]' "$f" "$body"
    first=0
  done
  printf '}'
}
UA=$(ua_section)

python3 - "$DAY" "$UA" <<'PY' >> "$RESULTS"
import json, sys, os

day, ua_raw = sys.argv[1], sys.argv[2]

def rows(blob):
    try:
        d = json.loads(blob)["data"]["result"]
    except Exception:
        return None
    return d

def scalar(blob):
    d = rows(blob)
    if not d:
        return None
    try:
        return float(d[0]["value"][1])
    except Exception:
        return None

servlets = rows(os.environ["SERVLETS"])
out = []
if servlets:
    for r in servlets:
        m = r["metric"]
        try:
            v = float(r["value"][1])
        except Exception:
            continue
        if v < 0.5:
            continue
        out.append({
            "servlet": m.get("servlet"),
            "instance": m.get("instance"),
            "job": m.get("job"),
            "count": round(v),
        })
    out.sort(key=lambda x: -x["count"])

try:
    ua = json.loads(ua_raw) if ua_raw else {}
except Exception:
    ua = {}

print(json.dumps({
    "day": day,
    # Absent rather than zero when the query failed: a missing key is a gap,
    # a zero is a claim about traffic.
    "targets_up": scalar(os.environ["TARGETS"]),
    "total_requests": scalar(os.environ["TOTAL"]),
    "servlets": out,
    "user_agents": ua,
}))
PY

tail -1 "$RESULTS" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print("day=%s targets_up=%s total=%s servlet_rows=%d" % (
    d["day"], d["targets_up"],
    ("%.0f" % d["total_requests"]) if d["total_requests"] is not None else "?",
    len(d["servlets"])))
for r in d["servlets"][:12]:
    print("  %8d  %-38s %s / %s" % (r["count"], r["servlet"], r["instance"], r["job"]))'

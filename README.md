# Synapse GoPro Worker

A Go implementation of the read-only Matrix federation endpoints that Synapse
Pro's closed-source Rust worker serves:

- `GET /_matrix/federation/v1/event/{eventId}`
- `GET /_matrix/federation/v1/state_ids/{roomId}?event_id=…`
- `GET /_matrix/federation/v1/state/{roomId}?event_id=…`
- `POST /_matrix/federation/v1/get_missing_events/{roomId}`

These are pure PostgreSQL reads. The goal is one process that scales with
goroutines and a shared cache, in place of N static Python workers each with its
own heap and its own cold caches.

## Status

**All four endpoints are served natively in production**, and Synapse does no
work for any of them.

| Endpoint | Native since |
| --- | --- |
| `event` | 2026-08-28 |
| `state_ids` | 2026-08-28 |
| `get_missing_events` | 2026-09-01 |
| `state` | 2026-09-03 |

Each got there through the ladder below. Over 100,000 comparisons against
Synapse were recorded before promotion, and shadow mode caught a dozen real
defects on the way — several of which would have served wrong data, including
redacted content, to remote servers.

Where it pays off is the tail and the database, not the median. Measured on
this deployment:

| | Native | Synapse |
| --- | --- | --- |
| `state_ids` p99 | ~50 ms | ~860 ms – 7 s under load |
| `event` p99 | ~10 ms | ~36 ms |
| `state`, largest room (97 MB) | ~15 s, bounded memory | ~87 s, buffered in memory |
| `state_ids` database time | cache hits | 13,530 s/day |

That last row is the point. `/state_ids` resolution is a recursive walk of a
17 GB `state_groups_state` table; a shared, invalidation-aware cache removes it
rather than merely serving it faster, and that is load taken off a Postgres
shared with every other worker. Idle p50, by contrast, is roughly par — Synapse
is about a millisecond faster on `/event` when nothing is happening.

## Approach: never guess, always compare

Federation is unforgiving. A wrong canonical JSON serialisation or a missed
history-visibility check produces subtle breakage with remote servers, or leaks
private room history. So the worker is built in stages that are safe by
construction:

| Mode | Client gets | Synapse still does the work | Disagreements found |
| --- | --- | --- | --- |
| `proxy` | Synapse's answer | yes | — |
| `shadow` | Synapse's answer; ours is computed and diffed | yes | yes |
| `canary:N` | ours for N% of *events*, then replayed to Synapse and diffed | **yes, all of it** | yes |
| `native` | ours, with the proxy as a fallback on error | **no** | **no** |

The mode is set per endpoint, and three properties are worth knowing before
changing one:

- **A canary verifies what it served.** After the client has its answer the
  same request is replayed to Synapse and compared, off the request path. So
  `canary:100` saves the client latency but none of Synapse's load — only
  `native` does that. Watch `gopro_canary_compared_total /
  gopro_native_served_total`; if it is not ~1, some served answers went
  unchecked.
- **`canary:N` samples events, not requests.** The choice hashes the event ID
  so a retrying server always gets the same implementation. When traffic
  concentrates on a few hot events, the served share can be far from N% —
  judge it by `gopro_native_served_total`.
- **`native` stops verifying.** It is the first mode in which Synapse could be
  right and we wrong with nobody noticing, so the evidence has to be complete
  before promotion. To start checking again, drop back to `canary:N`, or run
  one of the [sweeps](#sweeps) against main Synapse.

Everything on the native path falls back to the proxy with nothing written —
verification failure, resolver error, timeout, panic — so a canary can cost
latency but not correctness. `/state` is the one narrowing of that guarantee;
see [Streaming `/state`](#streaming-state).

A remote server that has already hung up gets no answer from anyone: neither
we nor Synapse compute a response nobody will read
(`gopro_upstream_skipped_total`).

## Why this is tractable

[mautrix-go](https://github.com/mautrix/go)'s `federation` package already
implements the parts that are genuinely hard to get right, so this project does
not reimplement any cryptography:

| Need | Provided by |
| --- | --- |
| Parse and verify `X-Matrix` headers | `federation.ServerAuth.Authenticate` |
| Remote server key fetch and cache | `federation.ServerAuth.GetKeysWithCache` |
| Per-room-version redaction | `federation/pdu.Redact` |
| Event ID and reference hashes | `federation/pdu.GetEventID` |

## Shadow diff log

Shadow mode runs for weeks before an endpoint is promoted, so the evidence has
to be durable and bounded. Disagreements are written as JSON Lines to a rotating
file set under `diff_log.dir`, and cumulative counters are checkpointed to
`stats.json` — unlike Prometheus counters, these survive restarts, which matters
when the promotion gate is phrased as "seven days at 100% agreement".

```sh
gopro-worker -diffstats /data/diffs
```

```
Shadow comparison since 2026-08-01T09:14:22Z (7.4d, 3 restarts)

ENDPOINT   COMPARED  MATCHED   MISMATCHED  MATCH RATE  LAST MISMATCH
state_ids  4821094   4821094   0           100.0000%   never
state      1204553   1204551   2           99.9998%    3.1d ago

state_ids: ready - 4821094 comparisons, no mismatches
state: NOT READY - 2 mismatches, most recent 3.1d ago; the clock restarts when they stop
```

Design points that matter over a long run:

- **Logging never blocks a request.** Records go to a background writer through
  a bounded queue and are dropped if it fills, because a stalled federation
  request is worse than a missing log line. Any drop is counted and reported as
  a warning, since a lossy log invalidates the gate.
- **Bounded disk.** Rotated files are gzipped and capped at `max_files`.
- **Bodies are optional.** `body_kb: -1` records that a mismatch happened and
  its shape without ever writing room content to disk.
- **Atomic stats.** `stats.json` is written via temp-and-rename, so a crash
  mid-write cannot lose a whole run's counters. A corrupt stats file does not
  prevent startup.

The diff itself is expressed per response field as which event IDs each side
has that the other does not. `extra_in_native` is the dangerous direction: it
means we may be returning data Synapse would have withheld.

Not every disagreement is a defect, and a few are reclassified as
`gopro_shadow_skipped_total{reason}` rather than counted either way — each
because Synapse never produced a verdict to compare against:

| Reason | Why |
| --- | --- |
| `upstream_key_fetch_failed` | Synapse's 401 "Failed to find any key to satisfy": it could not fetch the origin's key, so never judged the signature |
| `upstream_rate_limited` | Synapse's 429: it shed load, including our own verification replays |
| `upstream_unavailable` | no answer from Synapse at all; a failure to verify, never a match |
| `walk_truncated` | a `get_missing_events` walk that hit its limit has no single correct answer |

The predicates are deliberately narrow — a 401 for an *invalid* signature is
still a mismatch.

## Request authentication

Incoming requests are authenticated with `mautrix-go`'s
`federation.ServerAuth`, which parses the `X-Matrix` header, fetches and caches
the origin server's published keys, and checks the ed25519 signature over the
canonical JSON of the request. None of that is reimplemented here: each part is
somewhere a subtle mistake is a security hole rather than a bug.

**No private key is required.** Verifying an incoming request needs only the
*remote* server's public keys, fetched over unauthenticated federation, so the
homeserver's signing key is never mounted into this worker.

The signature covers the request URI, so a signature obtained legitimately for
one room cannot be replayed against another. That property is what the tests in
`internal/fedauth` actually check — a genuine signature is accepted, and
altering the room, the event, the query string or the method all reject.

Published keys are resolved in the same order Synapse uses:

1. **Synapse's own cache** — the `server_keys_json` table, read-only. This is
   why the tier is nearly free: that table already holds keys for tens of
   thousands of servers, so a restart does not start cold and no extra
   federation traffic is generated for a server Synapse has already seen.
2. **Notaries** — `auth.trusted_key_servers`, which must match Synapse's
   `trusted_key_servers`. A notary can answer for an origin that is unreachable
   right now, which a direct fetch cannot; without this, a briefly-down server
   would have its legitimate requests rejected even though Synapse accepts them.
3. **The origin server directly**, as a last resort.

A hostile notary cannot forge keys. The response it relays carries the origin
server's own signature over its key list, and that self-signature is verified
before anything is trusted — for cached and notary-supplied keys alike.

While an endpoint is in shadow mode, verification runs off the request path
(a key fetch must never delay a response) and its verdict is only compared with
Synapse's. `gopro_auth_verdicts_total{result="we_accept_synapse_rejects"}` is
the dangerous direction and must be zero before anything is served natively.

## Streaming `/state`

`/state` responses reach **97 MB** on this deployment (Matrix HQ, 145k events).
Building that in memory would make us no better than Synapse, which buffers the
whole thing before sending a byte. So `/state` is the one endpoint that
streams: the state and auth-chain IDs are resolved from the same caches
`/state_ids` uses, then event JSON is streamed from Postgres straight to the
response in batches, so peak memory is bounded by a batch regardless of room
size.

That has three consequences the other endpoints do not share:

- **The fallback guarantee narrows.** Every check that can produce a Matrix
  error runs before the first byte, so a failure up to that point falls back as
  usual. Once bytes are committed the proxy cannot take over; a failure after
  that **aborts the connection** rather than ending a short body cleanly, so
  the peer sees a transfer error it can retry rather than a valid 200 carrying
  truncated JSON.
- **Two budgets, because "we are slow" and "the client is slow" are different
  problems.** How long a stream takes is set mostly by how fast the peer drains
  it — measured on the same 18.8 MB room within one minute, one peer read at
  23 KB/s and another at 5.3 MB/s. `stream_idle_timeout_seconds` (60) cuts a
  client that stops reading; `stream_timeout_seconds` (900) is only a backstop
  against a stream that never ends. A total-duration budget alone would cut the
  honest slow reader.
- **Comparison is by digest.** Neither side's body can be held for diffing, so
  both are streamed and folded into a canonical-JSON digest per array. On a
  mismatch, a second pass names the events behind it
  (`gopro_state_diagnoses_total`).

`/state` also differs from `/event` in two ways read from Synapse rather than
guessed: it drops PDUs whose depth is outside canonical JSON's integer range,
and it does not convert `age_ts` to `age`.

## Rate limiting

Rate limiting applies **only to requests this worker answers itself** — that is,
only in `canary` and `native` modes. While an endpoint is proxied or shadowed,
Synapse answers every request and its own limiter already protects it; applying
ours as well would put each request through two limiters in series, delaying
traffic Synapse would have served and inflating the upstream latency the shadow
comparison exists to measure.

Per-origin rate limiting is ported from Synapse's `FederationRateLimiter`, and
the configuration block is `rc_federation` with Synapse's own field names, so
settings can be copied from `homeserver.yaml` without translation. Keep the two
in step: looser than Synapse and we accept load Synapse would shed; tighter and
we throttle servers Synapse would answer. Copy the *active* settings — if
`rc_federation` is commented out in `homeserver.yaml`, Synapse is running its
defaults, and copying the commented values would apply limits Synapse never has.

Three stages, in Synapse's order:

1. **reject** — if too many of a server's requests are already waiting, answer
   429 immediately, before the request is even counted towards the window;
2. **sleep** — if the server has exceeded `sleep_limit` within `window_size`,
   delay this request by `sleep_delay`;
3. **queue** — allow only `concurrent` of a server's requests to run at once.

The 429 carries Synapse's exact shape — `M_LIMIT_EXCEEDED`, `"Too Many
Requests"`, `retry_after_ms` of `window_size / sleep_limit`, and a `Retry-After`
header rounded up to whole seconds — because remote servers back off on those
fields rather than on the status alone.

### Observing it

| Signal | Where |
| --- | --- |
| Rejections and delays, by endpoint | `gopro_rate_limited_total`, `gopro_rate_limit_slept_total` |
| Latency the limiter adds | `gopro_rate_limit_queue_wait_seconds` |
| How many servers are affected | `gopro_rate_limited_origins`, `gopro_rate_limit_hosts` |
| **Which** server | the worker log |

There is deliberately no per-origin metric label. This server has exchanged keys
with over forty thousand others, and one series per origin is the cardinality
explosion Prometheus handles worst. Instead a rejection is logged at **warn**
with the origin and URI, and a delayed request at **info** — logs carry high
cardinality for free. `gopro_rate_limited_origins` answers "one noisy server or
many?" without naming them.

```sh
docker logs av-gopro-worker-1 | grep "Rate limited"
```

Note that `gopro_rate_limit_queue_wait_seconds` is latency we *impose*, not
latency we measure, so it is excluded from the native-versus-Synapse comparison.

Limiting is applied on the *claimed* origin rather than a verified one.
Verifying first would mean a network key fetch before the limiter runs, which
would itself be a way to generate load; Synapse limits on the claimed origin for
the same reason.

## Caching

Only **immutable** data is cached: a state group's contents are fixed once
written, stored event JSON does not change, and an auth chain is a function of
immutable events. Membership, server ACLs, partial-state status and erasure are
deliberately never cached, because those do change and we have no signal
telling us when.

**Immutable is not the same as permanent.** Rooms get purged and events get
deleted, and a cached entry then describes something that is gone — this
served a `403` where Synapse served `404` before it was fixed. So the caches
follow Synapse's own cache-invalidation stream over Redis (`replication`, a
read-only `SUBSCRIBE`), and are **armed only while that subscription is
healthy**. Every connect, reconnect, unparseable row, disconnect and shutdown
empties them; a disarmed cache neither serves nor admits. Losing Redis costs
latency, never correctness. Watch `gopro_replication_connected`.

| Row | Meaning | Action |
| --- | --- | --- |
| `ph_cache_fake` | `purge_history` in a room | empty every cache |
| `dr_cache_fake` | room deleted | empty every cache |
| `_get_state_group_for_event`, `have_seen_event` | one event | drop that event |
| anything unparseable | unknown | empty every cache |

The channel is named after `server_name`, and one Redis can carry several
homeservers' streams — subscribing to the wrong one looks perfectly healthy and
never invalidates anything.

Two derived caches follow the same rule — *cache what is derived from
immutable data, keep the mutable lookup cheap*:

- **Filtered state**, keyed by `(state_group, filter)`. `/event` only needs the
  history visibility and sometimes one server's memberships; caching that
  answer on its own serves ~10× repeat traffic where reusing whole-map entries
  hit 0.8% of the time.
- **Parsed server ACLs**, keyed by the ACL *event's* ID. Which event is current
  stays a live lookup on every request, so a changed ACL applies immediately;
  only the regex compilation (5.9 ms on a 17 KB deny list) is cached.

Caches are bounded by **bytes, not entry count**. A resolved state map ranges
from a few kilobytes for a small room to nearly a hundred megabytes for the
largest on this deployment, so an entry bound would either waste memory or fail
to bound it.

`state_groups` is the cache that matters. Resolving state at an event is by far
the most expensive read this worker performs; measured against the largest room
here, a 145k-event state group took **1.04s cold and 2µs warm**. That is the
tail latency the whole project is trying to improve. `state_ids` p99 went from
2,296 ms to 44 ms when it landed.

There is deliberately no shared (Redis) cache tier. A bug once poisoned the
in-process state-group cache, and restarts were what flushed it; a cache that
outlives the process would need versioned keys, a purge path and a spot-check
against the database first.

Cache statistics are sampled on scrape from the cache itself rather than being
incremented alongside it, so the metrics and the cache cannot disagree.

## Layout

```
cmd/gopro-worker/     entry point
cmd/statesweep/       /state soak across every room        (see Sweeps)
cmd/eventsweep/       /event soak sampled by risk surface
cmd/gmesweep/         /get_missing_events soak across every room
internal/cache/       size-aware LRU for immutable data
internal/config/      configuration and serving modes
internal/proxy/       URI-preserving reverse proxy
internal/fedapi/      routing, canary, streaming, rate limiting glue
internal/fedauth/     X-Matrix request verification, key resolution
internal/native/      the native answer, shared by serving and shadow
internal/matrixstate/ visibility, redaction, ACLs, the four endpoints
internal/pducmp/      PDU comparison and canonical digests
internal/ratelimit/   port of Synapse's FederationRateLimiter
internal/replication/ Synapse's cache-invalidation stream over Redis
internal/shadow/      comparison against Synapse, /state diagnosis
internal/store/       read-only PostgreSQL access and caches
internal/difflog/     shadow diff log, rotation, persistent stats, metrics
internal/metrics/     Prometheus instrumentation
deploy/               example config, nginx snippet, read-only role, Grafana
scripts/              daily per-servlet traffic snapshot, /state compute probe
```

`internal/native` is shared deliberately: the answer a canary serves and the
answer shadow mode compared are produced by the same code. Two copies is what
made the diff-log replay drift twice.

## The one invariant that must not break

A Matrix server-server request is signed over its **request URI**. Room and
event IDs are percent-encoded in these paths (`!room%3Aserver`, `%24event`), so
any normalisation or re-encoding anywhere in the chain invalidates the signature
and turns every request into a 401. Worse, a decoded `%2F` becomes a real path
separator and changes the route.

`TestForwardPreservesRequestURI` pins this down by sending hand-built HTTP bytes
over a socket and asserting the upstream sees a byte-identical URI. If you touch
the proxy, that test is the one to watch.

## Building

`GOEXPERIMENT=jsonv2` is required. mautrix's `federation/pdu` package, which
implements per-room-version redaction, is gated behind it. Redaction is what
strips content from an event a requesting server may not fully see, so it is
worth depending on a maintained implementation rather than hand-rolling the
per-version rules.

Be aware this also switches `encoding/json` to its v2 backend process-wide.
Shadow comparison against Synapse is what would surface any resulting
difference in serialisation.

## Synapse behaviour you would not guess

Each of these would have been a bug had Synapse's Python been followed
literally — or had it not been read at all. Much of the event logic is now Rust
(`rust/src/events/`), so grepping only `synapse/**/*.py` misses it.

- **`unsigned` is a strict allowlist** of six fields (`age_ts`,
  `replaces_state`, `invite_room_state`, `knock_room_state`, `prev_content`,
  `prev_sender`), always serialised, `{}` when empty. Redaction *rebuilds* it,
  keeping `age_ts` and `replaces_state`; mautrix's `Redact` nils it.
- **Redaction is applied on read**, and has a second source: MSC4293's
  `room_ban_redactions` redacts everything a banned user sent, with no
  `m.room.redaction` event and no `redactions` row.
- **`/event` serves rejected events; `/state_ids` and `/state` do not.** And
  `/event` does not check the server ACL while the state endpoints do.
- **The outlier flag is a column on `events`**, not the `internal_metadata`
  field Synapse's handler reads it from.
- **Room v12's `m.room.create` has no `room_id`**, and rooms v1–2 use the
  `[event_id, hashes]` reference format.
- **Remote servers send padded base64 hashes**, Synapse stores them verbatim,
  and mautrix refuses to parse them — so redaction parses a normalised copy and
  splices the original bytes back.
- **`state_groups_state` needs `SET LOCAL enable_seqscan = off`**, exactly as
  Synapse does, or the planner scans all of it. Separately, auth-chain lookups
  are batched at 100 IDs per query: a large `ANY($1)` array makes the planner
  choose a join order that does not finish (865 ms batched, >60 s unbatched).

## Running

```sh
GOEXPERIMENT=jsonv2 go test ./...
GOEXPERIMENT=jsonv2 go build ./cmd/gopro-worker
./gopro-worker -config deploy/gopro-worker.example.yaml
./gopro-worker -version
./gopro-worker -diffstats /data/diffs
```

The tests ending in `_live_test.go` run against a real Synapse database and
are skipped unless `GOPRO_TEST_DSN` is set; the ones that also compare
against a running server read `GOPRO_LIVE_BASE` or `GOPRO_SYNAPSE_SOCK`. Several of them check our code
against an independent SQL oracle rather than against itself, and find their
own target rooms by querying for the condition under test rather than naming
any.

Container images are built and published to GHCR by
`.github/workflows/docker.yml` on every push.

### Transports

Every connection can be a unix socket or plain TCP. **All HTTP here is
unencrypted by design** — the worker sits behind nginx, which terminates TLS.

| | unix socket | TCP | TLS |
| --- | :-: | :-: | :-: |
| `listen` | yes | yes | no — terminate at nginx |
| `upstream` → Synapse | yes | yes | **no** |
| `replication` → Redis/KeyDB | yes | yes | no |
| `database` → PostgreSQL | yes | yes | yes |

```yaml
listen:
  socket: /var/sockets/nginx/av-gopro-worker-1.sock
  socket_mode: "0666"
  # ...or TCP. Exactly one of the two:
  # addr: ":8090"

upstream:
  sockets: [/var/sockets/nginx/av-inbound-federation-worker-1.sock]
  addrs:   ["127.0.0.1:8008"]      # additive, not a fallback — see below
  timeout_seconds: 180

replication:
  address: /var/sockets/keydb      # leading "/" means unix; else host:port
  # address: "keydb:6379"

database:
  dsn: "host=/var/sockets user=gopro_ro dbname=synapse-db"
  # dsn: "postgres://gopro_ro:pw@postgres:5432/synapse-db?sslmode=require"
```

Four details that are not guessable from the shape of the config:

- **`listen` takes exactly one** of `socket` or `addr`. Setting both, or
  neither, is a startup error rather than a silent preference.
- **`upstream` sockets and addrs are additive.** Every entry in both lists
  becomes a backend and requests balance round-robin across all of them, so a
  mixed fleet is a valid configuration rather than a mistake.
- **`replication` picks its transport by shape**: an address starting with `/`
  is dialled as a unix socket, anything else as TCP.
- **PostgreSQL is the only leg that can be encrypted**, because the DSN is an
  ordinary libpq string parsed by pgx — `sslmode` and the rest come free. The
  HTTP legs cannot: the upstream URL scheme is fixed at `http`, so Synapse must
  be reachable over loopback, a private network, or a socket.

The unix socket path is the one this deployment uses, and it is what
`docker-compose.yaml` and the nginx snippet in `deploy/` assume. TCP exists for
hosts with no shared socket volume, or when nginx and Synapse are elsewhere.

### With Docker

```sh
mkdir -p ./gopro/diffs
cp deploy/gopro-worker.example.yaml ./gopro/gopro-worker.yaml
chown -R 991:991 ./gopro          # match the uid the Synapse workers run as
docker compose up -d
```

`docker-compose.yaml` is standalone but written to match the conventions of an
existing Synapse stack (unix sockets under `/var/sockets`, the `npm-nw`
networks), so the service block can be copied straight into one.

Two deployment details that are easy to get wrong:

- **`socket_mode` must be `0666`**, matching the existing Synapse worker
  sockets. nginx runs in a separate container as a different uid, so the
  `0660` default would lock it out.
- **The healthcheck is the binary itself** (`-healthcheck`). The runtime image
  is distroless, so there is no `curl` to probe with as the Synapse services do.

See `deploy/` for the example config, the nginx routing snippet, the
read-only database role and the Grafana dashboard.

The database role (`deploy/readonly-role.sql`) is `SELECT`-only with
`default_transaction_read_only = on`, so a bug cannot commit a write even if
it tries; the worker warns at startup if its role is not read-only. Do not
mount `homeserver.yaml` to share configuration: it carries the macaroon secret,
the registration secret and the database password inline.

## Sweeps

Organic traffic tests only what remote servers happen to ask for. `/state`
saw three rooms in four days; a bug in code shared with `/event` had never
fired in 100,000 requests because nobody had asked for the eight events that
triggered it. The sweeps trade realism for exhaustiveness:

| Tool | Covers |
| --- | --- |
| `statesweep` | `/state` at each room's forward extremity, every room |
| `eventsweep` | `/event`, sampled by risk stratum — notably events from before our own join in `joined`/`invited` rooms, which must come back redacted |
| `gmesweep` | `/get_missing_events` in every room, shaped so the walk completes |

They **generate traffic and do not judge it**: requests go to the worker, which
does the comparison on the real code path and records it to the diff log. So
the endpoint must be in `shadow` or `canary` for a sweep to mean anything.

```sh
statesweep -key /path/to/signing.key -socket /var/sockets/nginx/av-gopro-worker-1.sock \
    -server-name example.org -interval 4s -progress state.progress
```

The sweeps sign requests as our own server, so — unlike the worker — they need
the homeserver's signing key. Run them by hand; never give the worker that key.
`-dry-run` lists targets, and `-progress` lets an interrupted sweep resume.

Main Synapse serves every federation read endpoint, so it remains a permanent
oracle once the workers it replaced are gone; pointing `upstream` at it is a
config change. Pace sweeps conservatively there — it is the most contended
process on a typical deployment, and `/state` costs it up to a minute and a
half per large room.

## What is not served, and will not be

- **Anything that writes** — `send`, `send_join`/`leave`/`knock`, `invite`,
  `exchange_third_party_invite`, and `user/keys/claim`, which consumes one-time
  keys.
- **`key/v2/query`** — the notary role signs with the homeserver's private key,
  which this worker never holds.
- **`make_join`/`leave`/`knock`** — they build prototype events from live DAG
  state.

`backfill` and `event_auth` are near-total code reuse but see a handful of
requests a day; `query/*` and `user/devices` carry more volume and share
nothing with what exists.

## Metrics

Exposed on `metrics.addr` (`:9200` by default):

- `gopro_requests_total{endpoint,mode,status}` — `status="429"` covers both
  our limiter and Synapse's relayed 429; use the rate-limit metrics to tell
  them apart
- `gopro_upstream_duration_seconds{endpoint}`,
  `gopro_upstream_errors_total{endpoint,backend}`
- `gopro_upstream_skipped_total{endpoint}` — Synapse work avoided because the
  client had already gone
- `gopro_response_bytes{endpoint}`

Native serving:

- `gopro_native_served_total`, `gopro_native_fallback_total{reason}`,
  `gopro_native_fallback_served_total`
- `gopro_native_request_seconds` (verification plus answer — what the
  federation waits for) and `gopro_native_duration_seconds` (the answer only);
  the gap between them is key resolution
- `gopro_native_fallback_upstream_total`, `gopro_native_fallback_total_seconds`
  — what a fallback cost the client, ours plus Synapse's
- `gopro_canary_compared_total` — served answers verified against Synapse;
  zero by design in `native`

Fallback reasons worth telling apart: `timeout` is our budget and worth
investigating; `client_gone` is the remote server hanging up, which some
implementations do constantly; `stream_stalled` is a `/state` reader that stopped
draining.

Caches and replication: `gopro_cache_*`, `gopro_filtered_state_total{source}`,
`gopro_replication_connected`, `gopro_replication_invalidations_total`,
`gopro_replication_cache_flushes_total`, `gopro_replication_malformed_total`.

Authentication: `gopro_auth_verdicts_total{result}`,
`gopro_auth_expired_key_total` — a remote server publishing an expired key,
not a fault here.

Shadow comparison populations: compare `gopro_shadow_duration_seconds` only
against `gopro_shadow_upstream_duration_seconds`, never against
`gopro_upstream_duration_seconds`. The latter covers every request, including
the cancelled ones excluded from comparison — which are by definition the slow
ones, and produced a fake 9× speedup the first time they were mixed.

When diff logging is enabled, the persisted shadow statistics are exported too,
read from the writer on each scrape so Prometheus and `stats.json` cannot
disagree:

- `gopro_shadow_compared_total{endpoint}`, `gopro_shadow_matched_total{endpoint}`,
  `gopro_shadow_mismatched_total{endpoint}`, `gopro_shadow_match_rate{endpoint}`
- `gopro_shadow_last_mismatch_timestamp_seconds{endpoint}` — drives the
  promotion clock
- `gopro_difflog_dropped_total` — must be zero, or the log is incomplete

These are restored from disk at startup, so unlike ordinary Prometheus counters
they survive restarts.

A Grafana dashboard covering all of it is in
[`deploy/grafana/`](deploy/grafana/).

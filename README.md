# Synapse GoPro Worker

A Go implementation of the read-only Matrix federation endpoints that Synapse
Pro's closed-source Rust worker serves:

- `GET /_matrix/federation/v1/event/{eventId}`
- `GET /_matrix/federation/v1/state/{roomId}?event_id=…`
- `GET /_matrix/federation/v1/state_ids/{roomId}?event_id=…`

These are pure PostgreSQL reads. The goal is one process that scales with
goroutines and a shared cache, in place of N static Python workers each with its
own heap and its own cold caches.

## Approach: never guess, always compare

Federation is unforgiving. A wrong canonical JSON serialisation or a missed
history-visibility check produces subtle breakage with remote servers, or leaks
private room history. So the worker is built in stages that are safe by
construction:

| Mode | Behaviour |
| --- | --- |
| `proxy` | Forward to a real Synapse worker, return its answer verbatim. |
| `shadow` | Serve Synapse's answer, but also compute our own and diff the two. |
| `canary:N` | Serve our own answer for N% of requests, falling back on error. |
| `native` | Serve our own answer, with the proxy retained as a fallback. |

The mode is set per endpoint, so `/state_ids` can go native while `/event` — by
far the riskiest, since it is the one that applies history-visibility filtering —
stays proxied.

**Status: Phase 1 complete.** Every request is proxied. The mode field is parsed
and plumbed through but not yet acted on.

## Why this is tractable

[mautrix-go](https://github.com/mautrix/go)'s `federation` package already
implements the parts that are genuinely hard to get right, so this project does
not reimplement any cryptography:

| Need | Provided by |
| --- | --- |
| Parse and verify `X-Matrix` headers | `federation.ServerAuth.Authenticate` |
| Remote server key fetch and cache | `federation.ServerAuth.GetKeysWithCache` |
| Read Synapse's `signing.key` file | `federation.ParseSynapseKey` |
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

## Known gaps before native serving

- **`/state` is not served here.** It is left routed directly to Synapse: a
  single call on a large room returns tens of megabytes (97 MB for the largest
  room measured), which needs a streaming encoder rather than the in-memory
  response model used by the other endpoints. It also sees almost no traffic,
  because a joining server receives room state in the `send_join` response
  rather than by calling `/state`.

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

## Layout

```
cmd/gopro-worker/     entry point
internal/config/      configuration and serving modes
internal/proxy/       URI-preserving reverse proxy
internal/fedapi/      routing and request handling
internal/fedauth/     X-Matrix request verification
internal/matrixstate/ server ACLs and state-at-an-event
internal/shadow/      comparison against Synapse
internal/store/       read-only PostgreSQL access
internal/difflog/     shadow diff log, rotation, persistent stats, metrics
internal/metrics/     Prometheus instrumentation
deploy/               example config, nginx snippet, Grafana dashboard
```

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

## Running

```sh
GOEXPERIMENT=jsonv2 go test ./...
GOEXPERIMENT=jsonv2 go build ./cmd/gopro-worker
./gopro-worker -config deploy/gopro-worker.example.yaml
./gopro-worker -version
./gopro-worker -diffstats /data/diffs
```

Container images are built and published to GHCR by
`.github/workflows/docker.yml` on every push.

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

See `deploy/` for the example config, the nginx routing snippet and the Grafana
dashboard.

## Metrics

Exposed on `metrics.addr` (`:9200` by default):

- `gopro_requests_total{endpoint,mode,status}`
- `gopro_upstream_duration_seconds{endpoint}` — the latency baseline the native
  implementation will be measured against
- `gopro_upstream_errors_total{endpoint,backend}`
- `gopro_response_bytes{endpoint}`

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

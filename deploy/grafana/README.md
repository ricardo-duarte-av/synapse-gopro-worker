# Grafana dashboard

`gopro-worker-dashboard.json` — import via **Dashboards → New → Import**, or
provision it (below). It picks up any Prometheus data source through a
`datasource` template variable, so no UID editing is needed.

Template variables: **Bucket**, **Data source**, **Job** and **Endpoint** (the
last two multi-select, default All).

### Bucket

Selects the range window used by every `rate()` and `histogram_quantile()` on
the dashboard, from 5s to 1h.

Two things constrain the useful range, both measured against this deployment
rather than assumed:

- Prometheus scrapes the worker every **15s**, and `rate()` needs at least two
  samples inside the window. **5s, 10s and 15s return no data at all**; 30s is
  the practical floor.
- Federation volume here is low enough that even a 1m window frequently reads
  as zero between requests. The default is therefore **5m**, which was the
  narrowest window returning a meaningful rate during a period with traffic.

Narrow the bucket to inspect a burst — a load test, or a single server going
haywire. Widen it to see a trend without the sawtooth that sparse traffic
produces.

## Rows

| Row | What it answers |
| --- | --- |
| Overview | Is it healthy right now? Rate, 5xx ratio, p99 upstream latency, upstream errors, client disconnects. |
| Traffic breakdown | Where does the load and cost come from? Status codes, serving mode, latency heatmap, response sizes. |
| Shadow comparison | Has an endpoint earned promotion? Match rate, mismatch count, time since last mismatch. |
| Process | Is one Go process actually cheaper than the Python workers it replaces? Goroutines, memory, GC. |

## Reading it

- **403 and 404 are normal.** A remote server that is not in a room gets 403, as
  does one barred by `m.room.server_acl`; `/event` returns 404 for events we do
  not have. Rising **502** or **none** is the real signal.
- **`canceled`** means the remote server hung up before we answered. Routine on
  slow `/state` calls; a spike means we became slower than remote timeouts.
- **Serving mode** shows the cutover. Traffic on a `native` endpoint appearing
  as `proxy` means the native path is falling back on errors.
- **Time since last mismatch** is the promotion clock, and it restarts on every
  disagreement. It turns green at seven days.
- **Dropped diff records** must be zero. Any other value means the diff log is
  incomplete and the promotion gate cannot be trusted.

The shadow row stays empty until an endpoint is put into `shadow` mode, since
those series come from the persisted comparison stats.

## Prometheus scrape config

```yaml
scrape_configs:
  - job_name: gopro-worker
    static_configs:
      - targets: ["av-gopro-worker-1:9200"]
```

## Provisioning

```yaml
# /etc/grafana/provisioning/dashboards/gopro-worker.yaml
apiVersion: 1
providers:
  - name: gopro-worker
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

Copy `gopro-worker-dashboard.json` into that directory.

## Note on shadow counters

`gopro_shadow_*` counters are restored from `stats.json` at startup rather than
starting at zero, so they survive restarts and deploys. That is deliberate: a
promotion gate measured in weeks cannot be judged from counters that reset on
every deploy. It does mean `rate()` over a restart is meaningful, but a counter
reset only happens if the diff log directory is wiped.

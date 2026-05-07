# Grafana provisioning

Five dashboards covering the gateway's `eveys_ocpp_*` metrics, plus a
Prometheus datasource provisioning file. These ship as deploy artifacts
— the gateway's compose stack does not run Grafana itself; you mount
these files into your own Grafana instance.

## What's here

| File | Purpose |
| --- | --- |
| `provisioning/datasources/prometheus.yml` | Default Prometheus datasource (URL via `${PROMETHEUS_URL}`, defaults to `http://prometheus:9090`) |
| `provisioning/dashboards/eveys.yml` | Auto-loads dashboards from `/var/lib/grafana/dashboards/eveys` into the `eveys/ocpp` folder |
| `dashboards/01-fleet-overview.json` | Fleet-wide rollup — first stop when triaging |
| `dashboards/02-per-pod.json` | One pod under the lens (templated `pod_id` picker) |
| `dashboards/03-per-charger.json` | Per-charger context — note: handler metrics are intentionally **not** labelled by `cp_id` (cardinality), so this dashboard pairs Prometheus with logs/Postgres/ClickHouse for true per-charger drill-down |
| `dashboards/04-reconnect-storms.json` | Reconnect / handshake / heartbeat health — the page-at-3am view |
| `dashboards/05-transactions.json` | Charging-session lifecycle: starts, stops, Kafka publish, webhook delivery, consumer lag |

All dashboards reference the `prometheus` datasource UID. If your
Grafana already has a Prometheus datasource with a different UID,
either override it via this provisioning file or rename your existing
datasource UID to `prometheus`.

## Mounting into Grafana (Docker)

```yaml
# add to your own docker-compose.yml that already runs grafana
services:
  grafana:
    image: grafana/grafana:11.0.0
    volumes:
      - ./deploy/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./deploy/grafana/dashboards:/var/lib/grafana/dashboards/eveys:ro
    environment:
      - PROMETHEUS_URL=http://prometheus:9090
```

## Mounting into Grafana (Kubernetes)

If you're running grafana-operator or the official Helm chart with
`sidecar.dashboards.enabled=true`, the dashboards can be loaded as
ConfigMaps:

```bash
kubectl create configmap eveys-ocpp-dashboards \
  --from-file=deploy/grafana/dashboards/ \
  -n monitoring
kubectl label configmap eveys-ocpp-dashboards \
  grafana_dashboard=1 -n monitoring
```

The Helm-chart sidecar picks them up automatically.

## Prometheus scrape config

The gateway exposes metrics on the metrics port from `Settings.metrics_port`
(default `9100`). Sample scrape config:

```yaml
scrape_configs:
  - job_name: eveys-ocpp
    metrics_path: /metrics
    static_configs:
      - targets: ['eveys-ocpp:9100']
```

Every metric series carries `pod_id` as a label (set from
`EVEYS_OCPP_POD_ID` at gateway boot), which is what the per-pod
dashboard's templated picker reads.

## Conventions used in the JSON

- All metric names are the literal `eveys_ocpp_*` prefix from the registry.
- Histograms use `histogram_quantile(p, sum by (label, le) (rate(_bucket[5m])))` — per-pod aggregation happens implicitly because we sum across all `pod_id` labels.
- Counters use `rate(...)[1m]` for stat panels and `[5m]` for timeseries — stat panels want responsiveness, timeseries want smoothing.
- `ops` unit on rate panels, `s` on latency, `short` on gauges, `percent` where formula already multiplies by 100.

## Updating dashboards

Edit in the Grafana UI, then **export** (Share → Export → Save to file)
and copy the JSON back into this directory. The provider yaml has
`allowUiUpdates: true` so live edits work; `disableDeletion: false`
means you can iterate freely.

When checking dashboard JSON in, **strip the `id` field** (Grafana
re-numbers per instance) and **keep `uid` stable** (links between
dashboards rely on it).

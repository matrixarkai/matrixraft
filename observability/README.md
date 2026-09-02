# MatrixRaft observability

Prometheus and Grafana, provisioned from the model in `src/metrics.rs`.

```sh
cargo run --example metrics_exporter -- 0.0.0.0:9464   # something to scrape
docker compose -f observability/docker-compose.yml up  # Prometheus + Grafana
```

Grafana comes up on <http://localhost:3000> with the dashboard already loaded
(anonymous admin, no login). Prometheus is on <http://localhost:9090>, with the
alert rules loaded and evaluating.

## What is here

| Path | What it is |
| --- | --- |
| `grafana/dashboards/matrixraft-runtime-overview.json` | 55 panels, importable as-is |
| `grafana/provisioning/datasources/prometheus.yaml` | points Grafana at Prometheus |
| `grafana/provisioning/dashboards/matrixraft.yaml` | loads the dashboard on start |
| `prometheus/prometheus.yml` | scrape config, 15s interval |
| `prometheus/rules/matrixraft-alerts.yaml` | the 16 modelled alert rules |
| `docker-compose.yml` | both services, already wired together |

## These files are generated

They are rendered from `matrixraft_grafana_dashboard()` and
`matrixraft_alert_rules()`, not maintained beside them. Add a panel or an alert
in `src/metrics.rs` and regenerate:

```sh
cargo run --example render_observability_artifacts
cargo run --example render_observability_artifacts -- --check   # CI form
```

`tests/observability_artifacts.rs` fails if the checked-in files stop matching
the model, so a panel added without regenerating is a test failure rather than a
dashboard that quietly lacks it.

The crate's own dashboard model is not importable into Grafana: its panels carry
one `expr` each and no `gridPos`, `targets`, or datasource. That model is the
right shape for the validation `metrics.rs` does against advertised metric
names; it is the wrong shape for Grafana. The rendering in
`src/observability_artifacts.rs` exists to bridge exactly that, which is why the
dashboard is generated rather than serialised.

## What the exporter does and does not serve

The crate renders Prometheus text but does not serve it — every `*_prometheus`
function takes a report and returns a `PrometheusMetricSet`, leaving exposition
to the embedder. `examples/metrics_exporter` closes that gap so the stack has
something to scrape, but it only serves what exists without a running cluster:
the operator runbook, and the provisioning validation.

**The per-node series the dashboard also plots — append and vote latency, peer
queue depth, snapshot indices — come from a live node.** A real deployment
should serve its own reports through the same functions. Those panels stay empty
against the example exporter, and that is the exporter's limit rather than the
dashboard being wrong.

## Names here are an interface

The metric names, the alert names, and the dashboard `uid`
(`rustraft-runtime-overview`) are what existing scrapes, alert routes, and
dashboard links already refer to. They are deliberately not renamed to match the
crate's Rust identifiers — renaming them would break every consumer to make the
strings look tidier.

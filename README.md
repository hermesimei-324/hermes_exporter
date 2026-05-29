# hermes_exporter

An independent Go Prometheus exporter that lives outside the Hermes Agent codebase.
It pulls status and usage data from the Hermes Dashboard API and exposes `/metrics` for Prometheus/Grafana.

> Looking for Traditional Chinese? See [README-ZH.md](./README-ZH.md).

## Features

- Go implementation
- Exposes `GET /metrics`
- Defaults to listening on `127.0.0.1:9209`
- Defaults to reading from `http://127.0.0.1:9119`
- Supported data sources:
  - `/api/status`
  - `/api/cron/jobs`
  - `/api/analytics/usage` (parsed only when present for token / cost / session metrics)
- Endpoint errors are caught and never crash the exporter
- Missing fields are ignored safely

## Project Files

- `main.go`
- `go.mod`
- `go.sum`
- `LICENSE`
- `CHANGELOG.md`
- `systemd/hermes-exporter.service`
- `prometheus/hermes-exporter-scrape.yml`
- `dashboards/hermes-exporter-overview.json`

## Installation

Build the exporter binary with Go:

```bash
cd ~/hermes_exporter
go build -o hermes_exporter .
```

If you want to run the tests first:

```bash
go test ./...
```

## Running

Run directly:

```bash
export HERMES_BASE_URL=http://127.0.0.1:9119
export HERMES_EXPORTER_PORT=9209
export HERMES_EXPORTER_INTERVAL=15
export HERMES_EXPORTER_TIMEOUT=5
./hermes_exporter
```

If the dashboard requires a token, the app also supports the optional environment variables `HERMES_DASHBOARD_TOKEN` or `HERMES_EXPORTER_TOKEN`; token values are never logged.

## systemd user service

See `systemd/hermes-exporter.service`.

After building the binary:

```bash
systemctl --user daemon-reload
systemctl --user enable --now hermes-exporter.service
systemctl --user status hermes-exporter.service --no-pager -n 50
```

## Prometheus configuration

See `prometheus/hermes-exporter-scrape.yml` and add this to your `scrape_configs`:

```yaml
- job_name: hermes_exporter
  static_configs:
    - targets: ['127.0.0.1:9209']
```

If Prometheus runs on another host, change the target to the exporter’s reachable host:port. In most setups it is still best to keep the exporter bound to `127.0.0.1` and let local Prometheus scrape it.

## Grafana dashboard JSON

This repo also includes an importable dashboard:

- `dashboards/hermes-exporter-overview.json`

### Import method 1: Grafana UI

1. Open Grafana
2. Go to **Dashboards → New → Import**
3. Upload `dashboards/hermes-exporter-overview.json`
4. Select your Prometheus datasource
5. After import, you should see **Hermes Exporter Overview**

### Import method 2: Provisioning

If you use file provisioning, place the JSON into Grafana’s dashboards directory.

This dashboard includes:

- Prometheus / Node Exporter status
- Hermes Dashboard status
- Core usage / cost / session statistics
- Cron timing table
- Model token usage / top models charts

## Verification

### 1) Make sure the dashboard is alive first

```bash
curl -fsS http://127.0.0.1:9119/api/status | python3 -m json.tool
```

### 2) Start the exporter

```bash
HERMES_BASE_URL=http://127.0.0.1:9119 \
HERMES_EXPORTER_PORT=9209 \
HERMES_EXPORTER_INTERVAL=15 \
HERMES_EXPORTER_TIMEOUT=5 \
./hermes_exporter
```

### 3) Read metrics

```bash
curl -fsS http://127.0.0.1:9209/metrics | grep '^hermes_'
```

### 4) Verify that Prometheus is scraping

In the Prometheus UI, query:

```promql
hermes_dashboard_endpoint_up
```

Or:

```promql
hermes_dashboard_active_sessions
```

## Design notes

- exporter binds to `127.0.0.1` only
- it does not touch the Hermes Agent source code
- it never outputs API keys
- failed endpoints are converted to 0 / empty values instead of interrupting the service
- missing fields are skipped without raising errors

## PromQL examples

- `hermes_dashboard_endpoint_up{endpoint="status"}`
- `hermes_dashboard_gateway_running`
- `hermes_dashboard_active_sessions`
- `hermes_dashboard_cron_jobs_total`
- `hermes_dashboard_cron_jobs_by_state{state="running"}`
- `hermes_dashboard_gateway_platform_connected{platform="discord"}`
- `hermes_dashboard_usage_tokens_total{kind="total"}`
- `hermes_dashboard_usage_cost_total{kind="total",currency="usd"}`

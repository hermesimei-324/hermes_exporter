# hermes_exporter

一個獨立於 Hermes Agent 原始碼之外的 Go Prometheus exporter。
它會從 Hermes Dashboard API 拉取狀態，並輸出 `/metrics` 供 Prometheus/Grafana 使用。

## 功能

- Go 實作
- 對外提供 `GET /metrics`
- 預設監聽 `127.0.0.1:9209`
- 預設讀取 `http://127.0.0.1:9119`
- 支援的資料來源：
  - `/api/status`
  - `/api/cron/jobs`
  - `/api/analytics/usage`（存在時才解析 token / cost / session metrics）
- 所有 endpoint 錯誤都會被攔截，不會讓 exporter crash
- 欄位不存在會直接略過

## 專案檔案

- `main.go`
- `go.mod`
- `go.sum`
- `LICENSE`
- `CHANGELOG.md`
- `systemd/hermes-exporter.service`
- `prometheus/hermes-exporter-scrape.yml`
- `dashboards/hermes-exporter-overview.json`

## 安裝

用 Go 編譯 exporter binary：

```bash
cd ~/hermes_exporter
go build -o hermes_exporter .
```

如果你想先跑測試：

```bash
go test ./...
```

## 啟動方式

直接執行：

```bash
export HERMES_BASE_URL=http://127.0.0.1:9119
export HERMES_EXPORTER_PORT=9209
export HERMES_EXPORTER_INTERVAL=15
export HERMES_EXPORTER_TIMEOUT=5
./hermes_exporter
```

如果 dashboard 需要 token，程式也支援可選環境變數 `HERMES_DASHBOARD_TOKEN` 或 `HERMES_EXPORTER_TOKEN`；不會輸出 token 值。

## systemd user service

參考 `systemd/hermes-exporter.service`。

編譯完成後：

```bash
systemctl --user daemon-reload
systemctl --user enable --now hermes-exporter.service
systemctl --user status hermes-exporter.service --no-pager -n 50
```

## Prometheus 設定

參考 `prometheus/hermes-exporter-scrape.yml`，把這段加入你的 `scrape_configs`：

```yaml
- job_name: hermes_exporter
  static_configs:
    - targets: ['127.0.0.1:9209']
```

如果 Prometheus 不在同一台主機，請改成 exporter 實際可達的 host:port。通常仍建議 exporter 只綁 `127.0.0.1`，再由本機 Prometheus scrape。

## Grafana Dashboard JSON

本 repo 也附上可直接匯入的 dashboard：

- `dashboards/hermes-exporter-overview.json`

### 匯入方式 1：Grafana 直接匯入

1. 打開 Grafana
2. 到 **Dashboards → New → Import**
3. 上傳 `dashboards/hermes-exporter-overview.json`
4. 選擇 Prometheus datasource
5. 匯入後就能看到 **Hermes Exporter Overview**

### 匯入方式 2：Provisioning

如果你是用檔案 provisioning，直接把這個 JSON 放進 Grafana 的 dashboards 目錄即可。

這份 dashboard 主要包含：

- Prometheus / Node Exporter 狀態
- Hermes Dashboard 狀態
- 主要 usage / cost / session 統計
- Cron Timing table
- Model token usage / top models 圖表

## 測試方式

### 1) 確認 dashboard 先活著

```bash
curl -fsS http://127.0.0.1:9119/api/status | python3 -m json.tool
```

### 2) 啟動 exporter

```bash
HERMES_BASE_URL=http://127.0.0.1:9119 \
HERMES_EXPORTER_PORT=9209 \
HERMES_EXPORTER_INTERVAL=15 \
HERMES_EXPORTER_TIMEOUT=5 \
./hermes_exporter
```

### 3) 讀取 metrics

```bash
curl -fsS http://127.0.0.1:9209/metrics | grep '^hermes_'
```

### 4) 驗證 Prometheus 是否抓到

在 Prometheus UI 內查：

```promql
hermes_dashboard_endpoint_up
```

或者：

```promql
hermes_dashboard_active_sessions
```

## 重要設計點

- exporter 只綁 `127.0.0.1`
- 不會碰 Hermes Agent 原始碼
- 不會輸出任何 API key
- endpoint 失敗只會變成 0/空值，不會中斷服務
- 欄位缺失會略過，不會 raise

## 可能的 PromQL 範例

- `hermes_dashboard_endpoint_up{endpoint="status"}`
- `hermes_dashboard_gateway_running`
- `hermes_dashboard_active_sessions`
- `hermes_dashboard_cron_jobs_total`
- `hermes_dashboard_cron_jobs_by_state{state="running"}`
- `hermes_dashboard_gateway_platform_connected{platform="discord"}`
- `hermes_dashboard_usage_tokens_total{kind="total"}`
- `hermes_dashboard_usage_cost_total{kind="total",currency="usd"}`

# hermes_exporter

一個獨立於 Hermes Agent 原始碼之外的 Prometheus exporter，預設只綁定 `127.0.0.1:9209`，從 Hermes Dashboard API 拉取狀態並輸出 `/metrics`。

## 功能

- Python 實作
- 對外提供 `GET /metrics`
- 預設監聽 `127.0.0.1:9209`
- 預設讀取 `http://127.0.0.1:9119`
- 先支援：
  - `/api/status`
  - `/api/cron/jobs`
  - `/api/analytics/usage`（存在時才解析 token / cost / session metrics）
- 所有 endpoint 錯誤都會被攔截，不會讓 exporter crash
- 欄位不存在會直接略過

## 專案檔案

- `hermes_exporter.py`
- `requirements.txt`
- `systemd/hermes-exporter.service`
- `prometheus/hermes-exporter-scrape.yml`

## 安裝

建議用獨立 venv：

```bash
cd ~/hermes_exporter
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## 啟動方式

直接執行：

```bash
export HERMES_BASE_URL=http://127.0.0.1:9119
export HERMES_EXPORTER_PORT=9209
export HERMES_EXPORTER_INTERVAL=15
export HERMES_EXPORTER_TIMEOUT=5
python3 hermes_exporter.py
```

如果 dashboard 需要 token，程式也支援可選環境變數 `HERMES_DASHBOARD_TOKEN` 或 `HERMES_EXPORTER_TOKEN`；不會輸出 token 值。

## systemd user service

參考 `systemd/hermes-exporter.service`。

安裝後：

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

如果 Prometheus 不在同一台主機，請改成 exporter 實際可達的 host:port，但仍建議 exporter 本身只綁 `127.0.0.1`，再由本機 Prometheus scrape。

## 測試方式

### 1) 確認 dashboard 先活著

```bash
curl -fsS http://127.0.0.1:9119/api/status | python3 -m json.tool
```

### 2) 啟動 exporter

```bash
HERMES_BASE_URL=http://127.0.0.1:9119     HERMES_EXPORTER_PORT=9209     HERMES_EXPORTER_INTERVAL=15     HERMES_EXPORTER_TIMEOUT=5     python3 hermes_exporter.py
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

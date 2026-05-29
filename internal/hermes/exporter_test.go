package hermes

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHelpers(t *testing.T) {
	if got := normalizeKey("Discord Bot-1"); got != "discord_bot_1" {
		t.Fatalf("normalizeKey = %q, want %q", got, "discord_bot_1")
	}
	if got := coerceBool("connected"); got == nil || *got != 1 {
		t.Fatalf("coerceBool(connected) = %v, want 1", got)
	}
	if got := coerceNumber("1,234.5"); got == nil || *got != 1234.5 {
		t.Fatalf("coerceNumber = %v, want 1234.5", got)
	}
}

func TestPollAndExposeMetrics(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "<html><script>window.__HERMES_SESSION_TOKEN__='token-123'</script></html>")
	})
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"version":"1.2.3","release_date":"2026-01-01","gateway_running":true,"gateway_pid":42,"active_sessions":7,"config_version":3,"latest_config_version":4,"gateway_platforms":{"discord":{"state":"connected"},"telegram":"disconnected"}}`)
	})
	mux.HandleFunc("/api/cron/jobs", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"jobs":[{"id":"job-a","name":"Job A","state":"running","schedule":{"kind":"cron","display":"* * * * *"},"next_run_at":"2026-01-02T15:04:05Z","last_run_at":"2026-01-01T15:04:05Z","last_status":"success"}]}`)
	})
	mux.HandleFunc("/api/analytics/usage", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"tokens":{"input_tokens":10,"output_tokens":5,"reasoning_tokens":2},"cost":{"usd":1.25},"sessions":{"active":3,"total_sessions":11}}`)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	exp := NewExporter(server.URL, time.Second, 2*time.Second, "")
	snap, err := exp.PollOnce()
	if err != nil {
		t.Fatalf("PollOnce error: %v", err)
	}
	exp.ApplySnapshot(snap)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	exp.MetricsHandler(rr, req)

	body := rr.Body.String()
	for _, want := range []string{
		"hermes_dashboard_gateway_running 1",
		"hermes_dashboard_active_sessions 7",
		"hermes_dashboard_gateway_platform_connected{platform=\"discord\"} 1",
		"hermes_dashboard_cron_jobs_total 1",
		"hermes_dashboard_usage_tokens_total{kind=\"input\"} 10",
		"hermes_dashboard_usage_cost_total{currency=\"usd\",kind=\"total\"} 1.25",
		"hermes_dashboard_usage_sessions_total{kind=\"active\"} 3",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q\n%s", want, body)
		}
	}
}

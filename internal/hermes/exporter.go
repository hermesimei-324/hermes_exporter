package hermes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
)

type Exporter struct {
	client       *DashboardClient
	interval     time.Duration
	textfilePath string
	registry     *prometheus.Registry
	mu           sync.Mutex
	stopOnce     sync.Once
	stopCh       chan struct{}
	snapshot     *Snapshot

	exporterUp          prometheus.Gauge
	lastPollSuccess     prometheus.Gauge
	lastPollTimestamp   prometheus.Gauge
	lastPollDuration    prometheus.Gauge
	endpointUp          *prometheus.GaugeVec
	endpointStatus      *prometheus.GaugeVec
	endpointLastSuccess *prometheus.GaugeVec
	dashboardVersion    *prometheus.GaugeVec
	gatewayRunning      prometheus.Gauge
	gatewayPID          prometheus.Gauge
	activeSessions      prometheus.Gauge
	configVersion       prometheus.Gauge
	latestConfigVersion prometheus.Gauge
	platformConnected   *prometheus.GaugeVec
	cronJobsTotal       prometheus.Gauge
	cronJobsByState     *prometheus.GaugeVec
	cronJobInfo         *prometheus.GaugeVec
	cronJobNextRunTS    *prometheus.GaugeVec
	cronJobLastRunTS    *prometheus.GaugeVec
	cronJobSecondsUntil *prometheus.GaugeVec
	cronJobLastRunAge   *prometheus.GaugeVec
	usageTokens         *prometheus.GaugeVec
	usageCost           *prometheus.GaugeVec
	usageSessions       *prometheus.GaugeVec
}

func NewExporter(baseURL string, interval, timeout time.Duration, textfilePath string) *Exporter {
	e := &Exporter{
		client:       NewDashboardClient(baseURL, timeout),
		interval:     interval,
		textfilePath: strings.TrimSpace(textfilePath),
		registry:     prometheus.NewRegistry(),
		stopCh:       make(chan struct{}),
		snapshot:     newSnapshot(),
	}
	e.buildMetrics()
	return e
}

func (e *Exporter) buildMetrics() {
	e.exporterUp = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_exporter_up", Help: "Whether the Hermes exporter process is running."})
	e.lastPollSuccess = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_exporter_last_poll_success", Help: "Whether the most recent poll cycle completed without unexpected exceptions."})
	e.lastPollTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_exporter_last_poll_timestamp_seconds", Help: "Unix timestamp of the most recent poll cycle."})
	e.lastPollDuration = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_exporter_last_poll_duration_seconds", Help: "Duration in seconds of the most recent poll cycle."})

	e.endpointUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_dashboard_endpoint_up", Help: "Whether a Hermes dashboard endpoint responded successfully."}, []string{"endpoint"})
	e.endpointStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_dashboard_endpoint_http_status", Help: "Last observed HTTP status code from a Hermes dashboard endpoint."}, []string{"endpoint"})
	e.endpointLastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_dashboard_endpoint_last_success_timestamp_seconds", Help: "Unix timestamp of the last successful response for a Hermes dashboard endpoint."}, []string{"endpoint"})

	e.dashboardVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_dashboard_version_info", Help: "Hermes dashboard version metadata."}, []string{"version", "release_date"})
	e.gatewayRunning = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_dashboard_gateway_running", Help: "Whether the Hermes gateway is running."})
	e.gatewayPID = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_dashboard_gateway_pid", Help: "Hermes gateway PID when available."})
	e.activeSessions = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_dashboard_active_sessions", Help: "Active Hermes sessions reported by the dashboard."})
	e.configVersion = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_dashboard_config_version", Help: "Current Hermes config version."})
	e.latestConfigVersion = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_dashboard_latest_config_version", Help: "Latest known Hermes config version."})
	e.platformConnected = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_dashboard_gateway_platform_connected", Help: "Whether a Hermes gateway platform is connected."}, []string{"platform"})

	e.cronJobsTotal = prometheus.NewGauge(prometheus.GaugeOpts{Name: "hermes_dashboard_cron_jobs_total", Help: "Total Hermes cron jobs reported by the dashboard."})
	e.cronJobsByState = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_dashboard_cron_jobs_by_state", Help: "Hermes cron jobs grouped by state/status."}, []string{"state"})
	e.cronJobInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_cron_job_info", Help: "Hermes cron job metadata."}, []string{"job_id", "name", "state", "schedule", "schedule_kind", "next_run_at", "last_run_at", "last_status"})
	e.cronJobNextRunTS = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_cron_job_next_run_timestamp_seconds", Help: "Unix timestamp for the next scheduled run of a Hermes cron job."}, []string{"job_id", "name"})
	e.cronJobLastRunTS = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_cron_job_last_run_timestamp_seconds", Help: "Unix timestamp for the last run of a Hermes cron job."}, []string{"job_id", "name"})
	e.cronJobSecondsUntil = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_cron_job_seconds_until_next_run", Help: "Seconds until the next scheduled run of a Hermes cron job."}, []string{"job_id", "name"})
	e.cronJobLastRunAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_cron_job_last_run_age_seconds", Help: "Seconds since the last run of a Hermes cron job."}, []string{"job_id", "name"})

	e.usageTokens = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_dashboard_usage_tokens_total", Help: "Hermes usage token counters discovered from /api/analytics/usage."}, []string{"kind"})
	e.usageCost = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_dashboard_usage_cost_total", Help: "Hermes usage cost counters discovered from /api/analytics/usage."}, []string{"kind", "currency"})
	e.usageSessions = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hermes_dashboard_usage_sessions_total", Help: "Hermes usage session counters discovered from /api/analytics/usage."}, []string{"kind"})

	e.registry.MustRegister(
		e.exporterUp,
		e.lastPollSuccess,
		e.lastPollTimestamp,
		e.lastPollDuration,
		e.endpointUp,
		e.endpointStatus,
		e.endpointLastSuccess,
		e.dashboardVersion,
		e.gatewayRunning,
		e.gatewayPID,
		e.activeSessions,
		e.configVersion,
		e.latestConfigVersion,
		e.platformConnected,
		e.cronJobsTotal,
		e.cronJobsByState,
		e.cronJobInfo,
		e.cronJobNextRunTS,
		e.cronJobLastRunTS,
		e.cronJobSecondsUntil,
		e.cronJobLastRunAge,
		e.usageTokens,
		e.usageCost,
		e.usageSessions,
	)
}

func (e *Exporter) Stop() {
	e.stopOnce.Do(func() { close(e.stopCh) })
}

func (e *Exporter) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	promhttp.HandlerFor(e.registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}

func (e *Exporter) healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "hermes exporter ok\n")
}

func (e *Exporter) ServeForever(host string, port int) error {
	e.exporterUp.Set(1)
	go e.pollLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", e.MetricsHandler)
	mux.HandleFunc("/healthz", e.healthHandler)
	mux.HandleFunc("/", e.healthHandler)

	server := &http.Server{Addr: fmt.Sprintf("%s:%d", host, port), Handler: mux}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("Received signal, stopping exporter")
		e.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	log.Printf("Serving on http://%s:%d/metrics", host, port)
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (e *Exporter) pollLoop() {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		started := time.Now()
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Unexpected error while polling Hermes dashboard API: %v", r)
					snapshot := newSnapshot()
					snapshot.PollTimestamp = float64(started.Unix())
					snapshot.PollDuration = time.Since(started).Seconds()
					snapshot.PollSuccess = 0
					e.ApplySnapshot(snapshot)
				}
			}()
			snapshot, _ := e.PollOnce()
			snapshot.PollTimestamp = float64(started.Unix())
			snapshot.PollDuration = time.Since(started).Seconds()
			snapshot.PollSuccess = 1
			e.ApplySnapshot(snapshot)
		}()
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (e *Exporter) ApplySnapshot(snapshot *Snapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snapshot = snapshot

	e.lastPollSuccess.Set(snapshot.PollSuccess)
	e.lastPollTimestamp.Set(snapshot.PollTimestamp)
	e.lastPollDuration.Set(snapshot.PollDuration)

	e.endpointUp.Reset()
	e.endpointStatus.Reset()
	e.endpointLastSuccess.Reset()
	e.dashboardVersion.Reset()
	e.platformConnected.Reset()
	e.cronJobsByState.Reset()
	e.cronJobInfo.Reset()
	e.cronJobNextRunTS.Reset()
	e.cronJobLastRunTS.Reset()
	e.cronJobSecondsUntil.Reset()
	e.cronJobLastRunAge.Reset()
	e.usageTokens.Reset()
	e.usageCost.Reset()
	e.usageSessions.Reset()

	for endpoint, value := range snapshot.EndpointUp {
		e.endpointUp.WithLabelValues(endpoint).Set(value)
	}
	for endpoint, value := range snapshot.EndpointStatus {
		e.endpointStatus.WithLabelValues(endpoint).Set(value)
	}
	for endpoint, value := range snapshot.EndpointLastSuccess {
		e.endpointLastSuccess.WithLabelValues(endpoint).Set(value)
	}
	if len(snapshot.VersionInfo) > 0 {
		version := snapshot.VersionInfo["version"]
		if version == "" {
			version = "unknown"
		}
		releaseDate := snapshot.VersionInfo["release_date"]
		if releaseDate == "" {
			releaseDate = "unknown"
		}
		e.dashboardVersion.WithLabelValues(version, releaseDate).Set(1)
	}

	e.gatewayRunning.Set(snapshot.GatewayRunning)
	e.gatewayPID.Set(snapshot.GatewayPID)
	e.activeSessions.Set(snapshot.ActiveSessions)
	e.configVersion.Set(snapshot.ConfigVersion)
	e.latestConfigVersion.Set(snapshot.LatestConfigVersion)

	for platform, value := range snapshot.PlatformConnected {
		e.platformConnected.WithLabelValues(platform).Set(value)
	}

	e.cronJobsTotal.Set(snapshot.CronJobsTotal)
	for state, value := range snapshot.CronJobsByState {
		e.cronJobsByState.WithLabelValues(state).Set(value)
	}
	for _, job := range snapshot.CronJobs {
		e.cronJobInfo.WithLabelValues(
			str(job["job_id"]),
			str(job["name"]),
			str(job["state"]),
			str(job["schedule"]),
			str(job["schedule_kind"]),
			str(job["next_run_at"]),
			str(job["last_run_at"]),
			str(job["last_status"]),
		).Set(1)
		if v, ok := job["next_run_ts"].(float64); ok {
			e.cronJobNextRunTS.WithLabelValues(str(job["job_id"]), str(job["name"])).Set(v)
		}
		if v, ok := job["last_run_ts"].(float64); ok {
			e.cronJobLastRunTS.WithLabelValues(str(job["job_id"]), str(job["name"])).Set(v)
		}
		if v, ok := job["seconds_until_next_run"].(float64); ok {
			e.cronJobSecondsUntil.WithLabelValues(str(job["job_id"]), str(job["name"])).Set(v)
		}
		if v, ok := job["last_run_age"].(float64); ok {
			e.cronJobLastRunAge.WithLabelValues(str(job["job_id"]), str(job["name"])).Set(v)
		}
	}

	for kind, value := range snapshot.UsageTokens {
		e.usageTokens.WithLabelValues(kind).Set(value)
	}
	for key, value := range snapshot.UsageCost {
		e.usageCost.WithLabelValues(key.Kind, key.Currency).Set(value)
	}
	for kind, value := range snapshot.UsageSessions {
		e.usageSessions.WithLabelValues(kind).Set(value)
	}

	e.writeTextfile()
}

func (e *Exporter) writeTextfile() {
	if strings.TrimSpace(e.textfilePath) == "" {
		return
	}
	path := e.textfilePath
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Printf("Failed to write textfile metrics to %s: %v", path, err)
		return
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Printf("Failed to write textfile metrics to %s: %v", path, err)
		return
	}
	if _, err := f.Write(prometheusText(e.registry)); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		log.Printf("Failed to write textfile metrics to %s: %v", path, err)
		return
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		log.Printf("Failed to write textfile metrics to %s: %v", path, err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		log.Printf("Failed to write textfile metrics to %s: %v", path, err)
	}
}

func prometheusText(reg *prometheus.Registry) []byte {
	mfs, err := reg.Gather()
	if err != nil {
		return nil
	}
	var buf bytes.Buffer
	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(&buf, mf); err != nil {
			return nil
		}
	}
	return buf.Bytes()
}

func escapeLabel(s string) string {
	replacer := strings.NewReplacer("\\", `\\`, "\n", `\n`, `"`, `\"`)
	return replacer.Replace(s)
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func (e *Exporter) PollOnce() (*Snapshot, error) {
	snapshot := newSnapshot()
	now := time.Now().UTC()

	type ep struct{ name, path string }
	endpoints := []ep{{"status", "/api/status"}, {"cron_jobs", "/api/cron/jobs"}, {"usage", "/api/analytics/usage"}}

	var statusPayload any
	var cronPayload any
	var usagePayload any

	for _, endpoint := range endpoints {
		status, payload, err := e.client.doJSON(endpoint.path)
		if err != nil {
			snapshot.EndpointUp[endpoint.name] = 0
			snapshot.EndpointStatus[endpoint.name] = float64(status)
			continue
		}
		if status >= 200 && status < 300 {
			snapshot.EndpointUp[endpoint.name] = 1
			snapshot.EndpointLastSuccess[endpoint.name] = float64(now.Unix())
			if payload != nil {
				switch endpoint.name {
				case "status":
					statusPayload = payload
				case "cron_jobs":
					cronPayload = payload
				case "usage":
					usagePayload = payload
				}
			}
		} else {
			snapshot.EndpointUp[endpoint.name] = 0
		}
		snapshot.EndpointStatus[endpoint.name] = float64(status)
	}

	if data, ok := statusPayload.(map[string]any); ok {
		e.parseStatusPayload(snapshot, data)
	}
	cronJobs := e.loadCronJobsFromFile()
	if cronPayload != nil {
		cronJobs = e.mergeCronJobs(cronJobs, cronPayload)
	}
	e.parseCronPayload(snapshot, cronJobs)
	if usagePayload != nil {
		e.parseUsagePayload(snapshot, usagePayload)
	}

	snapshot.PollSuccess = 1
	snapshot.PollTimestamp = float64(now.Unix())
	return snapshot, nil
}

func (e *Exporter) parseStatusPayload(snapshot *Snapshot, data map[string]any) {
	if v := data["version"]; v != nil {
		snapshot.VersionInfo["version"] = fmt.Sprint(v)
	}
	if v := data["release_date"]; v != nil {
		snapshot.VersionInfo["release_date"] = fmt.Sprint(v)
	}
	if v := boolToFloat(data["gateway_running"]); v != nil {
		snapshot.GatewayRunning = *v
	}
	if v := coerceNumber(data["gateway_pid"]); v != nil {
		snapshot.GatewayPID = *v
	}
	if v := coerceNumber(data["active_sessions"]); v != nil {
		snapshot.ActiveSessions = *v
	}
	if v := coerceNumber(data["config_version"]); v != nil {
		snapshot.ConfigVersion = *v
	}
	if v := coerceNumber(data["latest_config_version"]); v != nil {
		snapshot.LatestConfigVersion = *v
	}
	if gp, ok := data["gateway_platforms"].(map[string]any); ok {
		for platform, details := range gp {
			connected := 0.0
			if m, ok := details.(map[string]any); ok {
				if strings.ToLower(strings.TrimSpace(fmt.Sprint(m["state"]))) == "connected" {
					connected = 1
				}
			} else if strings.ToLower(strings.TrimSpace(fmt.Sprint(details))) == "connected" {
				connected = 1
			}
			snapshot.PlatformConnected[normalizeKey(platform)] = connected
		}
	}
}

func (e *Exporter) loadCronJobsFromFile() []map[string]any {
	path := filepath.Join(os.Getenv("HOME"), ".hermes", "cron", "jobs.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var payload any
	if err := json.Unmarshal(b, &payload); err != nil {
		return nil
	}
	var jobs any = payload
	if m, ok := payload.(map[string]any); ok {
		if v, ok := m["jobs"]; ok {
			jobs = v
		} else {
			jobs = m
		}
	}
	return toJobSlice(jobs)
}

func toJobSlice(v any) []map[string]any {
	switch x := v.(type) {
	case []any:
		out := make([]map[string]any, 0, len(x))
		for _, item := range x {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	case map[string]any:
		out := make([]map[string]any, 0, len(x))
		for _, item := range x {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	default:
		return nil
	}
}

func (e *Exporter) mergeCronJobs(fileJobs []map[string]any, apiPayload any) []map[string]any {
	merged := map[string]map[string]any{}
	for i, item := range fileJobs {
		jobID := fmt.Sprint(firstNonNil(item["id"], item["job_id"], item["name"], i))
		merged[jobID] = cloneMap(item)
	}
	for _, candidate := range candidateJobIterables(apiPayload) {
		for i, item := range candidate {
			jobID := fmt.Sprint(firstNonNil(item["id"], item["job_id"], item["name"], i))
			if existing, ok := merged[jobID]; ok {
				for k, v := range item {
					existing[k] = v
				}
			} else {
				merged[jobID] = cloneMap(item)
			}
		}
		break
	}
	out := make([]map[string]any, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	return out
}

func candidateJobIterables(payload any) [][]map[string]any {
	switch x := payload.(type) {
	case []any:
		return [][]map[string]any{toJobSlice(x)}
	case map[string]any:
		var out [][]map[string]any
		for _, key := range []string{"jobs", "cron_jobs", "items", "results", "data"} {
			if v, ok := x[key]; ok {
				if jobs := toJobSlice(v); len(jobs) > 0 {
					out = append(out, jobs)
				}
			}
		}
		allMaps := true
		for _, v := range x {
			if _, ok := v.(map[string]any); !ok {
				allMaps = false
				break
			}
		}
		if allMaps {
			jobs := make([]map[string]any, 0, len(x))
			for _, v := range x {
				if m, ok := v.(map[string]any); ok {
					jobs = append(jobs, m)
				}
			}
			if len(jobs) > 0 {
				out = append(out, jobs)
			}
		}
		return out
	default:
		return nil
	}
}

func (e *Exporter) parseCronPayload(snapshot *Snapshot, jobs []map[string]any) {
	var total float64
	stateCounts := map[string]float64{}
	parsed := make([]map[string]any, 0, len(jobs))
	for _, item := range jobs {
		total++
		state := e.extractJobState(item)
		if state != "" {
			stateCounts[state]++
		}
		parsed = append(parsed, e.normalizeCronJob(item, state))
	}
	snapshot.CronJobsTotal = total
	snapshot.CronJobsByState = stateCounts
	snapshot.CronJobs = parsed
}

func parseTime(value any) *time.Time {
	if value == nil {
		return nil
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" {
		return nil
	}
	if dt, err := time.Parse(time.RFC3339Nano, text); err == nil {
		if dt.Location() == time.Local {
			dt = dt.UTC()
		}
		return &dt
	}
	if dt, err := time.Parse(time.RFC3339, text); err == nil {
		if dt.Location() == time.Local {
			dt = dt.UTC()
		}
		return &dt
	}
	return nil
}

func (e *Exporter) normalizeCronJob(item map[string]any, state string) map[string]any {
	schedule := item["schedule"]
	scheduleKind := ""
	scheduleDisplay := ""
	if m, ok := schedule.(map[string]any); ok {
		scheduleKind = fmt.Sprint(m["kind"])
		scheduleDisplay = fmt.Sprint(m["display"])
	} else {
		scheduleDisplay = fmt.Sprint(item["schedule_display"])
	}
	nextRunAt := fmt.Sprint(item["next_run_at"])
	lastRunAt := fmt.Sprint(item["last_run_at"])
	nextRun := parseTime(nextRunAt)
	lastRun := parseTime(lastRunAt)
	now := time.Now().UTC()
	var nextRunTS, lastRunTS *float64
	if nextRun != nil {
		v := float64(nextRun.Unix())
		nextRunTS = &v
	}
	if lastRun != nil {
		v := float64(lastRun.Unix())
		lastRunTS = &v
	}
	job := map[string]any{
		"job_id":                 fmt.Sprint(firstNonNil(item["id"], item["job_id"], item["name"], "unknown")),
		"name":                   fmt.Sprint(firstNonNil(item["name"], "unknown")),
		"state":                  state,
		"schedule":               scheduleDisplay,
		"schedule_kind":          scheduleKind,
		"next_run_at":            nextRunAt,
		"last_run_at":            lastRunAt,
		"last_status":            fmt.Sprint(firstNonNil(item["last_status"], "unknown")),
		"next_run_ts":            nextRunTS,
		"last_run_ts":            lastRunTS,
		"seconds_until_next_run": nil,
		"last_run_age":           nil,
	}
	if nextRunTS != nil {
		v := maxFloat(*nextRunTS-float64(now.Unix()), 0)
		job["seconds_until_next_run"] = &v
	}
	if lastRunTS != nil {
		v := maxFloat(float64(now.Unix())-*lastRunTS, 0)
		job["last_run_age"] = &v
	}
	return job
}

func maxFloat(v, min float64) float64 {
	if v < min {
		return min
	}
	return v
}

func (e *Exporter) extractJobState(item map[string]any) string {
	for _, key := range []string{"state", "status", "phase", "kind"} {
		if value, ok := item[key]; ok && value != nil {
			if text := strings.ToLower(strings.TrimSpace(fmt.Sprint(value))); text != "" {
				return normalizeKey(text)
			}
		}
	}
	for _, key := range []string{"running", "active", "enabled", "paused", "disabled"} {
		if value, ok := item[key]; ok {
			if b := coerceBool(value); b != nil {
				switch key {
				case "paused", "disabled":
					if *b == 1 {
						return "paused"
					}
					return "active"
				default:
					if *b == 1 {
						return "running"
					}
					return "stopped"
				}
			}
		}
	}
	return "unknown"
}

func (e *Exporter) parseUsagePayload(snapshot *Snapshot, payload any) {
	tokens := map[string]float64{}
	costs := map[usageCostKey]float64{}
	sessions := map[string]float64{}

	var walk func(value any, path []string)
	walk = func(value any, path []string) {
		switch v := value.(type) {
		case map[string]any:
			keys := make([]string, 0, len(v))
			for key := range v {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				walk(v[key], append(path, normalizeKey(key)))
			}
			return
		case []any:
			if len(path) > 0 {
				for _, part := range path {
					if strings.Contains(part, "session") {
						sessions["total"] = float64(len(v))
						break
					}
				}
			}
			for _, child := range v {
				walk(child, path)
			}
			return
		}

		num := coerceNumber(value)
		if num == nil || len(path) == 0 {
			return
		}
		leaf := path[len(path)-1]
		ancestors := path[:len(path)-1]
		ancestorText := strings.Join(path, ".")

		if strings.Contains(ancestorText, "token") || tokenAliases[leaf] != "" {
			kind, ok := metricKindFromLeaf(leaf, tokenAliases, "")
			if !ok && containsAny(ancestors, []string{"token", "tokens"}) {
				kind, ok = metricKindFromLeaf(leaf, tokenAliases, "total")
			}
			if ok {
				tokens[kind] = *num
			}
		}

		if containsAny(path, []string{"cost", "spend", "billing"}) || costAliases[leaf] != "" {
			kind, ok := metricKindFromLeaf(leaf, costAliases, "")
			if !ok || kind == "" {
				kind = "total"
			}
			currency := "unknown"
			if containsAny(path, []string{"usd", "dollar", "dollars"}) {
				currency = "usd"
			}
			costs[usageCostKey{Kind: kind, Currency: currency}] = *num
		}

		if strings.Contains(ancestorText, "session") || sessionAliases[leaf] != "" {
			kind, ok := metricKindFromLeaf(leaf, sessionAliases, "")
			if !ok || kind == "" {
				kind = "total"
			}
			sessions[kind] = *num
		}
	}

	walk(payload, nil)
	if list, ok := payload.([]any); ok {
		sessions["total"] = float64(len(list))
	}

	snapshot.UsageTokens = tokens
	snapshot.UsageCost = costs
	snapshot.UsageSessions = sessions
}

func containsAny(parts []string, needles []string) bool {
	for _, part := range parts {
		for _, needle := range needles {
			if part == needle {
				return true
			}
		}
	}
	return false
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func cloneMap(src map[string]any) map[string]any {
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

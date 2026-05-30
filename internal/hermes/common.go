package hermes

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	defaultBaseURL  = "http://127.0.0.1:9119"
	defaultPort     = 9209
	defaultInterval = 15 * time.Second
	defaultTimeout  = 5 * time.Second
	userAgent       = "hermes-exporter/1.0"
)

var (
	rootTokenRegexes = []*regexp.Regexp{
		regexp.MustCompile(`__HERMES_SESSION_TOKEN__\s*=\s*["']([^"']+)["']`),
		regexp.MustCompile(`window\.__HERMES_SESSION_TOKEN__\s*=\s*["']([^"']+)["']`),
	}
)

var tokenAliases = map[string]string{
	"input_tokens":       "input",
	"output_tokens":      "output",
	"prompt_tokens":      "prompt",
	"completion_tokens":  "completion",
	"total_tokens":       "total",
	"cached_tokens":      "cached",
	"cache_read_tokens":  "cache_read",
	"cache_write_tokens": "cache_write",
	"input":              "input",
	"output":             "output",
	"prompt":             "prompt",
	"completion":         "completion",
	"total":              "total",
	"cached":             "cached",
	"cache_read":         "cache_read",
	"cache_write":        "cache_write",
	"count":              "total",
	"sum":                "total",
	"value":              "total",
}

var costAliases = map[string]string{
	"cost":            "total",
	"total_cost":      "total",
	"usd_cost":        "total",
	"spend":           "total",
	"billing_amount":  "total",
	"amount":          "total",
	"price":           "total",
	"input_cost":      "input",
	"output_cost":     "output",
	"prompt_cost":     "prompt",
	"completion_cost": "completion",
	"input":           "input",
	"output":          "output",
	"prompt":          "prompt",
	"completion":      "completion",
	"total":           "total",
}

var sessionAliases = map[string]string{
	"sessions":          "total",
	"session":           "total",
	"session_count":     "total",
	"total_sessions":    "total",
	"active_sessions":   "active",
	"inactive_sessions": "inactive",
	"open_sessions":     "open",
	"closed_sessions":   "closed",
	"running_sessions":  "running",
	"count":             "total",
	"total":             "total",
	"active":            "active",
	"inactive":          "inactive",
	"open":              "open",
	"closed":            "closed",
	"running":           "running",
}

type usageCostKey struct {
	Kind     string
	Currency string
}

type Snapshot struct {
	EndpointUp          map[string]float64
	EndpointStatus      map[string]float64
	EndpointLastSuccess map[string]float64
	VersionInfo         map[string]string
	GrafanaVersionInfo  map[string]string
	MacOSVersionInfo    map[string]string
	GatewayRunning      float64
	GatewayPID          float64
	ActiveSessions      float64
	ConfigVersion       float64
	LatestConfigVersion float64
	PlatformConnected   map[string]float64
	CronJobsTotal       float64
	CronJobsByState     map[string]float64
	CronJobs            []map[string]any
	CronRuns            []map[string]any
	UsageTokens         map[string]float64
	UsageCost           map[usageCostKey]float64
	UsageSessions       map[string]float64
	PollSuccess         float64
	PollTimestamp       float64
	PollDuration        float64
}

func newSnapshot() *Snapshot {
	return &Snapshot{
		EndpointUp:          map[string]float64{},
		EndpointStatus:      map[string]float64{},
		EndpointLastSuccess: map[string]float64{},
		VersionInfo:         map[string]string{},
		GrafanaVersionInfo:  map[string]string{},
		MacOSVersionInfo:    map[string]string{},
		PlatformConnected:   map[string]float64{},
		CronJobsByState:     map[string]float64{},
		CronJobs:            []map[string]any{},
		CronRuns:            []map[string]any{},
		UsageTokens:         map[string]float64{},
		UsageCost:           map[usageCostKey]float64{},
		UsageSessions:       map[string]float64{},
	}
}

func normalizeKey(value any) string {
	text := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
	text = strings.NewReplacer("-", "_", " ", "_").Replace(text)
	var b strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func intPtr(v int) *int { return &v }

func floatPtr(v float64) *float64 { return &v }

func coerceBool(value any) *int {
	switch v := value.(type) {
	case bool:
		if v {
			return intPtr(1)
		}
		return intPtr(0)
	case int:
		if v == 0 || v == 1 {
			return intPtr(v)
		}
	case int8:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case int16:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case int32:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case int64:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case uint:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case uint8:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case uint16:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case uint32:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case uint64:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case float32:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case float64:
		if v == 0 || v == 1 {
			return intPtr(int(v))
		}
	case string:
		lowered := strings.ToLower(strings.TrimSpace(v))
		switch lowered {
		case "true", "yes", "y", "on", "running", "connected", "active", "enabled":
			return intPtr(1)
		case "false", "no", "n", "off", "stopped", "disconnected", "inactive", "disabled":
			return intPtr(0)
		}
	}
	return nil
}

func coerceNumber(value any) *float64 {
	switch v := value.(type) {
	case bool:
		if v {
			return floatPtr(1)
		}
		return floatPtr(0)
	case int:
		return floatPtr(float64(v))
	case int8:
		return floatPtr(float64(v))
	case int16:
		return floatPtr(float64(v))
	case int32:
		return floatPtr(float64(v))
	case int64:
		return floatPtr(float64(v))
	case uint:
		return floatPtr(float64(v))
	case uint8:
		return floatPtr(float64(v))
	case uint16:
		return floatPtr(float64(v))
	case uint32:
		return floatPtr(float64(v))
	case uint64:
		return floatPtr(float64(v))
	case float32:
		return floatPtr(float64(v))
	case float64:
		return floatPtr(v)
	case string:
		text := strings.TrimSpace(strings.ReplaceAll(v, ",", ""))
		if text == "" {
			return nil
		}
		if n, err := strconv.ParseFloat(text, 64); err == nil {
			return floatPtr(n)
		}
	}
	return nil
}

func boolToFloat(value any) *float64 {
	if b := coerceBool(value); b != nil {
		return floatPtr(float64(*b))
	}
	return nil
}

func metricKindFromLeaf(leaf string, aliases map[string]string, defaultKind string) (string, bool) {
	leafN := normalizeKey(leaf)
	if kind, ok := aliases[leafN]; ok {
		return kind, true
	}
	if strings.HasSuffix(leafN, "_tokens") {
		trimmed := strings.TrimSuffix(leafN, "_tokens")
		if kind, ok := aliases[trimmed]; ok {
			return kind, true
		}
		switch trimmed {
		case "input", "output", "prompt", "completion", "total", "cached", "cache_read", "cache_write":
			return trimmed, true
		}
	}
	if defaultKind != "" {
		return defaultKind, true
	}
	return "", false
}

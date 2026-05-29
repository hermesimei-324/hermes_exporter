package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	baseURL := strings.TrimSpace(envString("HERMES_BASE_URL", defaultBaseURL))
	port := envInt("HERMES_EXPORTER_PORT", defaultPort, 1)
	interval := envDuration("HERMES_EXPORTER_INTERVAL", defaultInterval, time.Second)
	timeout := envDuration("HERMES_EXPORTER_TIMEOUT", defaultTimeout, 500*time.Millisecond)
	textfilePath := strings.TrimSpace(os.Getenv("HERMES_EXPORTER_TEXTFILE_PATH"))
	host := strings.TrimSpace(envString("HERMES_EXPORTER_HOST", "127.0.0.1"))
	if host == "" {
		host = "127.0.0.1"
	}

	exporter := NewExporter(baseURL, interval, timeout, textfilePath)
	if err := exporter.ServeForever(host, port); err != nil {
		log.Fatal(err)
	}
}

func envString(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}

func envInt(name string, def, min int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min {
		return def
	}
	return n
}

func envDuration(name string, def, min time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	if n, err := strconv.ParseFloat(v, 64); err == nil {
		d := time.Duration(n * float64(time.Second))
		if d < min {
			return min
		}
		return d
	}
	if d, err := time.ParseDuration(v); err == nil {
		if d < min {
			return min
		}
		return d
	}
	return def
}

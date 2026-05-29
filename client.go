package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type DashboardClient struct {
	baseURL    string
	timeout    time.Duration
	httpClient *http.Client
	mu         sync.Mutex
	token      string
}

func NewDashboardClient(baseURL string, timeout time.Duration) *DashboardClient {
	token := strings.TrimSpace(os.Getenv("HERMES_DASHBOARD_TOKEN"))
	if token == "" {
		token = strings.TrimSpace(os.Getenv("HERMES_EXPORTER_TOKEN"))
	}
	return &DashboardClient{
		baseURL:    strings.TrimRight(baseURL, "/") + "/",
		timeout:    timeout,
		httpClient: &http.Client{Timeout: timeout},
		token:      token,
	}
}

func (c *DashboardClient) discoverToken() string {
	req, err := http.NewRequest(http.MethodGet, c.baseURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	html := string(body)
	for _, re := range rootTokenRegexes {
		if match := re.FindStringSubmatch(html); len(match) > 1 {
			return strings.TrimSpace(match[1])
		}
	}
	return ""
}

func (c *DashboardClient) doJSON(path string) (int, any, error) {
	url := strings.TrimRight(c.baseURL, "/") + "/" + strings.TrimLeft(path, "/")
	doReq := func() (int, any, error) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", userAgent)
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		status := resp.StatusCode
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return status, nil, err
		}
		raw := strings.TrimSpace(string(body))
		var payload any
		if raw != "" {
			if err := json.Unmarshal([]byte(raw), &payload); err != nil {
				payload = raw
			}
		}
		if status >= 200 && status < 300 {
			return status, payload, nil
		}
		return status, payload, fmt.Errorf("http status %d", status)
	}

	status, payload, err := doReq()
	if err != nil && (status == http.StatusUnauthorized || status == http.StatusForbidden) && c.token == "" {
		if discovered := c.discoverToken(); discovered != "" {
			c.mu.Lock()
			c.token = discovered
			c.mu.Unlock()
			status, payload, err = doReq()
		}
	}
	return status, payload, err
}

# Changelog

All notable changes to this project will be documented in this file.

The format is intentionally simple for the first public release.

## [v0.1.0] - 2026-05-29

### Added
- Go implementation of `hermes_exporter`
- `/metrics` endpoint backed by Hermes Dashboard API polling
- Prometheus scrape example and systemd user service
- English `README.md` and Traditional Chinese `README-ZH.md`
- Importable Grafana dashboard JSON in `dashboards/hermes-exporter-overview.json`
- GitHub Actions CI workflow for `go test` and `go build`
- Public GitHub release `v0.1.0`

### Removed
- Legacy Python exporter sources were removed from the repo

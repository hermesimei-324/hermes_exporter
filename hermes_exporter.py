#!/usr/bin/env python3
"""Hermes Agent exporter for Prometheus.

Polls the Hermes Dashboard API on a fixed interval and exposes derived metrics
on /metrics. The exporter is intentionally defensive: every endpoint failure is
contained, missing fields are ignored, and the process keeps serving metrics.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from prometheus_client import CollectorRegistry, Gauge, CONTENT_TYPE_LATEST, generate_latest


DEFAULT_BASE_URL = 'http://127.0.0.1:9119'
DEFAULT_PORT = 9209
DEFAULT_INTERVAL = 15.0
DEFAULT_TIMEOUT = 5.0
USER_AGENT = 'hermes-exporter/1.0'


def _env_float(name: str, default: float, minimum: float = 0.1) -> float:
    raw = os.getenv(name, '').strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(value, minimum)


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, '').strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(value, minimum)


def _normalize_key(value: Any) -> str:
    text = str(value).strip().lower().replace('-', '_').replace(' ', '_')
    return ''.join(ch if ch.isalnum() or ch == '_' else '_' for ch in text)


def _coerce_bool(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        if value in (0, 1):
            return int(value)
        return None
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {'true', 'yes', 'y', 'on', 'running', 'connected', 'active', 'enabled'}:
            return 1
        if lowered in {'false', 'no', 'n', 'off', 'stopped', 'disconnected', 'inactive', 'disabled'}:
            return 0
    return None


def _coerce_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(',', '')
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _first_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _bool_to_float(value: Any) -> Optional[float]:
    b = _coerce_bool(value)
    if b is None:
        return None
    return float(b)


def _metric_kind_from_leaf(leaf: str, aliases: Mapping[str, str], default: Optional[str] = None) -> Optional[str]:
    leaf_n = _normalize_key(leaf)
    if leaf_n in aliases:
        return aliases[leaf_n]
    if leaf_n.endswith('_tokens'):
        trimmed = leaf_n[:-7]
        if trimmed in aliases:
            return aliases[trimmed]
        if trimmed in {'input', 'output', 'prompt', 'completion', 'total', 'cached', 'cache_read', 'cache_write'}:
            return trimmed
    return default


TOKEN_ALIASES = {
    'input_tokens': 'input',
    'output_tokens': 'output',
    'prompt_tokens': 'prompt',
    'completion_tokens': 'completion',
    'total_tokens': 'total',
    'cached_tokens': 'cached',
    'cache_read_tokens': 'cache_read',
    'cache_write_tokens': 'cache_write',
    'input': 'input',
    'output': 'output',
    'prompt': 'prompt',
    'completion': 'completion',
    'total': 'total',
    'cached': 'cached',
    'cache_read': 'cache_read',
    'cache_write': 'cache_write',
    'count': 'total',
    'sum': 'total',
    'value': 'total',
}

COST_ALIASES = {
    'cost': 'total',
    'total_cost': 'total',
    'usd_cost': 'total',
    'spend': 'total',
    'billing_amount': 'total',
    'amount': 'total',
    'price': 'total',
    'input_cost': 'input',
    'output_cost': 'output',
    'prompt_cost': 'prompt',
    'completion_cost': 'completion',
    'input': 'input',
    'output': 'output',
    'prompt': 'prompt',
    'completion': 'completion',
    'total': 'total',
}

SESSION_ALIASES = {
    'sessions': 'total',
    'session': 'total',
    'session_count': 'total',
    'total_sessions': 'total',
    'active_sessions': 'active',
    'inactive_sessions': 'inactive',
    'open_sessions': 'open',
    'closed_sessions': 'closed',
    'running_sessions': 'running',
    'count': 'total',
    'total': 'total',
    'active': 'active',
    'inactive': 'inactive',
    'open': 'open',
    'closed': 'closed',
    'running': 'running',
}


@dataclass
class PollSnapshot:
    endpoint_up: Dict[str, float]
    endpoint_status: Dict[str, float]
    endpoint_last_success: Dict[str, float]
    version_info: Dict[str, str]
    gateway_running: float = 0.0
    gateway_pid: float = 0.0
    active_sessions: float = 0.0
    config_version: float = 0.0
    latest_config_version: float = 0.0
    platform_connected: Dict[str, float] = None
    cron_jobs_total: float = 0.0
    cron_jobs_by_state: Dict[str, float] = None
    cron_jobs: list[dict[str, Any]] = None
    usage_tokens: Dict[str, float] = None
    usage_cost: Dict[str, float] = None
    usage_sessions: Dict[str, float] = None
    poll_success: float = 0.0
    poll_timestamp: float = 0.0
    poll_duration: float = 0.0

    def __post_init__(self) -> None:
        if self.platform_connected is None:
            self.platform_connected = {}
        if self.cron_jobs_by_state is None:
            self.cron_jobs_by_state = {}
        if self.cron_jobs is None:
            self.cron_jobs = []
        if self.usage_tokens is None:
            self.usage_tokens = {}
        if self.usage_cost is None:
            self.usage_cost = {}
        if self.usage_sessions is None:
            self.usage_sessions = {}


class HermesDashboardClient:
    def __init__(self, base_url: str, timeout: float) -> None:
        self.base_url = base_url.rstrip('/') + '/'
        self.timeout = timeout
        self.token = os.getenv('HERMES_DASHBOARD_TOKEN', '').strip() or os.getenv('HERMES_EXPORTER_TOKEN', '').strip()

    def _discover_token(self) -> str:
        try:
            request = Request(self.base_url, headers={'User-Agent': USER_AGENT}, method='GET')
            with urlopen(request, timeout=self.timeout) as response:
                html = response.read().decode('utf-8', 'replace')
        except Exception:
            return ''

        for pattern in (
            r'__HERMES_SESSION_TOKEN__\s*=\s*["\']([^"\']+)["\']',
            r'window\.__HERMES_SESSION_TOKEN__\s*=\s*["\']([^"\']+)["\']',
        ):
            match = re.search(pattern, html)
            if match:
                return match.group(1).strip()
        return ''

    def fetch_json(self, path: str) -> Tuple[int, Any]:
        url = urljoin(self.base_url, path.lstrip('/'))
        headers = {
            'Accept': 'application/json',
            'User-Agent': USER_AGENT,
        }
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'

        request = Request(url, headers=headers, method='GET')
        try:
            with urlopen(request, timeout=self.timeout) as response:
                status = getattr(response, 'status', 200)
                raw = response.read().decode('utf-8', 'replace')
                if not raw.strip():
                    return status, None
                try:
                    return status, json.loads(raw)
                except json.JSONDecodeError:
                    return status, raw
        except HTTPError as exc:
            if exc.code in (401, 403) and not self.token:
                discovered = self._discover_token()
                if discovered:
                    self.token = discovered
                    headers['Authorization'] = f'Bearer {self.token}'
                    request = Request(url, headers=headers, method='GET')
                    with urlopen(request, timeout=self.timeout) as response:
                        status = getattr(response, 'status', 200)
                        raw = response.read().decode('utf-8', 'replace')
                        if not raw.strip():
                            return status, None
                        try:
                            return status, json.loads(raw)
                        except json.JSONDecodeError:
                            return status, raw
            raise


class HermesExporter:
    def __init__(self, base_url: str, interval: float, timeout: float, textfile_path: str = '') -> None:
        self.client = HermesDashboardClient(base_url=base_url, timeout=timeout)
        self.interval = interval
        self.timeout = timeout
        self.textfile_path = textfile_path.strip()
        self.registry = CollectorRegistry(auto_describe=True)
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self._snapshot = PollSnapshot(endpoint_up={}, endpoint_status={}, endpoint_last_success={}, version_info={})
        self._build_metrics()

    def _build_metrics(self) -> None:
        self.exporter_up = Gauge('hermes_exporter_up', 'Whether the Hermes exporter process is running.', registry=self.registry)
        self.last_poll_success = Gauge('hermes_exporter_last_poll_success', 'Whether the most recent poll cycle completed without unexpected exceptions.', registry=self.registry)
        self.last_poll_timestamp = Gauge('hermes_exporter_last_poll_timestamp_seconds', 'Unix timestamp of the most recent poll cycle.', registry=self.registry)
        self.last_poll_duration = Gauge('hermes_exporter_last_poll_duration_seconds', 'Duration in seconds of the most recent poll cycle.', registry=self.registry)

        self.endpoint_up = Gauge('hermes_dashboard_endpoint_up', 'Whether a Hermes dashboard endpoint responded successfully.', ['endpoint'], registry=self.registry)
        self.endpoint_status = Gauge('hermes_dashboard_endpoint_http_status', 'Last observed HTTP status code from a Hermes dashboard endpoint.', ['endpoint'], registry=self.registry)
        self.endpoint_last_success = Gauge('hermes_dashboard_endpoint_last_success_timestamp_seconds', 'Unix timestamp of the last successful response for a Hermes dashboard endpoint.', ['endpoint'], registry=self.registry)

        self.dashboard_version = Gauge('hermes_dashboard_version_info', 'Hermes dashboard version metadata.', ['version', 'release_date'], registry=self.registry)
        self.gateway_running = Gauge('hermes_dashboard_gateway_running', 'Whether the Hermes gateway is running.', registry=self.registry)
        self.gateway_pid = Gauge('hermes_dashboard_gateway_pid', 'Hermes gateway PID when available.', registry=self.registry)
        self.active_sessions = Gauge('hermes_dashboard_active_sessions', 'Active Hermes sessions reported by the dashboard.', registry=self.registry)
        self.config_version = Gauge('hermes_dashboard_config_version', 'Current Hermes config version.', registry=self.registry)
        self.latest_config_version = Gauge('hermes_dashboard_latest_config_version', 'Latest known Hermes config version.', registry=self.registry)
        self.platform_connected = Gauge('hermes_dashboard_gateway_platform_connected', 'Whether a Hermes gateway platform is connected.', ['platform'], registry=self.registry)

        self.cron_jobs_total = Gauge('hermes_dashboard_cron_jobs_total', 'Total Hermes cron jobs reported by the dashboard.', registry=self.registry)
        self.cron_jobs_by_state = Gauge('hermes_dashboard_cron_jobs_by_state', 'Hermes cron jobs grouped by state/status.', ['state'], registry=self.registry)
        self.cron_job_info = Gauge(
            'hermes_cron_job_info',
            'Hermes cron job metadata.',
            ['job_id', 'name', 'state', 'schedule', 'schedule_kind', 'next_run_at', 'last_run_at', 'last_status'],
            registry=self.registry,
        )
        self.cron_job_next_run_ts = Gauge(
            'hermes_cron_job_next_run_timestamp_seconds',
            'Unix timestamp for the next scheduled run of a Hermes cron job.',
            ['job_id', 'name'],
            registry=self.registry,
        )
        self.cron_job_last_run_ts = Gauge(
            'hermes_cron_job_last_run_timestamp_seconds',
            'Unix timestamp for the last run of a Hermes cron job.',
            ['job_id', 'name'],
            registry=self.registry,
        )
        self.cron_job_seconds_until_next_run = Gauge(
            'hermes_cron_job_seconds_until_next_run',
            'Seconds until the next scheduled run of a Hermes cron job.',
            ['job_id', 'name'],
            registry=self.registry,
        )
        self.cron_job_last_run_age = Gauge(
            'hermes_cron_job_last_run_age_seconds',
            'Seconds since the last run of a Hermes cron job.',
            ['job_id', 'name'],
            registry=self.registry,
        )

        self.usage_tokens = Gauge('hermes_dashboard_usage_tokens_total', 'Hermes usage token counters discovered from /api/analytics/usage.', ['kind'], registry=self.registry)
        self.usage_cost = Gauge('hermes_dashboard_usage_cost_total', 'Hermes usage cost counters discovered from /api/analytics/usage.', ['kind', 'currency'], registry=self.registry)
        self.usage_sessions = Gauge('hermes_dashboard_usage_sessions_total', 'Hermes usage session counters discovered from /api/analytics/usage.', ['kind'], registry=self.registry)

    def stop(self) -> None:
        self._stop.set()

    def serve_forever(self, host: str, port: int) -> None:
        self.exporter_up.set(1)
        threading.Thread(target=self._poll_loop, name='hermes-exporter-poll', daemon=True).start()

        exporter = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path == '/metrics':
                    payload = generate_latest(exporter.registry)
                    self.send_response(200)
                    self.send_header('Content-Type', CONTENT_TYPE_LATEST)
                    self.send_header('Content-Length', str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                if self.path in {'/', '/healthz'}:
                    payload = b'hermes exporter ok\n'
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/plain; charset=utf-8')
                    self.send_header('Content-Length', str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                self.send_response(404)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.end_headers()
                self.wfile.write(b'not found\n')

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                logging.getLogger('hermes_exporter.http').debug(format, *args)

        server = ThreadingHTTPServer((host, port), Handler)
        server.daemon_threads = True
        logging.info('Serving on http://%s:%s/metrics', host, port)

        def _handle_signal(signum: int, frame: Any) -> None:  # noqa: ARG001
            logging.info('Received signal %s, stopping exporter', signum)
            self.stop()
            server.shutdown()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _handle_signal)
            except Exception:
                pass

        try:
            server.serve_forever(poll_interval=0.5)
        finally:
            self.stop()
            server.server_close()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            started = time.time()
            try:
                snapshot = self._poll_once()
                snapshot.poll_timestamp = started
                snapshot.poll_duration = max(time.time() - started, 0.0)
                snapshot.poll_success = 1.0
                self._apply_snapshot(snapshot)
            except Exception:
                logging.exception('Unexpected error while polling Hermes dashboard API')
                snapshot = PollSnapshot(endpoint_up={}, endpoint_status={}, endpoint_last_success={}, version_info={})
                snapshot.poll_timestamp = started
                snapshot.poll_duration = max(time.time() - started, 0.0)
                snapshot.poll_success = 0.0
                self._apply_snapshot(snapshot)
            self._stop.wait(self.interval)

    def _apply_snapshot(self, snapshot: PollSnapshot) -> None:
        with self.lock:
            self._snapshot = snapshot

            self.last_poll_success.set(snapshot.poll_success)
            self.last_poll_timestamp.set(snapshot.poll_timestamp)
            self.last_poll_duration.set(snapshot.poll_duration)

            for endpoint, value in snapshot.endpoint_up.items():
                self.endpoint_up.labels(endpoint=endpoint).set(value)
            for endpoint, value in snapshot.endpoint_status.items():
                self.endpoint_status.labels(endpoint=endpoint).set(value)
            for endpoint, value in snapshot.endpoint_last_success.items():
                self.endpoint_last_success.labels(endpoint=endpoint).set(value)

            if snapshot.version_info:
                self.dashboard_version.labels(version=snapshot.version_info.get('version', 'unknown'), release_date=snapshot.version_info.get('release_date', 'unknown')).set(1)

            self.gateway_running.set(snapshot.gateway_running)
            self.gateway_pid.set(snapshot.gateway_pid)
            self.active_sessions.set(snapshot.active_sessions)
            self.config_version.set(snapshot.config_version)
            self.latest_config_version.set(snapshot.latest_config_version)

            for platform, value in snapshot.platform_connected.items():
                self.platform_connected.labels(platform=platform).set(value)

            self.cron_jobs_total.set(snapshot.cron_jobs_total)
            for state, value in snapshot.cron_jobs_by_state.items():
                self.cron_jobs_by_state.labels(state=state).set(value)
            for job in snapshot.cron_jobs:
                labels = {
                    'job_id': job['job_id'],
                    'name': job['name'],
                    'state': job['state'],
                    'schedule': job['schedule'],
                    'schedule_kind': job['schedule_kind'],
                    'next_run_at': job['next_run_at'],
                    'last_run_at': job['last_run_at'],
                    'last_status': job['last_status'],
                }
                self.cron_job_info.labels(**labels).set(1)
                if job.get('next_run_ts') is not None:
                    self.cron_job_next_run_ts.labels(job_id=job['job_id'], name=job['name']).set(float(job['next_run_ts']))
                if job.get('last_run_ts') is not None:
                    self.cron_job_last_run_ts.labels(job_id=job['job_id'], name=job['name']).set(float(job['last_run_ts']))
                if job.get('seconds_until_next_run') is not None:
                    self.cron_job_seconds_until_next_run.labels(job_id=job['job_id'], name=job['name']).set(float(job['seconds_until_next_run']))
                if job.get('last_run_age') is not None:
                    self.cron_job_last_run_age.labels(job_id=job['job_id'], name=job['name']).set(float(job['last_run_age']))

            for kind, value in snapshot.usage_tokens.items():
                self.usage_tokens.labels(kind=kind).set(value)
            for (kind, currency), value in snapshot.usage_cost.items():
                self.usage_cost.labels(kind=kind, currency=currency).set(value)
            for kind, value in snapshot.usage_sessions.items():
                self.usage_sessions.labels(kind=kind).set(value)

            self._write_textfile()

    def _write_textfile(self) -> None:
        if not self.textfile_path:
            return
        target = Path(self.textfile_path)
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_suffix(target.suffix + '.tmp')
            tmp.write_bytes(generate_latest(self.registry))
            tmp.replace(target)
        except Exception:
            logging.exception('Failed to write textfile metrics to %s', target)

    def _poll_once(self) -> PollSnapshot:
        snapshot = PollSnapshot(endpoint_up={}, endpoint_status={}, endpoint_last_success={}, version_info={})

        endpoints = {
            'status': '/api/status',
            'cron_jobs': '/api/cron/jobs',
            'usage': '/api/analytics/usage',
        }

        status_payload: Optional[Any] = None
        cron_payload: Optional[Any] = None
        usage_payload: Optional[Any] = None

        for endpoint_name, path in endpoints.items():
            try:
                http_status, payload = self.client.fetch_json(path)
                snapshot.endpoint_up[endpoint_name] = 1.0 if 200 <= http_status < 300 else 0.0
                snapshot.endpoint_status[endpoint_name] = float(http_status)
                snapshot.endpoint_last_success[endpoint_name] = time.time() if 200 <= http_status < 300 else snapshot.endpoint_last_success.get(endpoint_name, 0.0)
                if endpoint_name == 'status' and 200 <= http_status < 300:
                    status_payload = payload
                elif endpoint_name == 'cron_jobs' and 200 <= http_status < 300:
                    cron_payload = payload
                elif endpoint_name == 'usage' and 200 <= http_status < 300:
                    usage_payload = payload
            except HTTPError as exc:
                snapshot.endpoint_up[endpoint_name] = 0.0
                snapshot.endpoint_status[endpoint_name] = float(getattr(exc, 'code', 0) or 0)
            except URLError:
                snapshot.endpoint_up[endpoint_name] = 0.0
                snapshot.endpoint_status[endpoint_name] = 0.0
            except Exception:
                snapshot.endpoint_up[endpoint_name] = 0.0
                snapshot.endpoint_status[endpoint_name] = 0.0

        if isinstance(status_payload, dict):
            self._parse_status_payload(snapshot, status_payload)
        cron_jobs = self._load_cron_jobs_from_file()
        if cron_payload is not None:
            cron_jobs = self._merge_cron_jobs(cron_jobs, cron_payload)
        self._parse_cron_payload(snapshot, cron_jobs)
        if usage_payload is not None:
            self._parse_usage_payload(snapshot, usage_payload)

        return snapshot

    def _parse_status_payload(self, snapshot: PollSnapshot, data: Mapping[str, Any]) -> None:
        version = data.get('version')
        release_date = data.get('release_date')
        if version is not None:
            snapshot.version_info['version'] = str(version)
        if release_date is not None:
            snapshot.version_info['release_date'] = str(release_date)

        snapshot.gateway_running = _bool_to_float(data.get('gateway_running')) or 0.0
        snapshot.gateway_pid = _coerce_number(data.get('gateway_pid')) or 0.0
        snapshot.active_sessions = _coerce_number(data.get('active_sessions')) or 0.0
        snapshot.config_version = _coerce_number(data.get('config_version')) or 0.0
        snapshot.latest_config_version = _coerce_number(data.get('latest_config_version')) or 0.0

        gateway_platforms = data.get('gateway_platforms')
        if isinstance(gateway_platforms, Mapping):
            for platform, details in gateway_platforms.items():
                platform_name = _normalize_key(platform)
                connected = 0.0
                if isinstance(details, Mapping):
                    state = details.get('state')
                    connected = 1.0 if str(state).strip().lower() == 'connected' else 0.0
                else:
                    connected = 1.0 if str(details).strip().lower() == 'connected' else 0.0
                snapshot.platform_connected[platform_name] = connected

    def _load_cron_jobs_from_file(self) -> list[dict[str, Any]]:
        path = Path.home() / '.hermes' / 'cron' / 'jobs.json'
        try:
            with path.open('r', encoding='utf-8') as fh:
                payload = json.load(fh)
        except Exception:
            return []

        jobs: Any = payload
        if isinstance(payload, dict):
            jobs = payload.get('jobs', payload)
        if isinstance(jobs, list):
            return [item for item in jobs if isinstance(item, dict)]
        if isinstance(jobs, dict):
            return [item for item in jobs.values() if isinstance(item, dict)]
        return []

    def _merge_cron_jobs(self, file_jobs: list[dict[str, Any]], api_payload: Any) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for item in file_jobs:
            job_id = str(item.get('id') or item.get('job_id') or item.get('name') or len(merged))
            merged[job_id] = dict(item)
        for candidate in self._candidate_job_iterables(api_payload):
            for item in candidate:
                if not isinstance(item, Mapping):
                    continue
                job_id = str(item.get('id') or item.get('job_id') or item.get('name') or len(merged))
                merged.setdefault(job_id, {}).update(dict(item))
            break
        return list(merged.values())

    def _candidate_job_iterables(self, payload: Any) -> Iterable[Any]:
        if isinstance(payload, list):
            yield payload
            return
        if isinstance(payload, dict):
            for key in ('jobs', 'cron_jobs', 'items', 'results', 'data'):
                value = payload.get(key)
                if isinstance(value, list):
                    yield value
                elif isinstance(value, dict):
                    yield list(value.values())
            # If the API returns a dict keyed by job id, treat values as jobs.
            if payload and all(isinstance(v, Mapping) for v in payload.values()):
                yield list(payload.values())

    def _parse_cron_payload(self, snapshot: PollSnapshot, jobs: Iterable[Any]) -> None:
        total = 0.0
        state_counts: Counter[str] = Counter()
        parsed_jobs: list[dict[str, Any]] = []
        for item in jobs:
            if not isinstance(item, Mapping):
                continue
            total += 1.0
            state = self._extract_job_state(item)
            if state:
                state_counts[state] += 1
            parsed_jobs.append(self._normalize_cron_job(item, state))
        snapshot.cron_jobs_total = total
        snapshot.cron_jobs_by_state = {state: float(count) for state, count in state_counts.items()}
        snapshot.cron_jobs = parsed_jobs

    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _normalize_cron_job(self, item: Mapping[str, Any], state: str) -> dict[str, Any]:
        schedule = item.get('schedule')
        schedule_kind = ''
        schedule_display = ''
        if isinstance(schedule, Mapping):
            schedule_kind = str(schedule.get('kind', '') or '')
            schedule_display = str(schedule.get('display', '') or '')
        else:
            schedule_display = str(item.get('schedule_display', '') or '')
        next_run_at = str(item.get('next_run_at', '') or '')
        last_run_at = str(item.get('last_run_at', '') or '')
        next_run_dt = self._parse_datetime(next_run_at)
        last_run_dt = self._parse_datetime(last_run_at)
        now = datetime.now(timezone.utc)
        next_run_ts = next_run_dt.timestamp() if next_run_dt else None
        last_run_ts = last_run_dt.timestamp() if last_run_dt else None
        return {
            'job_id': str(item.get('id', '') or item.get('job_id', '') or item.get('name', 'unknown')),
            'name': str(item.get('name', 'unknown')),
            'state': state,
            'schedule': schedule_display,
            'schedule_kind': schedule_kind,
            'next_run_at': next_run_at,
            'last_run_at': last_run_at,
            'last_status': str(item.get('last_status', '') or 'unknown'),
            'next_run_ts': next_run_ts,
            'last_run_ts': last_run_ts,
            'seconds_until_next_run': max((next_run_ts - now.timestamp()), 0.0) if next_run_ts is not None else None,
            'last_run_age': max((now.timestamp() - last_run_ts), 0.0) if last_run_ts is not None else None,
        }

    def _extract_job_state(self, item: Mapping[str, Any]) -> str:
        for key in ('state', 'status', 'phase', 'kind'):
            value = item.get(key)
            if value is None:
                continue
            text = str(value).strip().lower()
            if text:
                return _normalize_key(text)
        for key in ('running', 'active', 'enabled', 'paused', 'disabled'):
            if key in item:
                value = _coerce_bool(item.get(key))
                if value is not None:
                    if key in {'paused', 'disabled'}:
                        return 'paused' if value else 'active'
                    return 'running' if value else 'stopped'
        return 'unknown'

    def _parse_usage_payload(self, snapshot: PollSnapshot, payload: Any) -> None:
        tokens: Dict[str, float] = {}
        costs: Dict[Tuple[str, str], float] = {}
        sessions: Dict[str, float] = {}

        def walk(value: Any, path: Tuple[str, ...]) -> None:
            if isinstance(value, Mapping):
                for key, child in value.items():
                    walk(child, path + (_normalize_key(key),))
                return
            if isinstance(value, list):
                if path and any('session' in part for part in path):
                    sessions.setdefault('total', float(len(value)))
                for child in value:
                    walk(child, path)
                return

            num = _coerce_number(value)
            if num is None:
                return
            if not path:
                return
            leaf = path[-1]
            ancestors = path[:-1]
            ancestor_text = '.'.join(path)

            if 'token' in ancestor_text or leaf in TOKEN_ALIASES:
                kind = _metric_kind_from_leaf(leaf, TOKEN_ALIASES)
                if kind is None and any(part in {'token', 'tokens'} for part in ancestors):
                    kind = _metric_kind_from_leaf(leaf, TOKEN_ALIASES, default='total')
                if kind:
                    tokens[kind] = num

            if any(part in {'cost', 'spend', 'billing'} for part in path) or leaf in COST_ALIASES:
                kind = _metric_kind_from_leaf(leaf, COST_ALIASES)
                if kind is None:
                    kind = 'total'
                currency = 'usd' if any(part in {'usd', 'dollar', 'dollars'} for part in path) else 'unknown'
                costs[(kind, currency)] = num

            if 'session' in ancestor_text or leaf in SESSION_ALIASES:
                kind = _metric_kind_from_leaf(leaf, SESSION_ALIASES)
                if kind is None:
                    kind = 'total'
                sessions[kind] = num

        walk(payload, tuple())

        # If the payload itself is a list of sessions, count it.
        if isinstance(payload, list):
            sessions.setdefault('total', float(len(payload)))

        snapshot.usage_tokens = tokens
        snapshot.usage_cost = costs
        snapshot.usage_sessions = sessions


def main() -> int:
    logging.basicConfig(level=os.getenv('HERMES_EXPORTER_LOG_LEVEL', 'INFO').upper(), format='%(asctime)s %(levelname)s %(name)s %(message)s')
    base_url = os.getenv('HERMES_BASE_URL', DEFAULT_BASE_URL)
    port = _env_int('HERMES_EXPORTER_PORT', DEFAULT_PORT, minimum=1)
    interval = _env_float('HERMES_EXPORTER_INTERVAL', DEFAULT_INTERVAL, minimum=1.0)
    timeout = _env_float('HERMES_EXPORTER_TIMEOUT', DEFAULT_TIMEOUT, minimum=0.5)
    textfile_path = os.getenv('HERMES_EXPORTER_TEXTFILE_PATH', '').strip()

    exporter = HermesExporter(base_url=base_url, interval=interval, timeout=timeout, textfile_path=textfile_path)
    host = os.getenv('HERMES_EXPORTER_HOST', '127.0.0.1').strip() or '127.0.0.1'
    exporter.serve_forever(host=host, port=port)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

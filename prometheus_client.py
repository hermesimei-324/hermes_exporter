"""Minimal Prometheus text exposition helpers used by hermes_exporter.

This is intentionally tiny and self-contained so the exporter can run
without external Python packages.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

CONTENT_TYPE_LATEST = 'text/plain; version=0.0.4; charset=utf-8'


def _escape_help(text: str) -> str:
    return str(text).replace('\\', r'\\').replace('\n', r'\n')


def _escape_label_value(text: str) -> str:
    return (
        str(text)
        .replace('\\', r'\\')
        .replace('\n', r'\n')
        .replace('"', r'\"')
    )


class CollectorRegistry:
    def __init__(self, *args, **kwargs) -> None:
        self._metrics: List[_MetricBase] = []

    def register(self, metric: '_MetricBase') -> None:
        self._metrics.append(metric)

    def collect(self) -> List['_MetricBase']:
        return list(self._metrics)


class _LabelChild:
    def __init__(self, metric: '_MetricBase', values: Tuple[str, ...]) -> None:
        self._metric = metric
        self._values = values

    def set(self, value: float) -> None:
        self._metric._samples[self._values] = float(value)


class _MetricBase:
    _type = 'gauge'

    def __init__(self, name: str, documentation: str, labelnames: Sequence[str] = (), registry: Optional[CollectorRegistry] = None) -> None:
        self._name = name
        self._documentation = documentation
        self._labelnames = tuple(labelnames)
        self._samples: Dict[Tuple[str, ...], float] = {}
        if registry is not None:
            registry.register(self)

    def labels(self, **kwargs: str) -> _LabelChild:
        values = tuple(str(kwargs[name]) for name in self._labelnames)
        return _LabelChild(self, values)

    def set(self, value: float) -> None:
        if self._labelnames:
            raise TypeError(f'{self._name} requires labels: {self._labelnames}')
        self._samples[tuple()] = float(value)

    def _format_labels(self, values: Tuple[str, ...]) -> str:
        if not self._labelnames:
            return ''
        parts = [f'{name}="{_escape_label_value(value)}"' for name, value in zip(self._labelnames, values)]
        return '{' + ','.join(parts) + '}'

    def render(self) -> List[str]:
        lines = [f'# HELP {self._name} {_escape_help(self._documentation)}', f'# TYPE {self._name} {self._type}']
        for values, sample in sorted(self._samples.items()):
            label_text = self._format_labels(values)
            lines.append(f'{self._name}{label_text} {sample:g}')
        return lines


class Gauge(_MetricBase):
    pass


class Info(_MetricBase):
    _type = 'info'

    def info(self, mapping: Mapping[str, str]) -> None:
        ordered = tuple(str(mapping[key]) for key in sorted(mapping.keys()))
        # Preserve a deterministic order for rendering while still exposing the
        # original label names in sorted lexical order.
        self._labelnames = tuple(sorted(mapping.keys()))
        self._samples = {ordered: 1.0}


def generate_latest(registry: CollectorRegistry) -> bytes:
    lines: List[str] = []
    for metric in registry.collect():
        lines.extend(metric.render())
    if lines and not lines[-1].endswith('\n'):
        lines.append('')
    return ('\n'.join(lines) + '\n').encode('utf-8')

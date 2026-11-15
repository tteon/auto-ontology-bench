"""OpenTelemetry tracer that writes finished spans to traces/spans.jsonl (one JSON object per line)."""

from __future__ import annotations

import json
import threading
from collections.abc import Sequence
from pathlib import Path

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult

TRACE_FILE = Path(__file__).resolve().parents[1] / "traces" / "spans.jsonl"


class JsonlExporter(SpanExporter):
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.Lock()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        lines = []
        for s in spans:
            lines.append(json.dumps({
                "trace_id": f"{s.context.trace_id:032x}", "span_id": f"{s.context.span_id:016x}",
                "parent_id": f"{s.parent.span_id:016x}" if s.parent else None, "name": s.name,
                "start_ns": s.start_time, "end_ns": s.end_time,
                "duration_ms": (s.end_time - s.start_time) / 1e6 if s.end_time else None,
                "status": s.status.status_code.name, "attributes": dict(s.attributes or {}),
            }, default=str))
        with self.lock, self.path.open("a") as f:
            f.write("\n".join(lines) + "\n")
        return SpanExportResult.SUCCESS


_provider = TracerProvider(resource=Resource.create({"service.name": "aob-gateway"}))
_provider.add_span_processor(BatchSpanProcessor(JsonlExporter(TRACE_FILE)))
trace.set_tracer_provider(_provider)
tracer = trace.get_tracer("aob.gateway")


def flush() -> None:
    _provider.force_flush()

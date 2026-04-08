from __future__ import annotations

import json
import logging
import os
import secrets
import socket
import time
from dataclasses import dataclass, field
from typing import Any, Mapping

from opentelemetry import baggage, trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
    TraceFlags,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

logger = logging.getLogger(__name__)

SCOPE_NAME = "ton-indexer.observability"
SCOPE_VERSION = "1.0.0"
OTEL_DATA_FIELD = "otel_data"

ACTION_CLASSIFIER_SERVICE_NAME = "action-classifier"
ACTION_CLASSIFIER_SPAN_NAME = "ton.action_classifier.process"
ACTION_CLASSIFIER_SERVICE_STAGE = "action_classifier"

PROPAGATION_BAGGAGE_KEYS = (
    "ton.trace.external_message_hash_norm",
    "ton.trace.external_message_hash",
    "ton.trace.finality",
    "ton.processing.pass_id",
    "ton.trace.root_tx_hash",
)

_TRUE_VALUES = {"1", "true", "yes", "on"}
_PROPAGATOR = CompositePropagator(
    [TraceContextTextMapPropagator(), W3CBaggagePropagator()]
)
_TRACER = None
_SERVICE_INSTANCE_ID = socket.gethostname().strip()


def _flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE_VALUES


def tracing_enabled() -> bool:
    return (
        _flag("TON_OTEL_ENABLED")
        and not _flag("OTEL_SDK_DISABLED")
        and os.getenv("OTEL_TRACES_EXPORTER", "otlp").strip().lower() != "none"
    )


def measure_logs_enabled() -> bool:
    return False


def _build_exporter():
    protocol = (
        os.getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        or os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
        or "http/protobuf"
    ).strip().lower()

    if protocol in {"http", "http/protobuf", ""}:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        return OTLPSpanExporter()

    if protocol == "grpc":
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter as OTLPGrpcSpanExporter,
        )
        return OTLPGrpcSpanExporter()

    raise ValueError(f"Unsupported OTLP protocol: {protocol}")

def init_tracing() -> None:
    global _TRACER
    if _TRACER is not None or not tracing_enabled():
        return

    try:
        resource_attributes = {
            "service.namespace": "ton",
            "service.name": ACTION_CLASSIFIER_SERVICE_NAME,
        }
        if _SERVICE_INSTANCE_ID:
            resource_attributes["service.instance.id"] = _SERVICE_INSTANCE_ID

        provider = TracerProvider(
            resource=Resource.create(resource_attributes)
        )
        provider.add_span_processor(BatchSpanProcessor(_build_exporter()))
        _TRACER = provider.get_tracer(SCOPE_NAME, SCOPE_VERSION)
    except Exception:
        logger.exception(
            "Failed to initialize tracing for %s",
            ACTION_CLASSIFIER_SERVICE_NAME,
        )


def new_pass_id() -> str:
    return secrets.token_hex(16)


def _normalized_pass_id(value: Any) -> str:
    if not isinstance(value, str):
        return ""

    pass_id = value.strip().lower()
    if len(pass_id) != 32:
        return ""

    try:
        trace_id = int(pass_id, 16)
    except ValueError:
        return ""

    if trace_id == 0:
        return ""

    return pass_id


def _root_context_from_pass_id(pass_id: str, base_context: Any) -> Any:
    normalized_pass_id = _normalized_pass_id(pass_id)
    if not normalized_pass_id:
        return base_context

    trace_id = int(normalized_pass_id, 16)
    span_id = secrets.randbits(64) or 1
    span_context = SpanContext(
        trace_id=trace_id,
        span_id=span_id,
        is_remote=True,
        trace_flags=TraceFlags(0x01),
    )
    return trace.set_span_in_context(NonRecordingSpan(span_context), base_context)


def _extract_stage_context(
    trace_hash: Mapping[str, Any],
) -> tuple[Any, dict[str, str], bool]:
    raw = trace_hash.get(OTEL_DATA_FIELD)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")

    payload = {}
    if raw:
        try:
            payload = json.loads(str(raw))
        except (TypeError, json.JSONDecodeError):
            pass

    carrier = {
        key: str(value)
        for key in ("traceparent", "tracestate", "baggage")
        if (value := payload.get(key))
    }

    parent_context = _PROPAGATOR.extract(carrier=carrier) if carrier else Context()
    baggage_values = {
        key: str(value)
        for key, value in baggage.get_all(context=parent_context).items()
    }
    context_missing = not trace.get_current_span(parent_context).get_span_context().is_valid

    return parent_context, baggage_values, context_missing


def _redis_otel_fields(context: Any, attributes: Mapping[str, Any]) -> dict[str, str]:
    for key in PROPAGATION_BAGGAGE_KEYS:
        value = attributes.get(key)
        if value not in (None, ""):
            context = baggage.set_baggage(key, str(value), context=context)

    carrier: dict[str, str] = {}
    _PROPAGATOR.inject(carrier=carrier, context=context)

    return {
        OTEL_DATA_FIELD: json.dumps(
            {
                "traceparent": carrier.get("traceparent", ""),
                "tracestate": carrier.get("tracestate", ""),
                "baggage": carrier.get("baggage", ""),
            },
            separators=(",", ":"),
        )
    }


@dataclass(slots=True)
class StageSpan:
    attributes: dict[str, Any] = field(default_factory=dict)
    context: Any = None
    span: Any = None
    error_type: str | None = None
    error_message: str | None = None
    ended: bool = False

    def add_attr(self, key: str, value: Any | None) -> None:
        if value not in (None, ""):
            self.attributes[key] = value

    def mark_error(self, error_type: str, message: str | None = None) -> None:
        self.error_type = error_type
        self.error_message = message
        self.attributes["ton.error.type"] = error_type

    def propagation_fields(self) -> dict[str, str]:
        if self.span is None:
            return {}
        return _redis_otel_fields(self.context, self.attributes)

    def emit(self) -> None:
        if self.span is None or self.ended:
            return

        self.span.set_attributes(self.attributes)

        if self.error_type:
            self.span.set_status(
                Status(StatusCode.ERROR, self.error_message or self.error_type)
            )

        self.span.end(end_time=time.time_ns())
        self.ended = True


def new_stage(*, start_time_ns: int, trace_hash: Mapping[str, Any]) -> StageSpan:
    parent_context, baggage_values, context_missing = _extract_stage_context(trace_hash)
    parent_span_context = trace.get_current_span(parent_context).get_span_context()

    attributes: dict[str, Any] = {
        "ton.processing.pipeline": "trace_to_actions_to_stream",
        "ton.processing.service_stage": ACTION_CLASSIFIER_SERVICE_STAGE,
        "transaction.type": "processing",
    }
    if _SERVICE_INSTANCE_ID:
        attributes["service.instance.id"] = _SERVICE_INSTANCE_ID

    for key in PROPAGATION_BAGGAGE_KEYS:
        if value := baggage_values.get(key):
            attributes[key] = value

    pass_id = _normalized_pass_id(attributes.get("ton.processing.pass_id"))
    if parent_span_context.is_valid:
        pass_id = format(parent_span_context.trace_id, "032x")
    if not pass_id:
        pass_id = new_pass_id()
    attributes["ton.processing.pass_id"] = pass_id

    if context_missing:
        attributes["ton.processing.context_missing"] = True
        parent_context = _root_context_from_pass_id(pass_id, parent_context)

    span = None
    context = parent_context

    if _TRACER is None and tracing_enabled():
        init_tracing()

    if _TRACER is not None:
        span = _TRACER.start_span(
            name=ACTION_CLASSIFIER_SPAN_NAME,
            context=parent_context,
            kind=SpanKind.CONSUMER,
            start_time=start_time_ns,
        )
        context = trace.set_span_in_context(span, parent_context)

    return StageSpan(attributes=attributes, context=context, span=span)

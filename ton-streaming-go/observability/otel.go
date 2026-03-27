package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	scopeName             = "ton-indexer.observability"
	scopeVersion          = "1.0.0"
	otelDataField         = "otel_data"
	streamingServiceName  = "ton-streaming-api"
	streamingSpanName     = "ton.streaming_api.process"
	streamingServiceStage = "streaming_api"
)

var (
	propagationBaggageKeys = []string{
		"ton.trace.external_message_hash",
		"ton.trace.finality",
		"ton.processing.pass_id",
		"ton.trace.root_tx_hash",
	}

	redisPropagator = propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	tracerOnce    sync.Once
	processTracer trace.Tracer
)

type otelData struct {
	Traceparent string `json:"traceparent"`
	Tracestate  string `json:"tracestate"`
	Baggage     string `json:"baggage"`
}

type StageSpan struct {
	Attributes   map[string]any
	ErrorType    string
	ErrorMessage string

	context context.Context
	span    trace.Span
	ended   bool
}

func NowUnixNano() int64 {
	return time.Now().UnixNano()
}

func TracingEnabled() bool {
	if !envFlag("TON_OTEL_ENABLED", false) {
		return false
	}
	if envFlag("OTEL_SDK_DISABLED", false) {
		return false
	}
	return strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER"))) != "none"
}

func MeasureLogsEnabled() bool {
	return envFlag("MEASURE_LOGS_ENABLED", false)
}

func NewStage(startTimeUnix int64, traceHash map[string]string) *StageSpan {
	parentContext, baggageValues, contextMissing := extractStageContext(traceHash)

	attributes := map[string]any{
		"ton.processing.pipeline":      "trace_to_actions_to_stream",
		"ton.processing.service_stage": streamingServiceStage,
	}

	for _, key := range propagationBaggageKeys {
		if value := baggageValues[key]; value != "" {
			attributes[key] = value
		}
	}

	if _, ok := attributes["ton.processing.pass_id"]; !ok {
		attributes["ton.processing.pass_id"] = randomHex(16)
	}

	if contextMissing {
		attributes["ton.processing.context_missing"] = true
	}

	stage := &StageSpan{
		Attributes: attributes,
		context:    parentContext,
	}

	tracer := currentTracer()
	if tracer == nil {
		return stage
	}

	spanContext, span := tracer.Start(
		parentContext,
		streamingSpanName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(time.Unix(0, startTimeUnix)),
	)

	stage.context = spanContext
	stage.span = span
	return stage
}

func (s *StageSpan) AddAttr(key string, value any) {
	if s == nil || value == nil {
		return
	}
	if str, ok := value.(string); ok && str == "" {
		return
	}
	s.Attributes[key] = value
}

func (s *StageSpan) MarkError(errorType, message string) {
	if s == nil {
		return
	}
	s.ErrorType = errorType
	s.ErrorMessage = message
	s.Attributes["ton.error.type"] = errorType
}

func (s *StageSpan) PreparePropagationFields() map[string]string {
	if s == nil || s.span == nil {
		return nil
	}
	return redisOtelFields(s.context, s.Attributes)
}

func (s *StageSpan) Emit() {
	if s == nil || s.span == nil || s.ended {
		return
	}

	s.span.SetAttributes(attributesToKeyValues(s.Attributes)...)

	if s.ErrorType != "" {
		message := s.ErrorMessage
		if message == "" {
			message = s.ErrorType
		}
		s.span.SetStatus(codes.Error, message)
	}

	s.span.End()
	s.ended = true
}

func extractStageContext(traceHash map[string]string) (context.Context, map[string]string, bool) {
	raw := strings.TrimSpace(traceHash[otelDataField])

	var payload otelData
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}

	carrier := propagation.MapCarrier{}
	if payload.Traceparent != "" {
		carrier.Set("traceparent", payload.Traceparent)
	}
	if payload.Tracestate != "" {
		carrier.Set("tracestate", payload.Tracestate)
	}
	if payload.Baggage != "" {
		carrier.Set("baggage", payload.Baggage)
	}

	parentContext := redisPropagator.Extract(context.Background(), carrier)

	baggageValues := map[string]string{}
	for _, member := range baggage.FromContext(parentContext).Members() {
		baggageValues[member.Key()] = member.Value()
	}

	contextMissing := !trace.SpanContextFromContext(parentContext).IsValid()
	return parentContext, baggageValues, contextMissing
}

func redisOtelFields(ctx context.Context, attributes map[string]any) map[string]string {
	if ctx == nil {
		ctx = context.Background()
	}

	merged := map[string]baggage.Member{}
	for _, member := range baggage.FromContext(ctx).Members() {
		merged[member.Key()] = member
	}

	for _, key := range propagationBaggageKeys {
		value := strings.TrimSpace(asString(attributes[key]))
		if value == "" {
			continue
		}

		member, err := baggage.NewMember(key, value)
		if err != nil {
			continue
		}
		merged[key] = member
	}

	members := make([]baggage.Member, 0, len(merged))
	for _, member := range merged {
		members = append(members, member)
	}

	if len(members) > 0 {
		bg, err := baggage.New(members...)
		if err == nil {
			ctx = baggage.ContextWithBaggage(ctx, bg)
		}
	}

	carrier := propagation.MapCarrier{}
	redisPropagator.Inject(ctx, carrier)

	payload, err := json.Marshal(otelData{
		Traceparent: carrier.Get("traceparent"),
		Tracestate:  carrier.Get("tracestate"),
		Baggage:     carrier.Get("baggage"),
	})
	if err != nil {
		return nil
	}

	return map[string]string{
		otelDataField: string(payload),
	}
}

func currentTracer() trace.Tracer {
	if !TracingEnabled() {
		return nil
	}

	tracerOnce.Do(func() {
		processTracer = buildTracer()
	})

	return processTracer
}

func buildTracer() trace.Tracer {
	exporter := buildExporter()
	if exporter == nil {
		return nil
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.namespace", "ton"),
			attribute.String("service.name", streamingServiceName),
		)),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter)),
	)

	return provider.Tracer(
		scopeName,
		trace.WithInstrumentationVersion(scopeVersion),
	)
}

func buildExporter() sdktrace.SpanExporter {
	protocol := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")))
	if protocol == "" {
		protocol = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	if protocol == "" {
		protocol = "http/protobuf"
	}

	switch protocol {
	case "http", "http/protobuf":
		exporter, err := otlptracehttp.New(context.Background())
		if err != nil {
			log.Printf(
				"[otel] failed to create OTLP HTTP exporter for %s: %v",
				streamingServiceName,
				err,
			)
			return nil
		}
		return exporter

	case "grpc":
		exporter, err := otlptracegrpc.New(context.Background())
		if err != nil {
			log.Printf(
				"[otel] failed to create OTLP gRPC exporter for %s: %v",
				streamingServiceName,
				err,
			)
			return nil
		}
		return exporter

	default:
		log.Printf(
			"[otel] unsupported OTLP traces protocol %q for %s; configure OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf or grpc",
			protocol,
			streamingServiceName,
		)
		return nil
	}
}

func envFlag(name string, defaultValue bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if raw == "" {
		return defaultValue
	}

	switch raw {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func randomHex(size int) string {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return strings.Repeat("0", size*2)
	}
	return hex.EncodeToString(buf)
}

func attributesToKeyValues(attributes map[string]any) []attribute.KeyValue {
	items := make([]attribute.KeyValue, 0, len(attributes))

	for key, value := range attributes {
		switch typed := value.(type) {
		case bool:
			items = append(items, attribute.Bool(key, typed))
		case int:
			items = append(items, attribute.Int(key, typed))
		case int64:
			items = append(items, attribute.Int64(key, typed))
		default:
			text := asString(value)
			if text != "" {
				items = append(items, attribute.String(key, text))
			}
		}
	}

	return items
}

func asString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

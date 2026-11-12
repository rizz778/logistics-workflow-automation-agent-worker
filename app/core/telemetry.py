from core.config import settings
from core.logging import logger

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
except ImportError:
    trace = None
    TracerProvider = None
    BatchSpanProcessor = None
    Resource = None
    OTLPSpanExporter = None
    FastAPIInstrumentor = None
    TraceContextTextMapPropagator = None

tracer = None

def setup_telemetry(app=None):
    """Initializes OpenTelemetry distributed tracing."""
    global tracer

    if not settings.otel_tracing_enabled or not trace:
        logger.info("OpenTelemetry tracing is disabled.")
        return

    try:
        resource = Resource.create({
            "service.name": settings.otel_service_name,
            "environment": settings.app_env
        })

        provider = TracerProvider(resource=resource)
        
        # Configure OTLP Exporter (Jaeger / Tempo / Datadog)
        exporter_kwargs = {}
        if settings.otel_exporter_otlp_endpoint:
            exporter_kwargs["endpoint"] = settings.otel_exporter_otlp_endpoint
            exporter_kwargs["insecure"] = True

        otlp_exporter = OTLPSpanExporter(**exporter_kwargs)
        processor = BatchSpanProcessor(otlp_exporter)
        provider.add_span_processor(processor)

        trace.set_tracer_provider(provider)
        tracer = trace.get_tracer(settings.otel_service_name)

        if app and FastAPIInstrumentor:
            FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)

        logger.info(f"OpenTelemetry tracing initialized. Service: {settings.otel_service_name}")
    except Exception as e:
        logger.warning(f"Failed to initialize OpenTelemetry: {e}. Running without tracing.")

def get_tracer():
    """Returns the active tracer or a no-op tracer."""
    global tracer
    if tracer:
        return tracer
    if trace:
        return trace.get_tracer(settings.otel_service_name)
    return None

def inject_trace_context(carrier: dict) -> dict:
    """Injects W3C traceparent into a dictionary (e.g. queue message payload)
    so the Agent Worker continues the distributed trace.
    """
    if TraceContextTextMapPropagator:
        TraceContextTextMapPropagator().inject(carrier)
    return carrier


import logging
import json
import sys
from datetime import datetime, timezone
from core.config import settings

try:
    from opentelemetry import trace
except ImportError:
    trace = None

class JSONLogFormatter(logging.Formatter):
    """Formats log records as JSON and injects OpenTelemetry trace/span IDs."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "app": settings.app_name,
            "env": settings.app_env
        }

        # Inject OpenTelemetry trace and span IDs if active
        if trace:
            current_span = trace.get_current_span()
            if current_span and current_span.get_span_context().is_valid:
                span_ctx = current_span.get_span_context()
                log_data["trace_id"] = format(span_ctx.trace_id, "032x")
                log_data["span_id"] = format(span_ctx.span_id, "016x")

        # Include exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)

def setup_logging():
    """Initializes structured logging for the application."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONLogFormatter())

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if settings.debug else logging.INFO)
    
    # Remove existing default handlers to avoid duplicate output
    root_logger.handlers = [handler]

    # Silence overly verbose third-party loggers
    logging.getLogger("uvicorn.access").handlers = [handler]
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(settings.app_name)


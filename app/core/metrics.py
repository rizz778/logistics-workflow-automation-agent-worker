from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response

# 1. HTTP Traffic Metrics
HTTP_REQUESTS_TOTAL = Counter(
    "gateway_http_requests_total",
    "Total HTTP requests received by the gateway",
    ["method", "endpoint", "status"]
)

HTTP_REQUEST_DURATION = Histogram(
    "gateway_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"]
)

# 2. Ingestion & Cache Metrics
UPLOADS_TOTAL = Counter(
    "gateway_uploads_total",
    "Total document batches uploaded",
    ["source"]  # 'manual_upload' | 'inbox'
)

CACHE_HITS_TOTAL = Counter(
    "gateway_cache_hits_total",
    "Total document batches matching Redis cache (bypassing LLM)"
)

CACHE_MISSES_TOTAL = Counter(
    "gateway_cache_misses_total",
    "Total document batches requiring worker LLM processing"
)

# 3. Downstream Infrastructure Metrics
S3_UPLOAD_DURATION = Histogram(
    "gateway_s3_upload_duration_seconds",
    "Time taken to stream files to S3/MinIO in seconds"
)

QUEUE_PUSH_DURATION = Histogram(
    "gateway_queue_push_duration_seconds",
    "Time taken to push a job to the Redis queue in seconds"
)

def metrics_response() -> Response:
    """Returns Prometheus formatted metrics."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


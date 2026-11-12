import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from core.config import settings
from core.logging import setup_logging, logger
from core.telemetry import setup_telemetry
from core.metrics import (
    metrics_response,
    HTTP_REQUESTS_TOTAL,
    HTTP_REQUEST_DURATION
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Structured JSON Logging
    setup_logging()
    logger.info(f"Starting {settings.app_name} in [{settings.app_env}] mode...")

    # Initialize Distributed Tracing
    setup_telemetry(app)
    
    yield

    logger.info(f"Shutting down {settings.app_name}...")

app = FastAPI(
    title=settings.app_name,
    debug=settings.debug,
    lifespan=lifespan
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus Metrics Middleware
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    if not settings.enable_metrics or request.url.path == "/metrics":
        return await call_next(request)

    start_time = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start_time

    endpoint = request.url.path
    status = str(response.status_code)
    method = request.method

    HTTP_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status=status).inc()
    HTTP_REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)

    return response

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": settings.app_name,
        "environment": settings.app_env
    }

@app.get("/healthz")
async def health_check():
    return {
        "status": "healthy",
        "app": settings.app_name
    }

@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics scrape endpoint."""
    if not settings.enable_metrics:
        return {"metrics": "disabled"}
    return metrics_response()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=settings.port, reload=settings.debug)

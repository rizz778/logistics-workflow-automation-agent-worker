import sys
import asyncio
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

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
from db.pool import init_db_pool, close_db_pool, init_db_schema, check_db_health
from db.redis import init_redis_pool, close_redis_pool, check_redis_health
from services.s3_service import s3_service
from api.router import api_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Structured JSON Logging
    setup_logging()
    logger.info(f"Starting {settings.app_name} in [{settings.app_env}] mode...")

    # Initialize Distributed Tracing
    setup_telemetry(app)

    # Initialize PostgreSQL Connection Pool and Verify Schema
    await init_db_pool()
    await init_db_schema()

    # Initialize Redis Connection Pool
    await init_redis_pool()

    # Verify or initialize S3 bucket
    s3_service.ensure_bucket_exists()

    # Launch Embedded Agent Worker (Queue consumer) if enabled
    worker_task = None
    worker_daemon = None
    if getattr(settings, "run_embedded_worker", False):
        try:
            from pathlib import Path
            _app_dir = Path(__file__).resolve().parent    # .../app/
            _repo_root = _app_dir.parent                  # repo root (gateway repo OR parent monorepo)
            # Render: agent/ is at repo root (copied alongside app/)
            # Local monorepo: agent is at ../../agent/logistics-workflow-automation-agent1
            agent_path = _repo_root / "agent"
            if not agent_path.exists():
                agent_path = _repo_root.parent / "agent" / "logistics-workflow-automation-agent1"
            if str(agent_path) not in sys.path:
                sys.path.insert(0, str(agent_path))

            from services.worker_daemon import TradeDocWorkerDaemon
            worker_daemon = TradeDocWorkerDaemon()
            worker_task = asyncio.create_task(worker_daemon.start())
            logger.info("Embedded Trade Document Agent Worker started in background task.")
        except Exception as we:
            logger.warning(f"Could not start embedded agent worker: {we}")

    yield

    # Clean up on shutdown
    logger.info("Shutting down and draining database and Redis pools...")
    if worker_daemon:
        try:
            worker_daemon.stop()
        except Exception:
            pass
    if worker_task and not worker_task.done():
        worker_task.cancel()
    await close_redis_pool()
    await close_db_pool()
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

# Mount API Routers
app.include_router(api_router)

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
    db_ok = await check_db_health()
    storage_ok = s3_service.check_storage_health()
    redis_ok = await check_redis_health()
    all_healthy = db_ok and storage_ok and redis_ok

    return {
        "status": "healthy" if all_healthy else "degraded",
        "app": settings.app_name,
        "database": "connected" if db_ok else "disconnected",
        "storage": "connected" if storage_ok else "disconnected",
        "redis": "connected" if redis_ok else "disconnected"
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

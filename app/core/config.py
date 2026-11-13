import os
from typing import List, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Application
    app_name: str = "Trade Document Gateway"
    app_env: str = "production"
    port: int = 8000
    debug: bool = False
    allowed_origins: List[str] = ["*"]

    # PostgreSQL Database
    database_url: str
    db_pool_min: int = 2
    db_pool_max: int = 10

    # Redis (Caching & Task Queue)
    redis_url: str
    redis_queue_name: str = "trade_doc_tasks"
    redis_consumer_group: str = "trade_doc_worker_group"
    redis_consumer_name: str = "worker-1"
    cache_ttl_days: int = 7

    # Object Storage (MinIO local or AWS S3)
    s3_endpoint_url: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_bucket_name: str = "trade-documents"
    s3_region: str = "ap-northeast-1"

    # LLM Settings
    default_llm_provider: str = "mistral"
    gemini_api_key: Optional[str] = None
    gemini_model: str = "gemini-1.5-flash"
    mistral_api_key: Optional[str] = None
    mistral_ocr_model: str = "mistral-ocr-latest"
    mistral_extract_model: str = "pixtral-large-latest"

    # Observability, Metrics & Distributed Tracing
    enable_metrics: bool = False
    otel_tracing_enabled: bool = False
    otel_service_name: str = "trade-doc-gateway"
    otel_exporter_otlp_endpoint: Optional[str] = None

    # Rate Limiting (Requests Per Minute)
    rate_limit_enabled: bool = False
    rate_limit_upload_rpm: int = 30
    rate_limit_query_rpm: int = 60
    rate_limit_status_rpm: int = 120

    # Embedded Agent Worker
    run_embedded_worker: bool = True

    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()

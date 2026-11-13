import os
from typing import List, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Application
    app_name: str
    app_env: str
    port: int
    debug: bool
    allowed_origins: List[str]

    # PostgreSQL Database
    database_url: str
    db_pool_min: int
    db_pool_max: int

    # Redis (Caching & Task Queue)
    redis_url: str
    redis_queue_name: str
    cache_ttl_days: int

    # Object Storage (MinIO local or AWS S3)
    s3_endpoint_url: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_bucket_name: str
    s3_region: str

    # Text-to-SQL Assistant Settings
    default_llm_provider: str
    gemini_api_key: Optional[str] = None
    gemini_model: str

    # Observability, Metrics & Distributed Tracing
    enable_metrics: bool
    otel_tracing_enabled: bool
    otel_service_name: str
    otel_exporter_otlp_endpoint: Optional[str] = None

    # Rate Limiting (Requests Per Minute)
    rate_limit_enabled: bool
    rate_limit_upload_rpm: int
    rate_limit_query_rpm: int
    rate_limit_status_rpm: int

    # Embedded Agent Worker
    run_embedded_worker: bool = True

    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()

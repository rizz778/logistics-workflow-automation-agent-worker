import boto3
import socket
from urllib.parse import urlparse
from botocore.config import Config
from botocore.exceptions import ClientError
from core.config import settings
from core.logging import logger
from interfaces.storage import ObjectStorageInterface

class S3StorageService(ObjectStorageInterface):
    """S3 and MinIO Object Storage implementation using boto3."""

    def __init__(self):
        self.bucket = settings.s3_bucket_name
        self.region = settings.s3_region
        
        # Build client kwargs based on environment settings
        client_kwargs = {
            "service_name": "s3",
            "region_name": self.region,
            "config": Config(signature_version="s3v4")
        }

        # If MinIO or custom endpoint is specified
        if settings.s3_endpoint_url:
            client_kwargs["endpoint_url"] = settings.s3_endpoint_url

        # If explicit credentials are provided (otherwise boto3 uses AWS IAM roles)
        if settings.s3_access_key_id and settings.s3_secret_access_key:
            client_kwargs["aws_access_key_id"] = settings.s3_access_key_id
            client_kwargs["aws_secret_access_key"] = settings.s3_secret_access_key

        self.client = boto3.client(**client_kwargs)

    def generate_presigned_upload_url(
        self,
        s3_key: str,
        content_type: str,
        expires_in: int = 900
    ) -> str:
        """Generates an authorized S3 presigned PUT URL for direct client upload."""
        try:
            url = self.client.generate_presigned_url(
                ClientMethod="put_object",
                Params={
                    "Bucket": self.bucket,
                    "Key": s3_key,
                    "ContentType": content_type
                },
                ExpiresIn=expires_in
            )
            return url
        except Exception as e:
            logger.error(f"Failed to generate presigned upload URL for {s3_key}: {e}")
            raise

    def generate_presigned_download_url(
        self,
        s3_key: str,
        expires_in: int = 3600
    ) -> str:
        """Generates an authorized S3 presigned GET URL for viewing the file."""
        try:
            url = self.client.generate_presigned_url(
                ClientMethod="get_object",
                Params={
                    "Bucket": self.bucket,
                    "Key": s3_key
                },
                ExpiresIn=expires_in
            )
            return url
        except Exception as e:
            logger.error(f"Failed to generate presigned download URL for {s3_key}: {e}")
            raise

    def verify_file_exists(self, s3_key: str) -> bool:
        """Verifies if an object actually exists in S3 (used for confirmation)."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            logger.warning(f"Error checking object {s3_key} in S3: {e}")
            return False

    def upload_bytes(self, s3_key: str, data: bytes, content_type: str = "application/pdf") -> None:
        """Uploads raw bytes directly to S3 / MinIO."""
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=data,
                ContentType=content_type
            )
            logger.info(f"Successfully uploaded {len(data)} bytes to S3 object '{s3_key}'.")
        except Exception as e:
            logger.error(f"Failed to upload bytes to S3 '{s3_key}': {e}")
            raise

    def download_bytes(self, s3_key: str) -> bytes:
        """Downloads raw bytes of an object from S3 / MinIO."""
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=s3_key)
            return response["Body"].read()
        except Exception as e:
            logger.error(f"Failed to download bytes from S3 '{s3_key}': {e}")
            raise

    def _is_storage_reachable(self, timeout: float = 1.5) -> bool:
        """Fast socket probe to check if S3/MinIO endpoint is active.
        For remote HTTPS endpoints (e.g. Supabase, AWS), always returns True
        since they are assumed reachable. Only probes localhost endpoints.
        """
        if not settings.s3_endpoint_url:
            return True
        try:
            parsed = urlparse(settings.s3_endpoint_url)
            host = parsed.hostname or "127.0.0.1"
            # Only probe localhost - remote cloud endpoints are assumed reachable
            if host not in ("localhost", "127.0.0.1"):
                return True
            # Determine correct port: use parsed port, then scheme default, then 9000
            if parsed.port:
                port = parsed.port
            elif parsed.scheme == "https":
                port = 443
            elif parsed.scheme == "http":
                port = 80
            else:
                port = 9000
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception:
            return False

    def ensure_bucket_exists(self) -> None:
        """Auto-creates the bucket if missing (ideal for MinIO in local dev) and ensures CORS is configured."""
        if not self._is_storage_reachable():
            logger.warning("MinIO/S3 storage is not running on port 9000. Continuing startup.")
            return

        try:
            self.client.head_bucket(Bucket=self.bucket)
            logger.info(f"S3/MinIO bucket '{self.bucket}' verified.")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code in ("404", "NoSuchBucket"):
                logger.info(f"Bucket '{self.bucket}' not found. Creating it...")
                try:
                    if self.region == "us-east-1":
                        self.client.create_bucket(Bucket=self.bucket)
                    else:
                        self.client.create_bucket(
                            Bucket=self.bucket,
                            CreateBucketConfiguration={"LocationConstraint": self.region}
                        )
                    logger.info(f"Bucket '{self.bucket}' created successfully.")
                except Exception as create_err:
                    logger.error(f"Failed to auto-create bucket '{self.bucket}': {create_err}")
            else:
                logger.warning(f"Could not connect to S3 bucket '{self.bucket}': {e}")
        except Exception as conn_err:
            logger.warning(f"S3/MinIO storage is not running ({conn_err}). Continuing startup.")
            return

        # Ensure CORS is configured for direct browser presigned PUT uploads
        try:
            cors_configuration = {
                'CORSRules': [{
                    'AllowedHeaders': ['*'],
                    'AllowedMethods': ['GET', 'PUT', 'POST', 'HEAD'],
                    'AllowedOrigins': ['*'],
                    'ExposeHeaders': ['ETag']
                }]
            }
            self.client.put_bucket_cors(Bucket=self.bucket, CORSConfiguration=cors_configuration)
            logger.info(f"S3/MinIO bucket '{self.bucket}' CORS policy configured for direct browser uploads.")
        except Exception as cors_err:
            logger.debug(f"Could not set bucket CORS (may already be set or not supported): {cors_err}")

    def check_storage_health(self) -> bool:
        """Healthcheck verification probe."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except Exception as e:
            logger.warning(f"Storage health check failed: {e}")
            return False

s3_service: ObjectStorageInterface = S3StorageService()


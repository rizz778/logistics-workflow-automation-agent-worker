import boto3
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

    def ensure_bucket_exists(self) -> None:
        """Auto-creates the bucket if missing (ideal for MinIO in local dev)."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            logger.info(f"S3/MinIO bucket '{self.bucket}' verified.")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
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

    def check_storage_health(self) -> bool:
        """Healthcheck verification probe."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except Exception as e:
            logger.warning(f"Storage health check failed: {e}")
            return False

s3_service: ObjectStorageInterface = S3StorageService()


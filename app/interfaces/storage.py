from abc import ABC, abstractmethod

class ObjectStorageInterface(ABC):
    """Abstract interface defining object storage operations for S3 / MinIO."""

    @abstractmethod
    def generate_presigned_upload_url(
        self,
        s3_key: str,
        content_type: str,
        expires_in: int = 900
    ) -> str:
        """Generates an authorized S3 presigned PUT URL allowing clients to upload directly to S3."""
        pass

    @abstractmethod
    def generate_presigned_download_url(
        self,
        s3_key: str,
        expires_in: int = 3600
    ) -> str:
        """Generates a temporary S3 presigned GET URL for viewing/downloading the document."""
        pass

    @abstractmethod
    def verify_file_exists(self, s3_key: str) -> bool:
        """Checks if a specific object key exists in the bucket."""
        pass

    @abstractmethod
    def upload_bytes(self, s3_key: str, data: bytes, content_type: str = "application/pdf") -> None:
        """Uploads raw bytes directly to the object storage bucket."""
        pass

    @abstractmethod
    def ensure_bucket_exists(self) -> None:
        """Verifies or auto-initializes the destination bucket (useful for local MinIO)."""
        pass

    @abstractmethod
    def check_storage_health(self) -> bool:
        """Pings the S3 storage endpoint to verify connectivity."""
        pass


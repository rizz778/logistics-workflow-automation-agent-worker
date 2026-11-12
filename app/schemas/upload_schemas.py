from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class FilePresignItem(BaseModel):
    filename: str = Field(description="Name of the file (e.g. bol.pdf)")
    content_type: str = Field(default="application/pdf", description="MIME type of the file")
    file_hash: Optional[str] = Field(default=None, description="SHA-256 hash of file bytes calculated on client")

class PresignedUrlsRequest(BaseModel):
    files: List[FilePresignItem] = Field(min_length=1, description="List of files to upload for the shipment")
    source: str = Field(default="manual_upload", description="'manual_upload' or 'inbox'")
    email_sender: Optional[str] = None
    email_metadata: Optional[Dict[str, Any]] = None

class PresignedUrlResponseItem(BaseModel):
    filename: str
    s3_key: str
    upload_url: str

class PresignedUrlsResponse(BaseModel):
    run_id: int
    shipment_id: str
    status: str
    is_cached: bool = False
    files: List[PresignedUrlResponseItem]
    cached_results: Optional[Dict[str, Any]] = None

class ConfirmUploadRequest(BaseModel):
    shipment_id: str = Field(description="The shipment identifier whose files were uploaded directly to S3")

class ConfirmUploadResponse(BaseModel):
    shipment_id: str
    status: str
    message: str
    all_files_ready: bool


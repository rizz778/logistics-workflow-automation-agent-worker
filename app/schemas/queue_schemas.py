from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field

TaskEventType = Literal["INITIAL_PROCESS", "RETRY", "HUMAN_EDIT_REPROCESS"]

class TaskFileItem(BaseModel):
    filename: str
    s3_key: str
    file_hash: Optional[str] = None
    content_type: Optional[str] = "application/pdf"

class ShipmentTaskEvent(BaseModel):
    """Payload contract pushed to Redis Queue for Agent Worker consumption."""
    run_id: int
    shipment_id: str
    event_type: TaskEventType = "INITIAL_PROCESS"
    retry_count: int = Field(default=0, ge=0)
    max_retries: int = Field(default=3, ge=1)
    files: List[TaskFileItem]
    source: str = "manual_upload"
    email_metadata: Optional[Dict[str, Any]] = None
    traceparent: Optional[str] = None
    error_history: List[str] = Field(default_factory=list)


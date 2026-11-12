from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

class UpdateStatusRequest(BaseModel):
    status: str = Field(description="Updated shipment status: 'approved', 'flagged', or 'amendment_requested'")
    edited_data: Optional[Dict[str, Any]] = Field(default=None, description="Human auditor field overrides")
    amendment_draft: Optional[str] = Field(default=None, description="Updated supplier email draft")


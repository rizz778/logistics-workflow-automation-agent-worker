from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class QueryRequest(BaseModel):
    query: str = Field(min_length=3, max_length=500, description="Plain English question about shipment compliance runs")

class QueryResponse(BaseModel):
    success: bool
    sql: Optional[str] = None
    results: Optional[List[Dict[str, Any]]] = None
    answer: str
    error: Optional[str] = None


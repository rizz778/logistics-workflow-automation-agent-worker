from fastapi import APIRouter, Depends
from schemas.query_schemas import QueryRequest, QueryResponse
from services.nl_query_service import nl_query_service
from services.rate_limiter import rate_limit_dependency
from core.config import settings

router = APIRouter(prefix="/pipeline", tags=["Natural Language Query Assistant"])

@router.post(
    "/query",
    response_model=QueryResponse,
    dependencies=[Depends(rate_limit_dependency("query", settings.rate_limit_query_rpm, 60))]
)
async def query_pipeline_database(request: QueryRequest):
    """Translates plain English questions into safe PostgreSQL queries, executes them,
    and returns synthesized insights alongside the executed SQL and raw table rows.
    Guarded against prompt injection and unauthorized data modification.
    """
    return await nl_query_service.execute_query(request.query)

import json
from typing import List, Optional, Dict, Any
from db.pool import get_db_connection
from interfaces.repository import PipelineRunRepository
from interfaces.queries import (
    INSERT_QUEUED_RUN_SQL,
    INSERT_CACHED_RUN_SQL,
    SELECT_RUN_BY_ID_SQL,
    SELECT_INCOMING_RUNS_SQL,
    SELECT_PROCESSED_RUNS_SQL,
    UPDATE_RUN_STATUS_SQL,
    COUNT_TOTAL_RUNS_SQL,
    COUNT_DECISIONS_GROUPED_SQL,
    COUNT_STATUSES_GROUPED_SQL
)

class PostgresPipelineRunRepository(PipelineRunRepository):
    """PostgreSQL implementation of the PipelineRunRepository interface."""

    async def create_queued_run(
        self,
        filenames: List[str],
        source: str = "manual_upload",
        shipment_id: Optional[str] = None,
        email_sender: Optional[str] = None,
        email_metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Inserts an initial shipment record with 'queued' status and returns its generated ID."""
        filename_str = ", ".join(filenames)
        email_meta_json = json.dumps(email_metadata) if email_metadata else None

        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    INSERT_QUEUED_RUN_SQL,
                    (shipment_id, filename_str, source, email_sender, email_meta_json)
                )
                row = await cur.fetchone()
                return row["id"]

    async def create_cached_run(
        self,
        cached_data: Dict[str, Any],
        filenames: List[str],
        source: str = "manual_upload",
        shipment_id: Optional[str] = None,
        email_sender: Optional[str] = None,
        email_metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Clones a cached pipeline result into a new run record with zero worker latency."""
        filename_str = ", ".join(filenames)
        decision = cached_data.get("decision", "flag_review")
        status = "approved" if decision == "auto_approve" else "pending_review"

        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    INSERT_CACHED_RUN_SQL,
                    (
                        shipment_id,
                        filename_str,
                        json.dumps(cached_data.get("extracted_data", {})),
                        json.dumps(cached_data.get("validation_results", {})),
                        decision,
                        cached_data.get("decision_reason", "Instant verification from Redis cache."),
                        cached_data.get("amendment_draft"),
                        status,
                        json.dumps(cached_data.get("cross_doc_results", {"is_consistent": True, "discrepancies": []})),
                        source,
                        email_sender,
                        json.dumps(email_metadata) if email_metadata else None
                    )
                )
                row = await cur.fetchone()
                return row["id"]

    async def get_run_by_id(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a single shipment run by its database ID."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_RUN_BY_ID_SQL, (run_id,))
                return await cur.fetchone()

    async def get_all_runs(
        self,
        limit: int = 50,
        offset: int = 0,
        status: Optional[str] = None,
        source: Optional[str] = None
    ) -> Dict[str, Any]:
        """Retrieves paginated runs with total count for the Run Registry."""
        where_clauses = []
        params = []

        if status:
            where_clauses.append("status = %s")
            params.append(status)
        if source:
            where_clauses.append("source = %s")
            params.append(source)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        count_query = f"SELECT COUNT(*) as total FROM pipeline_runs {where_sql};"
        runs_query = f"""
        SELECT * FROM pipeline_runs
        {where_sql}
        ORDER BY id DESC
        LIMIT %s OFFSET %s;
        """

        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(count_query, params)
                count_res = await cur.fetchone()
                total = count_res["total"] if count_res else 0

                await cur.execute(runs_query, params + [limit, offset])
                runs = await cur.fetchall()

                return {
                    "total": total,
                    "runs": runs,
                    "limit": limit,
                    "offset": offset
                }

    async def get_incoming_runs(self) -> List[Dict[str, Any]]:
        """Retrieves all unprocessed inbox runs (status = 'pending_review', source = 'inbox')."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_INCOMING_RUNS_SQL)
                return await cur.fetchall()

    async def get_processed_runs(self) -> List[Dict[str, Any]]:
        """Retrieves all processed/resolved inbox runs (status != 'pending_review', source = 'inbox')."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_PROCESSED_RUNS_SQL)
                return await cur.fetchall()

    async def update_run_status(
        self,
        run_id: int,
        status: str,
        edited_data: Optional[Dict[str, Any]] = None,
        amendment_draft: Optional[str] = None
    ) -> bool:
        """Updates the status and human operator edits for a run."""
        edited_json = json.dumps(edited_data) if edited_data is not None else None
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(UPDATE_RUN_STATUS_SQL, (status, edited_json, amendment_draft, run_id))
                return cur.rowcount > 0

    async def get_analytics(self) -> Dict[str, Any]:
        """Calculates aggregated metrics for dashboard status cards."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(COUNT_TOTAL_RUNS_SQL)
                total_res = await cur.fetchone()
                total = total_res["total"] if total_res else 0

                await cur.execute(COUNT_DECISIONS_GROUPED_SQL)
                decisions = {row["decision"]: row["count"] for row in await cur.fetchall()}

                await cur.execute(COUNT_STATUSES_GROUPED_SQL)
                statuses = {row["status"]: row["count"] for row in await cur.fetchall()}

                return {
                    "total_runs": total,
                    "auto_approved": decisions.get("auto_approve", 0),
                    "flagged_for_review": decisions.get("flag_review", 0),
                    "amendments_requested": decisions.get("amendment_request", 0),
                    "statuses": statuses
                }

db_service: PipelineRunRepository = PostgresPipelineRunRepository()

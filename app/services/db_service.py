import json
from typing import List, Optional, Dict, Any
from db.pool import get_db_connection
from interfaces.repository import PipelineRunRepository
from interfaces.queries import (
    INSERT_QUEUED_RUN_SQL,
    INSERT_SHIPMENT_FILE_SQL,
    INSERT_CACHED_RUN_SQL,
    SELECT_RUN_BY_ID_SQL,
    SELECT_RUN_BY_SHIPMENT_ID_SQL,
    SELECT_SHIPMENT_FILES_SQL,
    MARK_FILE_UPLOADED_SQL,
    CHECK_AND_TRANSITION_SHIPMENT_SQL,
    SELECT_INCOMING_RUNS_SQL,
    SELECT_PROCESSED_RUNS_SQL,
    UPDATE_RUN_STATUS_SQL,
    COUNT_TOTAL_RUNS_SQL,
    COUNT_DECISIONS_GROUPED_SQL,
    COUNT_STATUSES_GROUPED_SQL,
    ANALYTICS_SOURCE_BREAKDOWN_SQL,
    ANALYTICS_CACHE_STATS_SQL,
    ANALYTICS_DAILY_THROUGHPUT_SQL,
    ANALYTICS_HOURLY_THROUGHPUT_SQL,
    ANALYTICS_TOP_MISMATCH_FIELDS_SQL,
    ANALYTICS_RECENT_RUNS_SQL,
    ANALYTICS_FAILURE_RATE_SQL,
    SELECT_RUN_BY_FILE_HASHES_SQL,
    UPDATE_FILE_HASH_SQL,
    APPLY_CACHED_OUTCOME_SQL
)

class PostgresPipelineRunRepository(PipelineRunRepository):
    """PostgreSQL implementation of the PipelineRunRepository interface."""

    async def create_shipment_with_files(
        self,
        shipment_id: str,
        files: List[Dict[str, Any]],
        source: str = "manual_upload",
        email_sender: Optional[str] = None,
        email_metadata: Optional[Dict[str, Any]] = None,
        initial_status: str = "pending_upload"
    ) -> int:
        """Atomically inserts a shipment run and its expected files into shipment_files."""
        filenames = [f["filename"] for f in files]
        filename_str = ", ".join(filenames)
        email_meta_json = json.dumps(email_metadata) if email_metadata else None
        total_files = len(files)

        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                # 1. Insert shipment pipeline run
                await cur.execute(
                    INSERT_QUEUED_RUN_SQL,
                    (
                        shipment_id,
                        filename_str,
                        total_files,
                        initial_status,
                        source,
                        email_sender,
                        email_meta_json
                    )
                )
                row = await cur.fetchone()
                run_id = row["id"]

                # 2. Insert all expected files
                for f in files:
                    await cur.execute(
                        INSERT_SHIPMENT_FILE_SQL,
                        (
                            shipment_id,
                            run_id,
                            f["filename"],
                            f["s3_key"],
                            f.get("file_hash"),
                            f.get("content_type")
                        )
                    )

                return run_id

    async def mark_file_uploaded_and_check_completion(
        self,
        shipment_id: str,
        s3_key: str
    ) -> bool:
        """Atomically marks a file as uploaded and transitions shipment to 'processing'
        if and only if all expected files have landed in S3.
        Returns True if this exact invocation transitioned the shipment.
        """
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                # 1. Mark this specific file uploaded
                await cur.execute(MARK_FILE_UPLOADED_SQL, (shipment_id, s3_key))

                # 2. Check and atomically transition run status if all files uploaded
                await cur.execute(CHECK_AND_TRANSITION_SHIPMENT_SQL, (shipment_id, shipment_id, shipment_id))
                res = await cur.fetchone()

                # If status returned is 'processing', this call completed the batch!
                return bool(res and res["status"] == "processing")

    async def get_shipment_files(self, shipment_id: str) -> List[Dict[str, Any]]:
        """Retrieves all tracked files for a shipment."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_SHIPMENT_FILES_SQL, (shipment_id,))
                return await cur.fetchall()

    @staticmethod
    def _format_run_row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Helper to format a raw pipeline_run row with filenames, email_subject, email_body, received_at."""
        if not row:
            return None
        row_dict = dict(row)

        meta = row_dict.get("email_metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        elif not isinstance(meta, dict):
            meta = {}

        if not row_dict.get("email_subject"):
            row_dict["email_subject"] = meta.get("subject")
        if not row_dict.get("email_body"):
            row_dict["email_body"] = meta.get("body")
        if not row_dict.get("received_at"):
            raw_time = row_dict.get("upload_time")
            if raw_time and hasattr(raw_time, "isoformat"):
                row_dict["received_at"] = raw_time.isoformat()
            else:
                row_dict["received_at"] = meta.get("received_at") or str(raw_time or "")

        filename_str = row_dict.get("filename") or ""
        if not row_dict.get("filenames"):
            row_dict["filenames"] = [f.strip() for f in filename_str.split(",") if f.strip()]

        return row_dict

    async def get_run_by_shipment_id(self, shipment_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a shipment run by its unique shipment_id string."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_RUN_BY_SHIPMENT_ID_SQL, (shipment_id,))
                return self._format_run_row(await cur.fetchone())

    async def create_cached_run(
        self,
        cached_data: Dict[str, Any],
        filenames: List[str],
        source: str = "manual_upload",
        shipment_id: Optional[str] = None,
        email_sender: Optional[str] = None,
        email_metadata: Optional[Dict[str, Any]] = None,
        files: Optional[List[Dict[str, Any]]] = None
    ) -> int:
        """Clones a cached pipeline result into a new run record with zero worker latency."""
        filename_str = ", ".join(filenames)
        decision = cached_data.get("decision", "flag_review")
        status = "approved" if decision == "auto_approve" else "pending_review"
        total_files = len(filenames)

        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    INSERT_CACHED_RUN_SQL,
                    (
                        shipment_id,
                        filename_str,
                        total_files,
                        total_files,  # uploaded_files_count matches total
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
                run_id = row["id"]

                # Insert expected files into shipment_files so files are tracked and preview URLs work
                for f in (files or []):
                    fn = f.get("filename") if isinstance(f, dict) else getattr(f, "filename", None)
                    if not fn:
                        continue
                    s3_k = (f.get("s3_key") if isinstance(f, dict) else getattr(f, "s3_key", None)) or f"shipments/{shipment_id}/{fn}"
                    fh = f.get("file_hash") if isinstance(f, dict) else getattr(f, "file_hash", None)
                    ct = (f.get("content_type") if isinstance(f, dict) else getattr(f, "content_type", None)) or "application/pdf"
                    await cur.execute(
                        """
                        INSERT INTO shipment_files (
                            shipment_id, run_id, filename, s3_key, file_hash, content_type, status
                        ) VALUES (%s, %s, %s, %s, %s, %s, 'uploaded');
                        """,
                        (shipment_id, run_id, fn, s3_k, fh, ct)
                    )

                return run_id

    async def get_run_by_id(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a single shipment run by its database ID."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_RUN_BY_ID_SQL, (run_id,))
                return self._format_run_row(await cur.fetchone())

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
                    "runs": [self._format_run_row(r) for r in runs],
                    "limit": limit,
                    "offset": offset
                }

    async def get_incoming_runs(self) -> List[Dict[str, Any]]:
        """Retrieves all unprocessed inbox runs (status = 'pending_review', source = 'inbox')."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_INCOMING_RUNS_SQL)
                return [self._format_run_row(r) for r in await cur.fetchall()]

    async def get_processed_runs(self) -> List[Dict[str, Any]]:
        """Retrieves all processed/resolved inbox runs (status != 'pending_review', source = 'inbox')."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_PROCESSED_RUNS_SQL)
                return [self._format_run_row(r) for r in await cur.fetchall()]

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
        """Calculates comprehensive production analytics metrics."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                # Core counts
                await cur.execute(COUNT_TOTAL_RUNS_SQL)
                total_res = await cur.fetchone()
                total = total_res["total"] if total_res else 0

                await cur.execute(COUNT_DECISIONS_GROUPED_SQL)
                decisions = {row["decision"]: row["count"] for row in await cur.fetchall()}

                await cur.execute(COUNT_STATUSES_GROUPED_SQL)
                statuses = {row["status"]: row["count"] for row in await cur.fetchall()}

                # Source breakdown (manual_upload vs inbox)
                sources = {}
                try:
                    await cur.execute(ANALYTICS_SOURCE_BREAKDOWN_SQL)
                    sources = {row["source"]: row["count"] for row in await cur.fetchall()}
                except Exception:
                    pass

                # Cache hit/miss rates
                cache_hits = 0
                cache_misses = 0
                try:
                    await cur.execute(ANALYTICS_CACHE_STATS_SQL)
                    cache_row = await cur.fetchone()
                    if cache_row:
                        cache_hits = cache_row.get("cache_hits") or 0
                        cache_misses = cache_row.get("cache_misses") or 0
                except Exception:
                    pass

                # Daily throughput (last 30 days)
                daily_throughput = []
                try:
                    await cur.execute(ANALYTICS_DAILY_THROUGHPUT_SQL)
                    for row in await cur.fetchall():
                        daily_throughput.append({
                            "date": row["date"].isoformat() if row.get("date") else None,
                            "total": row.get("total") or 0,
                            "auto_approved": row.get("auto_approved") or 0,
                            "flagged": row.get("flagged") or 0,
                            "amendments": row.get("amendments") or 0
                        })
                except Exception:
                    pass

                # Hourly distribution (peak hours)
                hourly_dist = {}
                try:
                    await cur.execute(ANALYTICS_HOURLY_THROUGHPUT_SQL)
                    hourly_dist = {row["hour"]: row["count"] for row in await cur.fetchall() if row.get("hour") is not None}
                except Exception:
                    pass

                # Top mismatch fields
                top_mismatches = []
                try:
                    await cur.execute(ANALYTICS_TOP_MISMATCH_FIELDS_SQL)
                    for row in await cur.fetchall():
                        top_mismatches.append({"field": row["field"], "count": row["count"]})
                except Exception:
                    pass

                # Recent runs
                recent_runs = []
                try:
                    await cur.execute(ANALYTICS_RECENT_RUNS_SQL)
                    for row in await cur.fetchall():
                        recent_runs.append({
                            "id": row["id"],
                            "shipment_id": row["shipment_id"],
                            "filename": row["filename"],
                            "decision": row["decision"],
                            "status": row["status"],
                            "source": row["source"],
                            "upload_time": row["upload_time"].isoformat() if row.get("upload_time") else None
                        })
                except Exception:
                    pass

                # Failure rate
                failed = 0
                completed = 0
                try:
                    await cur.execute(ANALYTICS_FAILURE_RATE_SQL)
                    fail_row = await cur.fetchone()
                    if fail_row:
                        failed = fail_row.get("failed") or 0
                        completed = fail_row.get("completed") or 0
                except Exception:
                    pass

                # Derived metrics
                auto_approve_count = decisions.get("auto_approve", 0)
                flag_review_count = decisions.get("flag_review", 0)
                amendment_count = decisions.get("amendment_request", 0)
                processed_total = auto_approve_count + flag_review_count + amendment_count
                auto_approve_rate = round((auto_approve_count / processed_total * 100), 1) if processed_total > 0 else 0.0
                compliance_score = round(((auto_approve_count) / processed_total * 100), 1) if processed_total > 0 else 0.0
                cache_hit_rate = round((cache_hits / (cache_hits + cache_misses) * 100), 1) if (cache_hits + cache_misses) > 0 else 0.0
                failure_rate = round((failed / (failed + completed) * 100), 1) if (failed + completed) > 0 else 0.0

                return {
                    # Core metrics (backward compatible)
                    "total_runs": total,
                    "decisions": {
                        "auto_approve": auto_approve_count,
                        "flag_review": flag_review_count,
                        "amendment_request": amendment_count
                    },
                    "statuses": {
                        "pending_review": statuses.get("pending_review", 0),
                        "approved": statuses.get("approved", 0),
                        "amended": statuses.get("amended", 0),
                        "processing": statuses.get("processing", 0),
                        "failed": statuses.get("failed", 0),
                        "pending_upload": statuses.get("pending_upload", 0)
                    },
                    # Production KPIs
                    "kpis": {
                        "auto_approve_rate": auto_approve_rate,
                        "compliance_score": compliance_score,
                        "cache_hit_rate": cache_hit_rate,
                        "failure_rate": failure_rate,
                        "total_processed": processed_total
                    },
                    # Source breakdown
                    "sources": {
                        "manual_upload": sources.get("manual_upload", 0),
                        "inbox": sources.get("inbox", 0)
                    },
                    # Cache performance
                    "cache": {
                        "hits": cache_hits,
                        "misses": cache_misses,
                        "hit_rate": cache_hit_rate
                    },
                    # Time-series data
                    "daily_throughput": daily_throughput,
                    "hourly_distribution": hourly_dist,
                    # Compliance insights
                    "top_mismatch_fields": top_mismatches,
                    "recent_runs": recent_runs,
                    # Backward compat flat fields
                    "auto_approved": auto_approve_count,
                    "flagged_for_review": flag_review_count,
                    "amendments_requested": amendment_count
                }

    async def find_cached_outcome_by_hashes(self, file_hashes: List[str]) -> Optional[Dict[str, Any]]:
        """Queries PostgreSQL for any previously completed run matching the file hashes."""
        if not file_hashes:
            return None
        num_hashes = len(file_hashes)
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_RUN_BY_FILE_HASHES_SQL, (file_hashes, num_hashes, num_hashes, num_hashes))
                row = await cur.fetchone()
                if not row:
                    return None

                ext_data = row.get("extracted_data") or {}
                if isinstance(ext_data, str):
                    try:
                        ext_data = json.loads(ext_data)
                    except Exception:
                        ext_data = {}
                if not ext_data:
                    return None

                # For multi-document packets, verify that extracted_data actually has entries for each document
                if num_hashes > 1:
                    first_val = list(ext_data.values())[0] if ext_data else None
                    is_multi = first_val and isinstance(first_val, dict) and "value" not in first_val
                    if not is_multi or len(ext_data) < num_hashes:
                        return None

                val_results = row.get("validation_results") or {}
                if isinstance(val_results, str):
                    try:
                        val_results = json.loads(val_results)
                    except Exception:
                        val_results = {}

                cross_results = row.get("cross_doc_results") or {"is_consistent": True, "discrepancies": []}
                if isinstance(cross_results, str):
                    try:
                        cross_results = json.loads(cross_results)
                    except Exception:
                        cross_results = {"is_consistent": True, "discrepancies": []}

                return {
                    "extracted_data": ext_data,
                    "validation_results": val_results,
                    "decision": row.get("decision", "auto_approve"),
                    "decision_reason": row.get("decision_reason", "Verified from previous run"),
                    "amendment_draft": row.get("amendment_draft"),
                    "status": row.get("status", "approved"),
                    "cross_doc_results": cross_results
                }

    async def update_file_hash(self, shipment_id: str, s3_key: str, file_hash: str) -> None:
        """Updates the SHA-256 fingerprint of a shipment file record."""
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(UPDATE_FILE_HASH_SQL, (file_hash, shipment_id, s3_key))

    async def apply_cached_outcome_to_run(self, shipment_id: str, cached_data: Dict[str, Any]) -> None:
        """Applies cached compliance results directly to a queued shipment run in PostgreSQL."""
        decision = cached_data.get("decision", "auto_approve")
        status = "approved" if decision == "auto_approve" else "pending_review"

        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    APPLY_CACHED_OUTCOME_SQL,
                    (
                        json.dumps(cached_data.get("extracted_data", {})),
                        json.dumps(cached_data.get("validation_results", {})),
                        decision,
                        cached_data.get("decision_reason", "Instant verification from cache"),
                        cached_data.get("amendment_draft"),
                        status,
                        json.dumps(cached_data.get("cross_doc_results", {"is_consistent": True, "discrepancies": []})),
                        shipment_id
                    )
                )

db_service: PipelineRunRepository = PostgresPipelineRunRepository()

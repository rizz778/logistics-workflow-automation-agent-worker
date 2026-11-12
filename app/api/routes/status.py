from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Query
from schemas.status_schemas import UpdateStatusRequest
from schemas.queue_schemas import ShipmentTaskEvent, TaskFileItem
from services.db_service import db_service
from services.s3_service import s3_service
from services.queue_service import queue_service
from core.logging import logger

router = APIRouter(prefix="/pipeline", tags=["Status, History & Review"])

@router.get("/status/{run_id}")
async def get_run_status(run_id: int):
    """Retrieves live status, decision, and results for a single shipment run.
    Also generates temporary presigned download URLs for document previews.
    """
    run = await db_service.get_run_by_id(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")

    # Attach presigned download URLs for files if shipment_id exists
    files_with_urls = []
    if run.get("shipment_id"):
        files = await db_service.get_shipment_files(run["shipment_id"])
        for f in files:
            download_url = None
            if f.get("status") == "uploaded":
                try:
                    download_url = s3_service.generate_presigned_download_url(f["s3_key"])
                except Exception:
                    pass
            files_with_urls.append({
                "filename": f["filename"],
                "s3_key": f["s3_key"],
                "status": f["status"],
                "download_url": download_url
            })

    return {
        **run,
        "files": files_with_urls
    }

@router.get("/history")
async def get_history(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    status: Optional[str] = None,
    source: Optional[str] = None
):
    """Retrieves paginated historical runs for the Run Registry table."""
    return await db_service.get_all_runs(limit=limit, offset=offset, status=status, source=source)

@router.get("/analytics")
async def get_analytics():
    """Retrieves aggregated metrics for the dashboard summary cards."""
    return await db_service.get_analytics()

@router.get("/incoming")
async def get_incoming_emails():
    """Retrieves incoming/unprocessed supplier email submissions."""
    return await db_service.get_incoming_runs()

@router.get("/processed")
async def get_processed_emails():
    """Retrieves processed/resolved supplier email submissions."""
    return await db_service.get_processed_runs()

@router.post("/update-status/{run_id}")
async def update_run_status(run_id: int, request: UpdateStatusRequest):
    """Human auditor review endpoint.
    Saves operator overrides and updated status in PostgreSQL.
    If fields were edited, dispatches a 'HUMAN_EDIT_REPROCESS' task to the queue
    so the Agent Worker re-evaluates compliance rules with the new values.
    """
    success = await db_service.update_run_status(
        run_id=run_id,
        status=request.status,
        edited_data=request.edited_data,
        amendment_draft=request.amendment_draft
    )

    if not success:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")

    logger.info(f"Human review saved for Run {run_id}. Status set to '{request.status}'.")

    # If operator modified fields, re-enqueue for validation re-check
    if request.edited_data:
        run = await db_service.get_run_by_id(run_id)
        if run and run.get("shipment_id"):
            files = await db_service.get_shipment_files(run["shipment_id"])
            task_files = [
                TaskFileItem(filename=f["filename"], s3_key=f["s3_key"])
                for f in files
            ]
            reprocess_task = ShipmentTaskEvent(
                run_id=run_id,
                shipment_id=run["shipment_id"],
                event_type="HUMAN_EDIT_REPROCESS",
                retry_count=0,
                max_retries=3,
                files=task_files,
                source=run.get("source", "manual_upload")
            )
            await queue_service.publish_shipment_task(reprocess_task)
            logger.info(f"Re-enqueued Run {run_id} as 'HUMAN_EDIT_REPROCESS' to re-verify compliance.")

    return {
        "status": "success",
        "message": f"Run {run_id} updated successfully.",
        "reprocess_queued": bool(request.edited_data)
    }


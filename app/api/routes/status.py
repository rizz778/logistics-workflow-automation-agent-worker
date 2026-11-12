import asyncio
import json
import time
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from fastapi.responses import StreamingResponse
from schemas.status_schemas import UpdateStatusRequest
from schemas.queue_schemas import ShipmentTaskEvent, TaskFileItem
from services.db_service import db_service
from services.s3_service import s3_service
from services.queue_service import queue_service
from services.rate_limiter import rate_limit_dependency
from db.redis import get_redis
from core.config import settings
from core.logging import logger

router = APIRouter(prefix="/pipeline", tags=["Status, History & Review"])

@router.get(
    "/status/{run_id}",
    dependencies=[Depends(rate_limit_dependency("status", settings.rate_limit_status_rpm, 60))]
)
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


@router.get("/runs/{run_identifier}/events")
@router.get("/events/{run_identifier}")
async def stream_run_events(
    run_identifier: str,
    request: Request
):
    """Server-Sent Events (SSE) endpoint for real-time document processing updates.
    
    Subscribes to Redis Pub/Sub channel 'run_events:{shipment_id}' and pushes
    live stage lifecycle events (download, extract, validate, route, human review, completed)
    directly to the connected client.
    """
    # 1. Resolve run_identifier to shipment_id and check initial state
    shipment_id = run_identifier
    run = None
    if run_identifier.isdigit():
        run = await db_service.get_run_by_id(int(run_identifier))
        if run and run.get("shipment_id"):
            shipment_id = run["shipment_id"]
    else:
        run = await db_service.get_run_by_shipment_id(run_identifier)

    async def event_generator():
        # A. Catch-up check: If the run is already finished in DB, emit final state and close
        if run and run.get("status") in ("approved", "completed", "amended", "rejected", "failed"):
            final_payload = {
                "shipment_id": shipment_id,
                "run_id": run.get("id"),
                "step": "completed",
                "status": run.get("status", "completed"),
                "decision": run.get("decision", "auto_approve"),
                "message": f"Run already completed with status '{run.get('status')}'.",
                "data": {
                    "extracted_data": run.get("extracted_data") or {},
                    "validation_results": run.get("validation_results") or {},
                    "cross_doc_results": run.get("cross_doc_results") or {},
                    "decision_reason": run.get("decision_reason") or "",
                    "amendment_draft": run.get("amendment_draft") or ""
                },
                "timestamp": time.time()
            }
            yield f"event: message\ndata: {json.dumps(final_payload)}\n\n"
            return

        # B. If run is currently paused on human review, emit the paused snapshot
        if run and run.get("status") in ("pending_review", "flag_review", "amendment_required", "human_correction"):
            paused_payload = {
                "shipment_id": shipment_id,
                "run_id": run.get("id"),
                "step": "human_approval" if run.get("status") != "human_correction" else "human_correction",
                "status": "paused",
                "decision": run.get("decision", "pending"),
                "message": f"Run paused awaiting human auditor review (status: '{run.get('status')}').",
                "data": {
                    "extracted_data": run.get("extracted_data") or {},
                    "validation_results": run.get("validation_results") or {},
                    "cross_doc_results": run.get("cross_doc_results") or {},
                    "decision_reason": run.get("decision_reason") or "",
                    "amendment_draft": run.get("amendment_draft") or ""
                },
                "timestamp": time.time()
            }
            yield f"event: message\ndata: {json.dumps(paused_payload)}\n\n"

        # C. Initial connection acknowledgment handshake
        handshake_payload = {
            "shipment_id": shipment_id,
            "run_id": run.get("id") if run else None,
            "step": "connected",
            "status": "listening",
            "message": f"Subscribed to live event stream for shipment {shipment_id}.",
            "timestamp": time.time()
        }
        yield f"event: connected\ndata: {json.dumps(handshake_payload)}\n\n"

        # D. Connect to Redis Pub/Sub
        redis = get_redis()
        if not redis:
            err_payload = {
                "shipment_id": shipment_id,
                "step": "error",
                "status": "error",
                "message": "Redis connection pool unavailable for SSE broadcasting.",
                "timestamp": time.time()
            }
            yield f"event: error\ndata: {json.dumps(err_payload)}\n\n"
            return

        pubsub = redis.pubsub()
        channel_name = f"run_events:{shipment_id}"
        await pubsub.subscribe(channel_name)
        logger.info(f"SSE client subscribed to Redis channel '{channel_name}'.")

        keep_alive_interval = 15.0  # seconds
        last_keep_alive = time.time()

        try:
            while True:
                # Disconnect guard
                if await request.is_disconnected():
                    logger.info(f"SSE client disconnected for shipment {shipment_id}.")
                    break

                # Non-blocking poll with 1.0s timeout
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("type") == "message":
                    raw_data = msg.get("data")
                    if raw_data:
                        yield f"event: message\ndata: {raw_data}\n\n"
                        last_keep_alive = time.time()

                        # Terminal event check
                        try:
                            parsed = json.loads(raw_data)
                            step = parsed.get("step")
                            status = parsed.get("status")
                            if step in ("completed", "dlq_exhausted") or status in ("completed", "failed", "exhausted"):
                                logger.info(f"SSE received terminal event '{step}' for shipment {shipment_id}. Closing stream.")
                                break
                        except Exception:
                            pass
                else:
                    # Keep-alive heartbeat
                    now = time.time()
                    if now - last_keep_alive >= keep_alive_interval:
                        yield ": keep-alive\n\n"
                        last_keep_alive = now

                await asyncio.sleep(0.05)

        except asyncio.CancelledError:
            logger.info(f"SSE stream cancelled for shipment {shipment_id}.")
        except Exception as e:
            logger.error(f"Error in SSE stream for shipment {shipment_id}: {e}")
        finally:
            try:
                await pubsub.unsubscribe(channel_name)
                await pubsub.close()
                logger.info(f"Cleaned up Pub/Sub subscription for channel '{channel_name}'.")
            except Exception as e:
                logger.debug(f"Error closing pubsub for {channel_name}: {e}")

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
        "Access-Control-Allow-Origin": "*",
    }
    return StreamingResponse(event_generator(), media_type="text/event-stream", headers=headers)


@router.get("/events/stream/global")
async def stream_global_events(request: Request):
    """Server-Sent Events endpoint streaming fleet-wide pipeline events across all shipments."""
    async def global_event_generator():
        redis = get_redis()
        if not redis:
            yield f"event: error\ndata: {json.dumps({'error': 'Redis unavailable'})}\n\n"
            return

        pubsub = redis.pubsub()
        channel_name = "run_events:global"
        await pubsub.subscribe(channel_name)
        logger.info("SSE client subscribed to global fleet channel 'run_events:global'.")

        keep_alive_interval = 15.0
        last_keep_alive = time.time()

        try:
            while True:
                if await request.is_disconnected():
                    logger.info("SSE global client disconnected.")
                    break

                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("type") == "message":
                    raw_data = msg.get("data")
                    if raw_data:
                        yield f"event: message\ndata: {raw_data}\n\n"
                        last_keep_alive = time.time()
                else:
                    now = time.time()
                    if now - last_keep_alive >= keep_alive_interval:
                        yield ": keep-alive\n\n"
                        last_keep_alive = now

                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            pass
        finally:
            try:
                await pubsub.unsubscribe(channel_name)
                await pubsub.close()
            except Exception:
                pass

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
        "Access-Control-Allow-Origin": "*",
    }
    return StreamingResponse(global_event_generator(), media_type="text/event-stream", headers=headers)


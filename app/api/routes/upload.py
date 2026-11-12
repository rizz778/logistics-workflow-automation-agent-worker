import json
import uuid
from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, status, Request, Depends
from schemas.upload_schemas import (
    PresignedUrlsRequest,
    PresignedUrlsResponse,
    PresignedUrlResponseItem,
    ConfirmUploadRequest,
    ConfirmUploadResponse
)
from schemas.queue_schemas import ShipmentTaskEvent, TaskFileItem
from services.s3_service import s3_service
from services.db_service import db_service
from services.hash_service import hash_service
from services.cache_service import cache_service
from services.queue_service import queue_service
from services.rate_limiter import rate_limit_dependency
from core.config import settings
from core.logging import logger

router = APIRouter(prefix="/pipeline", tags=["Upload & Ingestion"])

async def _dispatch_shipment_to_queue(shipment_id: str, event_type: str = "INITIAL_PROCESS") -> None:
    """Helper to assemble verified shipment files and push a task to the Redis Queue."""
    run = await db_service.get_run_by_shipment_id(shipment_id)
    if not run:
        logger.error(f"Cannot dispatch shipment {shipment_id}: run record not found.")
        return

    files = await db_service.get_shipment_files(shipment_id)
    task_files = [
        TaskFileItem(
            filename=f["filename"],
            s3_key=f["s3_key"],
            file_hash=f.get("file_hash"),
            content_type=f.get("content_type", "application/pdf")
        )
        for f in files
    ]

    email_meta = None
    if run.get("email_metadata_json"):
        try:
            email_meta = json.loads(run["email_metadata_json"])
        except Exception:
            pass

    task_event = ShipmentTaskEvent(
        run_id=run["id"],
        shipment_id=shipment_id,
        event_type=event_type,
        retry_count=0,
        max_retries=3,
        files=task_files,
        source=run.get("source", "manual_upload"),
        email_metadata=email_meta
    )

    await queue_service.publish_shipment_task(task_event)

@router.post(
    "/presigned-urls",
    response_model=PresignedUrlsResponse,
    dependencies=[Depends(rate_limit_dependency("upload", settings.rate_limit_upload_rpm, 60))]
)
async def generate_presigned_urls(request: PresignedUrlsRequest):
    """Generates S3 presigned PUT URLs for direct client-to-S3 upload.
    Checks Redis Cache first: if the document packet has already been verified,
    it returns the cached results immediately (bypassing S3 upload and worker queue).
    """
    shipment_id = f"ship_{uuid.uuid4().hex[:12]}"
    filenames = [f.filename for f in request.files]

    # 1. Fast-Path Redis Cache Check (if client provided file hashes)
    all_hashed = all(f.file_hash for f in request.files)
    if all_hashed:
        file_hashes = [f.file_hash for f in request.files if f.file_hash]
        cache_key = hash_service.build_shipment_cache_key(file_hashes)
        cached_result = await cache_service.get_shipment_cache(cache_key)

        if cached_result:
            logger.info(f"⚡ FAST-PATH CACHE HIT for shipment {shipment_id}. Bypassing S3 upload and worker queue.")
            try:
                run_id = await db_service.create_cached_run(
                    cached_data=cached_result,
                    filenames=filenames,
                    source=request.source,
                    shipment_id=shipment_id,
                    email_sender=request.email_sender,
                    email_metadata=request.email_metadata
                )
                return PresignedUrlsResponse(
                    run_id=run_id,
                    shipment_id=shipment_id,
                    status=cached_result.get("status", "approved"),
                    is_cached=True,
                    files=[],  # Empty list instructs client to skip S3 upload
                    cached_results=cached_result
                )
            except Exception as e:
                logger.error(f"Failed to record cached run in database: {e}")

    # 2. Cache MISS: Build S3 keys and generate presigned URLs
    files_to_track = []
    response_items = []

    for file_item in request.files:
        s3_key = f"shipments/{shipment_id}/{file_item.filename}"
        upload_url = s3_service.generate_presigned_upload_url(
            s3_key=s3_key,
            content_type=file_item.content_type,
            expires_in=900  # 15 minutes
        )

        files_to_track.append({
            "filename": file_item.filename,
            "s3_key": s3_key,
            "file_hash": file_item.file_hash,
            "content_type": file_item.content_type
        })

        response_items.append(PresignedUrlResponseItem(
            filename=file_item.filename,
            s3_key=s3_key,
            upload_url=upload_url
        ))

    # 3. Atomically persist shipment and expected files in PostgreSQL
    try:
        run_id = await db_service.create_shipment_with_files(
            shipment_id=shipment_id,
            files=files_to_track,
            source=request.source,
            email_sender=request.email_sender,
            email_metadata=request.email_metadata,
            initial_status="pending_upload"
        )
    except Exception as e:
        logger.error(f"Failed to record shipment in database: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error registering shipment: {str(e)}"
        )

    logger.info(f"Registered shipment {shipment_id} (DB ID: {run_id}) with {len(files_to_track)} expected files.")

    return PresignedUrlsResponse(
        run_id=run_id,
        shipment_id=shipment_id,
        status="pending_upload",
        is_cached=False,
        files=response_items
    )

@router.post("/confirm-upload", response_model=ConfirmUploadResponse)
async def confirm_upload(request: ConfirmUploadRequest):
    """Client calls this endpoint after uploading all files to S3 via presigned URLs.
    Verifies that all files exist in S3 and transitions status to 'processing' before queueing.
    """
    shipment_id = request.shipment_id

    # 1. Fetch expected files from PostgreSQL
    files = await db_service.get_shipment_files(shipment_id)
    if not files:
        raise HTTPException(status_code=404, detail=f"Shipment {shipment_id} not found.")

    # 2. Verify each file in S3 and mark uploaded in DB
    all_ready = True
    for f in files:
        s3_key = f["s3_key"]
        if f["status"] != "uploaded":
            exists = s3_service.verify_file_exists(s3_key)
            if exists:
                completed = await db_service.mark_file_uploaded_and_check_completion(shipment_id, s3_key)
                if completed:
                    all_ready = True
            else:
                all_ready = False
                logger.warning(f"File {s3_key} not yet found in S3 for shipment {shipment_id}.")

    run = await db_service.get_run_by_shipment_id(shipment_id)
    current_status = run["status"] if run else "unknown"

    if all_ready and current_status == "processing":
        logger.info(f"All files confirmed in S3 for shipment {shipment_id}. Pushing to Redis Queue...")
        await _dispatch_shipment_to_queue(shipment_id, event_type="INITIAL_PROCESS")

        return ConfirmUploadResponse(
            shipment_id=shipment_id,
            status="processing",
            message="All files verified in S3. Shipment is queued for agent worker processing.",
            all_files_ready=True
        )

    return ConfirmUploadResponse(
        shipment_id=shipment_id,
        status=current_status,
        message="Some files are still pending upload in S3.",
        all_files_ready=False
    )

@router.post("/webhooks/s3")
async def s3_event_webhook(request: Request):
    """Receives native S3 / MinIO ObjectCreated webhook notifications.
    Automatically marks files uploaded and triggers processing when all files land.
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")

    records = body.get("Records", [])
    if not records:
        return {"status": "ignored", "message": "No records in event."}

    for record in records:
        s3_info = record.get("s3", {})
        obj_info = s3_info.get("object", {})
        s3_key = obj_info.get("key")

        if not s3_key or not s3_key.startswith("shipments/"):
            continue

        parts = s3_key.split("/")
        if len(parts) >= 3:
            shipment_id = parts[1]
            logger.info(f"S3 Webhook: Detected completed upload for {s3_key} in shipment {shipment_id}.")

            is_completed = await db_service.mark_file_uploaded_and_check_completion(shipment_id, s3_key)
            if is_completed:
                logger.info(f"S3 Webhook: All files uploaded for {shipment_id}. Pushing to Redis Queue...")
                await _dispatch_shipment_to_queue(shipment_id, event_type="INITIAL_PROCESS")

    return {"status": "ok"}

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
import logging
from typing import Any, Optional

import boto3
from bson import ObjectId
from bson.errors import InvalidId
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from app.config.s3 import session as aws_session  # reuse configured AWS session

from .models import JobCreate, JobRead, JobUpdateStatus
from ..project.models import ProjectPublic
from app.api.deps import DbDep

JOB_COLLECTION = "jobs"

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
APP_ENV = os.getenv("APP_ENV", "dev").lower()
JOB_QUEUE_URL = os.getenv("JOB_QUEUE_URL")
JOB_QUEUE_FIFO = os.getenv("JOB_QUEUE_FIFO", "false").lower() == "true"
JOB_QUEUE_MESSAGE_GROUP_ID = os.getenv("JOB_QUEUE_MESSAGE_GROUP_ID")

_session = aws_session or boto3.Session(region_name=AWS_REGION)
_sqs_client = _session.client("sqs", region_name=AWS_REGION)
logger = logging.getLogger(__name__)


class SqsPublishError(Exception):
    """Raised when the job message cannot be enqueued to SQS."""


def _serialize_job(doc: dict[str, Any]) -> JobRead:
    return JobRead.model_validate(
        {
            "id": str(doc["_id"]),
            "project_id": doc["project_id"],
            "input_key": doc.get("input_key"),
            "status": doc["status"],
            "callback_url": doc["callback_url"],
            "result_key": doc.get("result_key"),
            "error": doc.get("error"),
            "metadata": doc.get("metadata"),
            "created_at": doc["created_at"],
            "updated_at": doc["updated_at"],
            "history": doc.get("history", []),
            "task": doc.get("task"),
            "task_payload": doc.get("task_payload"),
        }
    )


def _build_job_message(job: JobRead) -> dict[str, Any]:
    task = job.task or "full_pipeline"
    message: dict[str, Any] = {
        "task": task,
        "job_id": job.job_id,
        "project_id": job.project_id,
        "callback_url": str(job.callback_url),
    }
    if job.input_key:
        message["input_key"] = job.input_key

    payload = job.task_payload or {}
    if task == "segment_tts":
        message["segment"] = payload
    elif payload:
        message.update(payload)

    return message


def _normalize_segment_record(segment: dict[str, Any], *, index: int) -> dict[str, Any]:
    """
    Ensure segment documents stored in MongoDB follow the schema expected by /api/segment.
    """

    def _float_or_none(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    segment_id = (
        segment.get("segment_id") or segment.get("seg_id") or segment.get("id") or index
    )
    segment_text = (
        segment.get("segment_text")
        or segment.get("seg_txt")
        or segment.get("text")
        or ""
    )
    translate_context = (
        segment.get("translate_context")
        or segment.get("trans_txt")
        or segment.get("translation")
        or ""
    )
    sub_length = (
        segment.get("sub_langth") or segment.get("length") or segment.get("duration")
    )
    start_point = segment.get("start_point")
    if start_point is None:
        start_point = segment.get("start")
    end_point = segment.get("end_point")
    if end_point is None:
        end_point = segment.get("end")

    issues = segment.get("issues") or []
    if not isinstance(issues, list):
        issues = [issues]

    assets = segment.get("assets")
    if not isinstance(assets, dict):
        assets = None

    normalized = {
        "segment_id": str(segment_id),
        "segment_text": segment_text,
        "score": segment.get("score"),
        "editor_id": segment.get("editor_id"),
        "translate_context": translate_context,
        "sub_langth": _float_or_none(sub_length),
        "start_point": _float_or_none(start_point) or 0.0,
        "end_point": _float_or_none(end_point) or 0.0,
        "issues": issues,
    }

    if assets:
        normalized["assets"] = assets

    for key in ("source_key", "bgm_key", "tts_key", "mix_key", "video_key"):
        value = segment.get(key)
        if not value and assets:
            value = assets.get(key)
        if value:
            normalized[key] = str(value)

    return normalized


def _resolve_callback_base() -> str:
    callback_base = os.getenv("JOB_CALLBACK_BASE_URL")
    if callback_base:
        return callback_base

    app_env = os.getenv("APP_ENV", "dev").lower()
    if app_env in {"dev", "development", "local"}:
        return "http://localhost:8000"

    raise HTTPException(status_code=500, detail="JOB_CALLBACK_BASE_URL env not set")


def _collect_segment_assets(segment: dict[str, Any]) -> dict[str, Any]:
    assets = (segment.get("assets") or {}).copy()
    collected: dict[str, Any] = {}
    for key in ("source_key", "bgm_key", "tts_key", "mix_key", "video_key"):
        value = segment.get(key) or assets.get(key)
        if value:
            collected[key] = value
    if assets:
        collected["raw"] = assets
    return collected


def _build_segment_field_updates(segment_patch: dict[str, Any]) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    try:
        index = int(segment_patch.get("segment_index"))
    except (TypeError, ValueError):
        return updates

    base = f"segments.{index}"

    translate_context = segment_patch.get("translate_context")
    if translate_context is not None:
        updates[f"{base}.translate_context"] = translate_context

    for key in ("tts_key", "mix_key"):
        value = segment_patch.get(key)
        if value:
            updates[f"{base}.{key}"] = value
            updates[f"{base}.assets.{key}"] = value

    return updates


def _build_segment_tts_task_payload(
    project: dict[str, Any],
    *,
    segment_index: int,
    segment: dict[str, Any],
    text: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "project_id": str(project["_id"]),
        "segment_index": segment_index,
        "segment_id": str(segment.get("segment_id", segment_index)),
        "text": text,
        "segment_text": segment.get("segment_text"),
        "start_point": float(segment.get("start_point", 0.0) or 0.0),
        "end_point": float(segment.get("end_point", 0.0) or 0.0),
        "assets": _collect_segment_assets(segment),
        "segment_assets_prefix": project.get("segment_assets_prefix"),
    }

    source_keys = []
    assets = payload["assets"]
    value = assets.get("source_key")
    if value:
        source_keys.append(value)
    raw_assets = segment.get("assets") or {}
    extra_sources = raw_assets.get("source_keys") or raw_assets.get("source_list")
    if isinstance(extra_sources, list):
        source_keys.extend(str(item) for item in extra_sources if item)
    payload["source_keys"] = list(dict.fromkeys(source_keys))

    if project.get("target_lang"):
        payload["target_lang"] = project["target_lang"]
    if project.get("source_lang"):
        payload["source_lang"] = project["source_lang"]
    if segment.get("sub_langth") is not None:
        try:
            payload["sub_length"] = float(segment["sub_langth"])
        except (TypeError, ValueError):
            pass

    return payload


async def create_job(
    db: AsyncIOMotorDatabase,
    payload: JobCreate,
    *,
    job_oid: Optional[ObjectId] = None,
) -> JobRead:
    now = datetime.utcnow()
    job_oid = job_oid or ObjectId()
    document = {
        "_id": job_oid,
        "project_id": payload.project_id,
        "input_key": payload.input_key,
        "callback_url": str(payload.callback_url),
        "status": "queued",
        "result_key": None,
        "error": None,
        "metadata": payload.metadata or None,
        "created_at": now,
        "updated_at": now,
        "history": [
            {
                "status": "queued",
                "ts": now,
                "message": "job created",
            }
        ],
        "task": payload.task or "full_pipeline",
        "task_payload": payload.task_payload or None,
    }

    try:
        await db[JOB_COLLECTION].insert_one(document)
    except PyMongoError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create job",
        ) from exc

    return _serialize_job(document)


async def get_job(db: AsyncIOMotorDatabase, job_id: str) -> JobRead:
    try:
        job_oid = ObjectId(job_id)
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid job_id"
        ) from exc

    document = await db[JOB_COLLECTION].find_one({"_id": job_oid})
    if not document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )

    return _serialize_job(document)


async def update_job_status(
    db: AsyncIOMotorDatabase,
    job_id: str,
    payload: JobUpdateStatus,
    *,
    message: Optional[str] = None,
) -> JobRead:
    try:
        job_oid = ObjectId(job_id)
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid job_id"
        ) from exc

    now = datetime.utcnow()
    update_operations: dict[str, Any] = {
        "$set": {
            "status": payload.status,
            "updated_at": now,
        },
        "$push": {
            "history": {
                "status": payload.status,
                "ts": now,
                "message": message or payload.message,
            }
        },
    }

    if payload.result_key is not None:
        update_operations["$set"]["result_key"] = payload.result_key

    if payload.error is not None:
        update_operations["$set"]["error"] = payload.error

    metadata_dict: dict[str, Any] | None = None
    if payload.metadata is not None:
        metadata_dict = (
            payload.metadata.model_dump()
            if hasattr(payload.metadata, "model_dump")
            else payload.metadata
        )
        update_operations["$set"]["metadata"] = metadata_dict

    updated = await db[JOB_COLLECTION].find_one_and_update(
        {"_id": job_oid},
        update_operations,
        return_document=ReturnDocument.AFTER,
    )

    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )

    project_updates: dict[str, Any] = {}
    if metadata_dict:
        segments_meta = metadata_dict.get("segments")
        if isinstance(segments_meta, list):
            normalized_segments = [
                _normalize_segment_record(seg if isinstance(seg, dict) else {}, index=i)
                for i, seg in enumerate(segments_meta)
            ]
            project_updates["segments"] = normalized_segments
            project_updates["segments_updated_at"] = now

        assets_prefix = metadata_dict.get("segment_assets_prefix")
        if assets_prefix:
            project_updates["segment_assets_prefix"] = assets_prefix

        target_lang = metadata_dict.get("target_lang")
        if target_lang:
            project_updates["target_lang"] = target_lang

        source_lang = metadata_dict.get("source_lang")
        if source_lang:
            project_updates["source_lang"] = source_lang

        metadata_key = metadata_dict.get("metadata_key")
        if metadata_key:
            project_updates["segment_metadata_key"] = metadata_key

        result_key_meta = metadata_dict.get("result_key")
        if result_key_meta:
            project_updates["segment_result_key"] = result_key_meta

        if metadata_dict.get("stage") == "segment_tts_completed":
            segment_patch = metadata_dict.get("segment")
            if isinstance(segment_patch, dict):
                field_updates = _build_segment_field_updates(segment_patch)
                if field_updates:
                    project_updates.update(field_updates)
                    project_updates["segments_updated_at"] = now

    if payload.status:
        project_updates.setdefault("status", payload.status)

    project_id = updated.get("project_id")
    if project_updates and project_id:
        try:
            project_oid = ObjectId(project_id)
        except InvalidId:
            project_oid = None
        if project_oid:
            try:
                await db["projects"].update_one(
                    {"_id": project_oid},
                    {"$set": project_updates},
                )
            except PyMongoError as exc:
                logger.error(
                    "Failed to update project %s with segment metadata: %s",
                    project_id,
                    exc,
                )

    return _serialize_job(updated)


async def mark_job_failed(
    db: AsyncIOMotorDatabase,
    job_id: str,
    *,
    error: str,
    message: Optional[str] = None,
) -> JobRead:
    payload = JobUpdateStatus(
        status="failed", error=error, result_key=None, message=message
    )
    return await update_job_status(db, job_id, payload, message=message)


async def enqueue_job(job: JobRead) -> None:
    if not JOB_QUEUE_URL:
        if APP_ENV in {"dev", "development", "local"}:
            logger.warning(
                "JOB_QUEUE_URL not set; skipping SQS enqueue for job %s in %s environment",
                job.job_id,
                APP_ENV,
            )
            return
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JOB_QUEUE_URL env not set",
        )

    message_body = json.dumps(
        _build_job_message(job),
    )

    message_kwargs: dict[str, Any] = {
        "QueueUrl": JOB_QUEUE_URL,
        "MessageBody": message_body,
        "MessageAttributes": {
            "job_id": {"StringValue": job.job_id, "DataType": "String"},
            "project_id": {"StringValue": job.project_id, "DataType": "String"},
            "task": {
                "StringValue": (job.task or "full_pipeline"),
                "DataType": "String",
            },
        },
    }

    if JOB_QUEUE_FIFO:
        group_id = JOB_QUEUE_MESSAGE_GROUP_ID or job.project_id
        message_kwargs["MessageGroupId"] = group_id
        message_kwargs["MessageDeduplicationId"] = job.job_id

    try:
        await asyncio.to_thread(_sqs_client.send_message, **message_kwargs)
    except (BotoCoreError, ClientError) as exc:
        if APP_ENV in {"dev", "development", "local"}:
            logger.error("SQS publish failed in %s env: %s", APP_ENV, exc)
            return
        raise SqsPublishError("Failed to publish job message to SQS") from exc


async def start_job(project: ProjectPublic, db: DbDep):
    callback_base = _resolve_callback_base()
    job_oid = ObjectId()
    callback_url = f"{callback_base.rstrip('/')}/api/jobs/{job_oid}/status"
    job_payload = JobCreate(
        project_id=project.project_id,
        input_key=project.video_source,
        callback_url=callback_url,
    )
    job = await create_job(db, job_payload, job_oid=job_oid)

    try:
        await enqueue_job(job)
    except SqsPublishError as exc:
        await mark_job_failed(
            db,
            job.job_id,
            error="sqs_publish_failed",
            message=str(exc),
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to enqueue job",
        ) from exc

    return {
        "project_id": project.project_id,
        "job_id": job.job_id,
        "status": job.status,
    }


async def start_segment_tts_job(
    db: DbDep,
    *,
    project: dict[str, Any],
    segment_index: int,
    segment: dict[str, Any],
    text: str,
) -> JobRead:
    callback_base = _resolve_callback_base()
    job_oid = ObjectId()
    callback_url = f"{callback_base.rstrip('/')}/api/jobs/{job_oid}/status"

    payload = JobCreate(
        project_id=str(project["_id"]),
        input_key=project.get("video_source"),
        callback_url=callback_url,
        task="segment_tts",
        task_payload=_build_segment_tts_task_payload(
            project,
            segment_index=segment_index,
            segment=segment,
            text=text,
        ),
    )
    job = await create_job(db, payload, job_oid=job_oid)

    try:
        await enqueue_job(job)
    except SqsPublishError as exc:
        await mark_job_failed(
            db,
            job.job_id,
            error="sqs_publish_failed",
            message=str(exc),
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to enqueue job",
        ) from exc

    return job

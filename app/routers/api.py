"""HTTP API routes for measurement ingestion and device state queries."""
from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query

from app import service
from app.schemas import (
    BatchIn,
    CommittedResponse,
    DeviceSummary,
    DuplicatesResponse,
    GapResponse,
    HealthResponse,
    IngestResult,
    PacketIn,
    StatusResponse,
)

router = APIRouter()


def _serialize_packet(packet: PacketIn) -> str:
    return json.dumps(
        {"measurements": packet.measurements}, separators=(",", ":"), sort_keys=True
    )


@router.get("/health", response_model=HealthResponse, tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/devices", response_model=list[DeviceSummary], tags=["devices"])
def devices() -> list[dict]:
    return service.list_devices()


@router.post(
    "/devices/{device_id}/packets",
    response_model=IngestResult,
    status_code=200,
    tags=["ingestion"],
)
def submit_packet(device_id: str, packet: PacketIn) -> dict:
    try:
        result = service.ingest_packet(
            device_id=device_id,
            seq=packet.seq,
            idempotency_key=packet.idempotency_key,
            payload=_serialize_packet(packet),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {
        "device_id": device_id,
        "received": 1,
        "committed_count": 1 if result["status"] == "committed" else 0,
        "staged_count": 1 if result["status"] == "staged" else 0,
        "duplicate_count": 1 if result["status"] == "duplicate" else 0,
        "committed_now": result["committed_now"],
        "staged_seqs": [packet.seq] if result["status"] == "staged" else [],
        "duplicates": (
            [
                {
                    "seq": result["seq"],
                    "reason": result["reason"],
                    "original_packet_id": result["original_packet_id"],
                    "duplicate_id": result["duplicate_id"],
                }
            ]
            if result["status"] == "duplicate"
            else []
        ),
        "results": [result],
    }


@router.post(
    "/devices/{device_id}/packets/batch",
    response_model=IngestResult,
    status_code=200,
    tags=["ingestion"],
)
def submit_batch(device_id: str, batch: BatchIn) -> dict:
    packets = [
        {
            "seq": packet.seq,
            "idempotency_key": packet.idempotency_key,
            "payload": _serialize_packet(packet),
        }
        for packet in batch.packets
    ]
    try:
        return service.ingest_batch(device_id, packets)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _require_device(device_id: str) -> None:
    if not service.device_exists(device_id):
        raise HTTPException(status_code=404, detail=f"device '{device_id}' not found")


@router.get(
    "/devices/{device_id}/gaps",
    response_model=GapResponse,
    tags=["queries"],
)
def device_gaps(device_id: str) -> dict:
    _require_device(device_id)
    return service.get_gaps(device_id)


@router.get(
    "/devices/{device_id}/committed",
    response_model=CommittedResponse,
    tags=["queries"],
)
def device_committed(
    device_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    _require_device(device_id)
    return service.get_committed(device_id, limit, offset)


@router.get(
    "/devices/{device_id}/duplicates",
    response_model=DuplicatesResponse,
    tags=["queries"],
)
def device_duplicates(
    device_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    _require_device(device_id)
    return service.get_duplicates(device_id, limit, offset)


@router.get(
    "/devices/{device_id}/status",
    response_model=StatusResponse,
    tags=["queries"],
)
def device_status(device_id: str) -> dict:
    _require_device(device_id)
    return service.get_status(device_id)

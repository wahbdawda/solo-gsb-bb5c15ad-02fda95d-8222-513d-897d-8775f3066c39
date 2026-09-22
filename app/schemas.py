"""Pydantic request/response models."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from app.config import settings


class PacketIn(BaseModel):
    seq: int = Field(..., ge=0, description="Per-device monotonic sequence number")
    idempotency_key: str = Field(
        ..., min_length=1, max_length=200, description="Unique key used for dedup"
    )
    measurements: list[dict[str, Any]] = Field(
        ..., description="Measurement payload (list of measurement records)"
    )


class BatchIn(BaseModel):
    packets: list[PacketIn] = Field(
        ..., min_length=1, max_length=settings.max_batch_size
    )


class BatchDuplicate(BaseModel):
    seq: int
    reason: str
    original_packet_id: int | None
    duplicate_id: int


class IngestResult(BaseModel):
    device_id: str
    received: int
    committed_count: int
    staged_count: int
    duplicate_count: int
    committed_now: list[int]
    staged_seqs: list[int]
    duplicates: list[BatchDuplicate]
    results: list[dict[str, Any]]


class GapResponse(BaseModel):
    device_id: str
    start_seq: int
    last_committed_seq: int | None
    next_expected_seq: int
    staged_seqs: list[int]
    gap_seqs: list[int]
    gap_ranges: list[list[int]]


class CommittedPacket(BaseModel):
    id: int
    seq: int
    idempotency_key: str
    payload: str
    received_at: str
    committed_at: str | None


class CommittedResponse(BaseModel):
    device_id: str
    total: int
    limit: int
    offset: int
    last_committed_seq: int
    packets: list[CommittedPacket]


class DuplicateRecord(BaseModel):
    id: int
    device_id: str
    seq: int
    idempotency_key: str
    payload: str
    reason: str
    original_packet_id: int | None
    received_at: str


class DuplicatesResponse(BaseModel):
    device_id: str
    total: int
    limit: int
    offset: int
    duplicates: list[DuplicateRecord]


class DeviceSummary(BaseModel):
    id: str
    start_seq: int
    last_seq: int | None
    created_at: str
    updated_at: str


class StatusResponse(BaseModel):
    device_id: str
    start_seq: int
    last_committed_seq: int | None
    next_expected_seq: int
    staged_seqs: list[int]
    gap_seqs: list[int]
    gap_ranges: list[list[int]]
    committed_count: int
    staged_count: int
    duplicate_count: int


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"

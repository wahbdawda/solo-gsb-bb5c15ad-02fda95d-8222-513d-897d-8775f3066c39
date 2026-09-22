"""Application configuration read from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"environment variable {name} must be an integer") from exc


@dataclass(frozen=True)
class Settings:
    db_path: str = os.getenv("DB_PATH", "/data/measurements.db")
    # First sequence number accepted for devices that are seen for the first time.
    default_start_seq: int = _int_env("DEFAULT_START_SEQ", 1)
    # Upper bound on how many packets a single batch may contain.
    max_batch_size: int = _int_env("MAX_BATCH_SIZE", 1000)
    api_prefix: str = "/api/v1"


settings = Settings()

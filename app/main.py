"""FastAPI application entry point.

The SQLite database file and all tables are created on startup, so a fresh
container is ready to receive packets with no manual migration step.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.database import init_db
from app.routers.api import router as api_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Laboratory Instrument Ingestion API",
    version="1.0.0",
    description=(
        "Receives batched, sequenced measurement packets from lab instruments. "
        "Deduplicates by idempotency key, stages out-of-order packets and "
        "commits them in sequence once gaps are filled."
    ),
    lifespan=lifespan,
)

app.include_router(api_router, prefix=settings.api_prefix)


@app.get("/", include_in_schema=False)
def root() -> dict[str, str]:
    return {"service": "instrument-ingestion", "docs": "/docs"}

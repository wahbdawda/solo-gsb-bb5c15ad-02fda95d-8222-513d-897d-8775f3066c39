"""Core ingestion and query logic.

Sequence model
--------------
Each device has a ``start_seq``. ``devices.last_seq`` is the highest sequence
that has been committed in order. A packet is only "committed" once every
sequence from ``start_seq`` up to its own seq has been received: out-of-order
packets are first stored as *staged* rows and committed together when the gap
closes. Resending an already known idempotency key, or sending a second packet
for an already known (device, seq), is recorded in ``duplicates``.
"""
from __future__ import annotations

from typing import Any

from app.config import settings
from app.database import get_conn, read_tx, transaction


class DeviceNotFoundError(Exception):
    def __init__(self, device_id: str) -> None:
        self.device_id = device_id
        super().__init__(f"device '{device_id}' not found")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_device(conn, device_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT id, start_seq, last_seq FROM devices WHERE id = ?", (device_id,)
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO devices (id, start_seq) VALUES (?, ?)",
            (device_id, settings.default_start_seq),
        )
        return {"id": device_id, "start_seq": settings.default_start_seq, "last_seq": None}
    return dict(row)


def to_ranges(numbers: list[int]) -> list[list[int]]:
    """Compress a sorted list of integers into inclusive [start, end] ranges."""
    if not numbers:
        return []
    ranges: list[list[int]] = []
    start = prev = numbers[0]
    for value in numbers[1:]:
        if value == prev + 1:
            prev = value
            continue
        ranges.append([start, prev])
        start = prev = value
    ranges.append([start, prev])
    return ranges


def _record_duplicate(
    conn,
    *,
    device_id: str,
    seq: int,
    idempotency_key: str,
    payload: str,
    reason: str,
    original_packet_id: int | None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO duplicates
            (device_id, seq, idempotency_key, payload, reason, original_packet_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (device_id, seq, idempotency_key, payload, reason, original_packet_id),
    )
    return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def ingest_packet(
    *,
    device_id: str,
    seq: int,
    idempotency_key: str,
    payload: str,
) -> dict[str, Any]:
    """Ingest one packet. Idempotent with respect to ``idempotency_key``."""
    with transaction() as conn:
        device = _ensure_device(conn, device_id)
        start_seq = int(device["start_seq"])
        last_seq = device["last_seq"]
        if last_seq is not None:
            last_seq = int(last_seq)

        if seq < start_seq:
            raise ValueError(
                f"seq {seq} is below the device's start sequence {start_seq}"
            )

        # 1) Idempotency key already seen -> never overwrite, record duplicate.
        existing = conn.execute(
            "SELECT id, device_id, seq, committed_at FROM packets WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if existing is not None:
            dup_id = _record_duplicate(
                conn,
                device_id=device_id,
                seq=seq,
                idempotency_key=idempotency_key,
                payload=payload,
                reason="idempotency_key",
                original_packet_id=existing["id"],
            )
            return {
                "status": "duplicate",
                "reason": "idempotency_key",
                "original_packet_id": existing["id"],
                "original_seq": existing["seq"],
                "duplicate_id": dup_id,
                "seq": seq,
                "committed_now": [],
            }

        # 2) Sequence already covered by a committed run -> stale duplicate.
        if last_seq is not None and seq <= last_seq:
            original = conn.execute(
                "SELECT id FROM packets WHERE device_id = ? AND seq = ?",
                (device_id, seq),
            ).fetchone()
            dup_id = _record_duplicate(
                conn,
                device_id=device_id,
                seq=seq,
                idempotency_key=idempotency_key,
                payload=payload,
                reason="seq_already_committed",
                original_packet_id=original["id"] if original else None,
            )
            return {
                "status": "duplicate",
                "reason": "seq_already_committed",
                "original_packet_id": original["id"] if original else None,
                "original_seq": seq,
                "duplicate_id": dup_id,
                "seq": seq,
                "committed_now": [],
            }

        # 3) A staged packet with the same seq but a different key -> duplicate.
        staged = conn.execute(
            "SELECT id FROM packets WHERE device_id = ? AND seq = ? AND committed_at IS NULL",
            (device_id, seq),
        ).fetchone()
        if staged is not None:
            dup_id = _record_duplicate(
                conn,
                device_id=device_id,
                seq=seq,
                idempotency_key=idempotency_key,
                payload=payload,
                reason="seq_already_staged",
                original_packet_id=staged["id"],
            )
            return {
                "status": "duplicate",
                "reason": "seq_already_staged",
                "original_packet_id": staged["id"],
                "original_seq": seq,
                "duplicate_id": dup_id,
                "seq": seq,
                "committed_now": [],
            }

        # 4) New packet: stage it, then commit every consecutive run available.
        cur = conn.execute(
            """
            INSERT INTO packets (device_id, seq, idempotency_key, payload)
            VALUES (?, ?, ?, ?)
            """,
            (device_id, seq, idempotency_key, payload),
        )
        packet_id = int(cur.lastrowid)

        watermark = last_seq if last_seq is not None else start_seq - 1
        rows = conn.execute(
            """
            SELECT seq FROM packets
            WHERE device_id = ? AND seq > ? AND committed_at IS NULL
            ORDER BY seq
            """,
            (device_id, watermark),
        ).fetchall()

        committed: list[int] = []
        expected = watermark + 1
        for row in rows:
            if row["seq"] != expected:
                break
            committed.append(row["seq"])
            expected += 1

        if committed:
            conn.execute(
                """
                UPDATE packets
                SET committed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE device_id = ? AND seq BETWEEN ? AND ? AND committed_at IS NULL
                """,
                (device_id, committed[0], committed[-1]),
            )
            conn.execute(
                """
                UPDATE devices
                SET last_seq = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE id = ?
                """,
                (committed[-1], device_id),
            )

        return {
            "status": "committed" if seq in committed else "staged",
            "reason": None,
            "original_packet_id": None,
            "original_seq": None,
            "duplicate_id": None,
            "seq": seq,
            "packet_id": packet_id,
            "committed_now": committed,
        }


def ingest_batch(device_id: str, packets: list[dict[str, Any]]) -> dict[str, Any]:
    """Ingest an ordered batch of packets for one device.

    Packets are applied one by one; the aggregate counts reflect each packet's
    *final* state after the whole batch has been processed, so a packet that
    arrived staged but had its gap filled later in the same batch is reported as
    committed.
    """
    results = [
        ingest_packet(
            device_id=device_id,
            seq=int(item["seq"]),
            idempotency_key=item["idempotency_key"],
            payload=item["payload"],
        )
        for item in packets
    ]

    final = get_gaps(device_id)
    last_committed = final["last_committed_seq"]
    staged_set = set(final["staged_seqs"])

    duplicates = [
        {
            "seq": r["seq"],
            "reason": r["reason"],
            "original_packet_id": r["original_packet_id"],
            "duplicate_id": r["duplicate_id"],
        }
        for r in results
        if r["status"] == "duplicate"
    ]
    accepted = [r for r in results if r["status"] != "duplicate"]
    committed_seqs = [
        r["seq"] for r in accepted if last_committed is None or r["seq"] <= last_committed
    ]
    staged_seqs = [r["seq"] for r in accepted if r["seq"] in staged_set]
    committed_now = sorted({s for r in results for s in r["committed_now"]})

    return {
        "device_id": device_id,
        "received": len(results),
        "committed_count": len(committed_seqs),
        "staged_count": len(staged_seqs),
        "duplicate_count": len(duplicates),
        "committed_now": committed_now,
        "staged_seqs": staged_seqs,
        "duplicates": duplicates,
        "results": results,
    }


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def device_exists(device_id: str) -> bool:
    with read_tx() as conn:
        return conn.execute(
            "SELECT 1 FROM devices WHERE id = ?", (device_id,)
        ).fetchone() is not None


def list_devices() -> list[dict[str, Any]]:
    with read_tx() as conn:
        rows = conn.execute(
            """
            SELECT id, start_seq, last_seq, created_at, updated_at
            FROM devices ORDER BY created_at
            """
        ).fetchall()
        return [dict(row) for row in rows]


def _device_row(conn, device_id: str):
    row = conn.execute(
        "SELECT id, start_seq, COALESCE(last_seq, start_seq - 1) AS last_seq FROM devices WHERE id = ?",
        (device_id,),
    ).fetchone()
    if row is None:
        raise DeviceNotFoundError(device_id)
    return row


def get_gaps(device_id: str) -> dict[str, Any]:
    """Missing sequences between the committed watermark and staged packets."""
    with read_tx() as conn:
        device = _device_row(conn, device_id)
        start_seq = int(device["start_seq"])
        last_seq = int(device["last_seq"])

        rows = conn.execute(
            """
            SELECT seq FROM packets
            WHERE device_id = ? AND seq > ? AND committed_at IS NULL
            ORDER BY seq
            """,
            (device_id, last_seq),
        ).fetchall()
        staged_seqs = [int(r["seq"]) for r in rows]

    gap_seqs: list[int] = []
    expected = last_seq + 1
    for value in staged_seqs:
        if value > expected:
            gap_seqs.extend(range(expected, value))
        expected = value + 1
    # The sequence that would let the first staged packet commit is always
    # considered missing (nothing staged yet -> gap starts at the watermark).
    # ``gap_seqs`` therefore contains all known holes; the open tail is exposed
    # separately as next_expected.
    return {
        "device_id": device_id,
        "start_seq": start_seq,
        "last_committed_seq": None if last_seq < start_seq else last_seq,
        "next_expected_seq": last_seq + 1,
        "staged_seqs": staged_seqs,
        "gap_seqs": gap_seqs,
        "gap_ranges": to_ranges(gap_seqs),
    }


def get_committed(
    device_id: str, limit: int, offset: int
) -> dict[str, Any]:
    with read_tx() as conn:
        device = _device_row(conn, device_id)
        total = conn.execute(
            "SELECT COUNT(*) AS n FROM packets WHERE device_id = ? AND committed_at IS NOT NULL",
            (device_id,),
        ).fetchone()["n"]
        rows = conn.execute(
            """
            SELECT id, seq, idempotency_key, payload, received_at, committed_at
            FROM packets
            WHERE device_id = ? AND committed_at IS NOT NULL
            ORDER BY seq
            LIMIT ? OFFSET ?
            """,
            (device_id, limit, offset),
        ).fetchall()
    return {
        "device_id": device_id,
        "total": total,
        "limit": limit,
        "offset": offset,
        "last_committed_seq": device["last_seq"],
        "packets": [dict(row) for row in rows],
    }


def get_duplicates(
    device_id: str, limit: int, offset: int
) -> dict[str, Any]:
    with read_tx() as conn:
        _device_row(conn, device_id)  # 404 if unknown
        total = conn.execute(
            "SELECT COUNT(*) AS n FROM duplicates WHERE device_id = ?",
            (device_id,),
        ).fetchone()["n"]
        rows = conn.execute(
            """
            SELECT id, device_id, seq, idempotency_key, payload, reason,
                   original_packet_id, received_at
            FROM duplicates
            WHERE device_id = ?
            ORDER BY received_at, id
            LIMIT ? OFFSET ?
            """,
            (device_id, limit, offset),
        ).fetchall()
    return {
        "device_id": device_id,
        "total": total,
        "limit": limit,
        "offset": offset,
        "duplicates": [dict(row) for row in rows],
    }


def get_status(device_id: str) -> dict[str, Any]:
    gaps = get_gaps(device_id)
    with read_tx() as conn:
        committed_count = conn.execute(
            "SELECT COUNT(*) AS n FROM packets WHERE device_id = ? AND committed_at IS NOT NULL",
            (device_id,),
        ).fetchone()["n"]
        duplicate_count = conn.execute(
            "SELECT COUNT(*) AS n FROM duplicates WHERE device_id = ?",
            (device_id,),
        ).fetchone()["n"]
    return {
        **gaps,
        "committed_count": committed_count,
        "staged_count": len(gaps["staged_seqs"]),
        "duplicate_count": duplicate_count,
    }

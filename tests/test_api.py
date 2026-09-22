"""End-to-end tests of the ingestion/dedup/staging logic via the ASGI app."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.database import reset_for_tests


@pytest.fixture()
def client(tmp_path) -> TestClient:
    reset_for_tests(str(tmp_path / "test.db"))
    from app.main import app

    with TestClient(app) as c:
        yield c


def _post_batch(client, device, packets):
    return client.post(f"/api/v1/devices/{device}/packets/batch", json={"packets": packets})


def _packet(seq, key, value=None):
    return {
        "seq": seq,
        "idempotency_key": key,
        "measurements": [{"channel": "temp", "value": value if value is not None else seq}],
    }


def test_in_order_batch_commits_immediately(client):
    resp = _post_batch(client, "inst-1", [_packet(1, "k1"), _packet(2, "k2"), _packet(3, "k3")])
    assert resp.status_code == 200
    body = resp.json()
    assert body["committed_count"] == 3
    assert body["committed_now"] == [1, 2, 3]
    assert body["duplicate_count"] == 0

    gaps = client.get("/api/v1/devices/inst-1/gaps").json()
    assert gaps["last_committed_seq"] == 3
    assert gaps["next_expected_seq"] == 4
    assert gaps["gap_seqs"] == []
    assert gaps["staged_seqs"] == []


def test_out_of_order_packets_are_staged_then_committed(client):
    # Gap at seq 2 and 4: seq 3 and 5 arrive first and must be staged.
    resp = _post_batch(client, "inst-2", [_packet(1, "a1"), _packet(3, "a3"), _packet(5, "a5")])
    body = resp.json()
    assert body["committed_now"] == [1]
    assert body["staged_seqs"] == [3, 5]

    gaps = client.get("/api/v1/devices/inst-2/gaps").json()
    assert gaps["last_committed_seq"] == 1
    assert gaps["gap_seqs"] == [2, 4]
    assert gaps["gap_ranges"] == [[2, 2], [4, 4]]

    # Filling seq 2 commits 2 and 3 together; seq 5 stays staged.
    resp = _post_batch(client, "inst-2", [_packet(2, "a2")])
    assert resp.json()["committed_now"] == [2, 3]

    gaps = client.get("/api/v1/devices/inst-2/gaps").json()
    assert gaps["last_committed_seq"] == 3
    assert gaps["gap_seqs"] == [4]
    assert gaps["staged_seqs"] == [5]

    # Filling the last gap commits the remaining staged packet.
    resp = _post_batch(client, "inst-2", [_packet(4, "a4")])
    assert resp.json()["committed_now"] == [4, 5]

    gaps = client.get("/api/v1/devices/inst-2/gaps").json()
    assert gaps["last_committed_seq"] == 5
    assert gaps["gap_seqs"] == []


def test_idempotency_key_dedup(client):
    assert _post_batch(client, "inst-3", [_packet(1, "dup-key")]).json()["committed_count"] == 1

    # Same key resent (instrument retry): rejected, recorded as duplicate.
    resp = _post_batch(client, "inst-3", [_packet(2, "dup-key")])
    body = resp.json()
    assert body["duplicate_count"] == 1
    assert body["duplicates"][0]["reason"] == "idempotency_key"
    assert body["committed_count"] == 0

    dups = client.get("/api/v1/devices/inst-3/duplicates").json()
    assert dups["total"] == 1
    assert dups["duplicates"][0]["idempotency_key"] == "dup-key"
    assert dups["duplicates"][0]["seq"] == 2  # seq claimed by the retry, not stored
    assert dups["duplicates"][0]["original_packet_id"] == 1

    # Seq 2 with a fresh key is still missing and gets staged/committed normally.
    resp = _post_batch(client, "inst-3", [_packet(2, "fresh-key")])
    assert resp.json()["committed_now"] == [2]


def test_same_seq_different_key_is_duplicate(client):
    _post_batch(client, "inst-4", [_packet(3, "k3"), _packet(1, "k1")])
    # Retry of seq 3 with a new key while it is still staged.
    resp = _post_batch(client, "inst-4", [_packet(3, "k3-retry")])
    assert resp.json()["duplicates"][0]["reason"] == "seq_already_staged"

    # After the gap closes, a replay of an old seq is flagged as committed dup.
    _post_batch(client, "inst-4", [_packet(2, "k2")])
    resp = _post_batch(client, "inst-4", [_packet(1, "k1-replay")])
    body = resp.json()
    assert body["duplicate_count"] == 1
    assert body["duplicates"][0]["reason"] == "seq_already_committed"

    dups = client.get("/api/v1/devices/inst-4/duplicates").json()
    assert {d["reason"] for d in dups["duplicates"]} == {
        "seq_already_staged",
        "seq_already_committed",
    }


def test_committed_listing_and_status(client):
    _post_batch(client, "inst-5", [_packet(1, "b1"), _packet(2, "b2"), _packet(4, "b4")])

    committed = client.get("/api/v1/devices/inst-5/committed").json()
    assert committed["total"] == 2
    assert [p["seq"] for p in committed["packets"]] == [1, 2]
    assert committed["last_committed_seq"] == 2

    page = client.get("/api/v1/devices/inst-5/committed?limit=1&offset=1").json()
    assert [p["seq"] for p in page["packets"]] == [2]

    status = client.get("/api/v1/devices/inst-5/status").json()
    assert status["committed_count"] == 2
    assert status["staged_count"] == 1
    assert status["gap_seqs"] == [3]
    assert status["duplicate_count"] == 0


def test_unknown_device_returns_404_and_below_start_seq_422(client):
    assert client.get("/api/v1/devices/nope/gaps").status_code == 404
    assert client.get("/api/v1/devices/nope/committed").status_code == 404

    resp = client.post(
        "/api/v1/devices/inst-6/packets",
        json=_packet(0, "zero"),
    )
    assert resp.status_code == 422


def test_batch_reports_final_status_when_gap_fills_within_batch(client):
    # Packet order in the batch is 2, 1, 4: 2 starts staged, then 1 arrives and
    # commits [1, 2]; 4 remains staged waiting for 3.
    resp = _post_batch(client, "inst-8", [_packet(2, "m2"), _packet(1, "m1"), _packet(4, "m4")])
    body = resp.json()
    assert body["committed_count"] == 2
    assert body["staged_count"] == 1
    assert body["committed_now"] == [1, 2]
    assert body["staged_seqs"] == [4]

    gaps = client.get("/api/v1/devices/inst-8/gaps").json()
    assert gaps["last_committed_seq"] == 2
    assert gaps["gap_seqs"] == [3]


def test_single_packet_endpoint(client):
    resp = client.post(
        "/api/v1/devices/inst-7/packets",
        json=_packet(1, "single-1"),
    )
    assert resp.status_code == 200
    assert resp.json()["committed_now"] == [1]

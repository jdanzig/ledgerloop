"""API integration: start -> poll -> approve -> terminal, idempotency,
event-feed pagination, cancellation, error mapping."""

import asyncio

import httpx
import pytest

from .chaos_workflow import ChaosWorkflow
from .test_engine import GatedWorkflow, run_worker

from ledgerloop.api.app import create_app


@pytest.fixture
async def client(pool):
    registry = {"chaos": ChaosWorkflow(), "gated": GatedWorkflow()}
    app = create_app(pool=pool, registry=registry)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            c.registry = registry
            yield c


async def poll_status(client, run_id, statuses, timeout=15.0):
    async def poll():
        while True:
            r = await client.get(f"/runs/{run_id}")
            if r.json()["status"] in statuses:
                return r.json()
            await asyncio.sleep(0.05)

    return await asyncio.wait_for(poll(), timeout)


async def test_full_lifecycle_with_approval(pool, client):
    r = await client.post(
        "/runs",
        json={"workflow_type": "gated", "idempotency_key": "api-1", "input": {"x": 1}},
    )
    assert r.status_code == 200 and r.json()["created"]
    run_id = r.json()["run_id"]

    task = run_worker(pool, client.registry)
    try:
        body = await poll_status(client, run_id, {"awaiting_approval"})
        assert body["pending_approvals"] == ["release"]

        r = await client.post(
            f"/runs/{run_id}/approvals",
            json={"gate_id": "release", "granted": True, "approver": "alice"},
        )
        assert r.status_code == 200

        body = await poll_status(client, run_id, {"completed"})
        assert body["result"] == "shipped"
    finally:
        task.cancel()

    # audit trail shows who released the gate
    r = await client.get(f"/runs/{run_id}/events", params={"type": "approval_granted"})
    events = r.json()["events"]
    assert len(events) == 1
    assert events[0]["payload"]["approver"] == "alice"


async def test_idempotent_post_runs(client):
    body = {"workflow_type": "chaos", "idempotency_key": "api-dup", "input": None}
    r1 = await client.post("/runs", json=body)
    r2 = await client.post("/runs", json=body)
    assert r1.json()["created"] and not r2.json()["created"]
    assert r1.json()["run_id"] == r2.json()["run_id"]


async def test_event_feed_pagination(pool, client):
    r = await client.post(
        "/runs", json={"workflow_type": "chaos", "idempotency_key": "api-page"}
    )
    run_id = r.json()["run_id"]
    task = run_worker(pool, client.registry)
    try:
        await poll_status(client, run_id, {"completed"})
    finally:
        task.cancel()

    seqs, cursor = [], 0
    while True:
        r = await client.get(
            f"/runs/{run_id}/events", params={"after_seq": cursor, "limit": 3}
        )
        page = r.json()
        seqs += [e["seq"] for e in page["events"]]
        if page["next_cursor"] is None:
            break
        cursor = page["next_cursor"]
    assert seqs == list(range(1, len(seqs) + 1))  # gapless, ordered, complete
    assert seqs[-1] >= 11  # 3 steps x (scheduled+claimed+succeeded) + start + complete


async def test_cancel(client):
    r = await client.post(
        "/runs", json={"workflow_type": "chaos", "idempotency_key": "api-cancel"}
    )
    run_id = r.json()["run_id"]
    r = await client.post(f"/runs/{run_id}/cancel")
    assert r.json()["status"] == "cancelled"
    # cancelling a terminal run is a 409, not a double event
    r = await client.post(f"/runs/{run_id}/cancel")
    assert r.status_code == 409


async def test_error_mapping(client):
    assert (await client.get("/runs/00000000-0000-0000-0000-0000000000ff")).status_code == 404
    r = await client.post(
        "/runs", json={"workflow_type": "nope", "idempotency_key": "x"}
    )
    assert r.status_code == 422
    # approval for a gate that isn't pending -> 409
    r = await client.post(
        "/runs", json={"workflow_type": "chaos", "idempotency_key": "api-409"}
    )
    run_id = r.json()["run_id"]
    r = await client.post(
        f"/runs/{run_id}/approvals",
        json={"gate_id": "ghost", "granted": True, "approver": "eve"},
    )
    assert r.status_code == 409

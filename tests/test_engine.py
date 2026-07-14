"""Engine integration: happy path, approval gate, retry/backoff, recovery,
cancellation — against real Postgres, worker running in-process."""

import asyncio

import pytest

from ledgerloop.engine.events import RunStatus, fold, load_events
from ledgerloop.engine.replay import recover
from ledgerloop.engine.scheduler import (
    Action,
    Complete,
    Fail,
    RequestApproval,
    Schedule,
    cancel_run,
    resolve_approval,
    start_run,
)
from ledgerloop.engine.worker import RetryableStepError, TerminalStepError, Worker

from .chaos_workflow import ChaosWorkflow


async def wait_status(pool, run_id, statuses, timeout=15.0):
    async def poll():
        while True:
            status = await pool.fetchval("SELECT status FROM runs WHERE id = $1", run_id)
            if status in statuses:
                return status
            await asyncio.sleep(0.05)

    return await asyncio.wait_for(poll(), timeout)


def run_worker(pool, registry, worker_id="test-worker", **kw):
    w = Worker(pool, registry, worker_id, lease_s=3.0, heartbeat_s=1.0, **kw)
    return asyncio.create_task(w.run_forever())


async def test_happy_path(pool):
    wf = ChaosWorkflow()
    async with pool.acquire() as conn, conn.transaction():
        run_id, created = await start_run(conn, wf, "k1", {"doc": "x"})
    assert created
    task = run_worker(pool, {"chaos": wf})
    try:
        assert await wait_status(pool, run_id, {"completed"}) == "completed"
    finally:
        task.cancel()
    async with pool.acquire() as conn:
        state = fold(run_id, await load_events(conn, run_id))
    assert state.status is RunStatus.COMPLETED
    assert [s.status for s in state.steps.values()] == ["succeeded"] * 3
    # queue drained — no orphans
    assert await pool.fetchval("SELECT count(*) FROM step_queue") == 0


async def test_idempotent_start(pool):
    wf = ChaosWorkflow()
    async with pool.acquire() as conn, conn.transaction():
        r1, c1 = await start_run(conn, wf, "dup", None)
    async with pool.acquire() as conn, conn.transaction():
        r2, c2 = await start_run(conn, wf, "dup", None)
    assert r1 == r2 and c1 and not c2
    assert await pool.fetchval("SELECT count(*) FROM runs") == 1


class GatedWorkflow:
    workflow_type = "gated"

    def plan(self, state) -> list[Action]:
        prep = state.steps.get("prep")
        if prep is None:
            return [Schedule("prep")]
        if prep.status != "succeeded":
            return []
        gate = state.approvals.get("release")
        if gate is None:
            return [RequestApproval("release", {"summary": "ship it?"})]
        if gate["status"] == "rejected":
            return [Fail("rejected_by_" + gate["approver"])]
        commit = state.steps.get("commit")
        if commit is None:
            return [Schedule("commit")]
        if commit.status != "succeeded":
            return []
        return [Complete(result="shipped")]

    async def run_step(self, step_id, ctx):
        return {"did": step_id}


async def test_approval_gate(pool):
    wf = GatedWorkflow()
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "gated1", None)
    task = run_worker(pool, {"gated": wf})
    try:
        # workflow pauses at the gate; nothing to claim
        assert await wait_status(pool, run_id, {"awaiting_approval"}) == "awaiting_approval"
        await asyncio.sleep(0.5)
        assert await pool.fetchval("SELECT count(*) FROM step_queue") == 0
        async with pool.acquire() as conn, conn.transaction():
            await resolve_approval(conn, wf, run_id, "release", True, "alice")
        assert await wait_status(pool, run_id, {"completed"}) == "completed"
    finally:
        task.cancel()
    async with pool.acquire() as conn:
        state = fold(run_id, await load_events(conn, run_id))
    assert state.approvals["release"]["approver"] == "alice"  # audit: who released


async def test_approval_rejected_fails_run(pool):
    wf = GatedWorkflow()
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "gated2", None)
    task = run_worker(pool, {"gated": wf})
    try:
        await wait_status(pool, run_id, {"awaiting_approval"})
        async with pool.acquire() as conn, conn.transaction():
            await resolve_approval(conn, wf, run_id, "release", False, "bob")
        assert await wait_status(pool, run_id, {"failed"}) == "failed"
    finally:
        task.cancel()


class FlakyWorkflow:
    workflow_type = "flaky"
    fail_times = 2

    def __init__(self):
        self.calls = 0

    def plan(self, state) -> list[Action]:
        step = state.steps.get("only")
        if step is None:
            return [Schedule("only")]
        if step.status == "succeeded":
            return [Complete()]
        return []

    async def run_step(self, step_id, ctx):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RetryableStepError("transient")
        return "ok"


async def test_retry_with_backoff(pool):
    wf = FlakyWorkflow()
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "flaky1", None)
    task = run_worker(pool, {"flaky": wf})
    try:
        assert await wait_status(pool, run_id, {"completed"}, timeout=30) == "completed"
    finally:
        task.cancel()
    async with pool.acquire() as conn:
        events = await load_events(conn, run_id)
    retries = [e for e in events if e[1] == "step_retry_scheduled"]
    assert len(retries) == 2
    state = fold(run_id, events)
    assert state.steps["only"].attempt == 3


class DoomedWorkflow(FlakyWorkflow):
    workflow_type = "doomed"

    async def run_step(self, step_id, ctx):
        raise TerminalStepError("validation says no")


async def test_terminal_failure_fails_run(pool):
    wf = DoomedWorkflow()
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "doom1", None)
    task = run_worker(pool, {"doomed": wf})
    try:
        assert await wait_status(pool, run_id, {"failed"}) == "failed"
    finally:
        task.cancel()
    async with pool.acquire() as conn:
        state = fold(run_id, await load_events(conn, run_id))
    assert state.failure_reason == "step_failed_unhandled"
    assert state.steps["only"].error["type"] == "TerminalStepError"


async def test_recovery_rematerializes_queue(pool):
    """Simulate the projection being lost: delete queue rows mid-run, recover()."""
    wf = ChaosWorkflow()
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "recov1", None)
    # no worker running; wipe the queue (crash-corruption stand-in)
    await pool.execute("DELETE FROM step_queue")
    restored = await recover(pool)
    assert restored == 1  # step "a" re-materialized from the log
    task = run_worker(pool, {"chaos": wf})
    try:
        assert await wait_status(pool, run_id, {"completed"}) == "completed"
    finally:
        task.cancel()


async def test_worker_never_claims_foreign_workflow_types(pool):
    """A worker must not lease steps for workflow types outside its registry —
    it can't execute them, and holding the lease starves workers that can."""
    wf = ChaosWorkflow()
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "foreign1", None)
    task = run_worker(pool, {"some_other_type": wf})  # knows nothing of "chaos"
    try:
        await asyncio.sleep(1.0)
        row = await pool.fetchrow("SELECT claimed_by, lease_expires_at FROM step_queue")
        assert row is not None and row["claimed_by"] is None  # untouched
    finally:
        task.cancel()
    # a capable worker picks it up normally
    task = run_worker(pool, {"chaos": wf})
    try:
        assert await wait_status(pool, run_id, {"completed"}) == "completed"
    finally:
        task.cancel()


async def test_cancellation_observed_on_claim(pool):
    wf = ChaosWorkflow()
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "cancel1", None)
    async with pool.acquire() as conn, conn.transaction():
        await cancel_run(conn, run_id)
    task = run_worker(pool, {"chaos": wf})
    try:
        await asyncio.sleep(1.0)  # worker claims, observes cancelled, drops row
        assert await pool.fetchval("SELECT count(*) FROM step_queue") == 0
        status = await pool.fetchval("SELECT status FROM runs WHERE id = $1", run_id)
        assert status == "cancelled"
        # no step ever executed
        n = await pool.fetchval(
            "SELECT count(*) FROM events WHERE run_id = $1 AND type = 'step_succeeded'",
            run_id,
        )
        assert n == 0
    finally:
        task.cancel()

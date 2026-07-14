"""Zombie fencing: a worker stalls past its lease, a peer steals the step and
completes it, the zombie wakes and tries to record its result — the fence
inside the commit transaction must discard it."""

import asyncio

from ledgerloop.engine.events import fold, load_events
from ledgerloop.engine.scheduler import Action, Complete, Schedule, start_run
from ledgerloop.engine.worker import Worker

from .test_engine import wait_status


class StallableWorkflow:
    """The step blocks while executed by the zombie until released."""

    workflow_type = "stall"

    def __init__(self):
        self.zombie_release = asyncio.Event()
        self.zombie_started = asyncio.Event()

    def plan(self, state) -> list[Action]:
        step = state.steps.get("only")
        if step is None:
            return [Schedule("only")]
        if step.status != "succeeded":
            return []
        return [Complete(result=step.result)]

    async def run_step(self, step_id, ctx):
        if ctx.worker_id == "zombie":
            self.zombie_started.set()
            await self.zombie_release.wait()  # SIGSTOP stand-in
            return "zombie-result"
        return "healthy-result"


async def test_zombie_late_result_is_fenced(pool):
    wf = StallableWorkflow()
    registry = {"stall": wf}
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "zombie-1", None)

    # zombie: 1s lease and a heartbeat too slow to ever renew it
    zombie = Worker(pool, registry, "zombie", lease_s=1.0, heartbeat_s=100.0)
    z_task = asyncio.create_task(zombie.run_forever())
    await asyncio.wait_for(wf.zombie_started.wait(), 10)
    await asyncio.sleep(1.2)  # lease expires while the zombie is stalled

    # peer steals the expired lease and completes the step
    healthy = Worker(pool, registry, "healthy", lease_s=5.0, heartbeat_s=1.0)
    h_task = asyncio.create_task(healthy.run_forever())
    try:
        assert await wait_status(pool, run_id, {"completed"}) == "completed"
        # wake the zombie; its success commit must hit the fence
        wf.zombie_release.set()
        await asyncio.sleep(0.5)
    finally:
        z_task.cancel()
        h_task.cancel()

    async with pool.acquire() as conn:
        events = await load_events(conn, run_id)
        state = fold(run_id, events)
    assert state.steps["only"].result == "healthy-result"  # zombie's discarded
    succeeded = [e for e in events if e[1] == "step_succeeded"]
    assert len(succeeded) == 1  # effective-once recording
    claims = [e[2]["worker"] for e in events if e[1] == "step_claimed"]
    assert claims == ["zombie", "healthy"]  # the audit trail shows the steal
    assert await pool.fetchval("SELECT count(*) FROM step_queue") == 0

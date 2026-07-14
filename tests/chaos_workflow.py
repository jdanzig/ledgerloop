"""Three-step linear workflow used by chaos and integration tests.

Each step records a side effect keyed by (run_id, step_id, attempt) — the
idempotency-key pattern the engine promises for external effects.
"""

import asyncio
import random

from ledgerloop.engine.scheduler import Action, Complete, Schedule
from ledgerloop.engine.events import RunState

EFFECTS_DDL = """
CREATE TABLE IF NOT EXISTS chaos_effects (
    run_id  UUID NOT NULL,
    step_id TEXT NOT NULL,
    attempt INT  NOT NULL,
    PRIMARY KEY (run_id, step_id, attempt)
)
"""

STEPS = ["a", "b", "c"]


class ChaosWorkflow:
    workflow_type = "chaos"

    def plan(self, state: RunState) -> list[Action]:
        for sid in STEPS:
            step = state.steps.get(sid)
            if step is None:
                return [Schedule(sid)]
            if step.status != "succeeded":
                return []  # in flight (or failed -> engine fails the run)
        return [Complete(result={"steps": len(STEPS)})]

    async def run_step(self, step_id: str, ctx) -> dict:
        async with ctx.pool.acquire() as conn:
            await conn.execute(EFFECTS_DDL)
            await conn.execute(
                "INSERT INTO chaos_effects (run_id, step_id, attempt)"
                " VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                ctx.run_id, step_id, ctx.attempt,
            )
        await asyncio.sleep(random.uniform(0.05, 0.3))
        return {"step": step_id}


WORKFLOWS = [ChaosWorkflow()]

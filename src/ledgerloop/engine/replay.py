"""Recovery: fold non-terminal runs, diff against the queue, re-materialize.

The queue is a projection of the log; because appends and queue mutations share
a transaction it should never diverge, but recovery re-derives it anyway —
that's what makes the queue rebuildable from scratch (drop it, run this).
Recorded results live in the log and are never re-executed.
"""

from __future__ import annotations

import asyncpg

from .events import TERMINAL_STATUSES, fold, load_events
from .queue import insert_step

_PENDING = {"scheduled", "claimed", "retrying"}


async def recover(pool: asyncpg.Pool) -> int:
    """Re-materialize missing queue rows for all non-terminal runs.
    Idempotent (ON CONFLICT DO NOTHING) — safe to race across workers."""
    restored = 0
    async with pool.acquire() as conn:
        run_ids = [
            str(r["id"])
            for r in await conn.fetch(
                "SELECT id FROM runs WHERE status NOT IN ('completed', 'failed', 'cancelled')"
            )
        ]
        for run_id in run_ids:
            async with conn.transaction():
                state = fold(run_id, await load_events(conn, run_id))
                await conn.execute(
                    "UPDATE runs SET status = $2 WHERE id = $1 AND status <> $2",
                    run_id, state.status.value,
                )
                if state.status in TERMINAL_STATUSES:
                    continue
                for step in state.steps.values():
                    if step.status in _PENDING:
                        await insert_step(conn, run_id, step.step_id, step.attempt)
                        restored += 1
    return restored

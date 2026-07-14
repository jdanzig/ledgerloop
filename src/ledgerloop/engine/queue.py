"""Step queue: claim, lease, heartbeat, backoff. The claim query is the reaper."""

from __future__ import annotations

import random

import asyncpg

CLAIM_SQL = """
UPDATE step_queue
SET    claimed_by = $1,
       lease_expires_at = now() + make_interval(secs => $2)
WHERE  id = (
    SELECT q.id FROM step_queue q
    JOIN runs r ON r.id = q.run_id
    WHERE  q.run_after <= now()
      AND (q.lease_expires_at IS NULL OR q.lease_expires_at < now())
      AND ($3::text[] IS NULL OR r.workflow_type = ANY($3::text[]))
    ORDER BY q.run_after
    FOR UPDATE OF q SKIP LOCKED
    LIMIT 1
)
RETURNING id, run_id, step_id, attempt
"""


async def claim(
    conn: asyncpg.Connection,
    worker_id: str,
    lease_s: float = 30.0,
    workflow_types: list[str] | None = None,
) -> asyncpg.Record | None:
    """Claim one due step this worker can actually execute — a worker must
    never lease work for a workflow_type outside its registry, or it starves
    the workers that could run it."""
    return await conn.fetchrow(CLAIM_SQL, worker_id, lease_s, workflow_types)


async def heartbeat(
    conn: asyncpg.Connection, queue_id: int, worker_id: str, lease_s: float = 30.0
) -> bool:
    """Extend the lease. False means we no longer own the row (fenced)."""
    tag = await conn.execute(
        "UPDATE step_queue SET lease_expires_at = now() + make_interval(secs => $3)"
        " WHERE id = $1 AND claimed_by = $2 AND lease_expires_at > now()",
        queue_id, worker_id, lease_s,
    )
    return tag == "UPDATE 1"


async def insert_step(
    conn: asyncpg.Connection,
    run_id: str,
    step_id: str,
    attempt: int = 1,
    delay_s: float = 0.0,
) -> None:
    """Idempotent: UNIQUE(run_id, step_id, attempt) absorbs re-materialization."""
    await conn.execute(
        "INSERT INTO step_queue (run_id, step_id, attempt, run_after)"
        " VALUES ($1, $2, $3, now() + make_interval(secs => $4))"
        " ON CONFLICT (run_id, step_id, attempt) DO NOTHING",
        run_id, step_id, attempt, delay_s,
    )


def backoff_delay(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """base * 2^attempt, capped, with full jitter."""
    return random.uniform(0, min(cap, base * 2**attempt))

"""Claim loop, step dispatch, crash safety.

Crash-safety shape: claiming appends step_claimed; completing re-verifies the
lease (zombie fence) and appends step_succeeded + next scheduling atomically.
A kill -9 anywhere leaves either an expired lease (reclaimed by the claim
query) or a committed result — never a lost or doubled recording.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
from dataclasses import dataclass

import asyncpg

from . import queue
from .db import create_pool
from .events import (
    Event,
    EventType,
    RunState,
    TERMINAL_STATUSES,
    append,
    fold,
    load_events,
)
from .replay import recover
from .scheduler import Workflow, advance

log = logging.getLogger("ledgerloop.worker")


class RetryableStepError(Exception):
    """Transient: network, 429, timeout. Backoff and retry."""


class TerminalStepError(Exception):
    """Permanent: validation, business rule. Scheduler decides what's next."""


class Fenced(Exception):
    """We no longer own the lease; a peer superseded us. Discard the result."""


@dataclass
class StepContext:
    run_id: str
    step_id: str
    attempt: int
    state: RunState
    pool: asyncpg.Pool
    worker_id: str
    queue_id: int


class Worker:
    def __init__(
        self,
        pool: asyncpg.Pool,
        registry: dict[str, Workflow],
        worker_id: str,
        lease_s: float = 30.0,
        heartbeat_s: float = 10.0,
        poll_s: float = 0.25,
        max_attempts: int = 5,
    ):
        self.pool = pool
        self.registry = registry
        self.worker_id = worker_id
        self.lease_s = lease_s
        self.heartbeat_s = heartbeat_s
        self.poll_s = poll_s
        self.max_attempts = max_attempts

    async def run_forever(self) -> None:
        while True:
            row = await self._claim()
            if row is None:
                await asyncio.sleep(self.poll_s)
                continue
            try:
                await self._process(row)
            except Fenced:
                log.warning("fenced: %s/%s", row["run_id"], row["step_id"])
            except Exception:
                # Process-level bug; the lease will expire and a peer retries.
                log.exception("unhandled error on %s", dict(row))

    async def _claim(self) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await queue.claim(
                conn, self.worker_id, self.lease_s, list(self.registry)
            )

    async def _process(self, row: asyncpg.Record) -> None:
        queue_id, run_id = row["id"], str(row["run_id"])
        step_id, attempt = row["step_id"], row["attempt"]

        async with self.pool.acquire() as conn, conn.transaction():
            state = fold(run_id, await load_events(conn, run_id))
            if state.status in TERMINAL_STATUSES:  # cancelled/failed while queued
                await conn.execute("DELETE FROM step_queue WHERE id = $1", queue_id)
                return
            await append(
                conn,
                run_id,
                [
                    Event(
                        type=EventType.STEP_CLAIMED,
                        payload={
                            "step_id": step_id,
                            "attempt": attempt,
                            "worker": self.worker_id,
                        },
                    )
                ],
            )

        wf = self.registry[state.workflow_type]
        ctx = StepContext(
            run_id=run_id,
            step_id=step_id,
            attempt=attempt,
            state=state,
            pool=self.pool,
            worker_id=self.worker_id,
            queue_id=queue_id,
        )
        hb = asyncio.create_task(self._heartbeat_loop(queue_id))
        try:
            result = await wf.run_step(step_id, ctx)
        except TerminalStepError as e:
            await self._record_failure(wf, queue_id, run_id, step_id, attempt, e, retry=False)
            return
        except Exception as e:  # unknown + retryable both retry, capped
            retry = attempt < self.max_attempts
            await self._record_failure(wf, queue_id, run_id, step_id, attempt, e, retry=retry)
            return
        finally:
            hb.cancel()

        async with self.pool.acquire() as conn, conn.transaction():
            await self._fence(conn, queue_id)
            await append(
                conn,
                run_id,
                [
                    Event(
                        type=EventType.STEP_SUCCEEDED,
                        payload={
                            "step_id": step_id,
                            "attempt": attempt,
                            "result": result,
                        },
                    )
                ],
            )
            await conn.execute("DELETE FROM step_queue WHERE id = $1", queue_id)
            await advance(conn, wf, run_id)

    async def _fence(self, conn: asyncpg.Connection, queue_id: int) -> None:
        """Zombie fence: verify we still hold a live lease, inside the commit txn."""
        row = await conn.fetchrow(
            "SELECT 1 FROM step_queue WHERE id = $1 AND claimed_by = $2"
            " AND lease_expires_at > now() FOR UPDATE",
            queue_id, self.worker_id,
        )
        if row is None:
            raise Fenced(str(queue_id))

    async def _record_failure(
        self,
        wf: Workflow,
        queue_id: int,
        run_id: str,
        step_id: str,
        attempt: int,
        exc: Exception,
        retry: bool,
    ) -> None:
        error = {"type": type(exc).__name__, "message": str(exc)}
        async with self.pool.acquire() as conn, conn.transaction():
            await self._fence(conn, queue_id)
            if retry:
                delay = queue.backoff_delay(attempt)
                await append(
                    conn,
                    run_id,
                    [
                        Event(
                            type=EventType.STEP_RETRY_SCHEDULED,
                            payload={
                                "step_id": step_id,
                                "attempt": attempt,
                                "next_attempt": attempt + 1,
                                "delay_s": delay,
                                "error": error,
                            },
                        )
                    ],
                )
                await queue.insert_step(conn, run_id, step_id, attempt + 1, delay)
                await conn.execute("DELETE FROM step_queue WHERE id = $1", queue_id)
            else:
                await append(
                    conn,
                    run_id,
                    [
                        Event(
                            type=EventType.STEP_FAILED,
                            payload={
                                "step_id": step_id,
                                "attempt": attempt,
                                "error": error,
                            },
                        )
                    ],
                )
                await conn.execute("DELETE FROM step_queue WHERE id = $1", queue_id)
                await advance(conn, wf, run_id)

    async def _heartbeat_loop(self, queue_id: int) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_s)
            async with self.pool.acquire() as conn:
                if not await queue.heartbeat(
                    conn, queue_id, self.worker_id, self.lease_s
                ):
                    log.warning("lost lease on queue row %s", queue_id)
                    return


def load_registry(spec: str) -> dict[str, Workflow]:
    """spec: comma-separated module paths, each exposing WORKFLOWS: list[Workflow]."""
    registry: dict[str, Workflow] = {}
    for mod_path in filter(None, spec.split(",")):
        mod = importlib.import_module(mod_path.strip())
        for wf in mod.WORKFLOWS:
            registry[wf.workflow_type] = wf
    return registry


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    pool = await create_pool()
    registry = load_registry(os.environ["LEDGERLOOP_WORKFLOWS"])
    worker_id = os.environ.get("WORKER_ID", f"worker-{os.getpid()}")
    await recover(pool)
    await Worker(
        pool,
        registry,
        worker_id,
        lease_s=float(os.environ.get("LEDGERLOOP_LEASE_S", "30")),
        heartbeat_s=float(os.environ.get("LEDGERLOOP_HEARTBEAT_S", "10")),
    ).run_forever()


if __name__ == "__main__":
    asyncio.run(main())

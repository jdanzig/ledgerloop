"""Chaos: N runs, M worker subprocesses, a supervisor SIGKILLs random workers
until quiescence. Assert: every run terminal (completed), every step_succeeded
recorded exactly once per step, no orphaned leases."""

import asyncio
import os
import pathlib
import random
import signal
import subprocess
import sys

from ledgerloop.engine.scheduler import start_run

from .chaos_workflow import ChaosWorkflow, EFFECTS_DDL, STEPS

REPO = pathlib.Path(__file__).parent.parent
N_RUNS = 25
N_WORKERS = 3
# ponytail: 1.5s cadence = ~2 kills per lease window; 0.75s converges on an
# idle box but flakes when the colima VM is busy (kills outpace 3s lease reaps)
KILL_EVERY_S = 1.5
TIMEOUT_S = 120.0


def spawn_worker(i: int) -> subprocess.Popen:
    env = os.environ | {
        "WORKER_ID": f"chaos-{i}-{random.randrange(1 << 30)}",
        "LEDGERLOOP_WORKFLOWS": "tests.chaos_workflow",
        "LEDGERLOOP_LEASE_S": "3",
        "LEDGERLOOP_HEARTBEAT_S": "1",
        "PYTHONPATH": str(REPO),
    }
    return subprocess.Popen(
        [sys.executable, "-m", "ledgerloop.engine.worker"],
        env=env, cwd=REPO,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


async def test_chaos_convergence(pool):  # own deadline assert below
    await pool.execute(EFFECTS_DDL)
    await pool.execute("TRUNCATE chaos_effects")

    wf = ChaosWorkflow()
    run_ids = []
    for i in range(N_RUNS):
        async with pool.acquire() as conn, conn.transaction():
            run_id, _ = await start_run(conn, wf, f"chaos-{i}", {"n": i})
            run_ids.append(run_id)

    workers = [spawn_worker(i) for i in range(N_WORKERS)]
    try:
        deadline = asyncio.get_event_loop().time() + TIMEOUT_S
        last_kill = 0.0
        while True:
            now = asyncio.get_event_loop().time()
            assert now < deadline, "chaos did not converge in time"
            remaining = await pool.fetchval(
                "SELECT count(*) FROM runs WHERE status NOT IN"
                " ('completed', 'failed', 'cancelled')"
            )
            if remaining == 0:
                break
            if now - last_kill >= KILL_EVERY_S:
                last_kill = now
                victim = random.randrange(N_WORKERS)
                workers[victim].kill()  # SIGKILL — no cleanup allowed
                workers[victim].wait()
                workers[victim] = spawn_worker(victim)
            await asyncio.sleep(0.25)
    finally:
        for w in workers:
            w.send_signal(signal.SIGKILL)
            w.wait()

    # every run completed (never failed/cancelled)
    statuses = await pool.fetch("SELECT id, status FROM runs")
    assert all(r["status"] == "completed" for r in statuses), [dict(r) for r in statuses]

    # effective-once recording: exactly one step_succeeded per (run, step)
    rows = await pool.fetch(
        "SELECT run_id, payload->>'step_id' AS step_id, count(*) AS n"
        " FROM events WHERE type = 'step_succeeded' GROUP BY 1, 2"
    )
    assert len(rows) == N_RUNS * len(STEPS)
    assert all(r["n"] == 1 for r in rows), [dict(r) for r in rows if r["n"] != 1]

    # no orphaned leases / queue drained
    assert await pool.fetchval("SELECT count(*) FROM step_queue") == 0

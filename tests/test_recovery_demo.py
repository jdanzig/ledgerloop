"""The money demo, as a test: kill -9 a real worker process mid-extraction,
restart, watch the run resume and complete exactly once — with the crash seam
visible in the audit trail."""

import asyncio
import os
import pathlib
import signal
import subprocess
import sys

from ledgerloop.engine.scheduler import resolve_approval, start_run

from .demo_workflow import make

REPO = pathlib.Path(__file__).parent.parent
CORPUS = REPO / "corpus"


def spawn_worker(worker_id: str) -> subprocess.Popen:
    env = os.environ | {
        "WORKER_ID": worker_id,
        "LEDGERLOOP_WORKFLOWS": "tests.demo_workflow",
        "LEDGERLOOP_LEASE_S": "3",
        "LEDGERLOOP_HEARTBEAT_S": "1",
        "PYTHONPATH": str(REPO),
    }
    return subprocess.Popen(
        [sys.executable, "-m", "ledgerloop.engine.worker"],
        env=env, cwd=REPO,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


async def wait_for(pool, query, *args, timeout=30.0):
    async def poll():
        while True:
            if row := await pool.fetchval(query, *args):
                return row
            await asyncio.sleep(0.1)

    return await asyncio.wait_for(poll(), timeout)


async def test_kill9_mid_extraction_resumes_and_completes_once(pool):
    wf = make()
    doc = {"title": "MSA", "text": (CORPUS / "acme-msa.txt").read_text()}
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "demo-recovery", doc)

    w1 = spawn_worker("victim")
    try:
        # wait until extraction is claimed and in flight, then SIGKILL
        await wait_for(
            pool,
            "SELECT 1 FROM events WHERE run_id = $1 AND type = 'step_claimed'"
            " AND payload->>'step_id' = 'extract-1'",
            run_id,
        )
        w1.send_signal(signal.SIGKILL)
        w1.wait()

        # the model call died with the worker: no decision was recorded
        assert not await pool.fetchval(
            "SELECT 1 FROM events WHERE run_id = $1 AND type = 'llm_decision'", run_id
        )

        w2 = spawn_worker("rescuer")
        try:
            await wait_for(
                pool,
                "SELECT 1 FROM runs WHERE id = $1 AND status = 'awaiting_approval'",
                run_id,
            )
            async with pool.acquire() as conn, conn.transaction():
                await resolve_approval(conn, wf, run_id, "review-1", True, "demo")
            await wait_for(
                pool, "SELECT 1 FROM runs WHERE id = $1 AND status = 'completed'", run_id
            )
        finally:
            w2.send_signal(signal.SIGKILL)
            w2.wait()
    finally:
        if w1.poll() is None:
            w1.kill()
            w1.wait()

    # the seam: extract-1 claimed by victim, then by rescuer — same attempt
    claims = await pool.fetch(
        "SELECT payload FROM events WHERE run_id = $1 AND type = 'step_claimed'"
        " AND payload->>'step_id' = 'extract-1' ORDER BY seq",
        run_id,
    )
    assert [c["payload"]["worker"] for c in claims] == ["victim", "rescuer"]
    # exactly once: one decision, one extraction success, one graph commit
    # 5 steps: ingest, extract-1, normalize-1, risk-1, commit-1 (approval is a gate)
    for etype, n in [("llm_decision", 1), ("step_succeeded", 5)]:
        count = await pool.fetchval(
            "SELECT count(*) FROM events WHERE run_id = $1 AND type = $2", run_id, etype
        )
        assert count == n, (etype, count)
    assert await pool.fetchval(
        "SELECT count(*) FROM entities WHERE type = 'contract'"
    ) == 1

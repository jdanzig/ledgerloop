"""Platform API: runs, events, approvals. OpenAPI docs are a deliverable —
this is the surface other teams build on."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..domain.contracts import graph
from ..engine.db import create_pool
from ..engine.events import IllegalTransition, RunState, fold, load_events
from ..engine.scheduler import Workflow, cancel_run, resolve_approval, start_run
from ..engine.worker import load_registry


class StartRunRequest(BaseModel):
    workflow_type: str
    idempotency_key: str = Field(min_length=1)
    input: Any = None


class StartRunResponse(BaseModel):
    run_id: str
    created: bool  # false = idempotency key already seen; same run returned


class ApprovalRequest(BaseModel):
    gate_id: str
    granted: bool
    approver: str = Field(min_length=1, description="Recorded in the audit trail")
    notes: str | None = None


def create_app(
    pool: asyncpg.Pool | None = None,
    registry: dict[str, Workflow] | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.pool = pool or await create_pool()
        app.state.registry = registry if registry is not None else load_registry(
            os.environ.get("LEDGERLOOP_WORKFLOWS", "")
        )
        yield
        if pool is None:
            await app.state.pool.close()

    app = FastAPI(
        title="ledgerloop",
        description="Durable agent workflow runtime. Events are the source of "
        "truth; every state below is a fold of the run's event log.",
        lifespan=lifespan,
    )

    def get_pool(request: Request) -> asyncpg.Pool:
        return request.app.state.pool

    def get_registry(request: Request) -> dict[str, Workflow]:
        return request.app.state.registry

    async def folded(conn: asyncpg.Connection, run_id: str) -> RunState:
        events = await load_events(conn, run_id)
        if not events:
            raise HTTPException(404, f"run {run_id} not found")
        return fold(run_id, events)

    @app.post("/runs", response_model=StartRunResponse)
    async def post_run(
        body: StartRunRequest,
        pool: asyncpg.Pool = Depends(get_pool),
        registry: dict[str, Workflow] = Depends(get_registry),
    ):
        wf = registry.get(body.workflow_type)
        if wf is None:
            raise HTTPException(422, f"unknown workflow_type {body.workflow_type!r}")
        async with pool.acquire() as conn, conn.transaction():
            run_id, created = await start_run(
                conn, wf, body.idempotency_key, body.input
            )
        return StartRunResponse(run_id=run_id, created=created)

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str, pool: asyncpg.Pool = Depends(get_pool)):
        async with pool.acquire() as conn:
            state = await folded(conn, run_id)
            timing = await conn.fetchrow(
                "SELECT min(created_at) AS started_at, max(created_at) AS updated_at"
                " FROM events WHERE run_id = $1",
                run_id,
            )
        in_flight = [
            s.step_id for s in state.steps.values()
            if s.status in ("scheduled", "claimed", "retrying")
        ]
        return {
            "run_id": run_id,
            "workflow_type": state.workflow_type,
            "status": state.status.value,
            "current_steps": in_flight,
            "steps": {
                sid: {"status": s.status, "attempt": s.attempt}
                for sid, s in state.steps.items()
            },
            "pending_approvals": [
                g for g, a in state.approvals.items() if a["status"] == "requested"
            ],
            "result": state.result,
            "failure_reason": state.failure_reason,
            "llm_decisions": state.llm_decisions,
            "started_at": timing["started_at"],
            "updated_at": timing["updated_at"],
            "last_seq": state.last_seq,
        }

    @app.get("/runs/{run_id}/events")
    async def get_events(
        run_id: str,
        pool: asyncpg.Pool = Depends(get_pool),
        after_seq: int = Query(0, description="Cursor: return events with seq > this"),
        limit: int = Query(100, ge=1, le=1000),
        type: str | None = Query(None, description="Filter by event type"),
    ):
        async with pool.acquire() as conn:
            if not await conn.fetchval("SELECT 1 FROM runs WHERE id = $1", run_id):
                raise HTTPException(404, f"run {run_id} not found")
            rows = await conn.fetch(
                "SELECT seq, type, payload, created_at FROM events"
                " WHERE run_id = $1 AND seq > $2 AND ($3::text IS NULL OR type = $3)"
                " ORDER BY seq LIMIT $4",
                run_id, after_seq, type, limit,
            )
        events = [dict(r) for r in rows]
        return {
            "events": events,
            "next_cursor": events[-1]["seq"] if len(events) == limit else None,
        }

    @app.post("/runs/{run_id}/approvals")
    async def post_approval(
        run_id: str,
        body: ApprovalRequest,
        pool: asyncpg.Pool = Depends(get_pool),
        registry: dict[str, Workflow] = Depends(get_registry),
    ):
        async with pool.acquire() as conn, conn.transaction():
            state = await folded(conn, run_id)
            wf = registry.get(state.workflow_type)
            if wf is None:
                raise HTTPException(422, f"no workflow registered for {state.workflow_type!r}")
            try:
                state = await resolve_approval(
                    conn, wf, run_id, body.gate_id, body.granted,
                    body.approver, body.notes,
                )
            except IllegalTransition as e:
                raise HTTPException(409, str(e))
        return {"run_id": run_id, "status": state.status.value}

    @app.get("/graph/vendors/{party_id}/spend")
    async def get_vendor_spend(party_id: str, pool: asyncpg.Pool = Depends(get_pool)):
        async with pool.acquire() as conn:
            if not await conn.fetchval(
                "SELECT 1 FROM entities WHERE id = $1 AND type = 'party'", party_id
            ):
                raise HTTPException(404, f"party {party_id} not found")
            return await graph.vendor_spend(conn, party_id)

    @app.get("/graph/obligations")
    async def get_obligations(
        due_within: str = Query("90d", pattern=r"^\d+d?$"),
        pool: asyncpg.Pool = Depends(get_pool),
    ):
        days = int(due_within.rstrip("d"))
        async with pool.acquire() as conn:
            return {"due_within_days": days,
                    "obligations": await graph.obligations_due(conn, days)}

    @app.get("/graph/contracts/{contract_id}/current")
    async def get_current_terms(
        contract_id: str, pool: asyncpg.Pool = Depends(get_pool)
    ):
        """Resolve current terms by walking the SUPERSEDES chain to its head."""
        async with pool.acquire() as conn:
            resolved = await graph.current_terms(conn, contract_id)
        if resolved is None:
            raise HTTPException(404, f"contract {contract_id} not found")
        return resolved

    @app.post("/runs/{run_id}/cancel")
    async def post_cancel(run_id: str, pool: asyncpg.Pool = Depends(get_pool)):
        async with pool.acquire() as conn, conn.transaction():
            state = await folded(conn, run_id)
            try:
                await cancel_run(conn, run_id)
            except IllegalTransition as e:
                raise HTTPException(409, str(e))
        return {"run_id": run_id, "status": "cancelled"}

    return app


app = create_app()

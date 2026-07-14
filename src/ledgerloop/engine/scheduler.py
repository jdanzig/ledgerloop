"""State transitions -> next steps. `advance` is the single choke point:
called after every append (by workers, the API, and ingress) inside the
same transaction, so "record what happened + schedule what's next" is atomic.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

import asyncpg

from .events import (
    Event,
    EventType,
    IllegalTransition,
    RunState,
    RunStatus,
    TERMINAL_STATUSES,
    append,
    apply,
    fold,
    load_events,
)
from .queue import insert_step


@dataclass
class Schedule:
    step_id: str
    delay_s: float = 0.0


@dataclass
class RequestApproval:
    gate_id: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class Complete:
    result: Any = None


@dataclass
class Fail:
    reason: str


Action = Schedule | RequestApproval | Complete | Fail


class StepContext(Protocol):
    """What a step handler gets. Fleshed out by the runtime (Phase 2)."""

    run_id: str
    state: RunState


class Workflow(Protocol):
    workflow_type: str

    def plan(self, state: RunState) -> list[Action]:
        """Pure: folded state -> next actions. Called after every append.
        Must be idempotent — the engine drops actions already reflected
        in state (scheduled steps, requested gates)."""
        ...

    async def run_step(self, step_id: str, ctx: Any) -> Any:
        """Execute one step; return its JSON-serializable result."""
        ...


async def advance(
    conn: asyncpg.Connection, wf: Workflow, run_id: str, state: RunState | None = None
) -> RunState:
    """Fold, plan, apply: append scheduling events + queue rows, sync runs.status.
    Caller must hold a transaction (same txn as the triggering append)."""
    if state is None:
        state = fold(run_id, await load_events(conn, run_id))
    if state.status in TERMINAL_STATUSES:
        await _sync_status(conn, state)
        return state

    new_events: list[Event] = []
    queue_rows: list[Schedule] = []
    if state.status is RunStatus.RUNNING:
        for action in wf.plan(state):
            match action:
                case Schedule(step_id=sid) if sid not in state.steps:
                    new_events.append(
                        Event(
                            type=EventType.STEP_SCHEDULED,
                            payload={"step_id": sid, "attempt": 1},
                        )
                    )
                    queue_rows.append(action)
                case RequestApproval(gate_id=gid) if gid not in state.approvals:
                    new_events.append(
                        Event(
                            type=EventType.HUMAN_APPROVAL_REQUESTED,
                            payload={"gate_id": gid, **action.payload},
                        )
                    )
                case Complete():
                    new_events.append(
                        Event(
                            type=EventType.RUN_COMPLETED,
                            payload={"result": action.result},
                        )
                    )
                case Fail():
                    new_events.append(
                        Event(
                            type=EventType.RUN_FAILED,
                            payload={"reason": action.reason},
                        )
                    )
                case _:
                    pass  # action already reflected in state
        # A failed step the plan didn't route anywhere fails the run (§5).
        if not new_events and any(
            s.status == "failed" for s in state.steps.values()
        ):
            new_events.append(
                Event(
                    type=EventType.RUN_FAILED,
                    payload={"reason": "step_failed_unhandled"},
                )
            )

    if new_events:
        seq = await append(conn, run_id, new_events)
        for e in new_events:
            apply(state, e.type, e.payload)
        state.last_seq = seq
        for s in queue_rows:
            await insert_step(conn, run_id, s.step_id, 1, s.delay_s)
    await _sync_status(conn, state)
    return state


async def _sync_status(conn: asyncpg.Connection, state: RunState) -> None:
    await conn.execute(
        "UPDATE runs SET status = $2 WHERE id = $1 AND status <> $2",
        state.run_id, state.status.value,
    )


async def start_run(
    conn: asyncpg.Connection,
    wf: Workflow,
    idempotency_key: str,
    input: Any = None,
) -> tuple[str, bool]:
    """Idempotent run start. Returns (run_id, created). Caller holds a txn."""
    run_id = str(uuid.uuid4())
    inserted = await conn.fetchrow(
        "INSERT INTO runs (id, workflow_type, idempotency_key, status)"
        " VALUES ($1, $2, $3, $4)"
        " ON CONFLICT (idempotency_key) DO NOTHING RETURNING id",
        run_id, wf.workflow_type, idempotency_key, RunStatus.RUNNING.value,
    )
    if inserted is None:  # duplicate delivery — absorbed
        existing = await conn.fetchval(
            "SELECT id FROM runs WHERE idempotency_key = $1", idempotency_key
        )
        return str(existing), False
    await append(
        conn,
        run_id,
        [
            Event(
                type=EventType.RUN_STARTED,
                payload={"workflow_type": wf.workflow_type, "input": input},
            )
        ],
    )
    await advance(conn, wf, run_id)
    return run_id, True


async def resolve_approval(
    conn: asyncpg.Connection,
    wf: Workflow,
    run_id: str,
    gate_id: str,
    granted: bool,
    approver: str,
    notes: str | None = None,
) -> RunState:
    """Grant/reject a pending gate, then advance. Caller holds a txn."""
    etype = EventType.APPROVAL_GRANTED if granted else EventType.APPROVAL_REJECTED
    await append(
        conn,
        run_id,
        [
            Event(
                type=etype,
                payload={"gate_id": gate_id, "approver": approver, "notes": notes},
            )
        ],
    )
    return await advance(conn, wf, run_id)


async def cancel_run(conn: asyncpg.Connection, run_id: str) -> None:
    """Append run_cancelled; workers observe on claim. Caller holds a txn.

    Folds first: appending run_cancelled to a terminal log would poison
    every future fold of that run."""
    state = fold(run_id, await load_events(conn, run_id))
    if state.status in TERMINAL_STATUSES:
        raise IllegalTransition(f"run {run_id} already {state.status.value}")
    await append(
        conn, run_id, [Event(type=EventType.RUN_CANCELLED, payload={})]
    )
    await conn.execute(
        "UPDATE runs SET status = $2 WHERE id = $1",
        run_id, RunStatus.CANCELLED.value,
    )

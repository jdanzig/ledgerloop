"""Event types, append, and fold. Events are the sole source of truth."""

from __future__ import annotations

import enum
import json
from typing import Any

import asyncpg
from pydantic import BaseModel, Field


class EventType(str, enum.Enum):
    RUN_STARTED = "run_started"
    STEP_SCHEDULED = "step_scheduled"
    STEP_CLAIMED = "step_claimed"
    STEP_SUCCEEDED = "step_succeeded"
    STEP_FAILED = "step_failed"
    STEP_RETRY_SCHEDULED = "step_retry_scheduled"
    TOOL_CALLED = "tool_called"
    TOOL_SUCCEEDED = "tool_succeeded"
    TOOL_FAILED = "tool_failed"
    LLM_DECISION = "llm_decision"
    HUMAN_APPROVAL_REQUESTED = "human_approval_requested"
    APPROVAL_GRANTED = "approval_granted"
    APPROVAL_REJECTED = "approval_rejected"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"
    RUN_CANCELLED = "run_cancelled"


class RunStatus(str, enum.Enum):
    RUNNING = "running"
    AWAITING_APPROVAL = "awaiting_approval"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}


class Event(BaseModel):
    type: EventType
    payload: dict[str, Any]


class IllegalTransition(Exception):
    pass


class StepState(BaseModel):
    step_id: str
    attempt: int = 1
    # scheduled | claimed | succeeded | failed | retrying
    status: str = "scheduled"
    result: Any = None
    error: Any = None


# claimed -> claimed covers a peer stealing an expired lease (same attempt).
_LEGAL_STEP = {
    None: {"scheduled"},
    "scheduled": {"claimed"},
    "claimed": {"claimed", "succeeded", "failed", "retrying"},
    "retrying": {"claimed"},
    "succeeded": set(),
    "failed": set(),
}


class RunState(BaseModel):
    run_id: str
    workflow_type: str = ""
    input: Any = None
    status: RunStatus = RunStatus.RUNNING
    steps: dict[str, StepState] = Field(default_factory=dict)
    approvals: dict[str, dict[str, Any]] = Field(default_factory=dict)
    llm_decisions: int = 0
    # recorded tool/llm outputs, verbatim: replay reads these, never re-executes
    records: dict[str, Any] = Field(default_factory=dict)
    result: Any = None
    failure_reason: str | None = None
    last_seq: int = 0

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


def _step_transition(state: RunState, step_id: str, new: str) -> StepState:
    step = state.steps.get(step_id)
    current = step.status if step else None
    if new not in _LEGAL_STEP[current]:
        raise IllegalTransition(
            f"run {state.run_id} step {step_id}: {current} -> {new}"
        )
    if step is None:
        step = StepState(step_id=step_id, status=new)
        state.steps[step_id] = step
    step.status = new
    return step


def apply(state: RunState, etype: EventType, payload: dict[str, Any]) -> None:
    """Apply one event to state in place. Raises IllegalTransition on violation."""
    if state.status in TERMINAL_STATUSES:
        raise IllegalTransition(f"run {state.run_id} is terminal, got {etype}")
    match etype:
        case EventType.RUN_STARTED:
            if state.workflow_type:
                raise IllegalTransition("run_started twice")
            state.workflow_type = payload["workflow_type"]
            state.input = payload.get("input")
        case EventType.STEP_SCHEDULED:
            step = _step_transition(state, payload["step_id"], "scheduled")
            step.attempt = payload.get("attempt", 1)
        case EventType.STEP_CLAIMED:
            step = _step_transition(state, payload["step_id"], "claimed")
            step.attempt = payload["attempt"]
        case EventType.STEP_SUCCEEDED:
            step = _step_transition(state, payload["step_id"], "succeeded")
            step.result = payload.get("result")
        case EventType.STEP_FAILED:
            step = _step_transition(state, payload["step_id"], "failed")
            step.error = payload.get("error")
        case EventType.STEP_RETRY_SCHEDULED:
            step = _step_transition(state, payload["step_id"], "retrying")
            step.attempt = payload["next_attempt"]
            step.error = payload.get("error")
        case EventType.TOOL_CALLED:
            pass  # observability only; outcome events carry the record
        case EventType.TOOL_SUCCEEDED | EventType.TOOL_FAILED:
            state.records[payload["record_key"]] = payload
        case EventType.LLM_DECISION:
            state.llm_decisions += 1
            state.records[payload["record_key"]] = payload
        case EventType.HUMAN_APPROVAL_REQUESTED:
            gate = payload["gate_id"]
            if gate in state.approvals:
                raise IllegalTransition(f"approval {gate} requested twice")
            state.approvals[gate] = {"status": "requested", **payload}
            state.status = RunStatus.AWAITING_APPROVAL
        case EventType.APPROVAL_GRANTED | EventType.APPROVAL_REJECTED:
            gate = payload["gate_id"]
            approval = state.approvals.get(gate)
            if approval is None or approval["status"] != "requested":
                raise IllegalTransition(f"approval {gate} not pending")
            approval["status"] = (
                "granted" if etype == EventType.APPROVAL_GRANTED else "rejected"
            )
            approval.update(payload)
            state.status = RunStatus.RUNNING
        case EventType.RUN_COMPLETED:
            state.status = RunStatus.COMPLETED
            state.result = payload.get("result")
        case EventType.RUN_FAILED:
            state.status = RunStatus.FAILED
            state.failure_reason = payload.get("reason")
        case EventType.RUN_CANCELLED:
            state.status = RunStatus.CANCELLED


def fold(run_id: str, events: list[tuple[int, str, dict[str, Any]]]) -> RunState:
    """Pure: fold((seq, type, payload) rows, seq-ordered) -> RunState."""
    state = RunState(run_id=run_id)
    for seq, etype, payload in events:
        if seq != state.last_seq + 1:
            raise IllegalTransition(
                f"run {run_id}: seq gap {state.last_seq} -> {seq}"
            )
        apply(state, EventType(etype), payload)
        state.last_seq = seq
    return state


async def load_events(
    conn: asyncpg.Connection, run_id: str
) -> list[tuple[int, str, dict[str, Any]]]:
    rows = await conn.fetch(
        "SELECT seq, type, payload FROM events WHERE run_id = $1 ORDER BY seq",
        run_id,
    )
    return [(r["seq"], r["type"], r["payload"]) for r in rows]


async def append(
    conn: asyncpg.Connection, run_id: str, events: list[Event]
) -> int:
    """Append events with gapless per-run seq. Caller must hold a transaction.

    Locks the run row to serialize appenders; UNIQUE(run_id, seq) is the
    OCC backstop.
    """
    await conn.execute("SELECT 1 FROM runs WHERE id = $1 FOR UPDATE", run_id)
    last = await conn.fetchval(
        "SELECT COALESCE(MAX(seq), 0) FROM events WHERE run_id = $1", run_id
    )
    await conn.executemany(
        "INSERT INTO events (run_id, seq, type, payload) VALUES ($1, $2, $3, $4)",
        [
            (run_id, last + i + 1, e.type.value, e.payload)
            for i, e in enumerate(events)
        ],
    )
    return last + len(events)

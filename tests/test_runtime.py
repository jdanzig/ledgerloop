"""Runtime: error taxonomy, backoff math, tool validation, and the two replay
guarantees — recorded LLM decisions are never re-executed, and malformed tool
calls feed back to the model instead of escaping the worker."""

import asyncio

import pytest
from pydantic import BaseModel

from ledgerloop.engine import queue
from ledgerloop.engine.queue import backoff_delay
from ledgerloop.engine.scheduler import start_run
from ledgerloop.engine.worker import (
    RetryableStepError,
    StepContext,
    TerminalStepError,
    Worker,
)
from ledgerloop.engine.events import fold, load_events
from ledgerloop.runtime.agent import AgentWorkflow
from ledgerloop.runtime.llm import LLM, Recorder
from ledgerloop.runtime.tools import (
    RetryableToolError,
    TerminalToolError,
    ToolRegistry,
)

from .test_engine import run_worker, wait_status


# -- pure units ---------------------------------------------------------------


def test_backoff_bounds():
    for attempt in range(1, 10):
        for _ in range(50):
            d = backoff_delay(attempt, base=1.0, cap=60.0)
            assert 0 <= d <= min(60.0, 2**attempt)


def test_taxonomy_maps_to_engine_errors():
    assert issubclass(RetryableToolError, RetryableStepError)
    assert issubclass(TerminalToolError, TerminalStepError)
    assert not issubclass(TerminalToolError, RetryableStepError)


# -- fakes --------------------------------------------------------------------


class AddArgs(BaseModel):
    a: int
    b: int


def make_registry() -> ToolRegistry:
    reg = ToolRegistry()

    @reg.register("Add two integers.", AddArgs, timeout_s=5.0)
    async def add(args: AddArgs, ctx):
        return args.a + args.b

    @reg.register("Sleeps forever.", AddArgs, timeout_s=0.1)
    async def hang(args: AddArgs, ctx):
        await asyncio.sleep(60)

    return reg


class FakeResponse:
    def __init__(self, content):
        self._d = {
            "id": "msg_fake",
            "content": content,
            "stop_reason": "tool_use" if any(b["type"] == "tool_use" for b in content) else "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5},
        }

    def model_dump(self, mode="json"):
        return self._d


class FakeAnthropicClient:
    """Call 1 -> scripted tool_use; later calls -> end_turn."""

    def __init__(self, first_args=None):
        self.calls = 0
        self.first_args = first_args if first_args is not None else {"a": 2, "b": 3}
        self.messages = self

    async def create(self, **request):
        self.calls += 1
        if self.calls == 1:
            return FakeResponse(
                [{"type": "tool_use", "id": "tu_1", "name": "add", "input": self.first_args}]
            )
        return FakeResponse([{"type": "text", "text": "done"}])


async def claimed_ctx(pool, wf, key="rt", worker_id="test-worker") -> StepContext:
    """Start a run and claim its first step, returning a live StepContext."""
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, key, {"task": "add 2 and 3"})
    async with pool.acquire() as conn:
        row = await queue.claim(conn, worker_id, lease_s=30.0)
        assert row is not None
        state = fold(run_id, await load_events(conn, run_id))
    return StepContext(
        run_id=run_id, step_id=row["step_id"], attempt=row["attempt"],
        state=state, pool=pool, worker_id=worker_id, queue_id=row["id"],
    )


def make_agent(client) -> AgentWorkflow:
    return AgentWorkflow(
        registry=make_registry(),
        llm=LLM(client=client, model="fake-model"),
        system="You are a calculator.",
        max_decisions=5,
    )


# -- registry against the log ---------------------------------------------------


async def test_tool_validation_failure_is_recorded_not_raised_raw(pool):
    wf = make_agent(FakeAnthropicClient())
    ctx = await claimed_ctx(pool, wf)
    rec = Recorder(ctx)
    with pytest.raises(TerminalToolError):
        await wf.registry.call(rec, "add", {"a": "not-an-int", "b": 3})
    events = await pool.fetch(
        "SELECT type, payload FROM events WHERE run_id = $1 AND type = 'tool_failed'",
        ctx.run_id,
    )
    assert len(events) == 1
    assert events[0]["payload"]["error"]["kind"] == "validation"


async def test_unknown_tool_is_terminal(pool):
    wf = make_agent(FakeAnthropicClient())
    ctx = await claimed_ctx(pool, wf, key="rt2")
    with pytest.raises(TerminalToolError):
        await wf.registry.call(Recorder(ctx), "nope", {})


async def test_tool_timeout_is_retryable(pool):
    wf = make_agent(FakeAnthropicClient())
    ctx = await claimed_ctx(pool, wf, key="rt3")
    with pytest.raises(RetryableToolError):
        await wf.registry.call(Recorder(ctx), "hang", {"a": 1, "b": 2})


async def test_recorded_tool_success_replays_without_reexecution(pool):
    wf = make_agent(FakeAnthropicClient())
    ctx = await claimed_ctx(pool, wf, key="rt4")
    rec1 = Recorder(ctx)
    assert await wf.registry.call(rec1, "add", {"a": 2, "b": 3}) == 5
    # simulate re-execution after crash: fresh recorder over re-folded state
    async with pool.acquire() as conn:
        ctx.state = fold(ctx.run_id, await load_events(conn, ctx.run_id))
    calls_before = await pool.fetchval(
        "SELECT count(*) FROM events WHERE run_id = $1 AND type = 'tool_called'", ctx.run_id
    )
    assert await wf.registry.call(Recorder(ctx), "add", {"a": 2, "b": 3}) == 5
    calls_after = await pool.fetchval(
        "SELECT count(*) FROM events WHERE run_id = $1 AND type = 'tool_called'", ctx.run_id
    )
    assert calls_after == calls_before  # not re-executed


# -- the agent loop end to end ---------------------------------------------------


async def test_agent_run_completes_as_event_chain(pool):
    client = FakeAnthropicClient()
    wf = make_agent(client)
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "agent1", {"task": "add 2 and 3"})
    task = run_worker(pool, {"agent": wf})
    try:
        assert await wait_status(pool, run_id, {"completed"}) == "completed"
    finally:
        task.cancel()
    async with pool.acquire() as conn:
        events = await load_events(conn, run_id)
        state = fold(run_id, events)
    assert state.result == "done"
    assert client.calls == 2
    types = [t for _, t, _ in events]
    assert types.count("llm_decision") == 2
    assert types.count("tool_succeeded") == 1
    # decide-1, act-1-0, decide-2 all ran as durable steps
    assert {"decide-1", "act-1-0", "decide-2"} <= set(state.steps)
    assert state.steps["act-1-0"].result["content"] == 5


async def test_malformed_llm_tool_call_feeds_back_not_crash(pool):
    client = FakeAnthropicClient(first_args={"a": "garbage"})
    wf = make_agent(client)
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "agent2", {"task": "add"})
    task = run_worker(pool, {"agent": wf})
    try:
        assert await wait_status(pool, run_id, {"completed"}) == "completed"
    finally:
        task.cancel()
    async with pool.acquire() as conn:
        state = fold(run_id, await load_events(conn, run_id))
    act = state.steps["act-1-0"].result
    assert act["is_error"] is True  # validation error became a tool_result


async def test_budget_exhausted(pool):
    class LoopingClient(FakeAnthropicClient):
        async def create(self, **request):
            self.calls += 1
            return FakeResponse(
                [{"type": "tool_use", "id": f"tu_{self.calls}", "name": "add",
                  "input": {"a": 1, "b": 1}}]
            )

    wf = make_agent(LoopingClient())
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "agent3", {"task": "loop forever"})
    task = run_worker(pool, {"agent": wf})
    try:
        assert await wait_status(pool, run_id, {"failed"}, timeout=30) == "failed"
    finally:
        task.cancel()
    reason = await pool.fetchval(
        "SELECT payload->>'reason' FROM events WHERE run_id = $1 AND type = 'run_failed'",
        run_id,
    )
    assert reason == "budget_exhausted"


async def test_crash_between_decision_and_tool_never_recalls_model(pool):
    """Kill the worker after llm_decision is recorded but before the step
    completes; recovery must replay the decision from the log."""
    client = FakeAnthropicClient()
    wf = make_agent(client)
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, "agent4", {"task": "add 2 and 3"})

    # "crash": claim decide-1 with a short lease, record the llm_decision,
    # then abandon the step without completing it.
    async with pool.acquire() as conn:
        row = await queue.claim(conn, "crasher", lease_s=1.0)
        state = fold(run_id, await load_events(conn, run_id))
    ctx = StepContext(
        run_id=run_id, step_id=row["step_id"], attempt=row["attempt"],
        state=state, pool=pool, worker_id="crasher", queue_id=row["id"],
    )
    await wf.llm.decide(
        Recorder(ctx), system=wf.system, messages=wf._messages(state),
        tools=wf.registry.anthropic_tools(),
    )
    assert client.calls == 1
    await asyncio.sleep(1.2)  # lease expires; the claim query will reap it

    task = run_worker(pool, {"agent": wf})
    try:
        assert await wait_status(pool, run_id, {"completed"}) == "completed"
    finally:
        task.cancel()
    # decide-1 was re-executed but its decision came from the log:
    # total API calls = 1 (crashed decide-1) + 1 (decide-2). Not 3.
    assert client.calls == 2
    n_decisions = await pool.fetchval(
        "SELECT count(*) FROM events WHERE run_id = $1 AND type = 'llm_decision'", run_id
    )
    assert n_decisions == 2

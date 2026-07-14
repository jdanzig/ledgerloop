"""Anthropic client wrapper + the Recorder that makes replay deterministic.

Every model response and tool outcome is appended to the event log verbatim,
keyed by a deterministic record_key (step, kind, name, call-ordinal). On
re-execution of a step — retry or crash-recovery — recorded successes are
read back from folded state instead of being re-executed.

Records are lease-fenced: a zombie worker's first mid-step append fails the
fence and aborts the step, so superseded work stops early and never pollutes
the audit trail.
"""

from __future__ import annotations

import os
from collections import Counter
from typing import Any

import anthropic

from ..engine.events import Event, EventType, append
from ..engine.worker import Fenced, RetryableStepError, StepContext, TerminalStepError


class Recorder:
    def __init__(self, ctx: StepContext):
        self.ctx = ctx
        self._counters: Counter[tuple[str, str]] = Counter()

    def key(self, kind: str, name: str) -> str:
        self._counters[(kind, name)] += 1
        n = self._counters[(kind, name)]
        return f"{self.ctx.step_id}:{kind}:{name}:{n}"

    def recorded(self, key: str) -> dict[str, Any] | None:
        return self.ctx.state.records.get(key)

    async def record(self, etype: EventType, payload: dict[str, Any]) -> None:
        """Append one event, fenced on our step lease."""
        async with self.ctx.pool.acquire() as conn, conn.transaction():
            held = await conn.fetchrow(
                "SELECT 1 FROM step_queue WHERE id = $1 AND claimed_by = $2"
                " AND lease_expires_at > now() FOR UPDATE",
                self.ctx.queue_id, self.ctx.worker_id,
            )
            if held is None:
                raise Fenced(f"queue row {self.ctx.queue_id}")
            await append(conn, self.ctx.run_id, [Event(type=etype, payload=payload)])
        if "record_key" in payload:
            self.ctx.state.records[payload["record_key"]] = payload


_RETRYABLE_API = (
    anthropic.APIConnectionError,
    anthropic.RateLimitError,
    anthropic.InternalServerError,
    anthropic.APITimeoutError,
)


class LLM:
    """Wrapper whose responses become llm_decision events."""

    def __init__(self, client: Any | None = None, model: str | None = None):
        self.client = client or anthropic.AsyncAnthropic()
        self.model = model or os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")

    async def decide(self, rec: Recorder, **request: Any) -> dict[str, Any]:
        """messages.create with replay: a recorded decision is returned from
        the log without calling the API."""
        key = rec.key("llm", "decide")
        if (prior := rec.recorded(key)) is not None:
            return prior["response"]
        request.setdefault("model", self.model)
        request.setdefault("max_tokens", 4096)
        try:
            resp = await self.client.messages.create(**request)
        except _RETRYABLE_API as e:
            raise RetryableStepError(f"anthropic: {e}") from e
        except anthropic.APIError as e:
            raise TerminalStepError(f"anthropic: {e}") from e
        response = resp.model_dump(mode="json")
        await rec.record(
            EventType.LLM_DECISION,
            {
                "record_key": key,
                "step_id": rec.ctx.step_id,
                "request": _jsonable(request),
                "response": response,  # verbatim, incl. usage
            },
        )
        return response


def _jsonable(request: dict[str, Any]) -> dict[str, Any]:
    import json

    return json.loads(json.dumps(request, default=str))

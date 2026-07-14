"""The decide-act loop expressed as engine steps — not a while-loop in memory.

    decide-1 -> act-1-0, act-1-1 -> decide-2 -> ... -> run_completed

Each decision and each tool execution is a separately scheduled, separately
durable step. A crash between "model chose tool X" and "tool X ran" loses
nothing: the llm_decision is already an event, and recovery re-materializes
the pending act step. The loop cap is enforced by plan() and recorded as
run_failed(reason=budget_exhausted).
"""

from __future__ import annotations

import json
from typing import Any

from ..engine.events import RunState
from ..engine.scheduler import Action, Complete, Fail, Schedule
from ..engine.worker import StepContext
from .llm import LLM, Recorder
from .tools import TerminalToolError, ToolRegistry


def _decide_id(n: int) -> str:
    return f"decide-{n}"


def _act_id(n: int, i: int) -> str:
    return f"act-{n}-{i}"


class AgentWorkflow:
    workflow_type = "agent"

    def __init__(
        self,
        registry: ToolRegistry,
        llm: LLM,
        system: str,
        max_decisions: int = 20,
        workflow_type: str | None = None,
    ):
        self.registry = registry
        self.llm = llm
        self.system = system
        self.max_decisions = max_decisions
        if workflow_type:
            self.workflow_type = workflow_type

    # -- scheduling ---------------------------------------------------------

    def plan(self, state: RunState) -> list[Action]:
        n = 1
        while True:
            decide = state.steps.get(_decide_id(n))
            if decide is None:
                if n > self.max_decisions:
                    return [Fail("budget_exhausted")]
                return [Schedule(_decide_id(n))]
            if decide.status != "succeeded":
                return []  # in flight
            decision = decide.result
            if decision["stop"]:
                return [Complete(result=decision["answer"])]
            acts = [
                state.steps.get(_act_id(n, i))
                for i in range(len(decision["tool_calls"]))
            ]
            missing = [
                Schedule(_act_id(n, i)) for i, a in enumerate(acts) if a is None
            ]
            if missing:
                return missing
            if not all(a.status == "succeeded" for a in acts):
                return []  # acts in flight
            n += 1  # round done -> next decision

    # -- execution ----------------------------------------------------------

    async def run_step(self, step_id: str, ctx: StepContext) -> Any:
        rec = Recorder(ctx)
        if step_id.startswith("decide-"):
            return await self._decide(rec, ctx)
        return await self._act(rec, ctx, step_id)

    async def _decide(self, rec: Recorder, ctx: StepContext) -> dict[str, Any]:
        response = await self.llm.decide(
            rec,
            system=self.system,
            messages=self._messages(ctx.state),
            tools=self.registry.anthropic_tools(),
        )
        tool_calls = [
            {"id": b["id"], "name": b["name"], "args": b["input"]}
            for b in response["content"]
            if b["type"] == "tool_use"
        ]
        answer = "".join(
            b["text"] for b in response["content"] if b["type"] == "text"
        )
        return {
            "stop": not tool_calls,
            "answer": answer or None,
            "tool_calls": tool_calls,
            "content": response["content"],  # verbatim blocks for next messages
        }

    async def _act(self, rec: Recorder, ctx: StepContext, step_id: str) -> dict[str, Any]:
        _, n, i = step_id.split("-")
        call = ctx.state.steps[_decide_id(int(n))].result["tool_calls"][int(i)]
        try:
            result = await self.registry.call(rec, call["name"], call["args"])
            return {"tool_use_id": call["id"], "content": result, "is_error": False}
        except TerminalToolError as e:
            # Business/validation failures feed back to the model, they don't
            # kill the run — the next decision sees the error and corrects.
            return {"tool_use_id": call["id"], "content": str(e), "is_error": True}

    # -- message rebuilding (deterministic from folded state) ---------------

    def _messages(self, state: RunState) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = [
            {
                "role": "user",
                "content": state.input if isinstance(state.input, str)
                else json.dumps(state.input),
            }
        ]
        n = 1
        while (decide := state.steps.get(_decide_id(n))) and decide.status == "succeeded":
            decision = decide.result
            messages.append({"role": "assistant", "content": decision["content"]})
            if decision["stop"]:
                break
            results = []
            for i in range(len(decision["tool_calls"])):
                act = state.steps.get(_act_id(n, i))
                if act is None or act.status != "succeeded":
                    break
                results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": act.result["tool_use_id"],
                        "content": json.dumps(act.result["content"])
                        if not isinstance(act.result["content"], str)
                        else act.result["content"],
                        "is_error": act.result["is_error"],
                    }
                )
            if results:
                messages.append({"role": "user", "content": results})
            n += 1
        return messages

"""ingest -> extract -> normalize -> risk_flag -> human_approval -> commit

Rejection routes to a fresh extraction generation (extract-2, ...) with the
reviewer's notes fed back into the prompt. Staged terms live in the event log
(step results); nothing touches the graph until a gate is granted.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from ...engine.events import RunState
from ...engine.scheduler import Action, Complete, RequestApproval, Schedule
from ...engine.worker import StepContext, TerminalStepError
from ...runtime.llm import LLM
from . import graph
from .extraction import extract_terms, normalize
from .risk import flag_risks


class ContractIngestionWorkflow:
    workflow_type = "contract_ingestion"

    def __init__(self, llm: LLM | None = None):
        self._llm = llm

    @property
    def llm(self) -> LLM:
        if self._llm is None:  # lazy: workers import this module without a key
            self._llm = LLM()
        return self._llm

    # -- scheduling ---------------------------------------------------------

    def plan(self, state: RunState) -> list[Action]:
        # generation r bumps on every rejected review
        r = 1
        while (gate := state.approvals.get(f"review-{r}")) and gate["status"] == "rejected":
            r += 1

        pipeline = ["ingest", f"extract-{r}", f"normalize-{r}", f"risk-{r}"]
        for sid in pipeline:
            step = state.steps.get(sid)
            if step is None:
                return [Schedule(sid)]
            if step.status != "succeeded":
                return []  # in flight (or failed -> engine fails the run)

        if gate is None:
            terms = state.steps[f"normalize-{r}"].result
            flags = state.steps[f"risk-{r}"].result
            return [
                RequestApproval(
                    f"review-{r}",
                    {"terms": terms, "flags": flags, "generation": r},
                )
            ]

        commit = state.steps.get(f"commit-{r}")
        if commit is None:
            return [Schedule(f"commit-{r}")]
        if commit.status != "succeeded":
            return []
        return [Complete(result=commit.result)]

    # -- execution ----------------------------------------------------------

    async def run_step(self, step_id: str, ctx: StepContext) -> Any:
        kind = step_id.split("-")[0]
        r = int(step_id.split("-")[1]) if "-" in step_id else 0
        return await getattr(self, f"_{kind}")(ctx, r)

    async def _ingest(self, ctx: StepContext, r: int) -> dict[str, Any]:
        doc = ctx.state.input or {}
        text = (doc.get("text") or "").strip()
        if not text:
            raise TerminalStepError("document has no text")
        return {"title_hint": doc.get("title"), "chars": len(text)}

    async def _extract(self, ctx: StepContext, r: int) -> dict[str, Any]:
        notes = [
            a["notes"]
            for g, a in sorted(ctx.state.approvals.items())
            if g.startswith("review-") and a["status"] == "rejected" and a.get("notes")
        ]
        return await extract_terms(self.llm, ctx, ctx.state.input, notes)

    async def _normalize(self, ctx: StepContext, r: int) -> dict[str, Any]:
        return normalize(ctx.state.steps[f"extract-{r}"].result)

    async def _risk(self, ctx: StepContext, r: int) -> list[dict[str, str]]:
        return flag_risks(ctx.state.steps[f"normalize-{r}"].result, today=dt.date.today())

    async def _commit(self, ctx: StepContext, r: int) -> dict[str, Any]:
        terms = ctx.state.steps[f"normalize-{r}"].result
        flags = ctx.state.steps[f"risk-{r}"].result
        async with ctx.pool.acquire() as conn, conn.transaction():
            return await graph.commit_contract(conn, ctx.run_id, terms, flags)


WORKFLOWS = [ContractIngestionWorkflow()]

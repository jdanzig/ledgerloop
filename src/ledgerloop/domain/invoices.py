"""Invoice ingestion: ingest -> extract -> match -> flag -> human_approval -> commit

The second workflow on the platform, and the reason the graph earns its keep:
an incoming invoice is matched against the vendor's *current* contract terms —
resolved through the SUPERSEDES chain — and discrepancies (overbilling vs the
committed spend, payment-terms mismatches, expired contracts, invoices with no
contract on file) are flagged by explainable rules before a human releases the
commit. Nothing touches the graph until the gate is granted.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import asyncpg
from pydantic import BaseModel, Field, ValidationError

from ..engine.events import RunState
from ..engine.scheduler import Action, Complete, RequestApproval, Schedule
from ..engine.worker import RetryableStepError, StepContext, TerminalStepError
from ..runtime.llm import LLM, Recorder
from .contracts.graph import current_terms, det_id


# -- extraction ----------------------------------------------------------------


class LineItem(BaseModel):
    description: str
    amount: float


class InvoiceFields(BaseModel):
    vendor: str
    invoice_number: str
    invoice_date: dt.date | None = None
    due_date: dt.date | None = None
    total_amount: float
    currency: str = "USD"
    payment_terms_days: int | None = Field(
        None, description="e.g. 30 for 'Net 30'"
    )
    contract_reference: str | None = Field(
        None, description="Exact title of the governing agreement, if the invoice cites one"
    )
    line_items: list[LineItem] = Field(default_factory=list)


INVOICE_TOOL = {
    "name": "record_invoice",
    "description": "Record the extracted invoice fields. Use null for anything "
    "the invoice does not state. Do not guess.",
    "input_schema": InvoiceFields.model_json_schema(),
}

SYSTEM = (
    "You extract structured fields from vendor invoices. Read the invoice and "
    "call record_invoice exactly once with every field you can support from "
    "the text."
)


def _norm(name: str) -> str:
    return " ".join(name.split()).strip()


# -- matching (deterministic, graph-backed) -------------------------------------


async def match_contract(
    conn: asyncpg.Connection, vendor: str, reference: str | None
) -> dict[str, Any]:
    """Resolve the vendor's current contract. A cited agreement title matches
    any version in a chain and resolves to the chain head, so invoices written
    against the original agreement are checked against the amended terms."""
    # ponytail: exact normalized-name party match; fuzzy matching when real
    # vendor name variance shows up
    party_id = det_id("party", _norm(vendor).lower())

    if reference:
        cited = await conn.fetchval(
            "SELECT c.id FROM entities c"
            " JOIN edges pt ON pt.src = c.id AND pt.dst = $2 AND pt.type = 'party_to'"
            " WHERE c.type = 'contract' AND c.attrs->>'title' = $1",
            reference, party_id,
        )
        if cited is None:
            return {"matched": False, "reason": f"cited agreement {reference!r} not on file"}
        current = await current_terms(conn, str(cited))
    else:
        currents = await conn.fetch(
            "SELECT c.id FROM entities c"
            " JOIN edges pt ON pt.src = c.id AND pt.dst = $1 AND pt.type = 'party_to'"
            " WHERE c.type = 'contract'"
            " AND NOT EXISTS (SELECT 1 FROM edges s WHERE s.dst = c.id AND s.type = 'supersedes')",
            party_id,
        )
        if not currents:
            return {"matched": False, "reason": "no contract on file for this vendor"}
        if len(currents) > 1:
            return {"matched": False, "reason": "multiple current contracts; invoice cites none"}
        current = await current_terms(conn, str(currents[0]["id"]))

    invoiced = await conn.fetchval(
        # cumulative invoiced across the whole SUPERSEDES chain, not just the
        # head — older invoices were committed against earlier versions
        """
        WITH RECURSIVE chain AS (
            SELECT $1::uuid AS id
            UNION
            SELECT e.dst FROM edges e JOIN chain ON e.src = chain.id
            WHERE e.type = 'supersedes'
        )
        SELECT COALESCE(sum((i.attrs->>'total_amount')::numeric), 0)
        FROM entities i
        JOIN edges b ON b.src = i.id AND b.type = 'billed_under'
        JOIN chain ON b.dst = chain.id
        WHERE i.type = 'invoice'
        """,
        current["contract_id"],
    )
    return {
        "matched": True,
        "contract_id": current["contract_id"],
        "title": current["title"],
        "terms": current["terms"],
        "invoiced_to_date": float(invoiced),
    }


# -- discrepancy rules (pure — every flag explainable) ---------------------------


def flag_discrepancies(
    invoice: dict[str, Any], match: dict[str, Any]
) -> list[dict[str, str]]:
    if not match["matched"]:
        return [
            {
                "rule": "no_contract_on_file",
                "explanation": f"Cannot verify this invoice: {match['reason']}.",
            }
        ]
    terms = match["terms"]
    flags: list[dict[str, str]] = []

    inv_days, ct_days = invoice.get("payment_terms_days"), terms.get("payment_terms_days")
    if inv_days is not None and ct_days is not None and inv_days < ct_days:
        flags.append(
            {
                "rule": "payment_terms_mismatch",
                "explanation": f"Invoice demands payment in {inv_days} days; the "
                f"contract grants {ct_days}.",
            }
        )

    term_end = terms.get("term_end")
    if term_end and invoice.get("invoice_date") and invoice["invoice_date"] > term_end:
        flags.append(
            {
                "rule": "contract_expired",
                "explanation": f"Invoice dated {invoice['invoice_date']} but the "
                f"contract term ended {term_end}.",
            }
        )

    # ponytail: commitment sum as a cumulative cap; per-period proration when
    # multi-year billing data makes it matter
    cap = sum(sc["amount"] for sc in terms.get("spend_commitments", []))
    if cap:
        projected = match["invoiced_to_date"] + invoice["total_amount"]
        if projected > cap:
            flags.append(
                {
                    "rule": "overbilling",
                    "explanation": f"This invoice brings total billed to "
                    f"{projected:,.0f}, exceeding the committed {cap:,.0f} "
                    f"({match['invoiced_to_date']:,.0f} already invoiced).",
                }
            )
    return flags


# -- graph commit (idempotent via deterministic ids) ------------------------------


async def commit_invoice(
    conn: asyncpg.Connection,
    run_id: str,
    invoice: dict[str, Any],
    match: dict[str, Any],
    flags: list[dict],
) -> dict[str, Any]:
    invoice_id = det_id("invoice", run_id)
    await conn.execute(
        "INSERT INTO entities (id, type, attrs) VALUES ($1, 'invoice', $2)"
        " ON CONFLICT (id) DO UPDATE SET attrs = EXCLUDED.attrs",
        invoice_id, {**invoice, "flags": flags, "run_id": run_id},
    )
    vendor = _norm(invoice["vendor"])
    party_id = det_id("party", vendor.lower())
    await conn.execute(
        "INSERT INTO entities (id, type, attrs) VALUES ($1, 'party', $2)"
        " ON CONFLICT (id) DO NOTHING",  # unmatched vendors still become visible
        party_id, {"name": vendor},
    )
    await conn.execute(
        "INSERT INTO edges (src, dst, type) VALUES ($1, $2, 'party_to')"
        " ON CONFLICT DO NOTHING",
        invoice_id, party_id,
    )
    if match["matched"]:
        await conn.execute(
            "INSERT INTO edges (src, dst, type) VALUES ($1, $2, 'billed_under')"
            " ON CONFLICT DO NOTHING",
            invoice_id, match["contract_id"],
        )
    return {
        "invoice_id": invoice_id,
        "matched_contract": match.get("contract_id"),
        "flags": [f["rule"] for f in flags],
    }


# -- the workflow -----------------------------------------------------------------


class InvoiceIngestionWorkflow:
    workflow_type = "invoice_ingestion"

    def __init__(self, llm: LLM | None = None):
        self._llm = llm

    @property
    def llm(self) -> LLM:
        if self._llm is None:  # lazy: workers import this module without a key
            self._llm = LLM()
        return self._llm

    def plan(self, state: RunState) -> list[Action]:
        r = 1
        while (gate := state.approvals.get(f"review-{r}")) and gate["status"] == "rejected":
            r += 1

        for sid in ["ingest", f"extract-{r}", f"match-{r}", f"flag-{r}"]:
            step = state.steps.get(sid)
            if step is None:
                return [Schedule(sid)]
            if step.status != "succeeded":
                return []

        if gate is None:
            return [
                RequestApproval(
                    f"review-{r}",
                    {
                        "invoice": state.steps[f"extract-{r}"].result,
                        "match": state.steps[f"match-{r}"].result,
                        "flags": state.steps[f"flag-{r}"].result,
                        "generation": r,
                    },
                )
            ]

        commit = state.steps.get(f"commit-{r}")
        if commit is None:
            return [Schedule(f"commit-{r}")]
        if commit.status != "succeeded":
            return []
        return [Complete(result=commit.result)]

    async def run_step(self, step_id: str, ctx: StepContext) -> Any:
        kind = step_id.split("-")[0]
        r = int(step_id.split("-")[1]) if "-" in step_id else 0
        return await getattr(self, f"_{kind}")(ctx, r)

    async def _ingest(self, ctx: StepContext, r: int) -> dict[str, Any]:
        doc = ctx.state.input or {}
        if not (doc.get("text") or "").strip():
            raise TerminalStepError("invoice has no text")
        return {"chars": len(doc["text"])}

    async def _extract(self, ctx: StepContext, r: int) -> dict[str, Any]:
        notes = [
            a["notes"]
            for g, a in sorted(ctx.state.approvals.items())
            if g.startswith("review-") and a["status"] == "rejected" and a.get("notes")
        ]
        prompt = ctx.state.input.get("text", "")
        if notes:
            prompt += "\n\n<reviewer_notes>\nA human reviewer rejected earlier " \
                "extractions of this invoice. Address these notes:\n" + \
                "\n".join(f"- {n}" for n in notes) + "\n</reviewer_notes>"
        response = await self.llm.decide(
            Recorder(ctx),
            system=SYSTEM,
            messages=[{"role": "user", "content": prompt}],
            tools=[INVOICE_TOOL],
            tool_choice={"type": "tool", "name": "record_invoice"},
        )
        tool_use = next(b for b in response["content"] if b["type"] == "tool_use")
        try:
            invoice = InvoiceFields(**tool_use["input"])
        except ValidationError as e:
            raise RetryableStepError(
                f"invoice extraction failed validation: {e.error_count()} errors"
            ) from e
        return invoice.model_dump(mode="json")

    async def _match(self, ctx: StepContext, r: int) -> dict[str, Any]:
        invoice = ctx.state.steps[f"extract-{r}"].result
        async with ctx.pool.acquire() as conn:
            return await match_contract(
                conn, invoice["vendor"], invoice.get("contract_reference")
            )

    async def _flag(self, ctx: StepContext, r: int) -> list[dict[str, str]]:
        return flag_discrepancies(
            ctx.state.steps[f"extract-{r}"].result,
            ctx.state.steps[f"match-{r}"].result,
        )

    async def _commit(self, ctx: StepContext, r: int) -> dict[str, Any]:
        async with ctx.pool.acquire() as conn, conn.transaction():
            return await commit_invoice(
                conn,
                ctx.run_id,
                ctx.state.steps[f"extract-{r}"].result,
                ctx.state.steps[f"match-{r}"].result,
                ctx.state.steps[f"flag-{r}"].result,
            )


WORKFLOWS = [InvoiceIngestionWorkflow()]

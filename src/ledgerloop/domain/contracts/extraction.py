"""LLM extraction of contract terms, tool-validated.

The model is forced onto a record_terms tool whose schema is the ContractTerms
model; its raw response lands in the log as an llm_decision, and only the
validated, normalized record moves through the pipeline.
"""

from __future__ import annotations

import calendar
import datetime as dt
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from ...engine.worker import RetryableStepError, StepContext
from ...runtime.llm import LLM, Recorder


class SpendCommitment(BaseModel):
    amount: float
    currency: str = "USD"
    period: str | None = Field(None, description="e.g. 'annual', 'total over term'")
    description: str | None = None


class ContractTerms(BaseModel):
    title: str
    parties: list[str] = Field(min_length=2)
    effective_date: dt.date | None = None
    initial_term_months: int | None = None
    renewal_terms: str | None = None
    auto_renew: bool = False
    renewal_notice_window_days: int | None = Field(
        None, description="Days before term end by which non-renewal notice is due"
    )
    spend_commitments: list[SpendCommitment] = Field(default_factory=list)
    payment_terms_days: int | None = None
    termination_for_convenience: bool = False
    liability_cap_amount: float | None = Field(
        None, description="Null if liability is uncapped"
    )
    has_price_cap: bool = False
    governing_law: str | None = None
    amendment_of: str | None = Field(
        None, description="Exact title of the agreement this document amends, if any"
    )


EXTRACT_TOOL = {
    "name": "record_terms",
    "description": "Record the extracted contract terms. For amendments, record "
    "the terms as amended and set amendment_of to the original agreement's title.",
    "input_schema": ContractTerms.model_json_schema(),
}

SYSTEM = (
    "You extract structured terms from legal agreements. Read the contract and "
    "call record_terms exactly once with every field you can support from the "
    "text. Use null for anything the contract does not state. Do not guess."
)


async def extract_terms(
    llm: LLM, ctx: StepContext, doc: dict[str, Any], reviewer_notes: list[str]
) -> dict[str, Any]:
    rec = Recorder(ctx)
    prompt = doc.get("text", "")
    if reviewer_notes:
        prompt += "\n\n<reviewer_notes>\nA human reviewer rejected earlier " \
            "extractions of this contract. Address these notes:\n" + \
            "\n".join(f"- {n}" for n in reviewer_notes) + "\n</reviewer_notes>"
    response = await llm.decide(
        rec,
        system=SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        tools=[EXTRACT_TOOL],
        tool_choice={"type": "tool", "name": "record_terms"},
    )
    tool_use = next(b for b in response["content"] if b["type"] == "tool_use")
    try:
        terms = ContractTerms(**tool_use["input"])
    except ValidationError as e:
        # model nondeterminism — a retry may well produce a valid record
        raise RetryableStepError(f"extraction failed validation: {e.error_count()} errors") from e
    return terms.model_dump(mode="json")


def add_months(day: dt.date, months: int) -> dt.date:
    y, m = divmod(day.month - 1 + months, 12)
    year, month = day.year + y, m + 1
    return dt.date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def normalize(terms: dict[str, Any]) -> dict[str, Any]:
    """Pure cleanup + derived fields; this record is what commit writes."""
    out = dict(terms)
    out["parties"] = [" ".join(p.split()).strip() for p in terms["parties"]]
    out["governing_law"] = (terms.get("governing_law") or None) and terms["governing_law"].strip()
    if terms.get("effective_date") and terms.get("initial_term_months"):
        effective = dt.date.fromisoformat(terms["effective_date"])
        term_end = add_months(effective, terms["initial_term_months"])
        out["term_end"] = term_end.isoformat()
        if terms.get("renewal_notice_window_days") is not None:
            out["renewal_notice_deadline"] = (
                term_end - dt.timedelta(days=terms["renewal_notice_window_days"])
            ).isoformat()
    return out

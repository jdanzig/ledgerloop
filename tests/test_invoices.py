"""Invoice workflow: matching against current contract terms (through the
supersedes chain), rule-based discrepancy flags, and gated graph commits."""

import datetime as dt

from ledgerloop.domain.contracts import graph
from ledgerloop.domain.contracts.extraction import normalize
from ledgerloop.domain.contracts.risk import flag_risks
from ledgerloop.domain.invoices import (
    InvoiceIngestionWorkflow,
    commit_invoice,
    flag_discrepancies,
    match_contract,
)
from ledgerloop.engine.scheduler import resolve_approval, start_run
from ledgerloop.runtime.llm import LLM

from .test_contracts import (
    AMENDMENT_TERMS,
    MSA_TERMS,
    ScriptedExtractionClient,
)
from .test_engine import run_worker, wait_status

MSA_RUN = "00000000-0000-0000-0000-00000000aaaa"
AMD_RUN = "00000000-0000-0000-0000-00000000bbbb"

INVOICE = {
    "vendor": "Acme Data Services LLC",
    "invoice_number": "INV-1042",
    "invoice_date": "2026-03-01",
    "due_date": "2026-04-15",
    "total_amount": 60000.0,
    "currency": "USD",
    "payment_terms_days": 45,
    "contract_reference": "Master Services Agreement",
    "line_items": [
        {"description": "Data processing services — February 2026", "amount": 48000.0},
        {"description": "Priority support surcharge", "amount": 12000.0},
    ],
}


async def seed_contract(pool, run_id=MSA_RUN, terms=MSA_TERMS):
    n = normalize(terms)
    async with pool.acquire() as conn, conn.transaction():
        await graph.commit_contract(conn, run_id, n, flag_risks(n, dt.date(2026, 7, 14)))


def make_wf(script):
    wf = InvoiceIngestionWorkflow(llm=LLM(client=ScriptedExtractionClient(script), model="fake"))
    return wf


async def run_to_gate(pool, wf, key, invoice_doc=None):
    doc = invoice_doc or {"title": "INV-1042", "text": "ACME INVOICE ..."}
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, key, doc)
    await wait_status(pool, run_id, {"awaiting_approval"}, timeout=20)
    return run_id


async def gate_payload(pool, run_id):
    row = await pool.fetchrow(
        "SELECT payload FROM events WHERE run_id = $1"
        " AND type = 'human_approval_requested' ORDER BY seq DESC LIMIT 1",
        run_id,
    )
    return row["payload"]


async def approve_and_finish(pool, wf, run_id, gate="review-1"):
    async with pool.acquire() as conn, conn.transaction():
        await resolve_approval(conn, wf, run_id, gate, True, "ap-clerk")
    await wait_status(pool, run_id, {"completed"}, timeout=20)


# -- pure rules -----------------------------------------------------------------


def test_flags_pure_rules():
    match = {
        "matched": True, "contract_id": "c1", "title": "MSA",
        "terms": normalize(MSA_TERMS), "invoiced_to_date": 0.0,
    }
    assert flag_discrepancies(INVOICE, match) == []  # clean invoice

    fast_pay = {**INVOICE, "payment_terms_days": 15}
    assert [f["rule"] for f in flag_discrepancies(fast_pay, match)] == [
        "payment_terms_mismatch"
    ]

    late = {**INVOICE, "invoice_date": "2030-01-01"}
    assert [f["rule"] for f in flag_discrepancies(late, match)] == ["contract_expired"]

    nearly_spent = {**match, "invoiced_to_date": 200000.0}
    flags = flag_discrepancies(INVOICE, nearly_spent)  # 200k + 60k > 250k
    assert [f["rule"] for f in flags] == ["overbilling"]
    assert "260,000" in flags[0]["explanation"] and "250,000" in flags[0]["explanation"]

    unmatched = {"matched": False, "reason": "no contract on file for this vendor"}
    assert [f["rule"] for f in flag_discrepancies(INVOICE, unmatched)] == [
        "no_contract_on_file"
    ]


# -- pipeline -------------------------------------------------------------------


async def test_clean_invoice_matches_and_commits(pool):
    await seed_contract(pool)
    wf = make_wf([INVOICE])
    worker = run_worker(pool, {"invoice_ingestion": wf})
    try:
        run_id = await run_to_gate(pool, wf, "inv-1")
        payload = await gate_payload(pool, run_id)
        assert payload["flags"] == []
        assert payload["match"]["title"] == "Master Services Agreement"
        # staged: not in the graph yet
        assert await pool.fetchval(
            "SELECT count(*) FROM entities WHERE type = 'invoice'"
        ) == 0
        await approve_and_finish(pool, wf, run_id)
    finally:
        worker.cancel()

    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT 1 FROM edges WHERE src = $1 AND dst = $2 AND type = 'billed_under'",
            graph.det_id("invoice", run_id), graph.det_id("contract", MSA_RUN),
        )
        spend = await graph.vendor_spend(
            conn, graph.det_id("party", "acme data services llc")
        )
    assert spend["total"] == 250000 and spend["invoiced_total"] == 60000


async def test_overbilling_flagged_against_cumulative_invoices(pool):
    await seed_contract(pool)
    # a prior 200k invoice already billed under this contract
    async with pool.acquire() as conn, conn.transaction():
        prior = {**INVOICE, "invoice_number": "INV-0900", "total_amount": 200000.0}
        match = await match_contract(conn, prior["vendor"], prior["contract_reference"])
        await commit_invoice(conn, "00000000-0000-0000-0000-00000000cccc",
                             prior, match, [])

    wf = make_wf([INVOICE])  # this 60k invoice pushes the total past 250k
    worker = run_worker(pool, {"invoice_ingestion": wf})
    try:
        run_id = await run_to_gate(pool, wf, "inv-over")
        payload = await gate_payload(pool, run_id)
        assert [f["rule"] for f in payload["flags"]] == ["overbilling"]
        assert payload["match"]["invoiced_to_date"] == 200000.0
    finally:
        worker.cancel()


async def test_unknown_vendor_flagged_but_still_recordable(pool):
    """Maverick spend: no contract on file — flagged, and once a human approves,
    the invoice still lands in the graph where it's visible."""
    maverick = {**INVOICE, "vendor": "Initech Solutions", "contract_reference": None}
    wf = make_wf([maverick])
    worker = run_worker(pool, {"invoice_ingestion": wf})
    try:
        run_id = await run_to_gate(pool, wf, "inv-maverick")
        payload = await gate_payload(pool, run_id)
        assert [f["rule"] for f in payload["flags"]] == ["no_contract_on_file"]
        await approve_and_finish(pool, wf, run_id)
    finally:
        worker.cancel()
    async with pool.acquire() as conn:
        attrs = await conn.fetchval(
            "SELECT attrs FROM entities WHERE id = $1",
            graph.det_id("invoice", run_id),
        )
        assert attrs["flags"][0]["rule"] == "no_contract_on_file"
        # vendor party created; no billed_under edge
        assert await conn.fetchval(
            "SELECT 1 FROM entities WHERE id = $1",
            graph.det_id("party", "initech solutions"),
        )
        assert not await conn.fetchval(
            "SELECT 1 FROM edges WHERE src = $1 AND type = 'billed_under'",
            graph.det_id("invoice", run_id),
        )


async def test_invoice_citing_original_is_checked_against_amendment(pool):
    """The reason matching walks the supersedes chain: the invoice cites the
    original MSA, but its terms must be checked against the amendment."""
    await seed_contract(pool, MSA_RUN, MSA_TERMS)
    await seed_contract(pool, AMD_RUN, AMENDMENT_TERMS)  # supersedes the MSA

    # 300k invoice: over the original 250k commitment, under the amended 400k
    big = {**INVOICE, "invoice_number": "INV-2000", "total_amount": 300000.0,
           "payment_terms_days": 30}
    wf = make_wf([big])
    worker = run_worker(pool, {"invoice_ingestion": wf})
    try:
        run_id = await run_to_gate(pool, wf, "inv-amended")
        payload = await gate_payload(pool, run_id)
        # matched through the chain to the amendment's terms
        assert payload["match"]["contract_id"] == graph.det_id("contract", AMD_RUN)
        assert payload["match"]["terms"]["payment_terms_days"] == 30
        # no overbilling flag: judged against 400k, not the superseded 250k
        assert payload["flags"] == []
        await approve_and_finish(pool, wf, run_id)
    finally:
        worker.cancel()
    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT 1 FROM edges WHERE src = $1 AND dst = $2 AND type = 'billed_under'",
            graph.det_id("invoice", run_id), graph.det_id("contract", AMD_RUN),
        )


async def test_commit_invoice_idempotent(pool):
    await seed_contract(pool)
    run_id = "00000000-0000-0000-0000-00000000dddd"
    for _ in range(2):
        async with pool.acquire() as conn, conn.transaction():
            match = await match_contract(conn, INVOICE["vendor"],
                                         INVOICE["contract_reference"])
            await commit_invoice(conn, run_id, INVOICE, match, [])
    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT count(*) FROM entities WHERE type = 'invoice'"
        ) == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM edges WHERE type = 'billed_under'"
        ) == 1

"""Contract domain: risk rules, normalization, the full pipeline with approval
gate, the rejection -> re-extract loop, amendment supersedes chains, and
idempotent graph commits."""

import datetime as dt
import pathlib

from ledgerloop.domain.contracts import graph
from ledgerloop.domain.contracts.extraction import add_months, normalize
from ledgerloop.domain.contracts.risk import flag_risks
from ledgerloop.domain.contracts.workflow import ContractIngestionWorkflow
from ledgerloop.engine.scheduler import resolve_approval, start_run
from ledgerloop.runtime.llm import LLM

from .test_engine import run_worker, wait_status
from .test_runtime import FakeResponse

CORPUS = pathlib.Path(__file__).parent.parent / "corpus"

MSA_TERMS = {
    "title": "Master Services Agreement",
    "parties": ["Globex Corporation", "Acme Data Services LLC"],
    "effective_date": "2026-01-15",
    "initial_term_months": 36,
    "renewal_terms": "successive 12-month periods",
    "auto_renew": True,
    "renewal_notice_window_days": 90,
    "spend_commitments": [
        {"amount": 250000, "currency": "USD", "period": "annual",
         "description": "Annual Commitment"}
    ],
    "payment_terms_days": 45,
    "termination_for_convenience": False,
    "liability_cap_amount": 500000,
    "has_price_cap": True,
    "governing_law": "Delaware",
    "amendment_of": None,
}

AMENDMENT_TERMS = {
    **MSA_TERMS,
    "title": "Amendment No. 1 to Master Services Agreement",
    "effective_date": "2026-09-01",
    "spend_commitments": [
        {"amount": 400000, "currency": "USD", "period": "annual",
         "description": "Annual Commitment as amended"}
    ],
    "payment_terms_days": 30,
    "termination_for_convenience": True,
    "amendment_of": "Master Services Agreement",
}


# -- pure units ----------------------------------------------------------------


def test_add_months_clamps_month_end():
    assert add_months(dt.date(2026, 1, 31), 1) == dt.date(2026, 2, 28)
    assert add_months(dt.date(2026, 1, 15), 36) == dt.date(2029, 1, 15)


def test_normalize_derives_deadlines():
    n = normalize(MSA_TERMS)
    assert n["term_end"] == "2029-01-15"
    assert n["renewal_notice_deadline"] == "2028-10-17"


def test_risk_rules_each_fire_explainably():
    today = dt.date(2026, 7, 14)
    base = normalize(MSA_TERMS)
    assert [f["rule"] for f in flag_risks(base, today)] == [
        "no_termination_for_convenience"
    ]
    # auto-renew notice deadline inside 90 days
    imminent = normalize({**MSA_TERMS, "effective_date": "2024-01-01",
                          "initial_term_months": 33})
    rules = [f["rule"] for f in flag_risks(imminent, today)]
    assert "auto_renew_notice_imminent" in rules
    # uncapped liability + long term without price caps
    risky = normalize({**MSA_TERMS, "liability_cap_amount": None,
                       "initial_term_months": 60, "has_price_cap": False})
    rules = [f["rule"] for f in flag_risks(risky, today)]
    assert {"uncapped_liability", "long_term_no_price_caps"} <= set(rules)
    assert all(f["explanation"] for f in flag_risks(risky, today))


# -- scripted extraction client --------------------------------------------------


class ScriptedExtractionClient:
    """Returns record_terms tool_use responses from a script, capturing requests."""

    def __init__(self, script: list[dict]):
        self.script = list(script)
        self.requests: list[dict] = []
        self.calls = 0
        self.messages = self

    async def create(self, **request):
        self.requests.append(request)
        self.calls += 1
        terms = self.script.pop(0)
        return FakeResponse(
            [{"type": "tool_use", "id": f"tu_{self.calls}", "name": "record_terms",
              "input": terms}]
        )


def make_wf(script: list[dict]) -> ContractIngestionWorkflow:
    client = ScriptedExtractionClient(script)
    wf = ContractIngestionWorkflow(llm=LLM(client=client, model="fake"))
    wf.client = client  # test hook
    return wf


def doc(path: str, title: str) -> dict:
    return {"title": title, "text": (CORPUS / path).read_text()}


async def ingest_to_gate(pool, wf, key, document):
    async with pool.acquire() as conn, conn.transaction():
        run_id, _ = await start_run(conn, wf, key, document)
    await wait_status(pool, run_id, {"awaiting_approval"}, timeout=20)
    return run_id


async def grant(pool, wf, run_id, gate, approver="reviewer", notes=None, granted=True):
    async with pool.acquire() as conn, conn.transaction():
        await resolve_approval(conn, wf, run_id, gate, granted, approver, notes)


# -- pipeline ----------------------------------------------------------------


async def test_full_pipeline_stages_then_commits(pool):
    wf = make_wf([MSA_TERMS])
    worker = run_worker(pool, {"contract_ingestion": wf})
    try:
        run_id = await ingest_to_gate(pool, wf, "msa-1", doc("acme-msa.txt", "MSA"))
        # staged: nothing in the graph before the gate
        assert await pool.fetchval("SELECT count(*) FROM entities") == 0
        # the gate payload carries terms + flags for the reviewer
        gate = await pool.fetchrow(
            "SELECT payload FROM events WHERE run_id = $1"
            " AND type = 'human_approval_requested'", run_id,
        )
        assert gate["payload"]["terms"]["title"] == "Master Services Agreement"
        assert [f["rule"] for f in gate["payload"]["flags"]] == [
            "no_termination_for_convenience"
        ]

        await grant(pool, wf, run_id, "review-1", approver="alice")
        await wait_status(pool, run_id, {"completed"}, timeout=20)
    finally:
        worker.cancel()

    async with pool.acquire() as conn:
        contract_id = graph.det_id("contract", run_id)
        acme = graph.det_id("party", "acme data services llc")
        spend = await graph.vendor_spend(conn, acme)
        assert spend["total"] == 250000
        obligations = await graph.obligations_due(conn, within_days=10000)
        assert [o["kind"] for o in obligations] == ["renewal_notice"]
        assert obligations[0]["due_date"] == "2028-10-17"
        current = await graph.current_terms(conn, contract_id)
        assert current["title"] == "Master Services Agreement"


async def test_rejection_reextracts_with_notes(pool):
    corrected = {**MSA_TERMS, "payment_terms_days": 45}
    wf = make_wf([{**MSA_TERMS, "payment_terms_days": 60}, corrected])
    worker = run_worker(pool, {"contract_ingestion": wf})
    try:
        run_id = await ingest_to_gate(pool, wf, "msa-rej", doc("acme-msa.txt", "MSA"))
        await grant(pool, wf, run_id, "review-1", granted=False,
                    notes="payment terms are 45 days, not 60 — reread section 3")
        await wait_status(pool, run_id, {"awaiting_approval"}, timeout=20)
        # second extraction saw the reviewer's notes
        assert wf.client.calls == 2
        second_prompt = wf.client.requests[1]["messages"][0]["content"]
        assert "reread section 3" in second_prompt
        await grant(pool, wf, run_id, "review-2")
        await wait_status(pool, run_id, {"completed"}, timeout=20)
    finally:
        worker.cancel()
    async with pool.acquire() as conn:
        current = await graph.current_terms(conn, graph.det_id("contract", run_id))
    assert current["terms"]["payment_terms_days"] == 45


async def test_amendment_supersedes_chain(pool):
    wf = make_wf([MSA_TERMS, AMENDMENT_TERMS])
    worker = run_worker(pool, {"contract_ingestion": wf})
    try:
        msa_run = await ingest_to_gate(pool, wf, "chain-msa", doc("acme-msa.txt", "MSA"))
        await grant(pool, wf, msa_run, "review-1")
        await wait_status(pool, msa_run, {"completed"}, timeout=20)

        amd_run = await ingest_to_gate(
            pool, wf, "chain-amd", doc("acme-msa-amendment-1.txt", "Amendment 1")
        )
        await grant(pool, wf, amd_run, "review-1")
        await wait_status(pool, amd_run, {"completed"}, timeout=20)
    finally:
        worker.cancel()

    msa_id = graph.det_id("contract", msa_run)
    amd_id = graph.det_id("contract", amd_run)
    async with pool.acquire() as conn:
        # supersedes edge exists
        assert await conn.fetchval(
            "SELECT 1 FROM edges WHERE src = $1 AND dst = $2 AND type = 'supersedes'",
            amd_id, msa_id,
        )
        # 'current terms' from EITHER end of the chain resolves to the amendment
        for start in (msa_id, amd_id):
            current = await graph.current_terms(conn, start)
            assert current["contract_id"] == amd_id
            assert current["terms"]["payment_terms_days"] == 30
        # vendor spend counts only the current version: 400k, not 650k
        spend = await graph.vendor_spend(
            conn, graph.det_id("party", "acme data services llc")
        )
        assert spend["total"] == 400000


async def test_commit_is_idempotent(pool):
    """A crash-retried commit step must not double-write the graph."""
    terms = normalize(MSA_TERMS)
    flags = flag_risks(terms, dt.date(2026, 7, 14))
    run_id = "00000000-0000-0000-0000-00000000c0de"
    for _ in range(2):
        async with pool.acquire() as conn, conn.transaction():
            result = await graph.commit_contract(conn, run_id, terms, flags)
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM entities") == 5
        # contract + 2 parties + 1 spend + 1 obligation; edges: 2 party_to + 2 obligates
        assert await conn.fetchval("SELECT count(*) FROM edges") == 4
    assert result["supersedes"] is None

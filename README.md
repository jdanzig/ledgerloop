# ledgerloop

**A durable execution engine for AI agent workflows, built on Postgres event
sourcing.** Agent runs survive `kill -9`. Every model decision is an immutable,
replayable event. Humans can gate any step. LLM agents in production fail in
boring ways — the process dies mid-run, a tool call times out, the same webhook
fires twice — and most agent frameworks treat durability as someone else's
problem. This project treats it as *the* problem.

The demo: start a contract-ingestion run, `kill -9` the worker mid-extraction,
start another worker, and watch the run resume and complete exactly once — with
the crash seam visible in the audit trail:

```bash
./scripts/demo_recovery.sh    # needs ANTHROPIC_API_KEY
```

![kill -9 recovery demo](demo_recovery.gif)

```
══ 💀 kill -9 worker-1 — mid-extraction, mid-model-call
══ starting worker-2; the lease expires and the run resumes
══ ✅ run completed exactly once. The seam in the audit trail:
  {"seq": 4, "type": "step_claimed", "payload": {"step_id": "extract-1", "worker": "worker-1"}}
  {"seq": 5, "type": "step_claimed", "payload": {"step_id": "extract-1", "worker": "worker-2"}}
```

(Record it with [vhs](https://github.com/charmbracelet/vhs): `vhs scripts/demo.tape`.)

## Quick start

```bash
docker compose up -d postgres redpanda
cp .env.example .env                      # add your ANTHROPIC_API_KEY
uv sync
DATABASE_URL=postgresql://ledgerloop:ledgerloop@localhost:5432/ledgerloop \
    uv run python scripts/migrate.py
uv run python scripts/seed_edgar.py       # real material agreements from SEC EDGAR
./scripts/demo_recovery.sh
```

Or run everything in containers: `docker compose up` (api on :8000, two
workers, Kafka ingress/egress).

## How it works

**Events are the sole source of truth.** Every step, tool call, and LLM
decision is an append-only row in an `events` table with a gapless per-run
sequence. `fold(events) -> RunState` is a pure function; folding the same log
twice yields byte-identical state, and the property test proves it with
random interleavings.

**The step queue is a projection.** Workers claim work with a single
`FOR UPDATE SKIP LOCKED` query — no coordinator. Appending events and mutating
the queue happen in the *same Postgres transaction*, so "record what happened +
schedule what's next" is atomic. Drop the queue entirely and
`scripts/rebuild_queue.py` re-materializes it from the log.

**Leases, not locks.** Long steps heartbeat their lease; if a worker dies, the
lease expires and the claim query itself is the reaper. A zombie that stalls
past its lease and gets superseded fails the lease re-check inside its commit
transaction and its late result is discarded — at-least-once execution,
effective-once recording.

**Replay never re-executes.** `llm_decision` and `tool_succeeded` payloads
carry complete inputs and outputs, verbatim. A step re-run after a crash reads
recorded results from the log instead of re-calling the model — a crash between
"model chose tool X" and "tool X ran" loses nothing.

**Kafka at the edges, not the core.** Postgres owns durability and ordering
per run. `contracts.inbound` starts runs idempotently (offset commits *after*
the run row commits, so crashes duplicate rather than lose, and the UNIQUE
idempotency key absorbs the duplicate). `audit.events` streams every event to
downstream consumers via a trigger-fed outbox drained in id order.

## API tour

```bash
# start a run (idempotency_key required — retries are no-ops)
curl -X POST localhost:8000/runs -H 'content-type: application/json' -d '{
  "workflow_type": "contract_ingestion",
  "idempotency_key": "acme-msa-v1",
  "input": {"title": "Acme MSA", "text": "..."}
}'

# folded state: status, in-flight steps, pending approvals
curl localhost:8000/runs/{id}

# the audit trail — paginated, filterable
curl 'localhost:8000/runs/{id}/events?type=llm_decision&after_seq=0&limit=100'

# the human gate: who released it goes into the audit trail
curl -X POST localhost:8000/runs/{id}/approvals -H 'content-type: application/json' \
  -d '{"gate_id": "review-1", "granted": true, "approver": "alice@corp.com",
       "notes": null}'

# rejection re-extracts with the reviewer notes fed back into the prompt
curl -X POST localhost:8000/runs/{id}/approvals -H 'content-type: application/json' \
  -d '{"gate_id": "review-1", "granted": false, "approver": "alice@corp.com",
       "notes": "payment terms are 45 days — reread section 3"}'

curl -X POST localhost:8000/runs/{id}/cancel

# demo domain: the contract knowledge graph
curl localhost:8000/graph/vendors/{party_id}/spend      # current versions only
curl 'localhost:8000/graph/obligations?due_within=90d'
curl localhost:8000/graph/contracts/{id}/current        # walks SUPERSEDES chain
```

OpenAPI docs at `localhost:8000/docs`.

## The demo domain

`ingest → extract → normalize → risk_flag → human_approval → commit`

LLM extraction (parties, term, auto-renew + notice window, spend commitments,
payment terms, termination for convenience, liability cap, governing law…) is
tool-forced and schema-validated; the raw model output stays in the log, the
normalized record is what commits. Risk flags are **rules, not a model** —
flags must be explainable in an audit context. Extracted terms are staged in
the event log; nothing touches the graph until `approval_granted`.

The graph is two tables — `entities` and `edges` — a knowledge graph in
relational clothing, deliberately: at this scale, recursive CTEs beat operating
a graph database, and the ontology is the interesting part. Ingesting an
amendment creates a `supersedes` edge; "current terms" resolves by walking the
chain. `corpus/` ships an agreement + its amendment to show exactly this, and
`scripts/seed_edgar.py` pulls real material agreements from SEC EDGAR, messy
formatting intact.

## Design decisions & non-goals

- **At-least-once, honestly.** External side effects can run twice; recording
  is effective-once (zombie fencing + idempotency keys threaded to every
  external write). Exactly-once *external* side effects are a lie nobody
  should sell you.
- **No multi-tenancy, no horizontal log partitioning, no workflow DSL, no UI.**
  This is a platform API other services build on, not an app.
- Payloads are append-only and never mutated. Redaction, if ever needed, is a
  compensating event, not an UPDATE.

## Why not Temporal?

Use it at work. This is built small to own the primitives Temporal
abstracts — claims, leases, folds, fences. And the audit projection and
human-approval gates are the parts Temporal doesn't give you anyway.

## Tests

```bash
uv run pytest                 # needs docker compose postgres + redpanda
```

- `test_replay_property.py` — hypothesis: fold determinism over random legal
  interleavings; illegal transitions raise
- `test_chaos.py` — 25 runs, 3 workers, SIGKILL every 1.5s until quiescence:
  every run terminal, every step recorded exactly once, no orphaned leases
- `test_zombie.py` — SIGSTOP stand-in: stalled worker's late result is fenced
- `test_recovery_demo.py` — the demo as a test: kill -9 mid-extraction,
  resume, complete exactly once
- `test_kafka.py` — duplicate deliveries absorbed; audit stream has no gaps
  under concurrent appenders

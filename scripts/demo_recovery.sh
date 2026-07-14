#!/usr/bin/env bash
# The money demo: start a contract ingestion run, kill -9 the worker
# mid-extraction, restart, watch the run resume and complete — then read the
# audit trail showing the seam.
set -euo pipefail
cd "$(dirname "$0")/.."

: "${ANTHROPIC_API_KEY:?set ANTHROPIC_API_KEY — the demo runs real extraction}"
export DATABASE_URL="${DATABASE_URL:-postgresql://ledgerloop:ledgerloop@localhost:5432/ledgerloop}"
export LEDGERLOOP_WORKFLOWS=ledgerloop.domain.contracts.workflow
export LEDGERLOOP_LEASE_S=5 LEDGERLOOP_HEARTBEAT_S=2

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT

echo "══ migrations"
uv run python scripts/migrate.py

echo "══ starting API on :8000"
uv run uvicorn ledgerloop.api.app:app --port 8000 --log-level warning &
PIDS+=($!)
sleep 2

echo "══ starting worker-1"
WORKER_ID=worker-1 uv run python -m ledgerloop.engine.worker &
W1=$!
PIDS+=($W1)

echo "══ POST /runs — ingesting the Acme Master Services Agreement"
RUN_ID=$(uv run python - <<'PY'
import httpx, pathlib, uuid
doc = {"title": "Acme MSA", "text": pathlib.Path("corpus/acme-msa.txt").read_text()}
r = httpx.post("http://localhost:8000/runs", json={
    "workflow_type": "contract_ingestion",
    "idempotency_key": f"demo-{uuid.uuid4()}",
    "input": doc,
}, timeout=10)
print(r.json()["run_id"])
PY
)
echo "   run: $RUN_ID"

echo "══ waiting for extraction to be claimed…"
until curl -sf "localhost:8000/runs/$RUN_ID/events?type=step_claimed" | grep -q 'extract-1'; do
    sleep 0.2
done

echo
echo "══ 💀 kill -9 worker-1 — mid-extraction, mid-model-call"
kill -9 "$W1"
echo

echo "══ starting worker-2; the lease expires and the run resumes"
WORKER_ID=worker-2 uv run python -m ledgerloop.engine.worker &
PIDS+=($!)

echo "══ waiting for the human approval gate…"
until curl -sf "localhost:8000/runs/$RUN_ID" | grep -q 'awaiting_approval'; do sleep 0.5; done
echo "── extracted terms + risk flags staged for review:"
curl -sf "localhost:8000/runs/$RUN_ID/events?type=human_approval_requested" | uv run python -m json.tool

echo "══ granting approval as demo@example.com"
curl -sf -X POST "localhost:8000/runs/$RUN_ID/approvals" \
    -H 'content-type: application/json' \
    -d '{"gate_id":"review-1","granted":true,"approver":"demo@example.com"}' > /dev/null

until curl -sf "localhost:8000/runs/$RUN_ID" | grep -q '"completed"'; do sleep 0.5; done
echo
echo "══ ✅ run completed exactly once. The seam in the audit trail:"
curl -sf "localhost:8000/runs/$RUN_ID/events?type=step_claimed" | uv run python -m json.tool
echo
echo "   note extract-1 claimed twice — worker-1 (killed), then worker-2."
echo "   GET /runs/$RUN_ID/events for the full immutable trail."

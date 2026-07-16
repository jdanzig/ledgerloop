# Anatomy of a real run

This is the unedited event log of run `11beb7da`, produced during a live,
unscripted session: one contract ingested, two human rejections, three model
extractions, both workers killed, and a commit executed by a worker that
booted into the wreckage. 44 events, nothing lost, nothing doubled.

Every line below is an immutable row in the `events` table. Folding them
reproduces the run's state exactly; this table *is* the system's memory,
not a report about it.

```
seq  time          type                       step/gate    who        notes
  1  13:06:51.469  run_started
  2  13:06:51.469  step_scheduled             ingest
  3  13:06:51.642  step_claimed               ingest       worker-1
  4  13:06:51.648  step_succeeded             ingest
  5  13:06:51.648  step_scheduled             extract-1
  6  13:06:51.651  step_claimed               extract-1    worker-1
  7  13:06:53.795  llm_decision               extract-1
  8  13:06:53.800  step_succeeded             extract-1
  9  13:06:53.800  step_scheduled             normalize-1
 10  13:06:53.802  step_claimed               normalize-1  worker-1
 11  13:06:53.803  step_succeeded             normalize-1
 12  13:06:53.803  step_scheduled             risk-1
 13  13:06:53.805  step_claimed               risk-1       worker-1
 14  13:06:53.806  step_succeeded             risk-1
 15  13:06:53.806  human_approval_requested   review-1
 16  13:12:58.533  approval_rejected          review-1     jon        confirm the annual commitment is 250000 and
                                                                      re-check the renewal notice window
 17  13:12:58.533  step_scheduled             extract-2
 18  13:12:58.606  step_claimed               extract-2    worker-1
 19  13:13:01.481  llm_decision               extract-2
 20  13:13:01.487  step_succeeded             extract-2
 21  13:13:01.487  step_scheduled             normalize-2
 22  13:13:01.494  step_claimed               normalize-2  worker-1
 23  13:13:01.496  step_succeeded             normalize-2
 24  13:13:01.496  step_scheduled             risk-2
 25  13:13:01.499  step_claimed               risk-2       worker-1
 26  13:13:01.501  step_succeeded             risk-2
 27  13:13:01.501  human_approval_requested   review-2
 28  13:17:21.357  approval_rejected          review-2     jon        re-verify the liability cap and confirm
                                                                      governing law
 29  13:17:21.357  step_scheduled             extract-3
 30  13:17:21.481  step_claimed               extract-3    worker-2   ← worker-1 is dead; nobody told the run
 31  13:17:23.782  llm_decision               extract-3
 32  13:17:23.787  step_succeeded             extract-3
 33  13:17:23.787  step_scheduled             normalize-3
 34  13:17:23.792  step_claimed               normalize-3  worker-2
 35  13:17:23.793  step_succeeded             normalize-3
 36  13:17:23.793  step_scheduled             risk-3
 37  13:17:23.795  step_claimed               risk-3       worker-2
 38  13:17:23.796  step_succeeded             risk-3
 39  13:17:23.796  human_approval_requested   review-3
 40  13:18:59.433  approval_granted           review-3     jon
 41  13:18:59.433  step_scheduled             commit-3
                   ┄┄┄ two minutes of silence ┄┄┄            ← by now worker-2 is dead too (SIGKILL).
                                                               Zero workers alive. The commit waits in
                                                               the queue. There is no event for a crash —
                                                               crashes don't get to write history.
 42  13:20:59.885  step_claimed               commit-3     worker-1  ← a fresh worker boots and claims
                                                                       the orphaned step 0.4s later
 43  13:20:59.889  step_succeeded             commit-3
 44  13:20:59.889  run_completed
```

## What to notice

**seq 7, 19, 31 — three `llm_decision` events.** Each carries the complete
request and response, verbatim. The model was called exactly three times:
once per extraction generation, never re-called during any recovery. A
recorded decision is replayed from the log, not re-executed.

**seq 16 and 28 — rejections carry the reviewer's words.** Those notes were
injected into the next extraction's prompt (`<reviewer_notes>` — it's in the
seq 19 and 31 request payloads). The audit trail shows the full conversation
between human and machine, not just the final answer.

**seq 30 — the first crash seam.** Generations 1–2 ran on worker-1;
generation 3 is suddenly claimed by worker-2. worker-1 had been SIGKILLed.
No handoff, no coordinator, no drama: worker-1's lease expired and the claim
query — which doubles as the reaper — let worker-2 take over.

**seq 41 → 42 — the two-minute gap.** The approval landed when *no workers
existed at all*; the second one was killed moments earlier. Note what's
absent: there is no `worker_died` event, no error, no timeout. A crashed
process can't write history, which is exactly why the log stays truthful.
The scheduled step simply waited in Postgres until the first worker booted
(seq 42) and claimed it within half a second of coming up.

**One commit, despite everything.** Three generations of extraction were
staged in the log; only the approved one touched the knowledge graph, once.
The result: a `renewal_notice` obligation due **2028-10-17** — a date no
human typed, derived from the contract's term math, now queryable via
`GET /graph/obligations?due_within=900d`.

## Reproduce it

```bash
docker compose up -d
./scripts/demo_recovery.sh        # scripted version of the same story
```

Or do it by hand like this run: start a run via `POST /runs`, reject the
gate with notes, `docker compose kill` whatever you like, and read the
ledger afterwards — `GET /runs/{id}/events` never forgets.

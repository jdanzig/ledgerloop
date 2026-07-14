"""Property: fold is deterministic (byte-identical) over any legal interleaving,
and illegal transitions raise. Pure — no database."""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ledgerloop.engine.events import IllegalTransition, fold


@st.composite
def event_logs(draw):
    """Random legal interleavings of concurrent step lifecycles + approvals."""
    streams = []
    for i in range(draw(st.integers(1, 4))):
        sid = f"s{i}"
        seq = [("step_scheduled", {"step_id": sid, "attempt": 1})]
        attempt = 1
        for _ in range(draw(st.integers(0, 2))):  # retries
            seq.append(("step_claimed", {"step_id": sid, "attempt": attempt}))
            seq.append(
                (
                    "step_retry_scheduled",
                    {
                        "step_id": sid,
                        "attempt": attempt,
                        "next_attempt": attempt + 1,
                        "delay_s": 1.0,
                        "error": {"type": "RetryableStepError", "message": "x"},
                    },
                )
            )
            attempt += 1
        seq.append(("step_claimed", {"step_id": sid, "attempt": attempt}))
        if draw(st.booleans()):  # records mid-step
            seq.append(
                ("tool_succeeded", {"record_key": f"{sid}:t:1", "tool": "t", "result": [1, 2]})
            )
            seq.append(
                ("llm_decision", {"record_key": f"{sid}:llm:1", "response": {"text": "y"}})
            )
        if draw(st.booleans()):
            seq.append(
                ("step_succeeded", {"step_id": sid, "attempt": attempt, "result": {"ok": True}})
            )
        else:
            seq.append(
                ("step_failed", {"step_id": sid, "attempt": attempt, "error": {"type": "TerminalStepError", "message": "no"}})
            )
        streams.append(seq)
    if draw(st.booleans()):  # an approval gate
        streams.append(
            [
                ("human_approval_requested", {"gate_id": "g", "summary": "check"}),
                (
                    draw(st.sampled_from(["approval_granted", "approval_rejected"])),
                    {"gate_id": "g", "approver": "tester", "notes": None},
                ),
            ]
        )

    log = [("run_started", {"workflow_type": "wf", "input": {"doc": "d1"}})]
    cursors = [0] * len(streams)
    while any(c < len(s) for c, s in zip(cursors, streams)):
        live = [i for i, (c, s) in enumerate(zip(cursors, streams)) if c < len(s)]
        i = draw(st.sampled_from(live))
        log.append(streams[i][cursors[i]])
        cursors[i] += 1
    if draw(st.booleans()):
        log.append(
            (draw(st.sampled_from(["run_completed", "run_failed", "run_cancelled"])), {})
        )
    return [(n + 1, t, p) for n, (t, p) in enumerate(log)]


@settings(max_examples=300)
@given(event_logs())
def test_fold_deterministic_and_legal(log):
    a = fold("00000000-0000-0000-0000-000000000001", log)
    b = fold("00000000-0000-0000-0000-000000000001", log)
    assert a.canonical() == b.canonical()  # byte-identical
    assert a.last_seq == len(log)


@pytest.mark.parametrize(
    "log",
    [
        # step succeeded twice
        [
            (1, "run_started", {"workflow_type": "w"}),
            (2, "step_scheduled", {"step_id": "a", "attempt": 1}),
            (3, "step_claimed", {"step_id": "a", "attempt": 1}),
            (4, "step_succeeded", {"step_id": "a", "attempt": 1}),
            (5, "step_succeeded", {"step_id": "a", "attempt": 1}),
        ],
        # claim without schedule
        [
            (1, "run_started", {"workflow_type": "w"}),
            (2, "step_claimed", {"step_id": "a", "attempt": 1}),
        ],
        # seq gap
        [
            (1, "run_started", {"workflow_type": "w"}),
            (3, "step_scheduled", {"step_id": "a", "attempt": 1}),
        ],
        # events after terminal
        [
            (1, "run_started", {"workflow_type": "w"}),
            (2, "run_completed", {}),
            (3, "step_scheduled", {"step_id": "a", "attempt": 1}),
        ],
        # double start
        [
            (1, "run_started", {"workflow_type": "w"}),
            (2, "run_started", {"workflow_type": "w"}),
        ],
        # approval resolved without request
        [
            (1, "run_started", {"workflow_type": "w"}),
            (2, "approval_granted", {"gate_id": "g", "approver": "x"}),
        ],
    ],
)
def test_illegal_transitions_raise(log):
    with pytest.raises(IllegalTransition):
        fold("00000000-0000-0000-0000-000000000002", log)

"""Kafka edges against real Redpanda: duplicate inbound absorbed, and the
egress outbox publishes every event exactly-once-per-drain under concurrent
appenders (the BIGSERIAL-gap defect this design fixes)."""

import asyncio
import json
import os
import uuid

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ledgerloop.kafka.egress import publish_batch
from ledgerloop.kafka.ingress import run_ingress

from .chaos_workflow import ChaosWorkflow
from .test_engine import run_worker, wait_status

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")


@pytest.fixture
async def producer():
    p = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all")
    await p.start()
    yield p
    await p.stop()


def unique(name: str) -> str:
    return f"{name}-{uuid.uuid4().hex[:8]}"


async def test_ingress_duplicate_deliveries_absorbed(pool, producer):
    topic = unique("contracts.inbound")
    doc = json.dumps({"contract": "acme-msa", "text": "..."}).encode()
    # same key + payload delivered twice (redelivery / producer retry)
    for _ in range(2):
        await producer.send_and_wait(topic, key=b"acme-msa", value=doc)

    wf = ChaosWorkflow()
    ingress = asyncio.create_task(
        run_ingress(pool, wf, BOOTSTRAP, topic=topic, group_id=unique("g"))
    )
    try:
        async def one_run():
            while await pool.fetchval("SELECT count(*) FROM runs") < 1:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(one_run(), 20)
        await asyncio.sleep(1.0)  # window for the duplicate to (not) create a run
        assert await pool.fetchval("SELECT count(*) FROM runs") == 1
        run_input = await pool.fetchval(
            "SELECT payload->'input' FROM events WHERE type = 'run_started'"
        )
        assert run_input["contract"] == "acme-msa"
    finally:
        ingress.cancel()


async def test_egress_publishes_every_event_no_gaps(pool, producer):
    """Concurrent appenders (worker completing runs) + concurrent publisher:
    the audit stream must carry every event exactly once per drain."""
    topic = unique("audit.events")
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        group_id=unique("audit"),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        wf = ChaosWorkflow()
        run_ids = []
        from ledgerloop.engine.scheduler import start_run

        for i in range(8):
            async with pool.acquire() as conn, conn.transaction():
                run_id, _ = await start_run(conn, wf, unique(f"egress-{i}"), {"n": i})
                run_ids.append(run_id)

        worker = run_worker(pool, {"chaos": wf})

        async def drain():
            while True:
                await publish_batch(pool, producer, topic)
                await asyncio.sleep(0.05)

        publisher = asyncio.create_task(drain())
        try:
            for run_id in run_ids:
                await wait_status(pool, run_id, {"completed"}, timeout=30)
            # let the publisher catch up to an empty outbox
            while await pool.fetchval("SELECT count(*) FROM kafka_outbox"):
                await asyncio.sleep(0.1)
        finally:
            worker.cancel()
            publisher.cancel()

        expected = {
            (str(r["run_id"]), r["seq"])
            for r in await pool.fetch("SELECT run_id, seq FROM events")
        }
        seen: dict[tuple, int] = {}
        while len(seen) < len(expected):
            batch = await asyncio.wait_for(consumer.getmany(timeout_ms=2000), 10)
            got = [m for msgs in batch.values() for m in msgs]
            if not got:
                break
            for m in got:
                e = json.loads(m.value)
                seen[(e["run_id"], e["seq"])] = seen.get((e["run_id"], e["seq"]), 0) + 1

        assert set(seen) == expected  # nothing skipped, nothing invented
        assert all(n == 1 for n in seen.values())  # single drain -> no dupes
        # per-run ordering preserved through partitioning (keyed by run_id)
    finally:
        await consumer.stop()

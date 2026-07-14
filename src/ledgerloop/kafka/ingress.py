"""contracts.inbound consumer -> idempotent run start.

Offsets are committed *after* the run row commits: a crash between the two
produces a duplicate delivery, which the UNIQUE(idempotency_key) constraint
absorbs — duplicates are a no-op, never a loss.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os

import asyncpg
from aiokafka import AIOKafkaConsumer

from ..engine.db import create_pool
from ..engine.scheduler import Workflow, start_run
from ..engine.worker import load_registry

log = logging.getLogger("ledgerloop.ingress")

TOPIC = "contracts.inbound"


def idempotency_key(topic: str, key: bytes | None, value: bytes) -> str:
    doc_hash = hashlib.sha256(value).hexdigest()
    return hashlib.sha256(
        f"{topic}|{(key or b'').decode(errors='replace')}|{doc_hash}".encode()
    ).hexdigest()


async def run_ingress(
    pool: asyncpg.Pool,
    wf: Workflow,
    bootstrap: str,
    topic: str = TOPIC,
    group_id: str = "ledgerloop-ingress",
) -> None:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=False,  # commit only after the run row commits
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            doc = json.loads(msg.value)
            key = idempotency_key(topic, msg.key, msg.value)
            async with pool.acquire() as conn, conn.transaction():
                run_id, created = await start_run(conn, wf, key, doc)
            log.info(
                "%s run %s (key %s…)",
                "started" if created else "duplicate, absorbed as", run_id, key[:12],
            )
            await consumer.commit()
    finally:
        await consumer.stop()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    pool = await create_pool()
    registry = load_registry(os.environ["LEDGERLOOP_WORKFLOWS"])
    wf = registry[os.environ.get("LEDGERLOOP_INGRESS_WORKFLOW", "contract_ingestion")]
    await run_ingress(pool, wf, os.environ["KAFKA_BOOTSTRAP"])


if __name__ == "__main__":
    asyncio.run(main())

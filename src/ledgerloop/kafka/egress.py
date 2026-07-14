"""audit.events publisher — outbox tail off the events table.

Drains kafka_outbox (populated by trigger inside the append transaction) in
event-id order. At-least-once: a crash between send and delete republishes.
Messages are keyed by run_id, so per-run ordering survives partitioning —
and per-run appends are serialized by the engine, so per-run order here
matches seq order.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

import asyncpg
from aiokafka import AIOKafkaProducer

from ..engine.db import create_pool

log = logging.getLogger("ledgerloop.egress")

TOPIC = "audit.events"


async def publish_batch(
    pool: asyncpg.Pool,
    producer: AIOKafkaProducer,
    topic: str = TOPIC,
    batch: int = 200,
) -> int:
    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch(
            "SELECT e.id, e.run_id, e.seq, e.type, e.payload, e.created_at"
            " FROM kafka_outbox o JOIN events e ON e.id = o.event_id"
            " ORDER BY o.event_id LIMIT $1"
            " FOR UPDATE OF o SKIP LOCKED",
            batch,
        )
        if not rows:
            return 0
        for r in rows:
            await producer.send_and_wait(
                topic,
                key=str(r["run_id"]).encode(),
                value=json.dumps(
                    {
                        "id": r["id"],
                        "run_id": str(r["run_id"]),
                        "seq": r["seq"],
                        "type": r["type"],
                        "payload": r["payload"],
                        "created_at": r["created_at"].isoformat(),
                    }
                ).encode(),
            )
        await conn.execute(
            "DELETE FROM kafka_outbox WHERE event_id = ANY($1::bigint[])",
            [r["id"] for r in rows],
        )
    return len(rows)


async def run_egress(
    pool: asyncpg.Pool, bootstrap: str, topic: str = TOPIC, poll_s: float = 0.25
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    try:
        while True:
            n = await publish_batch(pool, producer, topic)
            if n:
                log.info("published %d events", n)
            else:
                await asyncio.sleep(poll_s)
    finally:
        await producer.stop()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    pool = await create_pool()
    await run_egress(pool, os.environ["KAFKA_BOOTSTRAP"])


if __name__ == "__main__":
    asyncio.run(main())

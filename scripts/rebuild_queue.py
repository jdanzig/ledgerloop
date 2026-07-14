"""Prove the step queue is a projection: drop it, rebuild it from the log."""

import asyncio

from ledgerloop.engine.db import create_pool
from ledgerloop.engine.replay import recover


async def main() -> None:
    pool = await create_pool()
    await pool.execute("TRUNCATE step_queue")
    restored = await recover(pool)
    print(f"queue rebuilt from the event log: {restored} rows re-materialized")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

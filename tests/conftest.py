import os

import pytest

from ledgerloop.engine.db import create_pool

DSN = os.environ.setdefault(
    "DATABASE_URL", "postgresql://ledgerloop:ledgerloop@localhost:5432/ledgerloop"
)


@pytest.fixture
async def pool():
    p = await create_pool(DSN)
    async with p.acquire() as conn:
        await conn.execute("TRUNCATE step_queue, events, runs CASCADE")
    yield p
    await p.close()

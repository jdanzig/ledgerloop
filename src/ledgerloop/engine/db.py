"""Connection pool with JSONB <-> dict codecs."""

import json
import os

import asyncpg


async def _init_conn(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


async def create_pool(dsn: str | None = None, **kw) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn or os.environ["DATABASE_URL"], init=_init_conn, **kw
    )

"""Graph writes and queries. Entity ids are deterministic (uuid5 of run +
role), so a crash-retried commit step is idempotent by construction —
ON CONFLICT absorbs the second write."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from typing import Any

import asyncpg

_NS = uuid.uuid5(uuid.NAMESPACE_URL, "ledgerloop")


def det_id(*parts: str) -> str:
    return str(uuid.uuid5(_NS, ":".join(parts)))


async def commit_contract(
    conn: asyncpg.Connection, run_id: str, terms: dict[str, Any], flags: list[dict]
) -> dict[str, Any]:
    """Write the normalized record into the graph. Caller holds a txn."""
    contract_id = det_id("contract", run_id)
    await conn.execute(
        "INSERT INTO entities (id, type, attrs) VALUES ($1, 'contract', $2)"
        " ON CONFLICT (id) DO UPDATE SET attrs = EXCLUDED.attrs",
        contract_id,
        {"title": terms["title"], "terms": terms, "flags": flags, "run_id": run_id},
    )

    party_ids = []
    for name in terms["parties"]:
        pid = det_id("party", name.lower())
        party_ids.append(pid)
        await conn.execute(
            "INSERT INTO entities (id, type, attrs) VALUES ($1, 'party', $2)"
            " ON CONFLICT (id) DO NOTHING",
            pid, {"name": name},
        )
        await conn.execute(
            "INSERT INTO edges (src, dst, type) VALUES ($1, $2, 'party_to')"
            " ON CONFLICT DO NOTHING",
            contract_id, pid,
        )

    for i, sc in enumerate(terms.get("spend_commitments", [])):
        sid = det_id("spend", run_id, str(i))
        await conn.execute(
            "INSERT INTO entities (id, type, attrs) VALUES ($1, 'spend_commitment', $2)"
            " ON CONFLICT (id) DO UPDATE SET attrs = EXCLUDED.attrs",
            sid, sc,
        )
        await conn.execute(
            "INSERT INTO edges (src, dst, type) VALUES ($1, $2, 'obligates')"
            " ON CONFLICT DO NOTHING",
            contract_id, sid,
        )

    if terms.get("renewal_notice_deadline"):
        oid = det_id("obligation", run_id, "renewal_notice")
        await conn.execute(
            "INSERT INTO entities (id, type, attrs) VALUES ($1, 'obligation', $2)"
            " ON CONFLICT (id) DO UPDATE SET attrs = EXCLUDED.attrs",
            oid,
            {
                "kind": "renewal_notice",
                "due_date": terms["renewal_notice_deadline"],
                "description": f"Non-renewal notice for {terms['title']}",
            },
        )
        await conn.execute(
            "INSERT INTO edges (src, dst, type) VALUES ($1, $2, 'obligates')"
            " ON CONFLICT DO NOTHING",
            contract_id, oid,
        )

    superseded = None
    if terms.get("amendment_of"):
        # the head of the chain with that title (not itself superseded)
        superseded = await conn.fetchval(
            "SELECT c.id FROM entities c"
            " WHERE c.type = 'contract' AND c.attrs->>'title' = $1 AND c.id <> $2"
            " AND NOT EXISTS (SELECT 1 FROM edges s WHERE s.dst = c.id AND s.type = 'supersedes')",
            terms["amendment_of"], contract_id,
        )
        if superseded:
            await conn.execute(
                "INSERT INTO edges (src, dst, type) VALUES ($1, $2, 'supersedes')"
                " ON CONFLICT DO NOTHING",
                contract_id, superseded,
            )

    return {
        "contract_id": contract_id,
        "party_ids": party_ids,
        "supersedes": str(superseded) if superseded else None,
    }


async def vendor_spend(conn: asyncpg.Connection, party_id: str) -> dict[str, Any]:
    """Committed spend for a vendor across *current* contracts (superseded
    versions excluded)."""
    rows = await conn.fetch(
        """
        SELECT c.id AS contract_id, c.attrs->>'title' AS title,
               (sc.attrs->>'amount')::numeric AS amount,
               sc.attrs->>'currency' AS currency, sc.attrs->>'period' AS period
        FROM entities p
        JOIN edges pt ON pt.dst = p.id AND pt.type = 'party_to'
        JOIN entities c ON c.id = pt.src AND c.type = 'contract'
        JOIN edges ob ON ob.src = c.id AND ob.type = 'obligates'
        JOIN entities sc ON sc.id = ob.dst AND sc.type = 'spend_commitment'
        WHERE p.id = $1
          AND NOT EXISTS (SELECT 1 FROM edges s WHERE s.dst = c.id AND s.type = 'supersedes')
        """,
        party_id,
    )
    return {
        "party_id": party_id,
        "total": float(sum(r["amount"] for r in rows)),
        "commitments": [dict(r) | {"amount": float(r["amount"]),
                                   "contract_id": str(r["contract_id"])} for r in rows],
    }


async def obligations_due(conn: asyncpg.Connection, within_days: int) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT o.id, o.attrs->>'kind' AS kind, o.attrs->>'due_date' AS due_date,
               o.attrs->>'description' AS description,
               c.id AS contract_id, c.attrs->>'title' AS contract_title
        FROM entities o
        JOIN edges ob ON ob.dst = o.id AND ob.type = 'obligates'
        JOIN entities c ON c.id = ob.src
        WHERE o.type = 'obligation'
          AND (o.attrs->>'due_date')::date <= CURRENT_DATE + $1::int
        ORDER BY (o.attrs->>'due_date')::date
        """,
        within_days,
    )
    return [dict(r) | {"id": str(r["id"]), "contract_id": str(r["contract_id"])} for r in rows]


async def current_terms(conn: asyncpg.Connection, contract_id: str) -> dict[str, Any] | None:
    """Resolve 'current terms' by walking the SUPERSEDES chain to its head
    (recursive CTE — the reason the graph lives in Postgres)."""
    row = await conn.fetchrow(
        """
        WITH RECURSIVE chain AS (
            SELECT id FROM entities WHERE id = $1 AND type = 'contract'
            UNION
            SELECT e.src FROM edges e JOIN chain ON e.dst = chain.id
            WHERE e.type = 'supersedes'
        )
        SELECT c.id, c.attrs FROM entities c
        JOIN chain ON c.id = chain.id
        WHERE NOT EXISTS (SELECT 1 FROM edges s WHERE s.dst = c.id AND s.type = 'supersedes')
        """,
        contract_id,
    )
    if row is None:
        return None
    return {"contract_id": str(row["id"]), **row["attrs"]}

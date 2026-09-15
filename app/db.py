from pathlib import Path

import psycopg
from psycopg.abc import Params, Query
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from app.config import settings

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"

# Arbitrary key for pg_advisory_xact_lock, so that the web app and workers
# starting at the same time do not run the schema script concurrently.
SCHEMA_LOCK_KEY = 20_260_915

Pool = AsyncConnectionPool[psycopg.AsyncConnection[DictRow]]


def create_pool(max_size: int = 20) -> Pool:
    # Autocommit: every broker operation is a single atomic statement, so there
    # is no need to keep transactions (and connections) open between them.
    return AsyncConnectionPool(
        settings.database_url,
        min_size=2,
        max_size=max_size,
        open=False,
        connection_class=psycopg.AsyncConnection[DictRow],
        kwargs={"autocommit": True, "row_factory": dict_row},
    )


async def connect() -> psycopg.AsyncConnection[DictRow]:
    """A dedicated connection, outside the pool (for LISTEN and long transactions)."""
    return await psycopg.AsyncConnection.connect(
        settings.database_url, autocommit=True, row_factory=dict_row
    )


async def apply_schema(pool: Pool) -> None:
    async with pool.connection() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_KEY,))
        # No parameters, so psycopg sends the whole multi-statement script as-is.
        await conn.execute(SCHEMA_PATH.read_bytes())


async def execute(pool: Pool, query: Query, params: Params | None = None) -> int:
    """Run one statement on a pooled connection; return the number of affected rows."""
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        return cur.rowcount


async def fetch_one(pool: Pool, query: Query, params: Params | None = None) -> DictRow | None:
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchone()


async def fetch_all(pool: Pool, query: Query, params: Params | None = None) -> list[DictRow]:
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchall()

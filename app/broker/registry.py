"""The worker registry: which workers and consumers are alive, kept fresh by heartbeats.

A worker is one process (a replica of the `worker` compose service). It runs
several consumers, and its heartbeat renews the lease of every message they hold.
"""

from typing import Any

from app import db
from app.broker.messages import RENEW_LEASE_SQL
from app.db import Pool

WORKER_HEARTBEAT_SQL = """
INSERT INTO workers (id, hostname, pid, started_at, heartbeat_at)
VALUES (%(id)s, %(hostname)s, %(pid)s, %(started_at)s, now())
ON CONFLICT (id) DO UPDATE
SET heartbeat_at = now()
"""

CONSUMER_HEARTBEAT_SQL = """
INSERT INTO consumers (id, worker_id, queue, state, current_message_id,
                       work_ms, processed, failed, started_at, heartbeat_at)
VALUES (%(id)s, %(worker_id)s, %(queue)s, %(state)s, %(current_message_id)s,
        %(work_ms)s, %(processed)s, %(failed)s, %(started_at)s, now())
ON CONFLICT (id) DO UPDATE
SET state              = EXCLUDED.state,
    current_message_id = EXCLUDED.current_message_id,
    processed          = EXCLUDED.processed,
    failed             = EXCLUDED.failed,
    heartbeat_at       = now()
"""

LIST_WORKERS_SQL = """
SELECT *, extract(epoch FROM now() - heartbeat_at)::float AS heartbeat_age_s
FROM workers
ORDER BY started_at, id
"""

LIST_CONSUMERS_SQL = """
SELECT *, extract(epoch FROM now() - heartbeat_at)::float AS heartbeat_age_s
FROM consumers
ORDER BY started_at, id
"""


async def heartbeat(
    pool: Pool,
    *,
    worker: dict[str, Any],
    consumers: list[dict[str, Any]],
    leases: list[dict[str, Any]],
) -> None:
    """Refresh a worker and its consumers, and renew the lease of every in-flight message."""
    async with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
        await cur.execute(WORKER_HEARTBEAT_SQL, worker)
        if consumers:
            await cur.executemany(CONSUMER_HEARTBEAT_SQL, consumers)
        if leases:
            await cur.executemany(RENEW_LEASE_SQL, leases)


async def remove_consumers(pool: Pool, consumer_ids: list[str]) -> None:
    await db.execute(pool, "DELETE FROM consumers WHERE id = ANY(%s)", (consumer_ids,))


async def remove_worker(pool: Pool, worker_id: str) -> None:
    """Deregister a worker. Its consumers and unread commands are deleted by cascade."""
    await db.execute(pool, "DELETE FROM workers WHERE id = %s", (worker_id,))


async def forget_lost_workers(pool: Pool, after_s: float) -> None:
    # Consumers separately: a killed consumer can belong to a worker that is still alive.
    await db.execute(
        pool,
        "DELETE FROM consumers WHERE heartbeat_at < now() - make_interval(secs => %s)",
        (after_s,),
    )
    await db.execute(
        pool,
        "DELETE FROM workers WHERE heartbeat_at < now() - make_interval(secs => %s)",
        (after_s,),
    )


async def list_workers(pool: Pool) -> list[dict[str, Any]]:
    return await db.fetch_all(pool, LIST_WORKERS_SQL)


async def list_consumers(pool: Pool) -> list[dict[str, Any]]:
    return await db.fetch_all(pool, LIST_CONSUMERS_SQL)


async def get_consumer(pool: Pool, consumer_id: str) -> dict[str, Any] | None:
    return await db.fetch_one(pool, "SELECT * FROM consumers WHERE id = %s", (consumer_id,))

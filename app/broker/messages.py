"""The queue itself: publishing messages and moving them through their lifecycle.

pending ──claim──▶ processing ──ack──▶ done
   ▲                   │
   ├── nack / reaped ──┤  attempts < max_attempts
   │                   ▼  attempts ≥ max_attempts
   └───── requeue ─── dead
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg.abc import Params, Query
from psycopg.rows import class_row
from psycopg.types.json import Jsonb

from app import db
from app.db import Pool

STATUSES = ("pending", "processing", "done", "dead")


@dataclass
class Message:
    id: int
    queue: str
    payload: dict[str, Any]
    status: str
    priority: int
    attempts: int
    max_attempts: int
    available_at: datetime
    locked_by: str | None
    locked_at: datetime | None
    last_error: str | None
    created_at: datetime
    finished_at: datetime | None


async def fetch_messages(pool: Pool, query: Query, params: Params | None = None) -> list[Message]:
    """Run a query that returns whole `messages` rows, as Message objects."""
    async with pool.connection() as conn, conn.cursor(row_factory=class_row(Message)) as cur:
        await cur.execute(query, params)
        return await cur.fetchall()


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

PUBLISH_SQL = """
WITH inserted AS (
    INSERT INTO messages (queue, payload, priority, max_attempts, available_at)
    SELECT %(queue)s,
           %(payload)s::jsonb || jsonb_build_object('seq', seq),
           %(priority)s,
           %(max_attempts)s,
           now() + make_interval(secs => %(delay_s)s)
    FROM generate_series(1, %(count)s) AS seq
    RETURNING id
)
SELECT count(*) AS count, min(id) AS first_id, max(id) AS last_id
FROM inserted
"""


async def publish(
    pool: Pool,
    *,
    queue: str,
    payload: dict[str, Any],
    count: int = 1,
    priority: int = 0,
    delay_s: float = 0,
    max_attempts: int = 3,
) -> dict[str, Any]:
    """Insert `count` messages in one statement. Returns count and the id range."""
    row = await db.fetch_one(
        pool,
        PUBLISH_SQL,
        {
            "queue": queue,
            "payload": Jsonb(payload),
            "count": count,
            "priority": priority,
            "delay_s": delay_s,
            "max_attempts": max_attempts,
        },
    )
    assert row is not None
    return row


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

CLAIM_SQL = """
WITH next AS (
    SELECT id
    FROM messages
    WHERE queue = %(queue)s
      AND status = 'pending'
      AND available_at <= now()
    ORDER BY priority DESC, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED      -- rows locked by other consumers are skipped, not waited on
)
UPDATE messages AS m
SET status    = 'processing',
    locked_by = %(consumer_id)s,
    locked_at = now(),
    attempts  = m.attempts + 1
FROM next
WHERE m.id = next.id
RETURNING m.*
"""

ACK_SQL = """
UPDATE messages
SET status      = 'done',
    finished_at = now()
WHERE id = %(id)s
  AND status = 'processing'
  AND locked_by = %(consumer_id)s  -- only the current lease holder may ack
"""

NACK_SQL = """
UPDATE messages
SET status       = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
    -- exponential backoff: 2s, 4s, 8s, ... capped at 60s
    available_at = CASE WHEN attempts >= max_attempts THEN available_at
                        ELSE now() + make_interval(secs => least(power(2, attempts), 60))
                   END,
    finished_at  = CASE WHEN attempts >= max_attempts THEN now() END,
    last_error   = %(error)s,
    locked_by    = NULL,
    locked_at    = NULL
WHERE id = %(id)s
  AND status = 'processing'
  AND locked_by = %(consumer_id)s
RETURNING status
"""

# Sent with every heartbeat, see app.broker.registry.
RENEW_LEASE_SQL = """
UPDATE messages
SET locked_at = now()
WHERE id = %(message_id)s
  AND status = 'processing'
  AND locked_by = %(consumer_id)s
"""


async def claim(pool: Pool, queue: str, consumer_id: str) -> Message | None:
    rows = await fetch_messages(pool, CLAIM_SQL, {"queue": queue, "consumer_id": consumer_id})
    return rows[0] if rows else None


async def ack(pool: Pool, message_id: int, consumer_id: str) -> bool:
    """Mark a message done. False means the lease was lost (the message was reaped)."""
    return await db.execute(pool, ACK_SQL, {"id": message_id, "consumer_id": consumer_id}) == 1


async def nack(pool: Pool, message_id: int, consumer_id: str, error: str) -> str | None:
    """Record a failure.

    Returns the new status ('pending' or 'dead'), or None if the lease was lost.
    """
    row = await db.fetch_one(
        pool, NACK_SQL, {"id": message_id, "consumer_id": consumer_id, "error": error}
    )
    return row["status"] if row else None


# ---------------------------------------------------------------------------
# Crash recovery
# ---------------------------------------------------------------------------

REAP_SQL = """
UPDATE messages
SET status      = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
    finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
    last_error  = format('lease expired: %%s stopped renewing its lock', locked_by),
    locked_by   = NULL,
    locked_at   = NULL
WHERE id IN (
    SELECT id
    FROM messages
    WHERE status = 'processing'
      AND locked_at < now() - make_interval(secs => %(visibility_timeout_s)s)
    FOR UPDATE SKIP LOCKED      -- never block on a row a consumer is acking right now
)
RETURNING id
"""


async def reap_expired_leases(pool: Pool, visibility_timeout_s: float) -> list[int]:
    rows = await db.fetch_all(pool, REAP_SQL, {"visibility_timeout_s": visibility_timeout_s})
    return [row["id"] for row in rows]


# ---------------------------------------------------------------------------
# Dead letters
# ---------------------------------------------------------------------------

REQUEUE_SQL = """
UPDATE messages
SET status       = 'pending',
    attempts     = 0,
    available_at = now(),
    finished_at  = NULL
WHERE status = 'dead'
  AND (%(id)s::bigint IS NULL OR id = %(id)s)
  AND (%(queue)s::text IS NULL OR queue = %(queue)s)
"""

PURGE_DEAD_SQL = """
DELETE FROM messages
WHERE status = 'dead'
  AND (%(queue)s::text IS NULL OR queue = %(queue)s)
"""


async def requeue_dead(
    pool: Pool, *, message_id: int | None = None, queue: str | None = None
) -> int:
    return await db.execute(pool, REQUEUE_SQL, {"id": message_id, "queue": queue})


async def purge_dead(pool: Pool, queue: str | None = None) -> int:
    return await db.execute(pool, PURGE_DEAD_SQL, {"queue": queue})


async def reset(pool: Pool) -> None:
    """Delete every message in every queue."""
    await db.execute(pool, "TRUNCATE messages RESTART IDENTITY")

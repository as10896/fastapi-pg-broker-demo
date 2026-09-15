"""The message broker.

Every operation is a single SQL statement. The SQL lives in module-level
constants so the web pages can show exactly what runs.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import errors
from psycopg.rows import class_row
from psycopg.types.json import Jsonb

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
    async with pool.connection() as conn:
        cur = await conn.execute(
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
        row = await cur.fetchone()
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

RENEW_LEASE_SQL = """
UPDATE messages
SET locked_at = now()
WHERE id = %(message_id)s
  AND status = 'processing'
  AND locked_by = %(consumer_id)s
"""


async def claim(pool: Pool, queue: str, consumer_id: str) -> Message | None:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(Message)) as cur:
            await cur.execute(CLAIM_SQL, {"queue": queue, "consumer_id": consumer_id})
            return await cur.fetchone()


async def ack(pool: Pool, message_id: int, consumer_id: str) -> bool:
    """Mark a message done. False means the lease was lost (the message was reaped)."""
    async with pool.connection() as conn:
        cur = await conn.execute(ACK_SQL, {"id": message_id, "consumer_id": consumer_id})
        return cur.rowcount == 1


async def nack(pool: Pool, message_id: int, consumer_id: str, error: str) -> str | None:
    """Record a failure.

    Returns the new status ('pending' or 'dead'), or None if the lease was lost.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            NACK_SQL, {"id": message_id, "consumer_id": consumer_id, "error": error}
        )
        row = await cur.fetchone()
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
    async with pool.connection() as conn:
        cur = await conn.execute(REAP_SQL, {"visibility_timeout_s": visibility_timeout_s})
        return [row["id"] for row in await cur.fetchall()]


# ---------------------------------------------------------------------------
# Worker registry (heartbeats)
# ---------------------------------------------------------------------------

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
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM consumers WHERE id = ANY(%s)", (consumer_ids,))


async def remove_worker(pool: Pool, worker_id: str) -> None:
    """Deregister a worker. Its consumers and unread commands are deleted by cascade."""
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM workers WHERE id = %s", (worker_id,))


async def forget_lost_workers(pool: Pool, after_s: float) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM consumers WHERE heartbeat_at < now() - make_interval(secs => %s)",
            (after_s,),
        )
        await conn.execute(
            "DELETE FROM workers WHERE heartbeat_at < now() - make_interval(secs => %s)",
            (after_s,),
        )


async def list_workers(pool: Pool) -> list[dict[str, Any]]:
    async with pool.connection() as conn:
        cur = await conn.execute(LIST_WORKERS_SQL)
        return await cur.fetchall()


async def list_consumers(pool: Pool) -> list[dict[str, Any]]:
    async with pool.connection() as conn:
        cur = await conn.execute(LIST_CONSUMERS_SQL)
        return await cur.fetchall()


async def get_consumer(pool: Pool, consumer_id: str) -> dict[str, Any] | None:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM consumers WHERE id = %s", (consumer_id,))
        return await cur.fetchone()


# ---------------------------------------------------------------------------
# Remote control
# ---------------------------------------------------------------------------

SEND_COMMAND_SQL = """
INSERT INTO worker_commands (worker_id, command, args)
VALUES (%(worker_id)s, %(command)s, %(args)s)
-- the worker_commands_notify trigger sends NOTIFY broker_control on commit
"""

TAKE_COMMANDS_SQL = """
DELETE FROM worker_commands
WHERE id IN (
    SELECT id
    FROM worker_commands
    WHERE worker_id = %(worker_id)s
    ORDER BY id
    FOR UPDATE SKIP LOCKED
)
RETURNING id, command, args
"""


async def send_command(pool: Pool, worker_id: str, command: str, **args: Any) -> bool:
    """Queue a command for a worker. False if that worker is not registered."""
    try:
        async with pool.connection() as conn:
            await conn.execute(
                SEND_COMMAND_SQL,
                {"worker_id": worker_id, "command": command, "args": Jsonb(args)},
            )
    except errors.ForeignKeyViolation:
        return False
    return True


async def take_commands(pool: Pool, worker_id: str) -> list[dict[str, Any]]:
    """Receive and delete a worker's commands.

    Delivery is at-most-once: a worker that crashes right after this call loses
    the commands. That is fine for control commands, not for messages.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(TAKE_COMMANDS_SQL, {"worker_id": worker_id})
        return sorted(await cur.fetchall(), key=lambda row: row["id"])


# ---------------------------------------------------------------------------
# Dead letter queue
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
    async with pool.connection() as conn:
        cur = await conn.execute(REQUEUE_SQL, {"id": message_id, "queue": queue})
        return cur.rowcount


async def purge_dead(pool: Pool, queue: str | None = None) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute(PURGE_DEAD_SQL, {"queue": queue})
        return cur.rowcount


# ---------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------

STATS_SQL = """
SELECT queue,
       count(*) FILTER (WHERE status = 'pending' AND available_at <= now()) AS ready,
       count(*) FILTER (WHERE status = 'pending' AND available_at >  now()) AS delayed,
       count(*) FILTER (WHERE status = 'processing')                         AS processing,
       count(*) FILTER (WHERE status = 'done')                               AS done,
       count(*) FILTER (WHERE status = 'dead')                               AS dead,
       count(*)                                                               AS total,
       -- how long the oldest ready message has been waiting
       extract(epoch FROM now() - min(available_at)
               FILTER (WHERE status = 'pending' AND available_at <= now()))::float AS lag_s,
       count(*) FILTER (WHERE status = 'done'
                          AND finished_at > now() - interval '10 seconds') / 10.0 AS rate
FROM messages
GROUP BY queue
ORDER BY queue
"""

LIST_MESSAGES_SQL = """
SELECT *
FROM messages
WHERE (%(queue)s::text    IS NULL OR queue  = %(queue)s)
  AND (%(status)s::text   IS NULL OR status = %(status)s)
  AND (%(before_id)s::bigint IS NULL OR id  < %(before_id)s)   -- keyset pagination
ORDER BY id DESC
LIMIT %(limit)s
"""


async def queue_stats(pool: Pool) -> list[dict[str, Any]]:
    async with pool.connection() as conn:
        cur = await conn.execute(STATS_SQL)
        return await cur.fetchall()


async def list_messages(
    pool: Pool,
    *,
    queue: str | None = None,
    status: str | None = None,
    before_id: int | None = None,
    limit: int = 50,
) -> list[Message]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(Message)) as cur:
            await cur.execute(
                LIST_MESSAGES_SQL,
                {"queue": queue, "status": status, "before_id": before_id, "limit": limit},
            )
            return await cur.fetchall()


async def get_message(pool: Pool, message_id: int) -> Message | None:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=class_row(Message)) as cur:
            await cur.execute("SELECT * FROM messages WHERE id = %s", (message_id,))
            return await cur.fetchone()


async def list_queues(pool: Pool) -> list[str]:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT DISTINCT queue FROM messages ORDER BY queue")
        return [row["queue"] for row in await cur.fetchall()]


async def reset(pool: Pool) -> None:
    async with pool.connection() as conn:
        await conn.execute("TRUNCATE messages RESTART IDENTITY")

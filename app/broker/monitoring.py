"""Read-only views of the queue, for the web UI."""

from typing import Any

from app import db
from app.broker.messages import Message, fetch_messages
from app.db import Pool

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
    return await db.fetch_all(pool, STATS_SQL)


async def list_messages(
    pool: Pool,
    *,
    queue: str | None = None,
    status: str | None = None,
    before_id: int | None = None,
    limit: int = 50,
) -> list[Message]:
    return await fetch_messages(
        pool,
        LIST_MESSAGES_SQL,
        {"queue": queue, "status": status, "before_id": before_id, "limit": limit},
    )


async def get_message(pool: Pool, message_id: int) -> Message | None:
    rows = await fetch_messages(pool, "SELECT * FROM messages WHERE id = %s", (message_id,))
    return rows[0] if rows else None


async def list_queues(pool: Pool) -> list[str]:
    rows = await db.fetch_all(pool, "SELECT DISTINCT queue FROM messages ORDER BY queue")
    return [row["queue"] for row in rows]

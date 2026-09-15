"""Remote control: commands from the web app to workers, delivered through Postgres.

Workers expose no ports, so the web app inserts commands into `worker_commands`.
A trigger sends NOTIFY broker_control, and the worker consumes its commands with
the same SKIP LOCKED pattern as messages.
"""

from typing import Any

from psycopg import errors
from psycopg.types.json import Jsonb

from app import db
from app.db import Pool

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
        await db.execute(
            pool,
            SEND_COMMAND_SQL,
            {"worker_id": worker_id, "command": command, "args": Jsonb(args)},
        )
    except errors.ForeignKeyViolation:
        return False
    return True


async def take_commands(pool: Pool, worker_id: str) -> list[dict[str, Any]]:
    """Receive and delete a worker's commands, oldest first.

    Delivery is at-most-once: a worker that crashes right after this call loses
    the commands. That is fine for control commands, not for messages.
    """
    rows = await db.fetch_all(pool, TAKE_COMMANDS_SQL, {"worker_id": worker_id})
    return sorted(rows, key=lambda row: row["id"])

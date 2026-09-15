import asyncio
import contextlib
import logging

import psycopg
from psycopg import sql

from app import db

log = logging.getLogger(__name__)

# New messages, and commands for workers.
CHANNELS = ("broker_messages", "broker_control")


class Notifier:
    """Wakes up waiting loops in this process when Postgres sends a NOTIFY.

    One LISTEN connection per process fans out to every consumer (and the
    command loop). Loops read `version` *before* checking for work and then wait
    for a newer version, so a notification arriving in between is never lost.
    """

    def __init__(self) -> None:
        self.version = 0
        self.connected = False
        self._condition = asyncio.Condition()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._listen(), name="notifier")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def poke(self) -> None:
        async with self._condition:
            self.version += 1
            self._condition.notify_all()

    async def wait_newer(self, seen: int, max_wait_s: float) -> None:
        """Return once `version` is newer than `seen`, or after `max_wait_s` seconds."""
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(max_wait_s), self._condition:
                await self._condition.wait_for(lambda: self.version > seen)

    async def _listen(self) -> None:
        while True:
            try:
                async with await db.connect() as conn:
                    for channel in CHANNELS:
                        await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
                    self.connected = True
                    log.info("listening on %s", ", ".join(CHANNELS))
                    # Notifications sent while we were disconnected are gone.
                    await self.poke()
                    async for _ in conn.notifies():
                        await self.poke()
            except psycopg.OperationalError as exc:
                log.warning("LISTEN connection lost (%s); falling back to polling", exc)
            finally:
                self.connected = False
            await asyncio.sleep(1)

"""A consumer: one loop that claims a message, handles it, and acks or nacks it.

A worker process runs several consumers concurrently (see app.worker).
"""

import asyncio
import logging
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime

import psycopg

from app.broker import messages
from app.broker.messages import Message
from app.config import settings
from app.db import Pool
from app.notifier import Notifier

log = logging.getLogger(__name__)


class SimulatedFailure(Exception):
    pass


async def handle(message: Message, work_ms: int) -> None:
    """The "business logic". Replace with real work in a real application."""
    await asyncio.sleep(work_ms / 1000)
    fail_probability = float(message.payload.get("fail_probability", 0))
    if random.random() < fail_probability:
        raise SimulatedFailure(f"simulated failure (fail_probability={fail_probability})")


@dataclass(eq=False)
class Consumer:
    id: str
    queue: str
    work_ms: int
    state: str = "starting"  # starting | idle | processing | stopping
    current_message_id: int | None = None
    processed: int = 0
    failed: int = 0
    stopping: bool = False
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    task: asyncio.Task[None] | None = None

    async def run(self, pool: Pool, notifier: Notifier) -> None:
        while not self.stopping:
            try:
                await self._step(pool, notifier)
            except psycopg.Error:
                log.exception("%s: database error, retrying in 1s", self.id)
                self.current_message_id = None
                await asyncio.sleep(1)

    async def _step(self, pool: Pool, notifier: Notifier) -> None:
        seen = notifier.version
        message = await messages.claim(pool, self.queue, self.id)
        if message is None:
            self.state = "idle"
            await notifier.wait_newer(seen, settings.poll_interval_s)
            return

        self.state = "processing"
        self.current_message_id = message.id
        try:
            await handle(message, self.work_ms)
        except Exception as exc:
            self.failed += 1
            if await messages.nack(pool, message.id, self.id, str(exc)) is None:
                log.warning("%s: lease on message %s was lost before nack", self.id, message.id)
        else:
            self.processed += 1
            if not await messages.ack(pool, message.id, self.id):
                log.warning("%s: lease on message %s was lost before ack", self.id, message.id)
        finally:
            self.current_message_id = None
            self.state = "stopping" if self.stopping else "idle"

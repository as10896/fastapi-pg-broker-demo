"""Worker process: runs consumers against the broker, like `celery worker`.

    python -m app.worker --queue default --concurrency 2 --work-ms 300

In compose this is the `worker` service. It only talks to Postgres and exposes
no ports. Run more of them with `docker compose up -d --scale worker=3`.
"""

import argparse
import asyncio
import contextlib
import itertools
import logging
import os
import secrets
import signal
import socket
from datetime import UTC, datetime
from typing import Any

import psycopg

from app import db
from app.broker import control, registry
from app.config import settings
from app.consumer import Consumer
from app.notifier import Notifier
from app.reaper import run_reaper

log = logging.getLogger("app.worker")


class Worker:
    """Starts, stops and kills consumers, heartbeats, and executes remote commands.

    The heartbeat also renews the lease of every in-flight message. A killed
    consumer stops being heartbeated, so its message is reaped once the lease
    expires.
    """

    def __init__(self, pool: db.Pool, notifier: Notifier) -> None:
        self.pool = pool
        self.notifier = notifier
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        # Random suffix: a restarted container keeps its hostname and pid 1.
        self.id = f"{self.hostname}-{secrets.token_hex(2)}"
        self.started_at = datetime.now(UTC)
        self.consumers: dict[str, Consumer] = {}
        self._consumer_numbers = itertools.count(1)
        self._finished: set[str] = set()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await self._beat()  # register before anyone can send us commands
        self._tasks = [
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(self._command_loop(), name="commands"),
        ]

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self.stop_all()
        with contextlib.suppress(psycopg.Error):
            await registry.remove_worker(self.pool, self.id)

    # -- consumers ----------------------------------------------------------

    async def start_consumers(self, count: int, queue: str, work_ms: int) -> None:
        for _ in range(count):
            consumer = Consumer(
                id=f"{self.id}-c{next(self._consumer_numbers)}", queue=queue, work_ms=work_ms
            )
            consumer.task = asyncio.create_task(
                consumer.run(self.pool, self.notifier), name=consumer.id
            )
            consumer.task.add_done_callback(lambda _, c=consumer: self._on_consumer_exit(c))
            self.consumers[consumer.id] = consumer
        log.info("started %d consumer(s) on queue %r (%d ms per message)", count, queue, work_ms)
        await self._beat()

    async def stop_consumer(self, consumer_id: str) -> None:
        """Graceful stop: the consumer finishes its current message first."""
        consumer = self.consumers.get(consumer_id)
        if consumer is None:
            return
        consumer.stopping = True
        if consumer.state != "processing":
            consumer.state = "stopping"
        await self.notifier.poke()  # wake it up if it is waiting for work

    def kill_consumer(self, consumer_id: str) -> None:
        """Simulate a crash: cancel the consumer without acking its message."""
        consumer = self.consumers.pop(consumer_id, None)
        if consumer and consumer.task:
            consumer.task.cancel()
            log.warning(
                "killed %s while holding message %s", consumer.id, consumer.current_message_id
            )

    async def stop_all(self, grace_s: float = 5) -> None:
        """Stop every consumer, cancelling those still busy after `grace_s` seconds."""
        consumers = list(self.consumers.values())
        for consumer in consumers:
            await self.stop_consumer(consumer.id)
        tasks = [c.task for c in consumers if c.task]
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=grace_s)
            for task in pending:
                task.cancel()

    def _on_consumer_exit(self, consumer: Consumer) -> None:
        if consumer.task and consumer.task.cancelled() and consumer.id not in self.consumers:
            return  # killed: leave its registry row to go stale, like a real crash
        self.consumers.pop(consumer.id, None)
        self._finished.add(consumer.id)

    # -- remote control -----------------------------------------------------

    async def _command_loop(self) -> None:
        while True:
            seen = self.notifier.version
            try:
                for command in await control.take_commands(self.pool, self.id):
                    await self._execute(command["command"], command["args"])
            except psycopg.Error:
                log.exception("receiving commands failed")
            await self.notifier.wait_newer(seen, settings.poll_interval_s)

    async def _execute(self, command: str, args: dict[str, Any]) -> None:
        log.info("command: %s %s", command, args)
        try:
            match command:
                case "start":
                    await self.start_consumers(
                        int(args["count"]), str(args["queue"]), int(args["work_ms"])
                    )
                case "stop" if "consumer_id" in args:
                    await self.stop_consumer(args["consumer_id"])
                case "stop":
                    for consumer_id in list(self.consumers):
                        await self.stop_consumer(consumer_id)
                case "kill":
                    self.kill_consumer(args["consumer_id"])
        except (KeyError, ValueError):
            log.exception("invalid command: %s %s", command, args)

    # -- heartbeat ----------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(settings.heartbeat_interval_s)
            try:
                await self._beat()
            except psycopg.Error:
                log.exception("heartbeat failed")

    async def _beat(self) -> None:
        consumers = list(self.consumers.values())
        await registry.heartbeat(
            self.pool,
            worker={
                "id": self.id,
                "hostname": self.hostname,
                "pid": self.pid,
                "started_at": self.started_at,
            },
            consumers=[
                {
                    "id": c.id,
                    "worker_id": self.id,
                    "queue": c.queue,
                    "state": c.state,
                    "current_message_id": c.current_message_id,
                    "work_ms": c.work_ms,
                    "processed": c.processed,
                    "failed": c.failed,
                    "started_at": c.started_at,
                }
                for c in consumers
            ],
            leases=[
                {"message_id": c.current_message_id, "consumer_id": c.id}
                for c in consumers
                if c.current_message_id is not None
            ],
        )
        if self._finished:
            finished, self._finished = list(self._finished), set()
            await registry.remove_consumers(self.pool, finished)


async def main(queue: str, concurrency: int, work_ms: int) -> None:
    pool = db.create_pool()
    await pool.open(wait=True, timeout=30)
    await db.apply_schema(pool)
    notifier = Notifier()
    await notifier.start()
    worker = Worker(pool, notifier)
    await worker.start()
    reaper = asyncio.create_task(run_reaper(pool), name="reaper")
    await worker.start_consumers(concurrency, queue, work_ms)
    log.info("worker %s ready", worker.id)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        log.info("shutting down: letting consumers finish their current message")
        await worker.close()
        reaper.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reaper
        await notifier.stop()
        await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--queue", default="default")
    parser.add_argument("--concurrency", type=int, default=2, help="number of consumers")
    parser.add_argument("--work-ms", type=int, default=300, help="simulated work per message")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(args.queue, args.concurrency, args.work_ms))

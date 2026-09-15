import asyncio
import logging

import psycopg

from app import broker
from app.config import settings
from app.db import Pool

log = logging.getLogger(__name__)

# Registry rows that stopped heartbeating are kept this long, so the UI can show
# them as "lost", then deleted.
FORGET_LOST_WORKERS_AFTER_S = 60


async def run_reaper(pool: Pool) -> None:
    """Recover messages whose consumer crashed, and clean up the worker registry.

    Runs in the web app and in every worker. That is safe: REAP_SQL uses SKIP
    LOCKED, and a row reaped by one process no longer matches for the others.
    Running it in the web app too means messages are recovered even when every
    worker is gone.
    """
    while True:
        try:
            reaped = await broker.reap_expired_leases(pool, settings.visibility_timeout_s)
            if reaped:
                log.warning("recovered %d message(s) with expired leases: %s", len(reaped), reaped)
            await broker.forget_lost_workers(pool, FORGET_LOST_WORKERS_AFTER_S)
        except psycopg.Error:
            log.exception("reaper failed")
        await asyncio.sleep(settings.reaper_interval_s)

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.broker import messages, monitoring, registry
from app.db import SCHEMA_PATH, Pool
from app.web import WORKER_LOST_AFTER_S, PoolDep, Snippet, redirect, render

router = APIRouter()

SNIPPETS = [
    Snippet(
        "Queue statistics",
        monitoring.STATS_SQL,
        "One pass over the table, grouped by queue. Fine for a demo; a busy production "
        "system would archive done messages or maintain counters instead.",
    ),
    Snippet(
        "Schema (sql/schema.sql)",
        SCHEMA_PATH.read_text(),
        "Applied on every startup under an advisory lock, so every statement is idempotent.",
    ),
]

COUNTERS = ("ready", "delayed", "processing", "done", "dead", "total", "rate")


def _live(rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in rows if row["heartbeat_age_s"] < WORKER_LOST_AFTER_S)


async def _stats(pool: Pool) -> dict[str, Any]:
    queues = await monitoring.queue_stats(pool)
    return {
        "queues": queues,
        "totals": {key: sum(q[key] for q in queues) for key in COUNTERS},
        "live_workers": _live(await registry.list_workers(pool)),
        "live_consumers": _live(await registry.list_consumers(pool)),
    }


@router.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request, pool: PoolDep):
    return render(request, "dashboard.html", snippets=SNIPPETS, **await _stats(pool))


@router.get("/partials/stats", response_class=HTMLResponse)
async def stats_partial(request: Request, pool: PoolDep):
    return render(request, "partials/stats.html", **await _stats(pool))


@router.post("/reset")
async def reset_messages(pool: PoolDep):
    await messages.reset(pool)
    return redirect("/", "All messages deleted.")

from typing import Annotated, Any

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from app import broker
from app.config import settings
from app.db import Pool
from app.routes.publish import QUEUE_PATTERN
from app.web import WORKER_LOST_AFTER_S, PoolDep, Snippet, redirect, render

router = APIRouter()

SNIPPETS = [
    Snippet(
        "Claim the next message",
        broker.CLAIM_SQL,
        "The heart of the broker. SELECT ... FOR UPDATE SKIP LOCKED picks one ready row that no "
        "other consumer has locked, and the surrounding UPDATE marks it as ours in the same "
        "atomic statement. Autocommit ends the transaction right away, so the row lock is held "
        "for milliseconds; from then on the status and lease columns protect the message.",
    ),
    Snippet(
        "Ack (success)",
        broker.ACK_SQL,
        "The locked_by check means a consumer whose lease was reaped cannot ack a message that "
        "now belongs to someone else.",
    ),
    Snippet(
        "Nack (failure → retry or dead letter)",
        broker.NACK_SQL,
        "Retries are just the same row made pending again with available_at in the future.",
    ),
    Snippet(
        "Heartbeat and lease renewal",
        broker.WORKER_HEARTBEAT_SQL
        + "\n-- then, for each consumer:\n"
        + broker.CONSUMER_HEARTBEAT_SQL
        + "\n-- then, for every message still being processed:\n"
        + broker.RENEW_LEASE_SQL,
        f"Sent every {settings.heartbeat_interval_s:g}s by each worker, in one transaction.",
    ),
    Snippet(
        "Reaper (recover messages from crashed consumers)",
        broker.REAP_SQL,
        f"Runs every {settings.reaper_interval_s:g}s in the web app and in every worker. The "
        f"visibility timeout is {settings.visibility_timeout_s:g}s.",
    ),
    Snippet(
        "Send a command to a worker (web app)",
        broker.SEND_COMMAND_SQL,
        "Workers expose no ports, so the buttons on this page insert commands instead. A trigger "
        "on worker_commands sends NOTIFY broker_control, which wakes the worker immediately.",
    ),
    Snippet(
        "Receive commands (worker)",
        broker.TAKE_COMMANDS_SQL,
        "The worker's command inbox is a tiny queue of its own, consumed with the same "
        "SKIP LOCKED pattern.",
    ),
]


class StartConsumersForm(BaseModel):
    worker_id: str = ""
    count: int = Field(default=2, ge=1, le=16)
    queue: str = Field(default="default", min_length=1, max_length=64, pattern=QUEUE_PATTERN)
    work_ms: int = Field(default=500, ge=0, le=60_000)


async def _registry(pool: Pool) -> dict[str, Any]:
    workers = await broker.list_workers(pool)
    consumers = await broker.list_consumers(pool)
    for worker in workers:
        worker["lost"] = worker["heartbeat_age_s"] >= WORKER_LOST_AFTER_S
        worker["consumers"] = [c for c in consumers if c["worker_id"] == worker["id"]]
    return {"workers": workers}


@router.get("/workers", response_class=HTMLResponse)
async def workers_page(request: Request, pool: PoolDep):
    return render(
        request,
        "workers.html",
        form=StartConsumersForm(),
        queue_pattern=QUEUE_PATTERN,
        snippets=SNIPPETS,
        **await _registry(pool),
    )


@router.get("/partials/workers", response_class=HTMLResponse)
async def workers_partial(request: Request, pool: PoolDep):
    return render(request, "partials/workers.html", **await _registry(pool))


def _gone(worker_id: str):
    return redirect("/workers", f"Worker {worker_id} is not registered anymore.")


@router.post("/workers/start")
async def start_consumers(form: Annotated[StartConsumersForm, Form()], pool: PoolDep):
    sent = await broker.send_command(
        pool, form.worker_id, "start", count=form.count, queue=form.queue, work_ms=form.work_ms
    )
    if not sent:
        return _gone(form.worker_id)
    return redirect(
        "/workers",
        f"Asked {form.worker_id} to start {form.count} consumer(s) on queue '{form.queue}'.",
    )


@router.post("/workers/{worker_id}/stop")
async def stop_all_consumers(worker_id: str, pool: PoolDep):
    if not await broker.send_command(pool, worker_id, "stop"):
        return _gone(worker_id)
    return redirect(
        "/workers", f"Asked {worker_id} to stop all consumers after their current message."
    )


@router.post("/workers/{worker_id}/consumers/{consumer_id}/stop")
async def stop_consumer(worker_id: str, consumer_id: str, pool: PoolDep):
    if not await broker.send_command(pool, worker_id, "stop", consumer_id=consumer_id):
        return _gone(worker_id)
    return redirect("/workers", f"Asked {consumer_id} to stop after its current message.")


@router.post("/workers/{worker_id}/consumers/{consumer_id}/kill")
async def kill_consumer(worker_id: str, consumer_id: str, pool: PoolDep):
    consumer = await broker.get_consumer(pool, consumer_id)
    if not await broker.send_command(pool, worker_id, "kill", consumer_id=consumer_id):
        return _gone(worker_id)
    message_id = consumer["current_message_id"] if consumer else None
    if message_id is None:
        return redirect("/workers", f"Killed {consumer_id}. It was idle at its last heartbeat.")
    return redirect(
        "/workers",
        f"Killed {consumer_id} while it held message #{message_id}. The message stays "
        f"'processing' until its lease expires ({settings.visibility_timeout_s:g}s), then the "
        "reaper makes it pending again.",
    )

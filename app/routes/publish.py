import json
from typing import Annotated

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from app import broker
from app.web import PoolDep, Snippet, redirect, render

router = APIRouter()

QUEUE_PATTERN = r"^[A-Za-z0-9_.:-]+$"

SNIPPETS = [
    Snippet(
        "Publish (bulk insert)",
        broker.PUBLISH_SQL,
        "Publishing is just an INSERT. One statement inserts any number of messages; "
        "generate_series numbers them. Any client that can INSERT into this table is a producer.",
    ),
    Snippet(
        "Wake up consumers (trigger)",
        """
CREATE OR REPLACE FUNCTION notify_message_available() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('broker_messages', NEW.queue);
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER messages_notify_insert
    AFTER INSERT ON messages
    FOR EACH ROW
    WHEN (NEW.available_at <= now())
    EXECUTE FUNCTION notify_message_available();
""",
        "The NOTIFY is delivered only when the INSERT commits, and identical notifications in "
        "one transaction are collapsed: a bulk insert wakes consumers exactly once.",
    ),
]


class PublishForm(BaseModel):
    queue: str = Field(default="default", min_length=1, max_length=64, pattern=QUEUE_PATTERN)
    count: int = Field(default=1, ge=1, le=100_000)
    priority: int = Field(default=0, ge=-100, le=100)
    delay_s: float = Field(default=0, ge=0, le=3600)
    max_attempts: int = Field(default=3, ge=1, le=20)
    fail_percent: float = Field(default=0, ge=0, le=100)
    payload: str = "{}"


PRESETS = [
    ("Burst", "200 messages at once", {"count": 200}),
    ("Flaky", "50 messages, 30% fail and get retried", {"count": 50, "fail_percent": 30}),
    ("Poison", "5 messages that always fail and end up dead", {"count": 5, "fail_percent": 100}),
    ("Delayed", "20 messages that become visible in 15 s", {"count": 20, "delay_s": 15}),
    ("Urgent", "10 priority-10 messages that jump the line", {"count": 10, "priority": 10}),
]


@router.get("/publish", response_class=HTMLResponse)
async def publish_page(request: Request, pool: PoolDep):
    return render(
        request,
        "publish.html",
        form=PublishForm(),
        presets=PRESETS,
        queues=await broker.list_queues(pool),
        queue_pattern=QUEUE_PATTERN,
        snippets=SNIPPETS,
    )


@router.post("/publish")
async def publish_messages(form: Annotated[PublishForm, Form()], pool: PoolDep):
    try:
        payload = json.loads(form.payload.strip() or "{}")
    except json.JSONDecodeError as exc:
        return redirect("/publish", f"Payload is not valid JSON: {exc}")
    if not isinstance(payload, dict):
        return redirect("/publish", "Payload must be a JSON object.")
    payload["fail_probability"] = form.fail_percent / 100

    result = await broker.publish(
        pool,
        queue=form.queue,
        payload=payload,
        count=form.count,
        priority=form.priority,
        delay_s=form.delay_s,
        max_attempts=form.max_attempts,
    )
    ids = (
        f"#{result['first_id']}"
        if result["count"] == 1
        else f"#{result['first_id']}–#{result['last_id']}"
    )
    return redirect(
        "/publish", f"Published {result['count']} message(s) to '{form.queue}' ({ids})."
    )

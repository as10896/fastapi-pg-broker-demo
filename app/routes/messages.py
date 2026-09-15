from typing import Annotated, Any

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from app.broker import messages, monitoring
from app.broker.messages import Message
from app.db import Pool
from app.web import PoolDep, Snippet, redirect, render

router = APIRouter()

PAGE_SIZE = 50

MESSAGES_SNIPPETS = [
    Snippet(
        "List messages",
        monitoring.LIST_MESSAGES_SQL,
        "Keyset pagination (id < last seen id) stays fast on any page, unlike OFFSET.",
    ),
]

DEAD_LETTER_SNIPPETS = [
    Snippet(
        "How a message becomes dead",
        messages.NACK_SQL,
        "There is no separate dead letter table: a dead message is a row with status = 'dead'.",
    ),
    Snippet("Requeue", messages.REQUEUE_SQL, "The UPDATE to 'pending' fires the NOTIFY trigger."),
    Snippet("Purge", messages.PURGE_DEAD_SQL),
]


async def _messages(pool: Pool, queue: str, status: str, before_id: int | None) -> dict[str, Any]:
    rows = await monitoring.list_messages(
        pool,
        queue=queue or None,
        status=status if status in messages.STATUSES else None,
        before_id=before_id,
        limit=PAGE_SIZE,
    )
    return {
        "messages": rows,
        "next_before_id": rows[-1].id if len(rows) == PAGE_SIZE else None,
        "filters": {"queue": queue, "status": status, "before_id": before_id},
    }


@router.get("/messages", response_class=HTMLResponse)
async def messages_page(
    request: Request,
    pool: PoolDep,
    queue: Annotated[str, Query()] = "",
    status: Annotated[str, Query()] = "",
    before_id: Annotated[int | None, Query()] = None,
):
    return render(
        request,
        "messages.html",
        queues=await monitoring.list_queues(pool),
        statuses=messages.STATUSES,
        snippets=MESSAGES_SNIPPETS,
        **await _messages(pool, queue, status, before_id),
    )


@router.get("/partials/messages", response_class=HTMLResponse)
async def messages_partial(
    request: Request,
    pool: PoolDep,
    queue: Annotated[str, Query()] = "",
    status: Annotated[str, Query()] = "",
    before_id: Annotated[int | None, Query()] = None,
):
    return render(
        request, "partials/messages.html", **await _messages(pool, queue, status, before_id)
    )


async def _get_message(pool: Pool, message_id: int) -> Message:
    message = await monitoring.get_message(pool, message_id)
    if message is None:
        raise HTTPException(status_code=404, detail=f"Message {message_id} not found")
    return message


@router.get("/messages/{message_id}", response_class=HTMLResponse)
async def message_page(request: Request, message_id: int, pool: PoolDep):
    return render(request, "message.html", message=await _get_message(pool, message_id))


@router.get("/partials/messages/{message_id}", response_class=HTMLResponse)
async def message_partial(request: Request, message_id: int, pool: PoolDep):
    return render(request, "partials/message.html", message=await _get_message(pool, message_id))


@router.post("/messages/{message_id}/requeue")
async def requeue_message(message_id: int, pool: PoolDep):
    count = await messages.requeue_dead(pool, message_id=message_id)
    notice = f"Message #{message_id} requeued." if count else f"Message #{message_id} is not dead."
    return redirect(f"/messages/{message_id}", notice)


@router.get("/dead-letters", response_class=HTMLResponse)
async def dead_letters_page(request: Request, pool: PoolDep):
    return render(
        request,
        "dead_letters.html",
        snippets=DEAD_LETTER_SNIPPETS,
        messages=await monitoring.list_messages(pool, status="dead", limit=200),
    )


@router.get("/partials/dead-letters", response_class=HTMLResponse)
async def dead_letters_partial(request: Request, pool: PoolDep):
    return render(
        request,
        "partials/dead_letters.html",
        messages=await monitoring.list_messages(pool, status="dead", limit=200),
    )


@router.post("/dead-letters/requeue")
async def requeue_dead_letters(pool: PoolDep, message_id: Annotated[int | None, Form()] = None):
    count = await messages.requeue_dead(pool, message_id=message_id)
    return redirect("/dead-letters", f"Requeued {count} message(s).")


@router.post("/dead-letters/purge")
async def purge_dead_letters(pool: PoolDep):
    count = await messages.purge_dead(pool)
    return redirect("/dead-letters", f"Deleted {count} dead message(s).")

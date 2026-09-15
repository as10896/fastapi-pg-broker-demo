"""Helpers shared by the route modules: dependencies, templates, SQL snippets."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.datastructures import URL

from app.config import settings
from app.db import Pool
from app.experiment import ExperimentRunner


def get_pool(request: Request) -> Pool:
    return request.app.state.pool


def get_experiments(request: Request) -> ExperimentRunner:
    return request.app.state.experiments


PoolDep = Annotated[Pool, Depends(get_pool)]
ExperimentsDep = Annotated[ExperimentRunner, Depends(get_experiments)]


@dataclass(frozen=True)
class Snippet:
    """A piece of SQL shown in the "SQL used on this page" panel."""

    title: str
    sql: str
    note: str = ""


def _format_duration(seconds: float) -> str:
    if seconds < 1:
        return "<1s"
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.0f}m"
    return f"{seconds / 3600:.1f}h"


def relative_time(value: datetime | None) -> str:
    if value is None:
        return "—"
    seconds = (datetime.now(UTC) - value).total_seconds()
    return f"in {_format_duration(-seconds)}" if seconds < 0 else f"{_format_duration(seconds)} ago"


# A worker or consumer that has missed this many seconds of heartbeats is shown as lost.
WORKER_LOST_AFTER_S = settings.heartbeat_interval_s * 3

templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
templates.env.globals["settings"] = settings
templates.env.globals["lost_after_s"] = WORKER_LOST_AFTER_S
templates.env.globals["now"] = lambda: datetime.now(UTC)
templates.env.filters["ago"] = relative_time
templates.env.filters["duration"] = _format_duration
templates.env.filters["pretty_json"] = lambda value: json.dumps(value, indent=2, ensure_ascii=False)


def render(request: Request, name: str, **context: Any):
    return templates.TemplateResponse(request, name, context)


def redirect(path: str, notice: str | None = None, **params: Any) -> RedirectResponse:
    """Post/Redirect/Get, with an optional one-line notice shown on the next page."""
    url = URL(path)
    query = {k: v for k, v in {**params, "notice": notice}.items() if v not in (None, "")}
    if query:
        url = url.include_query_params(**query)
    return RedirectResponse(str(url), status_code=303)

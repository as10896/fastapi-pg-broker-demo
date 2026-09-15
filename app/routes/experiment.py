import math
from typing import Annotated, Any

from fastapi import APIRouter, Form, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from app import experiment
from app.db import Pool
from app.experiment import MODES, ExperimentRunner
from app.web import ExperimentsDep, PoolDep, Snippet, redirect, render

router = APIRouter()

SNIPPETS = [
    Snippet(
        "Each experiment consumer's loop",
        """
BEGIN;
  <claim query of the selected mode>           -- returns one job id, or nothing
  -- sleep job_ms: the work, done while the transaction (and any row lock) is open
  WITH done AS (
      UPDATE experiment_jobs SET status = 'done' WHERE run_id = $1 AND id = $2
  )
  INSERT INTO experiment_executions (run_id, job_id, consumer) VALUES ($1, $2, $3);
COMMIT;
-- If the claim returned nothing but pending jobs remain, try again.
""",
        "Every consumer is an asyncio task in the web app with its own connection. Each "
        "execution of a job is recorded, so executions − distinct jobs = jobs that were "
        "processed more than once.",
    ),
    Snippet("Complete a job", experiment.COMPLETE_JOB_SQL),
    Snippet("Results", experiment.RESULTS_SQL),
]


class ExperimentForm(BaseModel):
    jobs: int = Field(default=50, ge=1, le=500)
    consumers: int = Field(default=5, ge=1, le=20)
    job_ms: int = Field(default=100, ge=0, le=2000)
    modes: list[str] = []


async def _results(
    pool: Pool, runner: ExperimentRunner, experiment_id: int | None
) -> dict[str, Any]:
    experiment_id = experiment_id or await experiment.latest_experiment_id(pool)
    runs = await experiment.results(pool, experiment_id) if experiment_id else []
    context: dict[str, Any] = {
        "experiment_id": experiment_id,
        "runs": runs,
        "running": runner.running,
        "max_throughput": max((r["throughput"] for r in runs), default=0),
    }
    if runs:
        jobs, consumers, job_ms = runs[0]["jobs"], runs[0]["consumers"], runs[0]["job_ms"]
        context["setup"] = {"jobs": jobs, "consumers": consumers, "job_ms": job_ms}
        context["serial_s"] = jobs * job_ms / 1000
        context["parallel_s"] = math.ceil(jobs / consumers) * job_ms / 1000
    return context


@router.get("/experiment", response_class=HTMLResponse)
async def experiment_page(
    request: Request,
    pool: PoolDep,
    runner: ExperimentsDep,
    id: Annotated[int | None, Query()] = None,
):
    return render(
        request,
        "experiment.html",
        form=ExperimentForm(),
        modes=MODES.values(),
        history=await experiment.history(pool),
        snippets=SNIPPETS,
        **await _results(pool, runner, id),
    )


@router.get("/partials/experiment", response_class=HTMLResponse)
async def experiment_partial(
    request: Request,
    pool: PoolDep,
    runner: ExperimentsDep,
    id: Annotated[int | None, Query()] = None,
):
    return render(request, "partials/experiment.html", **await _results(pool, runner, id))


@router.post("/experiment")
async def run_experiment(form: Annotated[ExperimentForm, Form()], runner: ExperimentsDep):
    modes = [key for key in MODES if key in form.modes]
    if not modes:
        return redirect("/experiment", "Pick at least one mode.")
    if runner.running:
        return redirect("/experiment", "An experiment is already running.")
    experiment_id = await runner.start(
        modes=modes, jobs=form.jobs, consumers=form.consumers, job_ms=form.job_ms
    )
    return redirect("/experiment", f"Experiment #{experiment_id} started.", id=experiment_id)


@router.post("/experiment/clear")
async def clear_experiments(pool: PoolDep, runner: ExperimentsDep):
    if runner.running:
        return redirect("/experiment", "Wait for the running experiment to finish.")
    await experiment.clear_history(pool)
    return redirect("/experiment", "Experiment history cleared.")

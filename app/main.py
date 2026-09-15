import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app import db
from app.experiment import ExperimentRunner
from app.reaper import run_reaper
from app.routes import dashboard, experiment, messages, publish, workers

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # The web app is a producer and a UI. It runs no consumers: those live in
    # the `worker` service.
    pool = db.create_pool()
    await pool.open(wait=True, timeout=30)
    await db.apply_schema(pool)
    experiments = ExperimentRunner(pool)
    await experiments.recover()
    reaper = asyncio.create_task(run_reaper(pool), name="reaper")
    app.state.pool = pool
    app.state.experiments = experiments
    yield
    reaper.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await reaper
    await experiments.close()
    await pool.close()


app = FastAPI(title="Postgres Message Broker Demo", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")
app.include_router(dashboard.router)
app.include_router(publish.router)
app.include_router(workers.router)
app.include_router(messages.router)
app.include_router(experiment.router)

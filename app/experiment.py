"""The locking experiment: one workload, three claim queries.

Its consumers are asyncio tasks inside the web app, each with its own
connection; the `worker` service is not involved. To isolate the effect of the
locking clause, each job is processed *inside* the transaction that selected
it, so any row lock is held for the whole job. (The broker's consumers instead
commit the claim right away and rely on a lease.)
"""

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Any

from app import db
from app.db import Pool

log = logging.getLogger(__name__)

RUN_TIMEOUT_S = 120


@dataclass(frozen=True)
class Mode:
    key: str
    title: str
    claim_sql: str
    summary: str


_SELECT_NEXT_JOB = """
SELECT id
FROM experiment_jobs
WHERE run_id = %(run_id)s
  AND status = 'pending'
ORDER BY id
LIMIT 1"""

MODES = {
    mode.key: mode
    for mode in (
        Mode(
            key="no_lock",
            title="No lock",
            claim_sql=_SELECT_NEXT_JOB,
            summary="Consumers read the same 'pending' row at the same time and all process it: "
            "fast, but jobs run more than once.",
        ),
        Mode(
            key="for_update",
            title="FOR UPDATE",
            claim_sql=_SELECT_NEXT_JOB + "\nFOR UPDATE",
            summary="Every job runs exactly once, but all consumers queue up behind the one "
            "row lock: throughput collapses to that of a single consumer.",
        ),
        Mode(
            key="skip_locked",
            title="FOR UPDATE SKIP LOCKED",
            claim_sql=_SELECT_NEXT_JOB + "\nFOR UPDATE SKIP LOCKED",
            summary="Every job runs exactly once, and a consumer that meets a locked row simply "
            "takes the next one: full parallelism.",
        ),
    )
}

COMPLETE_JOB_SQL = """
WITH done AS (
    UPDATE experiment_jobs
    SET status = 'done'
    WHERE run_id = %(run_id)s AND id = %(job_id)s
)
INSERT INTO experiment_executions (run_id, job_id, consumer)
VALUES (%(run_id)s, %(job_id)s, %(consumer)s)
"""

RESULTS_SQL = """
SELECT r.id, r.mode, r.status, r.error, e.jobs, e.consumers, e.job_ms,
       extract(epoch FROM coalesce(r.finished_at, clock_timestamp()) - r.started_at)::float
           AS elapsed_s,
       count(x.job_id)            AS executions,
       count(DISTINCT x.job_id)   AS unique_jobs
FROM experiment_runs r
JOIN experiments e ON e.id = r.experiment_id
LEFT JOIN experiment_executions x ON x.run_id = r.id
WHERE r.experiment_id = %(experiment_id)s
GROUP BY r.id, e.id
ORDER BY r.id
"""


class ExperimentRunner:
    """Runs one experiment at a time in a background task of the web app."""

    def __init__(self, pool: Pool) -> None:
        self.pool = pool
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def recover(self) -> None:
        """Runs left 'running' or 'queued' by a previous process will never finish."""
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE experiment_runs SET status = 'interrupted', finished_at = now() "
                "WHERE status IN ('queued', 'running')"
            )

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            await self.recover()

    async def start(self, *, modes: list[str], jobs: int, consumers: int, job_ms: int) -> int:
        async with self.pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                "INSERT INTO experiments (jobs, consumers, job_ms) "
                "VALUES (%s, %s, %s) RETURNING id",
                (jobs, consumers, job_ms),
            )
            row = await cur.fetchone()
            assert row is not None
            experiment_id = row["id"]
            run_ids = []
            for mode in modes:
                cur = await conn.execute(
                    "INSERT INTO experiment_runs (experiment_id, mode) "
                    "VALUES (%s, %s) RETURNING id",
                    (experiment_id, mode),
                )
                run = await cur.fetchone()
                assert run is not None
                run_ids.append((run["id"], mode))
        self._task = asyncio.create_task(self._run_all(run_ids, jobs, consumers, job_ms))
        return experiment_id

    async def _run_all(
        self, runs: list[tuple[int, str]], jobs: int, consumers: int, job_ms: int
    ) -> None:
        for run_id, mode in runs:
            await self._run(run_id, MODES[mode], jobs, consumers, job_ms)

    async def _run(self, run_id: int, mode: Mode, jobs: int, consumers: int, job_ms: int) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO experiment_jobs (run_id, id) "
                "SELECT %s, g FROM generate_series(1, %s) g",
                (run_id, jobs),
            )
            await conn.execute(
                "UPDATE experiment_runs SET status = 'running', started_at = clock_timestamp() "
                "WHERE id = %s",
                (run_id,),
            )
        status, error = "finished", None
        try:
            async with asyncio.timeout(RUN_TIMEOUT_S), asyncio.TaskGroup() as group:
                for consumer in range(1, consumers + 1):
                    group.create_task(self._consumer(run_id, mode, consumer, job_ms))
        except TimeoutError:
            status, error = "failed", f"timed out after {RUN_TIMEOUT_S}s"
        except Exception as exc:
            log.exception("experiment run %s failed", run_id)
            status, error = "failed", repr(exc)
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE experiment_runs "
                "SET status = %s, error = %s, finished_at = clock_timestamp() "
                "WHERE id = %s",
                (status, error, run_id),
            )

    async def _consumer(self, run_id: int, mode: Mode, consumer: int, job_ms: int) -> None:
        params = {"run_id": run_id}
        # A dedicated connection: in FOR UPDATE mode a consumer blocks inside a
        # transaction, which must not starve the web app's pool.
        async with await db.connect() as conn:
            while True:
                async with conn.transaction():
                    cur = await conn.execute(mode.claim_sql, params)
                    job = await cur.fetchone()
                    if job is not None:
                        await asyncio.sleep(job_ms / 1000)  # the work, inside the transaction
                        await conn.execute(
                            COMPLETE_JOB_SQL,
                            {**params, "job_id": job["id"], "consumer": consumer},
                        )
                        continue
                # No row came back. Either the run is complete, or every pending
                # row was locked (SKIP LOCKED), or the row we waited for was taken
                # by the time its lock was released (FOR UPDATE ... LIMIT 1).
                cur = await conn.execute(
                    "SELECT EXISTS (SELECT 1 FROM experiment_jobs "
                    "WHERE run_id = %(run_id)s AND status = 'pending') AS more",
                    params,
                )
                more = await cur.fetchone()
                if not (more and more["more"]):
                    return
                await asyncio.sleep(0.005)


async def latest_experiment_id(pool: Pool) -> int | None:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT max(id) AS id FROM experiments")
        row = await cur.fetchone()
        return row["id"] if row else None


async def results(pool: Pool, experiment_id: int) -> list[dict[str, Any]]:
    async with pool.connection() as conn:
        cur = await conn.execute(RESULTS_SQL, {"experiment_id": experiment_id})
        runs = await cur.fetchall()
        cur = await conn.execute(
            "SELECT run_id, consumer, count(*) AS executions FROM experiment_executions "
            "WHERE run_id = ANY(%s) GROUP BY run_id, consumer ORDER BY run_id, consumer",
            ([r["id"] for r in runs],),
        )
        per_consumer = await cur.fetchall()
    for run in runs:
        run["mode_title"] = MODES[run["mode"]].title
        run["duplicates"] = run["executions"] - run["unique_jobs"]
        elapsed = run["elapsed_s"] or 0
        run["throughput"] = run["unique_jobs"] / elapsed if elapsed > 0 else 0
        run["per_consumer"] = [c for c in per_consumer if c["run_id"] == run["id"]]
    return runs


async def history(pool: Pool, limit: int = 10) -> list[dict[str, Any]]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT e.id, e.jobs, e.consumers, e.job_ms, e.created_at,
                   string_agg(r.mode || ':' || r.status, ', ' ORDER BY r.id) AS runs
            FROM experiments e
            LEFT JOIN experiment_runs r ON r.experiment_id = e.id
            GROUP BY e.id
            ORDER BY e.id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return await cur.fetchall()


async def clear_history(pool: Pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "TRUNCATE experiments, experiment_runs, experiment_jobs, "
            "experiment_executions RESTART IDENTITY"
        )

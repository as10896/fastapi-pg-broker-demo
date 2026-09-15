# fastapi-pg-broker-demo

A message broker built on **one PostgreSQL table**: no RabbitMQ, no Redis.
A FastAPI + Jinja2 (+ htmx) web app publishes messages and shows everything that happens;
worker containers consume them, like Celery workers. You can scale and crash workers,
watch retries and dead letters, and compare locking strategies side by side.
Every page shows the exact SQL it runs.

| Broker feature                       | How Postgres does it                                                |
| ------------------------------------ | ------------------------------------------------------------------- |
| Publish                              | `INSERT INTO messages`                                              |
| Competing consumers                  | `SELECT … FOR UPDATE SKIP LOCKED` inside an `UPDATE … RETURNING`    |
| Ack                                  | `UPDATE … SET status = 'done'`                                      |
| Retry with backoff, delayed delivery | `available_at` column                                               |
| Dead letter queue                    | `status = 'dead'` once `attempts >= max_attempts`                   |
| Crash recovery (visibility timeout)  | Leases renewed by heartbeats, a reaper that takes back expired ones |
| Push instead of polling              | `LISTEN` / `NOTIFY` fired by a trigger                              |
| Priorities                           | `ORDER BY priority DESC, id`                                        |
| Remote control of workers            | A `worker_commands` table, consumed the same way                    |

## Services

```
browser ──▶ app ──▶ db ◀── worker (× N)
```

| Service  | Role                                                            | Reachable from the host |
| -------- | --------------------------------------------------------------- | ----------------------- |
| `db`     | PostgreSQL 18, the broker                                       | No                      |
| `app`    | Web UI, producer, locking experiment                            | <http://localhost:8000> |
| `worker` | Consumer process (`python -m app.worker`), like `celery worker` | No                      |

`app` and `worker` never talk to each other. Everything goes through Postgres, including the
buttons in the UI that start, stop or kill consumers inside a worker.

## Quick start

```bash
docker compose up --build
```

Open <http://localhost:8000>. One worker with two consumers starts on the `default` queue.

```bash
docker compose up -d --scale worker=3    # three worker processes
docker compose logs -f worker
docker compose kill worker               # SIGKILL every worker: a real crash
docker compose up -d worker              # bring them back
docker compose exec db psql -U broker    # look at the tables
docker compose down -v                   # stop and delete all data
```

Worker options are in `compose.yaml`:
`python -m app.worker --queue default --concurrency 2 --work-ms 300`.

## Development

```bash
docker compose watch
```

Changes in `app/` or `sql/` are synced into the containers, which then restart; a change to
`uv.lock` rebuilds the image. Lint and format on the host with
`uv run ruff check . && uv run ruff format .`

## Things to try

1. **Publish → Burst.** Watch the worker drain 200 messages on the dashboard.
2. **Scale.** Start more consumers on the Workers page, or `docker compose up -d --scale worker=3`.
   Throughput goes up, and no message is ever processed twice.
3. **Crash a consumer.** Start consumers with 5000 ms of work, publish a few messages, and click
   _Kill_ on a busy one. Its message stays `processing` until its lease expires (10 s), then the
   reaper makes it `pending` and another consumer takes it. For a real crash,
   `docker compose kill worker` while messages are being processed.
4. **Publish → Flaky / Poison.** Failed messages come back after 2 s, 4 s, … Poison messages
   land in **Dead letters**, where you can requeue them.
5. **Publish → Delayed / Urgent.** Delayed delivery and priorities.
6. **Locking experiment.** Run the same workload with no lock, `FOR UPDATE`, and
   `FOR UPDATE SKIP LOCKED`.
7. **Produce from psql.** Any client that can `INSERT` is a producer; idle consumers wake up
   instantly through the `NOTIFY` trigger:
   ```bash
   docker compose exec db psql -U broker -c \
     "INSERT INTO messages (queue, payload) VALUES ('default', '{\"hello\": \"psql\"}')"
   ```

## How it works

### The table

```sql
CREATE TABLE messages (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue        text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}',
    status       text        NOT NULL DEFAULT 'pending',  -- pending | processing | done | dead
    priority     integer     NOT NULL DEFAULT 0,
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 3,
    available_at timestamptz NOT NULL DEFAULT now(),       -- delay and retry backoff
    locked_by    text,                                     -- lease holder (a consumer)
    locked_at    timestamptz,                              -- lease last renewed
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz
);

-- Only pending rows are ever searched, so only they are indexed.
CREATE INDEX messages_claim_idx ON messages (queue, priority DESC, id) WHERE status = 'pending';
```

Full schema, including triggers, the worker registry and the experiment tables:
[`sql/schema.sql`](sql/schema.sql). The app and every worker apply it on startup under an
advisory lock; every statement is idempotent.

### Claiming a message

```sql
WITH next AS (
    SELECT id
    FROM messages
    WHERE queue = %(queue)s AND status = 'pending' AND available_at <= now()
    ORDER BY priority DESC, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE messages AS m
SET status = 'processing', locked_by = %(consumer_id)s, locked_at = now(), attempts = m.attempts + 1
FROM next
WHERE m.id = next.id
RETURNING m.*;
```

- `FOR UPDATE` locks the chosen row, so two consumers cannot both claim it.
- `SKIP LOCKED` makes a consumer that meets a row locked by someone else move on to the next
  row instead of waiting. Without it, all consumers queue up behind one lock.
- The `UPDATE` marks the row as ours in the same statement. The connection is in autocommit,
  so the row lock lasts only milliseconds; afterwards `status = 'processing'` keeps other
  consumers away, and no transaction stays open while the message is handled.

### Ack, nack, retry, dead letters

- **Ack**: `status = 'done'`, but only if `locked_by` is still us.
- **Nack**: if `attempts < max_attempts`, back to `pending` with
  `available_at = now() + 2^attempts seconds` (capped at 60 s); otherwise `dead`.
- **Requeue** from the dead letter queue: back to `pending` with `attempts = 0`.

### Workers, leases and crash recovery

A worker process runs `--concurrency` consumers as asyncio tasks, each with its own claim, handle,
ack loop. A consumer that crashes mid-message never acks it. To recover such messages:

1. Every second, each worker sends a heartbeat (`workers` and `consumers` tables) that also
   renews `locked_at` on every message its consumers are processing.
2. A reaper, running in the web app and in every worker, makes `processing` messages whose
   `locked_at` is older than the visibility timeout (10 s) `pending` again, or `dead` if they have
   used up their attempts. A message that keeps crashing its consumers ends up dead.
   Because the web app runs a reaper too, messages are recovered even when every worker is gone.
3. Acks and nacks check `locked_by`, so a consumer that lost its lease cannot overwrite the
   result of the consumer that took over.

This gives **at-least-once delivery**: a message whose consumer was only slow (for example,
frozen for longer than the timeout) can be processed twice. Handlers should be idempotent.

On `SIGTERM` (`docker compose stop`), a worker lets its consumers finish their current message
(up to 5 s) and deregisters itself.

### LISTEN / NOTIFY

A trigger calls `pg_notify('broker_messages', queue)` when a ready message is inserted or
becomes pending again. Each worker keeps one `LISTEN` connection and wakes its idle consumers
when a notification arrives. Notifications are sent on commit, and identical ones in one
transaction are merged, so a bulk insert of 10,000 rows sends one. Consumers still poll every
second as a fallback, which also catches delayed messages and retries becoming due.

### Remote control

Workers expose no ports, so the web app cannot call them. To start, stop or kill consumers,
it inserts a row into `worker_commands`; a trigger sends `NOTIFY broker_control`, and the
worker takes its commands with `DELETE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)
RETURNING …`. This is how Celery's `celery control` works too, through the broker.

### The locking experiment

The experiment page processes the same jobs three times, changing only the claim query. Each
job is handled _inside_ the transaction that selected it, so any row lock lasts for the whole
job. It runs inside the web app, on its own tables, and does not involve the workers.

| Mode                     | Result                                                                             |
| ------------------------ | ---------------------------------------------------------------------------------- |
| No lock                  | Workers read the same pending row and all process it: many duplicates              |
| `FOR UPDATE`             | No duplicates, but workers wait on each other's locks: about as slow as one worker |
| `FOR UPDATE SKIP LOCKED` | No duplicates and full parallelism                                                 |

## Project layout

```
compose.yaml            db (internal), app (port 8000), worker (internal)
Dockerfile
sql/schema.sql          Tables, partial indexes, NOTIFY triggers
app/
  config.py             Settings from environment variables
  db.py                 Connection pool (psycopg_pool), schema bootstrap
  broker.py             Every broker operation, as SQL constants plus thin async functions
  notifier.py           LISTEN connection that wakes waiting loops
  consumer.py           The consumer loop: claim, handle, ack / nack
  worker.py             Worker process: consumers, heartbeats, remote commands (python -m app.worker)
  reaper.py             Crash recovery, run by the web app and every worker
  experiment.py         The locking experiment
  main.py               FastAPI app and lifespan
  web.py                Dependencies, templates, helpers
  routes/               One module per page
  templates/            Jinja2 pages; partials/ are refreshed by htmx
  static/               CSS and a vendored copy of htmx
```

Design choices:

- **psycopg 3 with plain SQL, no ORM.** The SQL is the point of the demo.
- **The schema is a plain SQL file.** Triggers and partial indexes are Postgres features that an
  ORM would only wrap. A real project would add a migration tool.
- **Jinja2 pages with post/redirect/get forms.** htmx only refreshes parts of pages every second
  or two; there is no JavaScript build step.

## Configuration

Environment variables, for both `app` and `worker`:

| Variable               | Default                                     | Meaning                                    |
| ---------------------- | ------------------------------------------- | ------------------------------------------ |
| `DATABASE_URL`         | `postgresql://broker:broker@db:5432/broker` | Connection string                          |
| `VISIBILITY_TIMEOUT_S` | `10`                                        | Lease duration before a message is reaped  |
| `POLL_INTERVAL_S`      | `1`                                         | Fallback poll interval for idle consumers  |
| `HEARTBEAT_INTERVAL_S` | `1`                                         | Heartbeat and lease renewal interval       |
| `REAPER_INTERVAL_S`    | `2`                                         | How often expired leases are checked       |
| `APP_PORT`             | `8000`                                      | Host port of the web app in `compose.yaml` |

Worker command-line options: `--queue`, `--concurrency` (number of consumers), `--work-ms`
(simulated work per message).

## When is this a good idea?

**Good fit:** you already run Postgres; you need to process a few hundred to a few thousand
messages per second; you want jobs to be enqueued **in the same transaction** as the business
data they belong to (no lost or phantom jobs, and no outbox pattern needed); and you want one
less piece of infrastructure to run.

**Watch out for:**

- Churn. Every message is an insert plus updates, which leaves dead tuples for vacuum.
  Delete or archive `done` rows (or partition by time) on busy queues.
- Connections. Every worker holds a small pool of database connections.
- No fan-out or pub/sub routing. This is a work queue; `NOTIFY` is only a wake-up signal.
- Very high throughput, or streaming and replay, is what Kafka, RabbitMQ and friends are for.

Production-grade libraries built on the same idea:

- [PGMQ](https://github.com/pgmq/pgmq)
- [Procrastinate](https://github.com/procrastinate-org/procrastinate) (Python)
- [River](https://github.com/riverqueue/river) (Go)
- [Oban](https://github.com/oban-bg/oban) (Elixir)
- [Graphile Worker](https://github.com/graphile/worker) (Node.js).

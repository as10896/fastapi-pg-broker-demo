# fastapi-pg-broker-demo

A message broker is the usual answer to long-running work. Instead of making a request wait
for a slow job, such as sending emails, generating a report or calling a third-party API, the
web app puts a message on a queue and returns right away. Background workers then pick the
job up, retry it when it fails, and scale independently of the web app.

The usual way to get one is to run a dedicated broker such as RabbitMQ or Kafka. That is one
more cluster to deploy, secure, monitor, back up and keep available, and in many systems it is
more machinery than the problem calls for.

In system design, a smaller tech stack is usually a better one. When the requirements are
modest and something you already run can provide a simple version of the feature, use it
rather than adding another component.

This repo shows how to build a message broker directly on **PostgreSQL**, so you don't need to
stand up a RabbitMQ cluster. It is most useful when your stack already keeps its business data
in a relational database: the queue lives in the same database, is backed up and monitored the
same way, and a job can even be enqueued in the same transaction as the data it belongs to.

The demo is a FastAPI + Jinja2 (+ htmx) web app that publishes messages and shows everything
that happens, plus worker containers that consume them. The broker, the workers and their
consumers are all implemented in this repo on top of psycopg: no Celery or other task-queue
library is involved. Celery only comes up below as a familiar point of comparison.

You can scale and crash workers, watch retries and dead letters, and compare locking strategies
side by side. Every page shows the exact SQL it runs, and every broker feature maps to plain SQL:

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

## Services

```
browser ──▶ app ──▶ db ◀── worker (× N)
```

| Service  | Role                                      | Reachable from the host |
| -------- | ----------------------------------------- | ----------------------- |
| `db`     | PostgreSQL 18, the broker                 | No                      |
| `app`    | Web UI, producer, locking experiment      | <http://localhost:8000> |
| `worker` | Consumer process (`python -m app.worker`) | No                      |

`app` and `worker` never talk to each other. Everything goes through Postgres, including the
buttons in the UI that start, stop or kill consumers inside a worker.

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

The Mermaid diagrams in [`docs/`](docs) show these mechanisms step by step.

| Diagram                                                           | Shows                                                          |
| ----------------------------------------------------------------- | -------------------------------------------------------------- |
| [`erd.mmd`](docs/erd.mmd)                                         | Every table and how they relate                                |
| [`sequence-message-flow.mmd`](docs/sequence-message-flow.mmd)     | A message from publish to ack, woken up by LISTEN / NOTIFY     |
| [`sequence-skip-locked.mmd`](docs/sequence-skip-locked.mmd)       | Two consumers claiming at once, with and without `SKIP LOCKED` |
| [`sequence-crash-recovery.mmd`](docs/sequence-crash-recovery.mmd) | Leases and the reaper recovering a crashed consumer's message  |
| [`sequence-remote-control.mmd`](docs/sequence-remote-control.mmd) | Killing a consumer from the web UI, through Postgres           |

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

[`docs/sequence-skip-locked.mmd`](docs/sequence-skip-locked.mmd) walks through two consumers
claiming at the same moment with `SKIP LOCKED`, with `FOR UPDATE` alone, and with no lock.

### Ack, nack, retry, dead letters

- **Ack**: `status = 'done'`, but only if `locked_by` is still us.
- **Nack**: if `attempts < max_attempts`, back to `pending` with
  `available_at = now() + 2^attempts seconds` (capped at 60 s); otherwise `dead`.
- **Requeue** from the dead letter queue: back to `pending` with `attempts = 0`.

### Workers, leases and crash recovery

A worker process runs `--concurrency` consumers as asyncio tasks, each with its own claim, handle,
ack loop. A consumer that crashes mid-message never acks it, and from the outside a crashed
consumer looks just like a slow one. Two ideas deal with that:

- A **lease** is a claim on a message that expires unless its holder keeps renewing it, like a
  library loan that must be extended before its due date. Here the lease is the `locked_by` and
  `locked_at` columns: claiming a message sets both, and renewing moves `locked_at` forward.
- A **reaper** is a background job that periodically finds expired leases and takes their
  messages back, because nothing in a database changes by itself as time passes. The name comes
  from Unix, where a parent process "reaps" its dead child processes; other job queues call the
  same job a rescuer, a lifeline or a janitor. This one also removes workers that stopped sending
  heartbeats from the registry.

Put together:

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

Step by step, including a consumer that was only frozen and comes back too late:
[`docs/sequence-crash-recovery.mmd`](docs/sequence-crash-recovery.mmd).

### LISTEN / NOTIFY

A trigger calls `pg_notify('broker_messages', queue)` when a ready message is inserted or
becomes pending again. Each worker keeps one `LISTEN` connection and wakes its idle consumers
when a notification arrives. Notifications are sent on commit, and identical ones in one
transaction are merged, so a bulk insert of 10,000 rows sends one. Consumers still poll every
second as a fallback, which also catches delayed messages and retries becoming due.
The whole path of a message, from the form to the ack, is in
[`docs/sequence-message-flow.mmd`](docs/sequence-message-flow.mmd).

### Remote control

Workers expose no ports, so the web app cannot call them. To start, stop or kill consumers,
it inserts a row into `worker_commands`; a trigger sends `NOTIFY broker_control`, and the
worker takes its commands with `DELETE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)
RETURNING …`. For comparison, Celery's `celery control` commands also reach workers through its
broker rather than by calling them directly.
See [`docs/sequence-remote-control.mmd`](docs/sequence-remote-control.mmd).

### The locking experiment

The experiment page processes the same jobs three times with the same number of consumers,
changing only the claim query. Each job is handled _inside_ the transaction that selected it, so
any row lock lasts for the whole job. Its consumers are asyncio tasks inside the web app, working
on their own tables; the `worker` service is not involved.

| Mode                     | Result                                                                                 |
| ------------------------ | -------------------------------------------------------------------------------------- |
| No lock                  | Consumers read the same pending row and all process it: many duplicates                |
| `FOR UPDATE`             | No duplicates, but consumers wait on each other's locks: about as slow as one consumer |
| `FOR UPDATE SKIP LOCKED` | No duplicates and full parallelism                                                     |

## Workers and consumers

A **worker** is a process; a **consumer** is a loop inside it. One worker runs several consumers.

|              | Worker                                                                                               | Consumer                                            |
| ------------ | ---------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| What it is   | An OS process: one replica of the `worker` service                                                   | An asyncio task inside a worker                     |
| Code         | `Worker` in `app/worker.py`                                                                          | `Consumer` in `app/consumer.py`                     |
| Job          | Owns what the process shares: connection pool, `LISTEN` connection, heartbeats, command loop, reaper | Claim a message, handle it, ack or nack it, repeat  |
| How many     | `docker compose up -d --scale worker=N`                                                              | `--concurrency` per worker, or more from the UI     |
| Table        | `workers`                                                                                            | `consumers` (`worker_id` → `workers.id`)            |
| Id           | `3bc3a49e6fdf-653e` (hostname and a random suffix)                                                   | `3bc3a49e6fdf-653e-c2`                              |
| When it dies | `docker compose kill worker` stops all of its consumers                                              | _Kill_ on the Workers page stops only that consumer |

Each responsibility sits at the level where it belongs:

- **A consumer holds messages.** It handles at most one at a time, so `messages.locked_by` is a
  consumer id, and acks and nacks check it.
- **A worker proves liveness.** Its heartbeat reports all of its consumers and renews the leases
  of the messages they hold.
- **A worker receives commands.** It is the process that actually exists, so the UI addresses
  commands to a worker, which then starts, stops or kills its consumers.

Throughput depends on the total number of consumers. Add them inside a worker (`--concurrency`)
or add workers (`--scale`).

If you know Celery, the roles map like this, even though Celery is not used here: a worker plays
the part of a `celery worker` process, and its consumers correspond to that process's pool
(`--concurrency`). Unlike Celery's default `prefork` pool, which runs each unit in a child
process, these consumers share one event loop, closer to Celery's `gevent` or `eventlet` pools.
They suit I/O-bound handlers; a CPU-bound handler would block the other consumers of its worker,
so scale that kind of work with more workers instead.

## Project layout

```
compose.yaml            db (internal), app (port 8000), worker (internal)
Dockerfile
sql/schema.sql          Tables, partial indexes, NOTIFY triggers
docs/                   ERD and sequence diagrams (Mermaid)
app/
  config.py             Settings from environment variables
  db.py                 Connection pool (psycopg_pool), schema bootstrap
  broker/               Every broker operation, as SQL constants plus thin async functions
    messages.py         Publishing and the message lifecycle (claim, ack, nack, reap, dead letters)
    monitoring.py       Read-only queries for the UI
    registry.py         Worker and consumer heartbeats
    control.py          Commands from the web app to workers
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

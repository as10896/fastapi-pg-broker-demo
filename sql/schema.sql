-- Schema for the Postgres message broker demo.
--
-- The app runs this file on every startup (under an advisory lock), so every
-- statement must be idempotent: IF NOT EXISTS / CREATE OR REPLACE.


-- ---------------------------------------------------------------------------
-- messages: the queue itself. One row = one message.
--
--   pending ──claim──▶ processing ──ack──▶ done
--      ▲                   │
--      └──nack / lease ────┤  (attempts < max_attempts, retried after a backoff)
--         expired          │
--                          └──▶ dead       (attempts >= max_attempts)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS messages (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue        text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}',
    status       text        NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending', 'processing', 'done', 'dead')),
    priority     integer     NOT NULL DEFAULT 0,
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    -- A pending message is invisible to consumers until available_at.
    -- Used for delayed delivery and for retry backoff.
    available_at timestamptz NOT NULL DEFAULT now(),
    -- The lease: which consumer holds the message and when it last renewed it.
    locked_by    text,
    locked_at    timestamptz,
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz
);

-- The claim query only ever looks at pending rows, so index only those.
-- The index stays small no matter how many done/dead rows pile up.
CREATE INDEX IF NOT EXISTS messages_claim_idx
    ON messages (queue, priority DESC, id)
    WHERE status = 'pending';

-- The reaper looks for processing rows whose lease has expired.
CREATE INDEX IF NOT EXISTS messages_lease_idx
    ON messages (locked_at)
    WHERE status = 'processing';

-- Filtering in the UI (e.g. the dead letter list).
CREATE INDEX IF NOT EXISTS messages_queue_status_idx
    ON messages (queue, status, id);


-- ---------------------------------------------------------------------------
-- LISTEN / NOTIFY: wake idle consumers as soon as work becomes available,
-- instead of making them poll in a tight loop.
--
-- Notifications are transactional: they are delivered only when the inserting
-- transaction commits, and identical notifications within one transaction are
-- collapsed, so a bulk insert of 10,000 rows sends a single NOTIFY.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION notify_message_available() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('broker_messages', NEW.queue);
    RETURN NULL;
END;
$$;

-- A new message that is ready right away.
CREATE OR REPLACE TRIGGER messages_notify_insert
    AFTER INSERT ON messages
    FOR EACH ROW
    WHEN (NEW.available_at <= now())
    EXECUTE FUNCTION notify_message_available();

-- An existing message that became pending again (requeued from the dead
-- letter queue, or recovered from a crashed consumer).
CREATE OR REPLACE TRIGGER messages_notify_requeue
    AFTER UPDATE OF status ON messages
    FOR EACH ROW
    WHEN (NEW.status = 'pending' AND OLD.status <> 'pending' AND NEW.available_at <= now())
    EXECUTE FUNCTION notify_message_available();


-- ---------------------------------------------------------------------------
-- Worker registry, kept fresh by heartbeats.
--
-- A worker is one process: a replica of the `worker` compose service. It runs
-- several consumers, concurrent loops that claim and handle messages (its
-- concurrency).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workers (
    id           text PRIMARY KEY,
    hostname     text        NOT NULL,
    pid          integer     NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS consumers (
    id                 text PRIMARY KEY,
    worker_id          text        NOT NULL REFERENCES workers (id) ON DELETE CASCADE,
    queue              text        NOT NULL,
    state              text        NOT NULL,
    current_message_id bigint,
    work_ms            integer     NOT NULL,
    processed          integer     NOT NULL DEFAULT 0,
    failed             integer     NOT NULL DEFAULT 0,
    started_at         timestamptz NOT NULL DEFAULT now(),
    heartbeat_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS consumers_worker_idx
    ON consumers (worker_id);


-- ---------------------------------------------------------------------------
-- Remote control. Workers expose no ports, so the web app cannot call them.
-- It sends commands through Postgres instead, the same way it sends messages.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS worker_commands (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    worker_id  text        NOT NULL REFERENCES workers (id) ON DELETE CASCADE,
    command    text        NOT NULL CHECK (command IN ('start', 'stop', 'kill')),
    args       jsonb       NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS worker_commands_worker_idx
    ON worker_commands (worker_id, id);

CREATE OR REPLACE FUNCTION notify_worker_command() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('broker_control', NEW.worker_id);
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER worker_commands_notify
    AFTER INSERT ON worker_commands
    FOR EACH ROW
    EXECUTE FUNCTION notify_worker_command();


-- ---------------------------------------------------------------------------
-- Locking experiment: the same workload run with three different claim
-- queries (no lock / FOR UPDATE / FOR UPDATE SKIP LOCKED).
-- Kept separate from `messages` so experiments never disturb the demo queue.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS experiments (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    jobs       integer     NOT NULL,
    consumers  integer     NOT NULL,
    job_ms     integer     NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS experiment_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    experiment_id bigint NOT NULL REFERENCES experiments (id) ON DELETE CASCADE,
    mode          text   NOT NULL CHECK (mode IN ('no_lock', 'for_update', 'skip_locked')),
    status        text   NOT NULL DEFAULT 'queued'
                         CHECK (status IN ('queued', 'running', 'finished', 'failed', 'interrupted')),
    started_at    timestamptz,
    finished_at   timestamptz,
    error         text
);

CREATE INDEX IF NOT EXISTS experiment_runs_experiment_idx
    ON experiment_runs (experiment_id);

CREATE TABLE IF NOT EXISTS experiment_jobs (
    run_id bigint  NOT NULL REFERENCES experiment_runs (id) ON DELETE CASCADE,
    id     integer NOT NULL,
    status text    NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'done')),
    PRIMARY KEY (run_id, id)
);

-- One row every time a job's handler actually ran. More rows than jobs means
-- the same job was processed more than once.
CREATE TABLE IF NOT EXISTS experiment_executions (
    run_id      bigint      NOT NULL REFERENCES experiment_runs (id) ON DELETE CASCADE,
    job_id      integer     NOT NULL,
    consumer    integer     NOT NULL,
    executed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS experiment_executions_run_idx
    ON experiment_executions (run_id);

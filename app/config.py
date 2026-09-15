import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # `db` is the Postgres service on the compose network.
    database_url: str = os.environ.get("DATABASE_URL", "postgresql://broker:broker@db:5432/broker")
    # A processing message whose lease has not been renewed for this long is
    # considered abandoned (its consumer crashed) and is handed out again.
    visibility_timeout_s: float = float(os.environ.get("VISIBILITY_TIMEOUT_S", "10"))
    # Idle consumers are woken by NOTIFY; this is the fallback poll interval,
    # needed for delayed messages and retries whose backoff has elapsed.
    poll_interval_s: float = float(os.environ.get("POLL_INTERVAL_S", "1"))
    heartbeat_interval_s: float = float(os.environ.get("HEARTBEAT_INTERVAL_S", "1"))
    reaper_interval_s: float = float(os.environ.get("REAPER_INTERVAL_S", "2"))


settings = Settings()

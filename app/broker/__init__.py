"""The message broker. Every operation is a single SQL statement.

The SQL lives in module-level constants so the web pages can show exactly what runs.

- messages:   the queue itself: publishing and the message lifecycle
- monitoring: read-only views of the queue for the web UI
- registry:   which workers and consumers are alive (heartbeats)
- control:    commands from the web app to workers
"""

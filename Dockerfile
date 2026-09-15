FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.8.19 /uv /uvx /bin/

# UV_COMPILE_BYTECODE: compile .pyc files at build time, so containers start faster.
# UV_LINK_MODE=copy:   the uv cache below is a separate mount, and hardlinks cannot
#                      cross filesystems; copy packages into the venv instead.
# UV_PYTHON_DOWNLOADS: use the image's Python, never download another one.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

WORKDIR /app

# An unprivileged system user (no home directory, no login shell) to run the app.
# Build steps below still run as root; only the running container uses this user.
RUN groupadd --system app && useradd --system --gid app --no-create-home app

# Step 1: install dependencies only.
#
# --mount=type=cache  keeps uv's download cache on the build host between builds
#                     (never in the image), so when uv.lock changes only new or
#                     updated packages are downloaded.
# --mount=type=bind   makes pyproject.toml and uv.lock readable during this step
#                     without copying them into a layer. This layer is therefore
#                     rebuilt only when those two files change, not on every code
#                     change.
# --no-install-project  install the dependencies but not the app itself, whose
#                     code has not been copied yet.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Step 2: add the code (.dockerignore keeps the host's .venv out).
# --chown: `docker compose watch` syncs changed files into /app as the container
# user, so that user must be able to write there. The .venv stays owned by root
# and read-only for the app.
COPY --chown=app:app . .

# Step 3: install the project itself. This app has no [build-system], so uv does
# not install it and this is a no-op today; it keeps the image correct if the
# project ever becomes an installable package.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"

USER app

# The web app. compose.yaml overrides the command for the worker service.
CMD ["fastapi", "run", "--port", "8000"]

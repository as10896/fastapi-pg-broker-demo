# Multi-stage build for a uv-managed FastAPI app.
#
#   builder: has uv, installs the dependencies into /app/.venv
#   runtime: gets only the virtual environment and the code; uv is not in the final image
#
# Both stages must use the same base image: the scripts and the `python` symlink in
# .venv point to the base image's interpreter (/usr/local/bin/python3.x).
ARG PYTHON_IMAGE=python:3.13-slim


# ------------------------------------------------------------------------------
# builder
# ------------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS builder

COPY --from=ghcr.io/astral-sh/uv:0.8.19 /uv /uvx /bin/

# UV_COMPILE_BYTECODE: compile .pyc files at build time, so containers start faster.
# UV_LINK_MODE=copy:   the uv cache below is a separate mount, and hardlinks cannot
#                      cross filesystems; copy packages into the venv instead.
# UV_PYTHON_DOWNLOADS: use the image's Python, never download another one.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

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

# Step 2: install the project itself. This app has no [build-system], so uv does
# not install it and this is a no-op here. For a packaged project it installs the
# package into the venv; --no-editable copies it in instead of linking to /app,
# so the venv works on its own in the runtime stage.
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable


# ------------------------------------------------------------------------------
# runtime
# ------------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS runtime

ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

# An unprivileged system user (no home directory, no login shell) to run the app.
RUN groupadd --system app && useradd --system --gid app --no-create-home app

WORKDIR /app

# The virtual environment from the builder, owned by root: read-only for the app.
COPY --from=builder /app/.venv /app/.venv

# The code, straight from the build context (.dockerignore keeps the host's .venv
# out, so it cannot overwrite the one above).
# --chown: `docker compose watch` syncs changed files into /app as the container
# user, so that user must be able to write there.
COPY --chown=app:app . .

USER app

# The web app. compose.yaml overrides the command for the worker service.
CMD ["fastapi", "run", "--port", "8000"]

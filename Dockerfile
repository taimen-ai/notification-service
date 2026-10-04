# The build context is the root of the umbrella, two levels above this repository
# (services/notification-service, TAI-ADR-0064): platform-auth-sdk (sdk/) and the
# Control Plane client (services/control-plane/client) are consumed as path
# dependencies (the platform has no internal package index yet); the image keeps
# the same relative paths under /app.
#
#   docker build -f services/notification-service/Dockerfile -t notification-service ../..

# --- build stage --------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app/services/notification-service

# Sibling packages have to be in place before the lockfile is resolved.
COPY sdk/platform-auth-sdk /app/sdk/platform-auth-sdk
COPY services/control-plane/client /app/services/control-plane/client

COPY services/notification-service/pyproject.toml services/notification-service/uv.lock services/notification-service/README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY services/notification-service/src ./src
COPY services/notification-service/alembic.ini ./
COPY services/notification-service/migrations ./migrations
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# --- runtime stage -------------------------------------------------------------
FROM python:3.12-slim-bookworm
RUN useradd --create-home --uid 10001 appuser
WORKDIR /app/services/notification-service
COPY --from=builder --chown=appuser:appuser /app /app
ENV PATH="/app/services/notification-service/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1
USER appuser
EXPOSE 8000
CMD ["notification-service"]

# The build context is the directory *above* this repository: platform-auth-sdk
# and the Control Plane client live in sibling repositories and are consumed as
# path dependencies (the platform has no internal package index yet).
#
#   docker build -f notification-service/Dockerfile -t notification-service ..

# --- build stage --------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app/notification-service

# Sibling packages have to be in place before the lockfile is resolved.
COPY platform-auth-sdk /app/platform-auth-sdk
COPY control-plane/client /app/control-plane/client

COPY notification-service/pyproject.toml notification-service/uv.lock notification-service/README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY notification-service/src ./src
COPY notification-service/alembic.ini ./
COPY notification-service/migrations ./migrations
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# --- runtime stage -------------------------------------------------------------
FROM python:3.12-slim-bookworm
RUN useradd --create-home --uid 10001 appuser
WORKDIR /app/notification-service
COPY --from=builder --chown=appuser:appuser /app /app
ENV PATH="/app/notification-service/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1
USER appuser
EXPOSE 8000
CMD ["notification-service"]

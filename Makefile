# Checks of the repository. The executor's `.agents/runner.yaml` calls these
# targets, and CI is to call them too (the umbrella has no job yet), instead of
# copying their commands (FR-010 of `universal-runner`).
.PHONY: help install lint fmt test migrations-check check

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

install: ## Install runtime and dev dependencies from uv.lock (neighbours by path)
	uv sync --locked

lint: ## ruff: lint and formatting
	uv run ruff check .
	uv run ruff format --check .

fmt: ## ruff: fix lint and reformat
	uv run ruff check . --fix
	uv run ruff format .

# One test run per working copy: the test database is shared, so a second
# `make test` exits at once instead of racing the first. The lock is held by
# fd 9, inherited by pytest, and released when the run ends, however it ends.
# Where flock(1) is missing (macOS) the run goes on unlocked with a warning.
# Extra pytest arguments: make test PYTEST_ARGS="tests/test_app.py -x".
TEST_LOCK := .pytest.lock

test: ## pytest against NS_TEST_DATABASE_URL
	@exec 9>$(TEST_LOCK); \
	if ! command -v flock >/dev/null; then \
		echo "flock not found (util-linux): running without the $(TEST_LOCK) lock" >&2; \
	elif ! flock -n 9; then \
		echo "tests are already running: $(TEST_LOCK) is held by another make test here" >&2; \
		exit 75; \
	fi; \
	if [ -z "$$NS_TEST_DATABASE_URL" ]; then \
		echo "NS_TEST_DATABASE_URL is not set: the tests need a PostgreSQL database" >&2; \
		exit 2; \
	fi; \
	uv run pytest -q $(PYTEST_ARGS)

migrations-check: ## Exactly one alembic head
	@heads=$$(uv run alembic heads 2>/dev/null | grep -c '(head)'); \
	if [ "$$heads" != 1 ]; then \
		echo "expected one alembic head, found $$heads:" >&2; uv run alembic heads >&2; exit 1; \
	fi

check: lint migrations-check test ## Everything CI is to run

# notification-service

*English. Russian version: [README.ru.md](README.ru.md)*

The notification service of the Taimen platform: it accepts notifications from any
service or skill, chooses channels according to the recipient's preferences (web
inbox, Telegram, email), keeps a delivery log and accepts human decisions from the
channels (Telegram buttons).

The design lives in `docs/specs/notifications/` of the superproject, the platform
decisions are TAI-ADR-0048 and TAI-ADR-0049; the service's own decisions are in
[docs/adr/](docs/adr/). The service skeleton is task N006 (TASK-000415): sending
with deduplication, addressing by principal / role / group, preferences and
mandatory rules, a delivery worker with retries, the `web` and `email` channels, a
web inbox with an SSE stream. The Telegram channel (N007,
[ADR-0003](docs/adr/0003-telegram-channel-and-decisions.md)): linking a private
chat with a code from IAM, linking groups to a workspace and a role, messages with
decision buttons — a button press becomes a human decision in the core. The
consumer of core events (N008,
[ADR-0002](docs/adr/0002-control-plane-event-consumer.md)) executes notification
rules — `NotificationRule` catalog objects applied to the service
([ADR-0005](docs/adr/0005-notification-rules-as-data.md), C007 of
`declarative-cycle`): which event becomes a notification, for whom, with what
text and buttons, and what closes them. The former behaviour (a decision request
with buttons, closed by the outcome; a failed check to the task owner) is three
rules of the `notify` package; with no rule applied the service reads no events.
Rules live at `/api/v1/notification-rules` (scope `notifications:admin`).
Packages send notifications with the `notify.send@1` skill
([ADR-0004](docs/adr/0004-notify-send-skill.md), contract —
[docs/skills/notify.send@1.json](docs/skills/notify.send@1.json)). The ADR
registry is [docs/adr/README.md](docs/adr/README.md).

ADRs are in Russian; English summaries on request.

## Architecture

One process: FastAPI (`/api/v1`), a background delivery worker and a consumer of
core events (the `control_plane_client.events` SDK), its own PostgreSQL, Alembic
migrations. Tokens are verified by `platform-auth-sdk` (audience
`notification-service`, scopes `notifications:send|read|admin`); principals, roles
and IAM links are read from the Control Plane with the service's own service
account. Details and the API contract —
[ADR-0001](docs/adr/0001-notification-service-foundation.md), the schema —
`GET /openapi.json`.

## Running

The `platform-auth-sdk` and `control-plane-client` dependencies are installed by
path from the directories of the umbrella layout (`../../sdk/platform-auth-sdk`,
`../control-plane`; TAI-ADR-0064).

```bash
uv sync
NS_DATABASE_URL=postgresql+psycopg://… uv run alembic upgrade head
uv run notification-service          # API and worker, port NS_PORT (8000)
```

Settings are `NS_*` environment variables (`src/notification_service/config.py`):

| Variable | Purpose |
|---|---|
| `NS_DATABASE_URL` | the service's PostgreSQL |
| `NS_IAM_URL`, `NS_IAM_ISSUER`, `NS_IAM_JWKS_URL` | token verification; JWKS defaults to IAM's `…/.well-known/jwks.json` |
| `NS_AUDIENCE` | own audience, `notification-service` by default |
| `NS_CONTROL_PLANE_URL`, `NS_SERVICE_CLIENT_ID`, `NS_SERVICE_CLIENT_SECRET` | reading the core directory with a service account |
| `NS_WORKER_ENABLED`, `NS_WORKER_POLL_SECONDS`, `NS_DELIVERY_MAX_ATTEMPTS`, `NS_DELIVERY_BACKOFF_SECONDS` | delivery worker |
| `NS_EMAIL_MODE` | `smtp`, `log` (staging: log only) or `disabled` |
| `NS_EMAIL_FROM`, `NS_SMTP_HOST`, `NS_SMTP_PORT`, `NS_SMTP_STARTTLS`, `NS_SMTP_USERNAME`, `NS_SMTP_PASSWORD` | SMTP |
| `NS_INBOX_POLL_SECONDS`, `NS_INBOX_KEEPALIVE_SECONDS` | inbox SSE stream |
| `NS_EVENTS_ENABLED`, `NS_EVENTS_START`, `NS_EVENTS_WORKSPACE_ID`, `NS_EVENTS_POLL_SECONDS` | core event consumer: on/off, where to start on the first run (`latest` by default, `earliest`), workspace subtree (empty — the whole tenant), polling period; the notification rules are re-read at the same period |
| `NS_TELEGRAM_BOT_TOKEN`, `NS_TELEGRAM_WEBHOOK_SECRET` | Telegram bot and webhook secret (`secret_token` in `setWebhook`); no token — no channel |
| `NS_TELEGRAM_API_URL`, `NS_TELEGRAM_BOT_USERNAME`, `NS_TELEGRAM_TIMEOUT_SECONDS` | Bot API; the bot name is used in commands and group link URLs |
| `NS_CHANNEL_GROUP_CODE_TTL_SECONDS` | lifetime of a group link code (600 s) |
| `NS_IAM_CHANNEL_AUDIENCE`, `NS_IAM_CHANNEL_SCOPE` | the service as a channel adapter in IAM: `iam` / `iam:channel-links` |
| `NS_HARNESS_LAUNCHER_URL`, `NS_HARNESS_AUDIENCE`, `NS_HARNESS_SCOPE` | the Telegram channel as an entry into the person's assistant conversation (TAI-ADR-0051 §7): the launcher of personal harnesses (e.g. `http://harness-launcher:8080/harness`), the service account needs `human-harness` / `harness:inbound`; empty URL — notifications only |

Free text a linked person writes to the bot in the private chat goes to the launcher of personal harnesses (`POST …/_launcher/internal/principals/{iamPrincipalId}/inbound`, `{channel, messageId, text}`) and becomes a message of their single assistant conversation; the answer comes back later through `notify.send`. A press of a harness confirmation button (`data.kind = "harness_approval"`, `{requestId, decision}`) goes the same way as `{channel, messageId, approval: {id, decision}}` — only in the person's own private chat, recorded once per callback. An unlinked account's text goes no further than the service; an unavailable launcher or a person without a harness is answered in the chat.

Without IAM settings the service answers `503` on every API route; without Control
Plane settings it answers `503` on sending to people and roles. The Telegram
webhook (`POST /channels/telegram/webhook`, public, checks the
`X-Telegram-Bot-Api-Secret-Token` header) answers `404` without a bot token and
`401` without a secret; the event consumer runs when both the Control Plane and IAM
are configured and at least one rule is enabled. Secrets live only in the environment or in `secrets/`, never in the
repository.

## Development

```bash
make install                                     # uv sync --locked
make lint                                        # ruff check and ruff format --check
NS_TEST_DATABASE_URL=postgresql+psycopg://… make test
make check                                       # lint, one alembic head, tests
```

The executor's checks (`.agents/runner.yaml`) call these targets, and CI is to
call the same ones (the umbrella repository has no job for it yet); the
conventions for executors are in [AGENTS.md](AGENTS.md) (in Russian).

The tests recreate the schema in `NS_TEST_DATABASE_URL` through the migration
chain; SMTP is a local `aiosmtpd` server, the SSE stream is a real uvicorn server,
the Bot API, IAM and core decisions are fakes built on their contracts
(`tests/telegram_fakes.py`); requests to IAM are checked against the models of the
neighbouring `../iam-service` when it is present. The core directory in the service
tests is a fake of the `Directory` protocol; the adapter to the core is pinned by a
contract test against the Control Plane API models.

## License

Apache License 2.0 — [LICENSE](LICENSE), [NOTICE](NOTICE). Third-party dependencies
and their licences are listed in [THIRD_PARTY.md](THIRD_PARTY.md) and `sbom.json`
(CycloneDX 1.5); both files are generated by `tools/generate_third_party.py` of the
`taimen` umbrella repository in the service's runtime environment (the service
itself and the platform packages are first-party and excluded):

```bash
uv sync --no-dev
uv run --no-sync python ../tools/generate_third_party.py --component notification-service \
  --exclude-prefix notification-service --exclude-prefix control-plane --exclude-prefix platform-
```

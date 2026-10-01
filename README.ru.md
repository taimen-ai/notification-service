*Русская версия. English: [README.md](README.md)*

# notification-service

Сервис уведомлений платформы Taimen: принимает уведомления от любого сервиса или
скилла, выбирает каналы по настройкам получателя (веб-инбокс, Telegram, email),
ведёт журнал доставки и принимает решения людей из каналов (кнопки Telegram).

Дизайн — `specs/notifications/` суперпроекта, решения — TAI-ADR-0048 и TAI-ADR-0049;
решения самого сервиса — [docs/adr/](docs/adr/). Каркас сервиса — задача N006
(TASK-000415): отправка с дедупликацией, адресация principal / роль / группа,
настройки и обязательные правила, воркер доставки с повторами, каналы `web` и
`email`, веб-инбокс с потоком SSE. Канал Telegram (N007,
[ADR-0003](docs/adr/0003-telegram-channel-and-decisions.md)): привязка личного
чата кодом из IAM, привязка групп к workspace и роли, сообщения с кнопками
решения — нажатие становится решением человека в ядре. Потребитель событий ядра
(N008, [ADR-0002](docs/adr/0002-control-plane-event-consumer.md)) исполняет
правила уведомлений — объекты каталога `NotificationRule`, применённые к сервису
([ADR-0005](docs/adr/0005-notification-rules-as-data.md), C007 фичи
`declarative-cycle`): какое событие становится уведомлением, кому, каким текстом,
с какими кнопками и что их закрывает. Прежнее поведение (запрос решения с
кнопками, закрытие по исходу, проваленная проверка — владельцу задачи) — три
правила пакета `notify`; без применённых правил сервис событий не читает.
Правила — `/api/v1/notification-rules` (scope `notifications:admin`). Пакеты шлют
уведомления скиллом `notify.send@1`
([ADR-0004](docs/adr/0004-notify-send-skill.md), контракт —
[docs/skills/notify.send@1.json](docs/skills/notify.send@1.json)). Реестр ADR —
[docs/adr/README.md](docs/adr/README.md).

## Устройство

Один процесс: FastAPI (`/api/v1`), фоновый воркер доставки и потребитель
событий ядра (SDK `control_plane_client.events`), своя PostgreSQL,
миграции Alembic. Токены проверяет `platform-auth-sdk` (audience
`notification-service`, scopes `notifications:send|read|admin`); principal'ов,
роли и привязки к IAM сервис читает у Control Plane своим service account'ом.
Подробности и контракт API — [ADR-0001](docs/adr/0001-notification-service-foundation.md),
схема — `GET /openapi.json`.

## Запуск

Зависимости `platform-auth-sdk` и `control-plane-client` подключаются путём из
соседних каталогов суперпроекта (`../platform-auth-sdk`, `../control-plane`).

```bash
uv sync
NS_DATABASE_URL=postgresql+psycopg://… uv run alembic upgrade head
uv run notification-service          # API и воркер, порт NS_PORT (8000)
```

Настройки — переменные окружения `NS_*` (`src/notification_service/config.py`):

| Переменная | Назначение |
|---|---|
| `NS_DATABASE_URL` | PostgreSQL сервиса |
| `NS_IAM_URL`, `NS_IAM_ISSUER`, `NS_IAM_JWKS_URL` | проверка токенов; JWKS по умолчанию — `…/.well-known/jwks.json` IAM |
| `NS_AUDIENCE` | собственный audience, по умолчанию `notification-service` |
| `NS_CONTROL_PLANE_URL`, `NS_SERVICE_CLIENT_ID`, `NS_SERVICE_CLIENT_SECRET` | чтение каталога ядра service account'ом |
| `NS_WORKER_ENABLED`, `NS_WORKER_POLL_SECONDS`, `NS_DELIVERY_MAX_ATTEMPTS`, `NS_DELIVERY_BACKOFF_SECONDS` | воркер доставки |
| `NS_EMAIL_MODE` | `smtp`, `log` (staging: только запись в лог) или `disabled` |
| `NS_EMAIL_FROM`, `NS_SMTP_HOST`, `NS_SMTP_PORT`, `NS_SMTP_STARTTLS`, `NS_SMTP_USERNAME`, `NS_SMTP_PASSWORD` | SMTP |
| `NS_INBOX_POLL_SECONDS`, `NS_INBOX_KEEPALIVE_SECONDS` | поток SSE инбокса |
| `NS_EVENTS_ENABLED`, `NS_EVENTS_START`, `NS_EVENTS_WORKSPACE_ID`, `NS_EVENTS_POLL_SECONDS` | потребитель событий ядра: вкл/выкл, откуда начать при первом запуске (`latest` по умолчанию, `earliest`), поддерево workspace (пусто — весь tenant), период опроса; с тем же периодом перечитываются правила уведомлений |
| `NS_TELEGRAM_BOT_TOKEN`, `NS_TELEGRAM_WEBHOOK_SECRET` | бот Telegram и секрет вебхука (`secret_token` в `setWebhook`); без токена канала нет |
| `NS_TELEGRAM_API_URL`, `NS_TELEGRAM_BOT_USERNAME`, `NS_TELEGRAM_TIMEOUT_SECONDS` | Bot API; имя бота — для команд и ссылок привязки групп |
| `NS_CHANNEL_GROUP_CODE_TTL_SECONDS` | срок кода привязки группы (600 с) |
| `NS_IAM_CHANNEL_AUDIENCE`, `NS_IAM_CHANNEL_SCOPE` | сервис как адаптер канала в IAM: `iam` / `iam:channel-links` |
| `NS_HARNESS_LAUNCHER_URL`, `NS_HARNESS_AUDIENCE`, `NS_HARNESS_SCOPE` | канал Telegram как вход в беседу ассистента человека (TAI-ADR-0051 п.7): launcher персональных харнессов (например `http://harness-launcher:8080/harness`), service account нужен `human-harness` / `harness:inbound`; пустой URL — только уведомления |

Свободный текст привязанного человека в личном чате с ботом уходит launcher'у персональных харнессов (`POST …/_launcher/internal/principals/{iamPrincipalId}/inbound`, `{channel, messageId, text}`) и становится репликой его единственной беседы с ассистентом; ответ приходит позже через `notify.send`. Нажатие кнопки подтверждения харнесса (`data.kind = "harness_approval"`, `{requestId, decision}`) уходит тем же путём как `{channel, messageId, approval: {id, decision}}` — только в личном чате самого человека, одно на callback. Текст непривязанного аккаунта дальше сервиса не уходит; недоступный launcher или отсутствие рабочего места объясняется в чате.

Без настроек IAM сервис отвечает `503` на все маршруты API, без настроек Control
Plane — `503` на отправку людям и ролям; вебхук Telegram
(`POST /channels/telegram/webhook`, публичный, проверяет заголовок
`X-Telegram-Bot-Api-Secret-Token`) без токена бота отвечает `404`, без секрета —
`401`; потребитель событий работает, когда
настроены и Control Plane, и IAM, и есть хотя бы одно включённое правило. Секреты — только в окружении или
`secrets/`, не в репозитории.

## Разработка

```bash
make install                                     # uv sync --locked
make lint                                        # ruff check и ruff format --check
NS_TEST_DATABASE_URL=postgresql+psycopg://… make test
make check                                       # lint, одна голова alembic, тесты
```

Эти цели зовут проверки исполнителя (`.agents/runner.yaml`), и их же должен звать
CI (job'а в umbrella-репозитории пока нет); соглашения для исполнителей —
[AGENTS.md](AGENTS.md).

Тесты пересоздают схему в `NS_TEST_DATABASE_URL` цепочкой миграций, SMTP —
локальный сервер `aiosmtpd`, поток SSE — настоящий сервер uvicorn, Bot API,
IAM и решения ядра — фейки по их контрактам (`tests/telegram_fakes.py`); запросы
к IAM сверяются с моделями соседнего `../iam-service`, если он рядом. Каталог ядра
в тестах сервиса — фейк по протоколу `Directory`; адаптер к ядру закреплён
contract-тестом на моделях API Control Plane.

## Лицензия

Apache License 2.0 — [LICENSE](LICENSE), [NOTICE](NOTICE). Сторонние зависимости
и их лицензии — [THIRD_PARTY.md](THIRD_PARTY.md) и `sbom.json` (CycloneDX 1.5);
оба файла генерирует `tools/generate_third_party.py` umbrella-репозитория `taimen`
в runtime-окружении сервиса (сам сервис и пакеты платформы — свои, их
исключают):

```bash
uv sync --no-dev
uv run --no-sync python ../tools/generate_third_party.py --component notification-service \
  --exclude-prefix notification-service --exclude-prefix control-plane --exclude-prefix platform-
```

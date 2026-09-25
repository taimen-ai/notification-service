# notification-service

Сервис уведомлений платформы Taimen: принимает уведомления от любого сервиса или
скилла, выбирает каналы по настройкам получателя (веб-инбокс, Telegram, email),
ведёт журнал доставки и принимает решения людей из каналов (кнопки Telegram).

Дизайн — `specs/notifications/` суперпроекта, решения — TAI-ADR-0048 и TAI-ADR-0049;
решения самого сервиса — [docs/adr/](docs/adr/). Каркас сервиса — задача N006
(TASK-000415): отправка с дедупликацией, адресация principal / роль / группа,
настройки и обязательные правила, воркер доставки с повторами, каналы `web` и
`email`, веб-инбокс с потоком SSE. Telegram — N007. Потребитель событий ядра
(N008, [ADR-0002](docs/adr/0002-control-plane-event-consumer.md)) превращает
`approval.requested` в уведомление с действиями решения (их закрывает
`approval.approved|rejected|cancelled`), а `task.verification_failed` — в
уведомление владельцу задачи.

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
| `NS_EVENTS_ENABLED`, `NS_EVENTS_START`, `NS_EVENTS_WORKSPACE_ID`, `NS_EVENTS_POLL_SECONDS` | потребитель событий ядра: вкл/выкл, откуда начать при первом запуске (`latest` по умолчанию, `earliest`), поддерево workspace (пусто — весь tenant), период опроса |
| `NS_TASK_URL_TEMPLATE` | ссылка на задачу в уведомлениях из событий, `{taskPublicId}` / `{taskId}`; пусто — без ссылки |

Без настроек IAM сервис отвечает `503` на все маршруты API, без настроек Control
Plane — `503` на отправку людям и ролям; потребитель событий работает, когда
настроены и Control Plane, и IAM. Секреты — только в окружении или
`secrets/`, не в репозитории.

## Разработка

```bash
uv sync
NS_TEST_DATABASE_URL=postgresql+psycopg://… uv run pytest
uv run ruff check . && uv run ruff format --check .
```

Тесты пересоздают схему в `NS_TEST_DATABASE_URL` цепочкой миграций, SMTP —
локальный сервер `aiosmtpd`, поток SSE — настоящий сервер uvicorn. Каталог ядра
в тестах сервиса — фейк по протоколу `Directory`; адаптер к ядру закреплён
contract-тестом на моделях API Control Plane.

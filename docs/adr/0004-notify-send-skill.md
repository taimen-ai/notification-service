# ADR-0004. Скилл `notify.send@1`: точка приёма и контракт

Статус: предложено (2026-09-25), задача TASK-000437 (часть N009, TASK-000418)

Основание: TAI-ADR-0048 п. 6 (пакеты шлют уведомления скиллом `notify.send@1`),
TAI-ADR-0045 (skill-sdk: ошибка `{"error": {code, retryable}}`), CP-ADR-0056
(контракт скилла, исполнитель протокола `http`); код соседей —
`control_plane_agent/skills.py` (`HttpProtocol`) и
`control_plane/domain/skill_contract.py` ядра.

## Контекст

Пакет шлёт уведомление не прямым вызовом API, а скиллом: вызов проходит через
ядро (права, аудит, повторы), исполняет его демон Control Plane. Исполнитель
протокола `http` делает `POST <endpoint>` с конвертом
`{invocationId, idempotencyKey, inputs}` и Bearer-токеном audience из
`implementation.auth.audience`; тело 2xx — outputs; ошибка с телом
`{"error": {code, retryable}}` берётся как есть, без такого тела 4xx —
неповторяемый отказ, 5xx — повторяемый. `POST /api/v1/notifications` ждёт
уведомление напрямую, ключ — в заголовке, и отвечает конвертом ошибки ядра
без `retryable`: исполнитель прочёл бы его 503 как неповторяемый отказ.

## Решение

### Маршрут

`POST /api/v1/skills/notify.send`, scope `notifications:send` — как у прямой
отправки. Тело — конверт исполнителя:

- `inputs` — уведомление **без действий**: `recipient` (principal, роль с
  `workspaceId`, группа), `type`, `title`, `body`, `links`. Это та же модель, что
  у `POST /notifications` (`NotificationContent`, от неё наследуется
  `NotificationCreate` с `actions`), и те же проверки. Поле `actions`
  отвергается: действия решения ставит только ядро через потребителя событий
  (ADR-0002, ADR-0003), пакет не может предложить человеку кнопку, нажатие
  которой станет его решением.
- `idempotencyKey` — ключ дедупликации уведомления `skill:<idempotencyKey>`
  (в том же пространстве отправителя, что и `Idempotency-Key` прямой отправки,
  но префикс не даёт им совпасть). Ключ один на все попытки вызова, поэтому
  повтор попытки возвращает то же уведомление; тот же ключ с другими `inputs` —
  `409 idempotency_conflict`. Ключ длиннее 194 символов не влезает в
  `dedup_key` (200) с префиксом и заменяется на `skill:sha256:<hex>` от него.
  Ключ обязателен: контракт объявляет `idempotency: required`, и ядро не
  создаёт вызов без ключа; конверт без ключа — `422 invalid_inputs`.
- `invocationId` принимается и не хранится: связь с вызовом — через ключ.

Отправитель уведомления (`senderId`) — principal токена, то есть сервисный
аккаунт исполнителя скиллов; кто вызвал скилл, видно в вызове в ядре.

Ответ — outputs: `201` новое уведомление, `200` повтор;
`{"notificationId", "deliveries": [{"channel", "status"}]}` — журнал
доставки на момент приёма (`status`: `pending|sending|delivered|failed`).

Ошибки — в форме skill-sdk `{"error": {code, message, retryable, details}}`,
все, включая аутентификацию (она проверяется внутри маршрута, а не
зависимостью, чтобы ответ шёл в этой форме): `retryable` — `true` для 5xx
(`dependency_unavailable` каталога ядра, `verification_unavailable`), `false`
для 4xx (`invalid_json`, `invalid_inputs` с `details.errors[{loc, msg}]`,
`unknown_recipient`, `idempotency_conflict`, `invalid_token`,
`insufficient_scope`). Коды отказов SDK — только стабильный код, как у
остального API.

### Контракт

Источник — `notification_service.skill.publication(endpoint, audience)`;
выгрузка с подстановкой `${NOTIFICATION_SERVICE_URL}` вместо адреса —
[`docs/skills/notify.send@1.json`](../skills/notify.send@1.json)
(`uv run python -m notification_service.skill`, тест сверяет файл с кодом).
Пакет `notify` суперпроекта (владелец) строит из него YAML скилла и сам решает
про `requiredPermissions` и права исполнителя.

| Поле | Значение |
|---|---|
| `name`, `version` | `notify.send`, `1` |
| `sideEffects` | `external_write` — человек получает сообщение |
| `riskLevel` | `low` — только сообщение, без действий решения |
| `contract.inputs`, `contract.outputs` | JSON Schema 2020-12 из моделей маршрута, `additionalProperties: false` |
| `timeoutSeconds` | `30` — приём с запросом к каталогу ядра, доставка асинхронна |
| `retryPolicy` | `maxAttempts: 3`, `backoffSeconds: 10` — повтор безопасен благодаря ключу |
| `idempotency` | `required` |
| `implementation` | `protocol: http`, `endpoint: <адрес сервиса>/api/v1/skills/notify.send`, `auth.audience: notification-service` |

Схема `inputs` не выражает одно правило модели — роль адресуется только с
`workspaceId`; его нарушение — `422 invalid_inputs` от сервиса.

## Последствия

- Скилл вызываем, когда владелец опубликует версию в ядре и выдаст сервисному
  аккаунту исполнителя токен audience `notification-service` со scope
  `notifications:send`, а исполнителю — origin сервиса в allow-list.
- Контракт закреплён тестами на коде ядра: вызовы идут через `HttpProtocol`
  исполнителя в приложение, публикация проходит `normalize_contract`,
  `validate_policy_columns` и `require_safe_retries`.
- Изменение `inputs`/`outputs` — новая версия скилла (`notify.send@2`) и новый
  маршрут: опубликованная версия в ядре иммутабельна.

## Conformance

```conformance
- grep: {path: src/notification_service/skill.py, pattern: 'DEDUP_PREFIX = "skill:"'}
- grep: {path: src/notification_service/skill.py, pattern: '"idempotency": "required"'}
- grep: {path: src/notification_service/schemas.py, pattern: 'class NotificationCreate\(NotificationContent\)'}
```

# Асинхронный сервис процессинга платежей

Тестовое задание: HTTP API принимает платёж, сохраняет его вместе с событием в PostgreSQL,
а один consumer обрабатывает платёж через эмулятор шлюза и отправляет результат на webhook.

**Стек:** Python 3.12+, FastAPI, Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL,
RabbitMQ + FastStream, Alembic, Docker Compose. Версии Python-зависимостей зафиксированы в `uv.lock`.

## Быстрый запуск

Нужны Docker и Docker Compose v2. Команды выполняются из корня репозитория.

```bash
cp .env.example .env
docker compose --profile demo up --build -d
docker compose ps
```

Миграции запускаются автоматически отдельным одноразовым контейнером `migrate`.
API и consumer стартуют после миграций и готовности RabbitMQ.
Профиль `demo` добавляет локальный приёмник webhook; без него достаточно `docker compose up --build -d`.

| Сервис | Адрес с хоста |
| --- | --- |
| API | `http://localhost:8000` |
| PostgreSQL | `localhost:55432` |
| RabbitMQ AMQP | `localhost:5672` |
| RabbitMQ Management | `http://localhost:15672` |
| Демонстрационный webhook | `http://localhost:8081` |

В `.env.example` находятся демонстрационные ключ и пароли. Перед внешним развёртыванием замените их.
Порты опубликованы только на `127.0.0.1`. При конфликте измените `API_PORT`, `POSTGRES_PORT`,
`RABBITMQ_PORT` или `RABBITMQ_MANAGEMENT_PORT` в `.env`.
Внутри Docker используются имена сервисов и стандартные порты; `DATABASE_URL` и `RABBITMQ_URL`
из `.env` нужны для запуска Python-команд на хосте.

```bash
# Проверка готовности API и БД
curl -f http://localhost:8000/health -H 'X-API-Key: dev-api-key-change-me'

# Логи обработки и уведомлений
docker compose logs -f api consumer webhook

# Остановка с сохранением данных
docker compose --profile demo down
```

## Создание и получение платежа

`X-API-Key` обязателен на всех маршрутах API, включая `/health`, `/docs` и `/openapi.json`.
Неверный или отсутствующий ключ даёт `401`. Демонстрационный приёмник webhook - отдельное приложение.

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key-change-me' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Заказ 42",
    "metadata": {"order_id": 42},
    "webhook_url": "http://webhook:8081/webhook"
  }'
```

Ответ: `202 Accepted`.

```json
{
  "payment_id": "7d5d7949-91bc-4b30-b763-b839ef759c41",
  "status": "pending",
  "created_at": "2026-11-16T10:00:00Z"
}
```

Подставьте `payment_id` из полученного ответа:

```bash
curl http://localhost:8000/api/v1/payments/7d5d7949-91bc-4b30-b763-b839ef759c41 \
  -H 'X-API-Key: dev-api-key-change-me'
```

Детальная информация включает сумму, валюту, описание, метаданные, webhook URL,
даты создания и обработки, `delivery_attempts`, `webhook_delivered_at`, `delivery_failed_at`.
Суммы передаются строками, чтобы сохранить точность десятичных чисел; принимаются также JSON-числа.
Валюта: `RUB`, `USD` или `EUR`; сумма положительная, до 16 целых и двух дробных цифр.
Суммы с большей точностью отклоняются, а не округляются. Некорректный запрос даёт `422`,
несуществующий UUID платежа - `404`.

Описание и метаданные необязательны; по умолчанию это пустая строка и объект.
Webhook URL принимает только HTTP/HTTPS без логина и пароля. Внешний адрес должен быть доступен
из контейнера consumer. `localhost` в URL указывает на сам consumer; для профиля `demo`
используется `http://webhook:8081/webhook`.

### Идемпотентность HTTP

`Idempotency-Key` обязателен, от 1 до 255 символов, не может состоять из пробелов.

- Тот же ключ и эквивалентное тело возвращают тот же `payment_id`, `created_at` и текущий статус
  с кодом `202`. Повтор после обработки может вернуть `succeeded` или `failed`.
- Тот же ключ с другим телом возвращает `409 Conflict`.
- Порядок ключей JSON и запись суммы `100`, `100.0`, `100.00` не влияют на сравнение.
- Уникальный индекс и `INSERT ... ON CONFLICT` защищают от конкурентных запросов.
  Событие outbox создаётся только для нового платежа.

Ключ глобален в рамках сервиса: по условию используется один статический API ключ.

## Обработка и webhook

Эмулятор ждёт случайные 2-5 секунд и завершает платёж с вероятностью успеха 90%.
Остальные 10% - бизнес-отказ: статус `failed`, который тоже отправляется клиенту.
HTTP-сбой webhook не меняет уже полученный результат платежа.

Consumer отправляет `POST` на указанный URL, например:

```json
{
  "event_id": "7d5d7949-91bc-4b30-b763-b839ef759c41",
  "event_type": "payment.processed",
  "payment_id": "7d5d7949-91bc-4b30-b763-b839ef759c41",
  "status": "succeeded",
  "amount": "1250.50",
  "currency": "RUB",
  "description": "Заказ 42",
  "metadata": {"order_id": 42},
  "created_at": "2026-11-16T10:00:00+00:00",
  "processed_at": "2026-11-16T10:00:03+00:00"
}
```

Заголовки `Idempotency-Key` и `X-Event-ID` содержат UUID платежа и стабильны между попытками.
Получатель должен устранять дубли по этому идентификатору. Успешна любая доставка с ответом `2xx`.
Редиректы не выполняются. HTTP-ошибки и таймауты приводят к повтору.

Всего **3 попытки**, включая первую; задержки перед второй и третьей - **1 и 2 секунды**.
Это минимальные задержки TTL, к которым добавляется время публикации и ожидания consumer.
После третьей ошибки событие попадает в `payments.dead`, а у платежа заполняется `delivery_failed_at`.

Проверка этой ветки: создайте другой платёж с новым `Idempotency-Key` и
`"webhook_url": "http://webhook:8081/fail"`. Приёмник отвечает `503`.
После трёх попыток откройте RabbitMQ Management (логин `payments`, пароль `payments-local`)
и проверьте очередь `payments.dead`. Логи consumer показывают номер каждой неудачной попытки.

## Архитектура и гарантии

```text
POST /payments
     │ одна транзакция PostgreSQL
     ├── payments (pending)
     └── outbox
            │ publisher в процессе API
            ▼
     payments.events ──► payments.new ──► один consumer
                                            ├── эмулятор шлюза
                                            ├── коммит результата платежа
                                            └── HTTP webhook
                                                  │ ошибка
                                                  ▼
                                         outbox (retry или DLQ)
                                                  │ publisher
                         ┌────────────────────────┴───────────────┐
                         ▼                                        ▼
                payments.retry.1 / .2                         payments.dlx
                  TTL 1 / 2 секунды                                │
                         │                                        ▼
                         └──► payments.events              payments.dead
```

- **Transactional outbox:** платеж и исходное событие создаются одним коммитом. Отдельный
  фоновый цикл API выбирает неопубликованные записи через `FOR UPDATE SKIP LOCKED`.
  `published_at` заполняется после publisher confirm. Используются durable-очереди и обменники,
  persistent-сообщения, `mandatory=True` и ошибка при отсутствии маршрута.
- **Сбой брокера:** событие остаётся в PostgreSQL и публикуется повторно с задержкой 2, 4, 8,
  16, 32, 64 секунды, далее 64 секунды. Количество таких попыток не ограничено: недоступность
  RabbitMQ не должна превращать принятый платёж в потерянное событие.
  API может принимать платежи во время последующего отказа RabbitMQ, если БД доступна.
- **Повторы и DLQ тоже через outbox:** номер неудачной попытки и следующее событие сохраняются
  одной транзакцией перед ACK. Разрыв соединения с RabbitMQ в этот момент не теряет повтор.
  Паузы реализованы очередями с TTL и dead-letter routing, без `sleep` в обработке webhook.
- **Один consumer:** один обработчик FastStream выполняет всю цепочку. Prefetch равен 1;
  задержанный retry не блокирует обработку других платежей. Отдельных consumer для retry нет.
- **Дубли сообщений:** блокировка строки платежа и сохранённый номер попытки исключают
  повторную обработку уже завершённой попытки. Статус платежа фиксируется до отправки webhook,
  поэтому retry уведомления не запускает шлюз заново.
- **ACK:** исходное сообщение подтверждается после доставки либо надёжной записи retry/DLQ.
  Если БД недоступна и записать попытку нельзя, используется NACK с возвратом в очередь;
  такая авария не расходует недостоверный счётчик попыток. После восстановления БД обработка продолжится.
  Невалидные сообщения и события для отсутствующего платежа отклоняются в DLQ.

Гарантия приложения - **at least once**. Если процесс упал после публикации, но до коммита outbox,
сообщение будет опубликовано повторно. Если webhook принят, а подтверждение потеряно, клиент
может получить дубликат. Именно поэтому у webhook стабильный идентификатор.
Для реального платёжного провайдера нужно также передавать UUID платежа как его idempotency key:
эмулятор не имеет внешнего денежного эффекта и не моделирует распределённую транзакцию.

Compose рассчитан на один узел PostgreSQL и RabbitMQ. Долговечность очередей не заменяет
резервное копирование или репликацию; classic dead-letter routing не даёт атомарности с PostgreSQL.
Публикующий цикл запускается вместе с API, поэтому API должен оставаться запущенным и для доставки
отложенных retry. API ключ задаёт доверенную границу для webhook URL; в публичном сервисе
потребуется политика исходящих сетевых запросов.

## Локальная разработка и тесты

Для работы на хосте нужен [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
uv sync --frozen
docker compose up -d --wait postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn app.api:app --reload
# В другом терминале
uv run python -m app.worker
```

`uv run pytest -q` без переменных `TEST_*` запускает тесты валидации; интеграционные тесты
явно помечаются как пропущенные. Полный набор:

```bash
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments-local@localhost:55432/payments \
TEST_RABBITMQ_URL=amqp://payments:payments-local@localhost:5672/ \
uv run pytest -q

uv run ruff check .
uv run ruff format --check .
uv run alembic check
```

Если порт Management изменён, задайте `TEST_RABBITMQ_MANAGEMENT_URL`, например
`http://localhost:15673`. Для тестов нужен пользователь RabbitMQ с правами управления vhost.
Каждый тест создаёт и удаляет собственную схему PostgreSQL и при необходимости отдельный vhost RabbitMQ;
таблицы и очереди работающего приложения не очищаются. Пользователю БД нужно право `CREATE` на схемы.

Проверяются:

- валидация сумм, URL и конфигурации;
- аутентификация, создание, чтение, ошибки API;
- 12 одновременных запросов с одним ключом, конфликт тела и атомарный rollback;
- повтор событий, бизнес-отказ шлюза и рестарт перед webhook;
- HTTP-ошибки, таймауты, успех на третьей попытке и конечный DLQ;
- восстановление outbox и параллельные publisher;
- настоящая RabbitMQ-доставка, persistent-флаг, отсутствие binding, TTL retry и невалидный JSON.

GitHub Actions запускает эти проверки с PostgreSQL и RabbitMQ, проверяет миграции
`upgrade → downgrade → upgrade` и собирает Docker-образ.

### Настройки приложения

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `API_KEY` | `dev-api-key-change-me` | Статический API ключ |
| `DATABASE_URL` | см. `.env.example` | DSN PostgreSQL с драйвером `asyncpg` |
| `RABBITMQ_URL` | см. `.env.example` | AMQP DSN |
| `OUTBOX_POLL_INTERVAL` | `0.5` | Интервал публикации outbox, секунды |
| `WEBHOOK_TIMEOUT` | `5` | Таймаут HTTP-операций webhook, секунды |
| `GATEWAY_DELAY_MIN` / `GATEWAY_DELAY_MAX` | `2` / `5` | Задержка эмулятора, секунды |
| `GATEWAY_SUCCESS_RATE` | `0.9` | Вероятность успеха эмулятора |

Настройки читаются из окружения и `.env`. После изменения `.env` пересоздайте контейнеры
командой `docker compose up -d`.

Защищённую схему OpenAPI можно получить так:

```bash
curl http://localhost:8000/openapi.json -H 'X-API-Key: dev-api-key-change-me'
```

Для `/docs` и загрузки схемы браузером также нужен заголовок `X-API-Key`, например через
расширение для заголовков. Ключи в URL не передаются.

## Структура

```text
app/
  api.py          HTTP API и жизненный цикл publisher
  payments.py     создание платежа и идемпотентность
  models.py       таблицы payments и outbox
  messaging.py    очереди, обменники, создание событий
  outbox.py       надёжная публикация
  processor.py    шлюз, webhook и сохранение retry/DLQ
  worker.py       единственный обработчик FastStream
  demo_webhook.py локальный приёмник для проверки
migrations/       Alembic
tests/            тесты, включая PostgreSQL и RabbitMQ
```

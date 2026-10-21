# subscriptions-api и БД подписок

Статус: черновик для ревью, 2026-10-19. Основа — [DESIGN.md](DESIGN.md).
Формат тела вебхука и устройство worker описываются отдельно.

## 1. Объём первой версии

- **Типы событий:** `transactions` и `actions`. `account_state_change` и
  `jettons_change` — позже, после того как будет зафиксирована их семантика
  при догоне (DESIGN.md §5).
- **Один набор адресов на подписку**, общий для всех её типов событий.
  Предикаты у каждого типа свои. Если транзакции и actions нужны по разным
  наборам адресов, это две подписки.
- **Начальная точка обязательна** при создании подписки, умолчания нет.
- Подписку можно создать с пустым набором адресов и наполнить его позже. Пока
  набор пуст, доставлять нечего.

## 2. Общие правила

- Базовый путь — `/api/webhooks/v1`, тела запросов и ответов в JSON.
- **Владелец** — идентификатор аккаунта из заголовка `X-Owner-Id`, который
  выставляет HAProxy. Это аккаунт, а не API-ключ, поэтому подписки не теряются
  при ротации ключа. `X-Limiting-Key` сервис не читает. HAProxy удаляет
  `X-Owner-Id` из входящего запроса клиента до того, как выставить свой. Без
  заголовка в многопользовательском режиме — `401`. В режиме без шлюза
  используется владелец по умолчанию.
- **Лимиты** приходят заголовками от HAProxy и проверяются только в момент
  запроса; хранимых лимитов владельца нет:
  - `X-Max-Webhook-Subscriptions` — максимум подписок у владельца;
  - `X-Max-Webhook-Addresses` — максимум адресов у владельца по всем подпискам.

  Новые имена выбраны, чтобы не путать с `X-Max-Subscribed-Addr` из streaming,
  где лимит относится к соединению.
- **Адреса** принимаются в любом формате. Допускаются только стандартные адреса,
  как в streaming v2. В ответах адреса отдаются в raw-форме `0:HEX`.
- **Пагинация** — по ключу: `limit` и `after`, ответ содержит `next`.
- **Ошибки:**

  ```json
  {"error": {"code": "start_too_old", "message": "…", "details": {}}}
  ```

  | Код | HTTP | Когда |
  |---|---|---|
  | `invalid_argument` | 400 | неверное поле, адрес, тип action, URL |
  | `unauthorized` | 401 | нет владельца |
  | `not_found` | 404 | подписки нет или она чужая |
  | `conflict` | 409 | недопустимый переход статуса, повтор `Idempotency-Key` с другим телом |
  | `start_too_old` | 409 | точка начала глубже доступной; в `details.earliest` самая ранняя позиция |
  | `limit_exceeded` | 422 | превышен лимит из заголовков или размер запроса |

- **Идемпотентность создания:** `POST /subscriptions` принимает заголовок
  `Idempotency-Key`. Повтор с тем же ключом и тем же телом возвращает ту же
  подписку, с другим телом — `409`. Ключ хранится сутки. Остальные операции
  идемпотентны сами: адреса — операции над множеством, `pause`, `resume` и
  `seek` с теми же параметрами дают тот же результат.

## 3. Ресурс подписки

```json
{
  "id": "0192f0c4-7d3e-7c4b-9a51-6f2d8e1b4a10",
  "url": "https://example.com/ton-webhook",
  "description": "депозиты",
  "event_types": ["transactions", "actions"],
  "filters": {
    "transactions": {
      "in_msg_opcodes": ["0x7362d09c"]
    },
    "actions": {
      "action_types": ["jetton_transfer", "ton_transfer"],
      "supported_action_types": ["v5"]
    }
  },
  "options": {
    "include_address_book": false,
    "include_metadata": false
  },
  "status": "active",
  "status_reason": null,
  "status_changed_at": "2026-10-19T10:00:00Z",
  "suspended": false,
  "delivery": {
    "transactions": {
      "cursor": "AQEDDccH",
      "position": {"mc_seqno": 51234567, "utime": 1760868000},
      "head": {"mc_seqno": 51234570, "utime": 1760868001},
      "failing_since": null,
      "last_success_at": "2026-10-19T10:00:00Z",
      "last_error": null
    },
    "actions": {
      "cursor": "AQIDDccE",
      "position": {"mc_seqno": 51234564, "utime": 1760867999},
      "head": {"mc_seqno": 51234566, "utime": 1760868000},
      "failing_since": null,
      "last_success_at": "2026-10-19T09:59:59Z",
      "last_error": null
    }
  },
  "address_count": 1532,
  "epoch": 1,
  "created_at": "2026-10-19T09:00:00Z",
  "updated_at": "2026-10-19T09:30:00Z"
}
```

### 3.1. Поля

| Поле | Смысл |
|---|---|
| `event_types` | непустое подмножество `transactions`, `actions` |
| `filters.transactions.in_msg_opcodes` | opcode входящего сообщения (§3.3); пусто — любая транзакция |
| `filters.actions.action_types` | типы actions; пусто — все поддерживаемые |
| `filters.actions.supported_action_types` | какие типы клиент понимает; `latest` при создании заменяется текущей версией (сейчас `v5`) и хранится уже в таком виде |
| `options` | обогащение как в streaming v2; на фильтрацию не влияет |
| `status`, `status_reason` | `active`, `paused` (`manual`, `endpoint_failing` или `endpoint_gone`), `expired` |
| `suspended` | подписка приостановлена приватной стороной; доставки нет при любом `status` |
| `delivery.<тип>` | состояние потока этого типа событий (DESIGN.md §4); потоки есть только для типов из `event_types` |
| `delivery.<тип>.cursor` | непрозрачный курсор последнего доставленного блока потока |
| `delivery.<тип>.position`, `delivery.<тип>.head` | позиция курсора и граница готовности потока; их разница — отставание. У actions граница ниже на время классификации |
| `epoch` | эпоха хранилища: растёт после восстановления из бэкапа, тогда клиент пересинхронизируется (§4.3) |

Внутри одного типа событий измерения складываются по AND: адрес из набора и
opcode или тип action из списка.

### 3.2. Начальная точка

Поле `start` обязательно при создании и в `seek`:

| Значение | Смысл |
|---|---|
| `{"from": "latest"}` | события с блока `head + 1` |
| `{"from_seqno": 51200000}` | с mc-блока N включительно; N берётся, например, из `mc_block_seqno` транзакции v3 |
| `{"after_cursor": "…"}` | после курсора; только в `seek` |

`latest` и `from_seqno` задают точку для всех потоков подписки. `after_cursor`
перематывает только поток, которому принадлежит курсор: курсор несёт свой тип
событий (§6.3). Для `latest` берётся граница индексера, общая для всех потоков:
поток actions начнёт с того же блока, просто дойдёт до него позже.

Точка не может быть позже `head + 1`. Самая ранняя доступная позиция — граница
окна hot, `_ton_hot_cold_split.seqno` (DESIGN.md §6). Точка раньше —
`start_too_old` с самой ранней позицией в `details.earliest`. Перевода времени
в mc-блок нет: seqno по времени клиент получает через v3 API.

Адреса, переданные при создании, действуют с начальной точки.

### 3.3. Фильтр по opcode

- Opcode — первые 32 бита тела входящего сообщения, то же значение, что поле
  `opcode` у сообщений в v3.
- Формат — шестнадцатеричная строка с префиксом `0x`, как v3 отдаёт opcode:
  `"0x7362d09c"`. Десятичная запись и hex без префикса не принимаются: в
  query-параметрах v3 строка `123` разбирается как hex, и такая двусмысленность
  в хранимом фильтре не нужна. В ответах opcode нормализуется к восьми
  hex-цифрам в нижнем регистре.
- Транзакция без входящего сообщения (tick-tock) или с телом короче 32 бит под
  непустой фильтр не попадает.
- Bounce-сообщения имеют opcode `0xffffffff`, по нему их можно выделить
  отдельно.
- Не больше 100 opcode в фильтре.

### 3.4. Изменения фильтров и URL

`PATCH` меняет `url`, `description`, `event_types`, `filters`, `options`.
Курсор не трогается: фильтр не входит в курсор (DESIGN.md §4). Ответ содержит
`effective_from_seqno` — блок, с которого новая конфигурация применяется
гарантированно (§5).

Смена URL не снимает автопаузу: после исправления endpoint клиент явно
вызывает `resume`.

## 4. Методы

| Метод | Путь | Назначение |
|---|---|---|
| `POST` | `/subscriptions` | создать |
| `GET` | `/subscriptions` | список подписок владельца |
| `GET` | `/subscriptions/{id}` | получить |
| `PATCH` | `/subscriptions/{id}` | изменить URL, описание, типы, фильтры, опции |
| `DELETE` | `/subscriptions/{id}` | удалить |
| `POST` | `/subscriptions/{id}/pause` | поставить на паузу |
| `POST` | `/subscriptions/{id}/resume` | продолжить с курсора |
| `POST` | `/subscriptions/{id}/seek` | перемотать |
| `POST` | `/subscriptions/{id}/rotate-secret` | выпустить новый секрет |
| `POST` | `/subscriptions/{id}/test` | отправить тестовую доставку |
| `GET` | `/subscriptions/{id}/logs` | журнал попыток доставки |
| `PATCH` | `/subscriptions/{id}/addresses` | добавить и удалить адреса |
| `GET` | `/subscriptions/{id}/addresses` | список адресов |
| `GET` | `/subscriptions/{id}/addresses/{address}` | проверить один адрес |

### 4.1. Создание

```http
POST /api/webhooks/v1/subscriptions
Idempotency-Key: 6f1c…

{
  "url": "https://example.com/ton-webhook",
  "description": "депозиты",
  "event_types": ["transactions", "actions"],
  "filters": {
    "actions": {"action_types": ["jetton_transfer", "ton_transfer"]}
  },
  "addresses": ["EQC…", "0:5B…"],
  "start": {"from": "latest"}
}
```

Ответ `201`:

```json
{
  "subscription": { "…": "ресурс из §3" },
  "secret": "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
}
```

- Секрет возвращается только здесь и в `rotate-secret`.
- В `addresses` при создании — не больше 1 000 адресов, остальное через
  `PATCH /addresses`.
- URL: только `https` (в режиме разработки флагом разрешается `http`), без
  логина и пароля в URL, хост не может быть IP-адресом из внутренних и
  зарезервированных диапазонов, длина до 2 048 символов. Это проверка для
  удобства, защиту обеспечивает Smokescreen.

### 4.2. Статусы

| Операция | Из статуса | В статус | Курсор |
|---|---|---|---|
| `pause` | `active`, `paused` | `paused` (`manual`) | не меняется |
| `resume` | `paused`, `active` | `active` | не меняется; если уже ниже доступной глубины — статус `expired` и ответ `start_too_old` |
| `seek` | любой | `active` | новая позиция |
| автопауза worker | `active` | `paused` (`endpoint_failing`) | не меняется |
| ответ `410 Gone` | `active` | `paused` (`endpoint_gone`) | не меняется |
| истечение | `active`, `paused` | `expired` | не меняется |

`suspended` — отдельный флаг, а не статус. Его ставит и снимает только
административный API (§4.6), а доставка идёт, только если `status = active` и
флаг снят. Клиентские операции меняют статус и при поднятом флаге, поэтому
после снятия флага помнить прежний статус не нужно.

### 4.3. Адреса

```http
PATCH /api/webhooks/v1/subscriptions/{id}/addresses

{"add": ["EQC…", "EQD…"], "remove": ["0:5B…"]}
```

```json
{
  "address_count": 1533,
  "added": 1,
  "removed": 1,
  "effective_from_seqno": 51234571
}
```

- Не больше 10 000 адресов в `add` и `remove` вместе.
- Запрос атомарный: применяется целиком или не применяется вовсе.
- Один и тот же адрес в `add` и `remove` одного запроса — `invalid_argument`.
- Добавление существующего и удаление отсутствующего адреса не ошибка; они
  просто не попадают в `added` и `removed`.

Чтение: `GET /addresses?after=…&limit=…` отдаёт адреса по порядку их байтового
представления. `GET /addresses/{address}` — `200` или `404`.

**Пересинхронизация после смены `epoch`:** клиент заново добавляет весь свой
набор через `PATCH` (повтор безопасен, это множество) и перематывает подписку
`seek` на момент бэкапа. Так восстанавливаются потерянные добавления — именно
они дают тихие пропуски событий. Удаления, потерянные при восстановлении,
вернут адреса, которые клиент уже убрал. Это лишние события, а не пропуски;
при желании клиент сверяет набор по `address_count` и списку.

### 4.4. Секрет и тест

- `rotate-secret` выпускает новый секрет и возвращает его. Старый остаётся
  действительным 24 часа: в это время доставки подписываются обоими, как
  разрешает Standard Webhooks.
- `test` синхронно отправляет на URL через Smokescreen тестовое тело с типом
  `webhook.test` и возвращает `result`, `http_status`, `duration_ms`, `error`
  в том же виде, что журнал (§4.5). Курсор не трогается, в журнал попытка не
  пишется. Не чаще раза в 10 секунд на подписку.

### 4.5. Журнал доставок

Каждая попытка отправки — одна запись. По журналу клиент видит, когда и
почему вызовы падали, а поддержка отвечает на вопрос «вебхук не пришёл».
Записи хранятся 7 дней, новые первыми.

```http
GET /api/webhooks/v1/subscriptions/{id}/logs?result=failed&limit=50
```

| Параметр | Смысл |
|---|---|
| `result` | `failed` — все неуспешные, `succeeded` — только успешные; без параметра — все |
| `webhook_id` | все попытки одной доставки: значение заголовка `webhook-id`, которое видел клиент |
| `after`, `limit` | пагинация, `limit` до 100 |

```json
{
  "logs": [
    {
      "attempted_at": "2026-10-19T10:00:05Z",
      "webhook_id": "msg_2fQ8kZ1x",
      "stream": "transactions",
      "attempt": 3,
      "range": {"from_mc_seqno": 51234560, "to_mc_seqno": 51234567},
      "events": 12,
      "bytes": 18234,
      "result": "http_error",
      "http_status": 503,
      "duration_ms": 412,
      "error": "HTTP 503",
      "response_excerpt": "Service Unavailable"
    }
  ],
  "next": "…"
}
```

| `result` | Когда |
|---|---|
| `succeeded` | ответ 2xx |
| `http_error` | ответ не 2xx |
| `timeout` | нет ответа за таймаут |
| `connection_error` | ошибка DNS, TCP или TLS |
| `blocked` | Smokescreen запретил соединение: URL ведёт во внутреннюю или запрещённую сеть |

- `attempt` — номер попытки этой доставки, то есть этого `webhook_id`.
- `response_excerpt` — первые 512 байт тела ответа, только для неуспешных
  попыток: туда фреймворки клиента обычно кладут причину отказа.
- `stream` — поток, которому принадлежит батч.
- Сводка по последней ошибке есть и в самом ресурсе подписки:
  `delivery.<тип>.failing_since` и `delivery.<тип>.last_error`.

### 4.6. Административный API

Отдельный порт, доступный только из приватной сети.

| Метод | Путь | Назначение |
|---|---|---|
| `POST` | `/admin/v1/owners/{owner}/suspend` | поднять флаг `suspended` у всех подписок владельца |
| `POST` | `/admin/v1/owners/{owner}/unsuspend` | снять флаг |
| `DELETE` | `/admin/v1/owners/{owner}` | удалить все подписки владельца |
| `GET` | `/admin/v1/subscriptions` | подписки с фильтрами `owner`, `status`, `changed_after` |
| `GET` | `/admin/v1/usage` | успешные доставки и события по владельцам за дни из журнала доставок |

Приватная сторона уведомляет пользователей, опрашивая
`/admin/v1/subscriptions?status=paused,expired&changed_after=…`. Время
смены статуса выставляется до коммита, поэтому опрос идёт с перекрытием в
несколько секунд. Ответ содержит отставание курсора и самую раннюю доступную
позицию: по ним приватная сторона сама предупреждает о скором `expired`.

Статистика считается по журналу доставок, поэтому приватной стороне нужно
забирать её чаще, чем раз в 7 дней.

## 5. Гарантия начала действия

Изменение адресов или конфигурации гарантированно учитывается с блока
`effective_from_seqno`. Запас по времени не нужен:

1. API коммитит изменение, увеличивая `version` подписки.
2. После коммита API читает `head` с primary hot-кластера и возвращает
   `effective_from_seqno = head + 1`. Берётся граница индексера: она не ниже
   границы любого потока, поэтому гарантия держится и для actions.
3. Worker перед обработкой каждого блока одним запросом читает `version`,
   статусы и флаги всех своих подписок из primary БД подписок и подгружает
   изменения.

Почему этого достаточно: блок `head + 1` не может начать обрабатываться раньше,
чем индексер до него дойдёт. А это случится после того, как API прочитал
`head`, то есть после того, как изменение стало видно.

Два требования к чтению:

- **`head` в API — только с primary.** С отстающей реплики он может оказаться
  меньше того, что worker уже обработал.
- **Версии в worker — только с primary БД подписок.** На реплике изменение
  может быть ещё не видно.

Изменение, сделанное, пока подписка отстаёт, может попасть и в более ранние
блоки: при обработке применяется текущий набор (DESIGN.md §7). Гарантия
касается только нижней границы.

## 6. Схема БД

PostgreSQL 18, отдельная база.

```sql
create type subscription_status as enum ('active', 'paused', 'expired');

create table meta
(
    key   text primary key,
    value text not null
);
insert into meta (key, value) values ('epoch', '1');

create table subscriptions
(
    id                     uuid primary key default uuidv7(),
    owner                  text                not null,
    url                    text                not null,
    description            text,
    event_types            text[]              not null,
    filters                jsonb               not null,
    options                jsonb               not null default '{}',
    status                 subscription_status not null default 'active',
    status_reason          text,
    status_changed_at      timestamptz         not null default now(),
    suspended              boolean             not null default false,
    version                bigint              not null default 1,
    address_count          bigint              not null default 0,
    secret_enc             bytea               not null,
    previous_secret_enc    bytea,
    previous_secret_until  timestamptz,
    created_at             timestamptz         not null default now(),
    updated_at             timestamptz         not null default now()
);
create index subscriptions_owner_idx on subscriptions (owner, id);
create index subscriptions_status_changed_idx on subscriptions (status_changed_at);

create table subscription_addresses
(
    subscription_id uuid  not null references subscriptions (id) on delete cascade,
    address         bytea not null,
    primary key (subscription_id, address)
);

create table address_changes
(
    subscription_id uuid        not null references subscriptions (id) on delete cascade,
    version         bigint      not null,
    address         bytea       not null,
    removed         boolean     not null,
    created_at      timestamptz not null default now(),
    primary key (subscription_id, version, address)
);
create index address_changes_created_idx on address_changes (created_at);

create table delivery_state
(
    subscription_id      uuid        not null references subscriptions (id) on delete cascade,
    stream               text        not null,
    cursor_seqno         integer     not null,
    cursor_partial_key   bytea,
    failing_since        timestamptz,
    consecutive_failures integer     not null default 0,
    next_attempt_at      timestamptz,
    last_success_at      timestamptz,
    last_error           text,
    updated_at           timestamptz not null default now(),
    primary key (subscription_id, stream)
);

create table leases
(
    subscription_id uuid primary key references subscriptions (id) on delete cascade,
    owner           text        not null,
    until           timestamptz not null
);
create index leases_until_idx on leases (until);

create table delivery_log
(
    id               bigint generated always as identity primary key,
    subscription_id  uuid        not null,
    owner            text        not null,
    attempted_at     timestamptz not null,
    webhook_id       text        not null,
    stream           text        not null,
    attempt          integer     not null,
    from_seqno       integer     not null,
    to_seqno         integer     not null,
    events           integer     not null,
    bytes            integer     not null,
    result           text        not null,
    http_status      smallint,
    duration_ms      integer,
    error            text,
    response_excerpt text
);
create index delivery_log_subscription_idx
    on delivery_log (subscription_id, attempted_at desc, id desc);
create index delivery_log_failed_idx
    on delivery_log (subscription_id, attempted_at desc, id desc) where result <> 'succeeded';
create index delivery_log_webhook_idx on delivery_log (subscription_id, webhook_id);
create index delivery_log_attempted_idx on delivery_log (attempted_at);

create table idempotency_keys
(
    owner           text        not null,
    key             text        not null,
    subscription_id uuid        not null,
    request_hash    bytea       not null,
    created_at      timestamptz not null default now(),
    primary key (owner, key)
);
```

### 6.1. Устройство таблиц

- **`subscriptions`** — конфигурация, статус, флаг приостановки и секреты.
  Любое изменение подписки или её набора адресов увеличивает `version` под
  блокировкой строки. Поэтому версии одной подписки растут без пропусков и
  коммитятся по порядку, и worker, опрашивающий изменения по версии, ничего не
  пропустит.
- **Секреты** зашифрованы ключом из конфигурации сервиса. Текущий — в
  `secret_enc`, предыдущий на время ротации — в `previous_secret_enc` до
  `previous_secret_until`.
- **`subscription_addresses`** — только живой набор, строка на пару
  (подписка, адрес). Адрес хранится как 33 байта: workchain (int8) и 32 байта
  account id.
- **`address_changes`** — журнал изменений набора для worker: каждая строка
  несёт версию, в которой адрес добавлен или удалён. Worker при получении
  аренды загружает набор целиком, а дальше читает изменения с версией больше
  известной. Строки старше часа удаляются: отставать от изменений дольше
  worker не может, иначе он теряет аренду.
- **`delivery_state`** — строка на поток подписки (DESIGN.md §4): курсор и
  ретраи. Всё это часто меняет worker, поэтому вынесено отдельно — частые
  обновления не переписывают строку с конфигурацией и пишутся с другой
  долговечностью (§6.2). Строка потока создаётся вместе с подпиской или при
  добавлении типа в `event_types` и удаляется при его удалении.
- **`leases`** — аренда одна на подписку, а не на поток: оба потока ведёт один
  worker, потому что набор адресов в памяти у них общий.
- **`delivery_log`** — журнал попыток. Строки старше 7 дней удаляются по
  `attempted_at`. Частичный индекс по неуспешным попыткам нужен, чтобы
  `result=failed` не перебирал миллионы успешных записей. По этой же таблице
  считается статистика для биллинга.

Курсор worker обновляет сравнением со старым значением: `cursor_seqno` и
`cursor_partial_key` меняются, только если оба равны тем, от которых строился
батч (`partial_key` сравнивается через `is not distinct from`). Если клиент за
это время сделал `seek`, обновление не проходит, и worker перечитывает
позицию. Отдельный счётчик перемоток не нужен.

### 6.2. Долговечность записей

В кластере по умолчанию `synchronous_commit = local` (DESIGN.md §11). Роли
сервиса задано `on`, а там, где допустимо, транзакции понижают его явно.

| Запись | `synchronous_commit` | Почему |
|---|---|---|
| подписки, адреса, секреты, статусы, флаг, `seek` | `on` | потеря незаметна для клиента и необратима |
| аренды | `on` | потерянная аренда даёт два worker на одну подписку |
| курсор и ретраи в `delivery_state` | `local` | откат курсора назад даёт только дубли |
| `delivery_log` | `local` | журнал и статистика |

### 6.3. Курсор

Base64url от структуры: байт версии формата (`1`), байт потока (`1` —
transactions, `2` — actions) и `cursor_seqno` (uint32, big-endian). Клиенту
формат не гарантируется.

Если блок разделён на несколько батчей (delivery-format.md §5), курсор
указывает внутрь блока: `cursor_seqno` — сам блок, а `cursor_partial_key` —
ключ сортировки последнего доставленного события. Такой курсор кодируется
форматом версии `2`: после seqno идёт ключ. `cursor_partial_key = null` значит,
что блок `cursor_seqno` доставлен целиком.

## 7. Связь с доставкой

Подробности — в спецификации worker. Здесь важно зафиксировать:

- Формат тела и правила дедупликации — в [delivery-format.md](delivery-format.md).
  `webhook-id` вычисляется из `id` подписки и тела запроса.
- Перед каждой отправкой worker проверяет, что его аренда действует дольше
  таймаута запроса. Так worker, потерявший аренду, не шлёт запросы параллельно
  с новым владельцем подписки.

## 8. Открытые вопросы

- **Числа:** 1 000 адресов при создании, 10 000 в `PATCH`, 100 opcode в
  фильтре, 24 часа на ротацию, 7 дней журнала доставок, 512 байт ответа в
  журнале.
Решено 2026-10-19: история только в пределах окна hot, без `from_utime`, без
фильтров по asset и `msg_body_hash`; `Idempotency-Key` остаётся; владелец —
`X-Owner-Id` от HAProxy; хранимых лимитов владельца пока нет.

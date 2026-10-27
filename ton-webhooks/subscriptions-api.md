# subscriptions-api и БД подписок

Статус: целевой контракт v1, 2026-10-27. Архитектура и гарантии — в
[DESIGN.md](DESIGN.md), формат HTTP-доставки — в
[delivery-format.md](delivery-format.md).

## 1. Общие правила

- Базовый путь `/api/webhooks/v1`, запросы и ответы в JSON.
- Одна подписка имеет один неизменяемый `event_type`: `transactions` или
  `jetton_transfers`. Поле-массив `event_types` не поддерживается.
- Сеть задаётся развёртыванием сервиса (`mainnet` или `testnet`) и возвращается
  в ресурсе и доставках. Менять сеть существующей подписки нельзя.
- Поле `start_mc_seqno` обязательно в create: целое число или строка `"latest"`.
  Во всех ответах это конкретный номер. Подписка создаётся `paused/manual`.
- Можно создать пустой набор адресов и загрузить его до `resume`.
- Изменять адреса, фильтры, URL и лимиты можно только в paused.
- `seek`, изменение начальной точки, отдельный replay API и `stop_at_seqno`
  не входят в v1. Для повторного чтения создаётся новая подписка.
- Полные payload обязательны; переключателей сокращённого payload, address
  book и metadata нет.

### 1.1. Владелец, квоты и адреса

Владелец — аккаунт из `X-Owner-Id`, установленного HAProxy. `X-Limiting-Key`
не используется. Шлюз удаляет присланные клиентом служебные заголовки и
выставляет собственные. Без владельца в многопользовательском режиме — `401`.
В самостоятельной установке без шлюза используется владелец по умолчанию.

Квоты приходят с запросом:

| Заголовок | Ограничение |
|---|---|
| `X-Max-Webhook-Subscriptions` | число подписок владельца, включая paused/expired |
| `X-Max-Webhook-Addresses` | сумма числа адресов во всех подписках владельца |

Тарифные лимиты не хранятся в сервисе. Проверка квот и изменения счётчиков
сериализуются блокировкой строки владельца; блокировки отдельных подписок
недостаточно. Ту же блокировку используют suspend/create/delete владельца.
Новая подписка наследует административный запрет владельца.

Адреса — стандартные TON-адреса в raw или user-friendly формате; ответы в raw
виде `0:HEX`/`-1:HEX`. Сравнение идёт после нормализации. Один адрес в двух
подписках учитывается в квоте дважды. Пагинация — `limit`, `after`, ответ `next`.

### 1.2. Ошибки

```json
{"error":{"code":"start_too_old","message":"Requested start is outside hot history","details":{"earliest_mc_seqno":51200000}}}
```

| Код | HTTP | Когда |
|---|---|---|
| `invalid_argument` | 400 | неизвестное/неподдерживаемое поле, тип, фильтр, адрес, URL или токен пагинации |
| `unauthorized` | 401 | нет владельца |
| `suspended` | 403 | запрещена исходящая test-доставка владельца |
| `not_found` | 404 | ресурс отсутствует или принадлежит другому владельцу |
| `conflict` | 409 | недопустимое состояние, незавершённая ротация или иной запрос с тем же Idempotency-Key |
| `start_too_old` | 409 | начальная/следующая нужная позиция вне hot; `details.earliest_mc_seqno` |
| `limit_exceeded` | 422 | превышены квота или размер запроса |
| `rate_limited` | 429 | слишком частый test |
| `source_not_ready` | 503 | нет пригодной границы источника для create |

`details` уточняет причину ошибки.
Установка paused/expired при выполнении команды отражается и в ресурсе, и в
outbox, даже если команда возвращает ошибку.

### 1.3. Идемпотентность

`Idempotency-Key` обязателен для create и rotate-secret. Для остальных
изменяющих команд его можно передать, чтобы повтор получил первоначальные
результат и границу действия. Ключ привязан к владельцу, HTTP-методу и пути;
сравнивается хеш нормализованного запроса. Иное тело с тем же ключом — `409`.

Результат успешной команды, HTTP-код и конкретные вычисленные значения
сохраняются атомарно с её изменениями на 24 часа. Хеш запроса create считается
до разрешения `"latest"`: повтор с тем же ключом и запросом возвращает прежний
числовой `start_mc_seqno` без повторного чтения head. Повтор rotate возвращает
тот же секрет.
Ответы с секретом хранятся зашифрованными.
Конкурирующие запросы одного ключа сериализуются. Если ответ потерян после
коммита, повтор возвращает сохранённый результат. После срока хранения
повторное использование ключа не имеет прежней гарантии.

Без ключа операции над множеством адресов идемпотентны по конечному состоянию,
но числа `added/removed` и возвращённая граница при повторе могут отличаться.
Повтор `resume` active-подписки не сбрасывает её состояние отказов. Повтор
`pause` paused-подписки не стирает текущую диагностическую причину паузы.

## 2. Ресурс подписки

Примеры с `...` обозначают сокращённые значения; это не дополнительные поля
реальных моделей и не сокращённый режим доставки.

```json
{
  "id": "0192f0c4-7d3e-7c4b-9a51-6f2d8e1b4a10",
  "network": "mainnet",
  "url": "https://example.com/ton-webhook",
  "description": "депозиты",
  "event_type": "jetton_transfers",
  "filters": {"direction": "incoming", "jetton_masters": []},
  "delivery_limits": {"max_events": 500, "max_bytes": 1048576},
  "start_mc_seqno": 51234568,
  "config_version": 1,
  "status": "paused",
  "status_reason": "manual",
  "status_changed_at": "2026-10-27T10:00:00Z",
  "suspended": false,
  "address_count": 0,
  "delivery": {
    "cursor": "cursor-before-start",
    "position": {"mc_seqno": 51234567, "block_complete": true, "lt": null, "hash": null},
    "head": {"mc_seqno": 51234570},
    "earliest_mc_seqno": 51200000,
    "prepared_through_cursor": "cursor-before-start",
    "pending": null,
    "blocked_reason": null,
    "failing_since": null,
    "next_attempt_at": null,
    "last_success_at": null,
    "last_error": null
  },
  "created_at": "2026-10-27T10:00:00Z",
  "updated_at": "2026-10-27T10:00:00Z"
}
```

| Поле | Смысл |
|---|---|
| `event_type` | один поток на всю жизнь подписки |
| `filters` | фильтры только выбранного типа (§3); пустой объект — значения по умолчанию |
| `delivery_limits` | лимиты будущих батчей; не меняют сохранённый pending |
| `start_mc_seqno` | конкретный целый номер первого masterchain-блока включительно, в том числе при create с `"latest"`; только чтение после create, не текущий cursor |
| `config_version` | версия конфигурации/набора адресов; не номер попытки и не lease token |
| `status`, `status_reason` | состояние из §4.3 |
| `suspended` | вычисляется из административного состояния владельца; блокирует любые исходящие вызовы |
| `delivery.cursor` | сохранённая позиция; в штатном режиме накопленный пустой прогресс сохраняется каждые 30 секунд либо покрывается следующим батчем, при сбоях отставание может быть больше |
| `delivery.position` | разбор позиции для диагностики; для jetton lt/hash принадлежат получателю |
| `delivery.head` | текущая доступная граница источников; `null`, если её нельзя достоверно прочитать |
| `delivery.prepared_through_cursor` | конец pending; без него равен сохранённому cursor |
| `delivery.pending` | `webhook_id`, `from_cursor`, `cursor`, `events`, `bytes`, `attempt`; без тела |
| `delivery.blocked_reason` | `source_not_ready` при временном ожидании источника, иначе `null` |
| `delivery.last_error` | структурированная последняя ошибка источника или доставки |

`lease_token` — внутреннее поле, в ресурс и тело вебхука не входит.
Публичного `epoch` нет. Разница head/position приблизительно показывает
отставание, но не является обещанием времени доставки или полноты контекста.

## 3. Начало, фильтры и лимиты

### 3.1. Начальная точка

В create обязательно передаётся поле `start_mc_seqno` одного из двух типов:

| Значение `start_mc_seqno` | Смысл |
|---|---|
| `"latest"` | зафиксировать head при создании, начать с head + 1 |
| `51200000` | целый номер masterchain-блока N включительно |

Числовые строки, дробные числа, другие строки, объекты, `null` и отсутствие
поля — `400 invalid_argument`. В ответах create/get/list и других командах,
возвращающих ресурс подписки, `start_mc_seqno` всегда число. Исходное значение
`"latest"` в ресурсе не возвращается.

Используется общая граница готовности чтения полного payload: Postgres и,
если он включён, выбранный Kvrocks (DESIGN.md §5.1). Нет пригодной границы —
`503`, подписка не создаётся. Допустимо `earliest_mc_seqno <= N <= head + 1`.
`after_cursor` и `from_utime` не принимаются. Seqno по времени можно получить
через v3 API. Для jetton N относится к блоку получения, не отправки.
Sender может быть в блоке до N: если receiver появился начиная с N и контекст
sender доступен, перевод входит в подписку после проверки получения.

При create в подписке сохраняется только разрешённый числовой `start_mc_seqno`.
Внутренняя начальная позиция — конец N−1. Она не меняется при загрузке адресов
на паузе или resume; идемпотентный повтор create возвращает тот же номер.
Повторное чтение истории — новая подписка с числовым `start_mc_seqno`;
курсор старой подписки не меняется.

### 3.2. Фильтры

Для `transactions`:

```json
{"in_msg_opcodes":["0x7362d09c"]}
```

Opcode — первые 32 бита входящего тела, как в v3. Принимается hex с `0x`,
ответ нормализуется к восьми цифрам нижнего регистра. Не более 100 значений.
Tick-tock, отсутствующее или короткое тело не совпадает с непустым списком.
Bounce можно выделить opcode `0xffffffff`. Пустой список означает любой
opcode; успешность транзакции этим фильтром не ограничивается.

Для `jetton_transfers`:

```json
{"direction":"incoming","jetton_masters":["0:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF"]}
```

`direction`: incoming/outgoing/both, по умолчанию both. Адреса подписки —
владельцы, а не jetton-wallets. `jetton_masters` — не более 100 нормализованных
master-адресов, пусто означает любой поддерживаемый jetton. Учитываются
только распознанные индексером переводы из `jetton_transfers` с подтверждённым
зачислением. При совпадении обоих владельцев событие
одно; `matched_addresses` содержит уникальные адреса, подходящие направлению.

Для всех направлений событие появляется по finalized-позиции receiver.
Sender в блоке 100 и receiver в 103 дают событие блока 103, в том числе для
подписки только на отправителя. Пока receiver не появился, вебхук о зачислении
не отправляется, курсор не удерживается на sender и подписка не блокируется.
Worker обнаружит receiver при обходе последующих блоков, когда они готовы.
Нет фиксированного ожидания одного блока и отдельного API ожидания перевода.

Внутри измерения OR, между измерениями AND. Пустой набор адресов не совпадает
ни с чем. Фильтры другого типа и `supported_action_types` отвергаются.

### 3.3. Лимиты батча

По умолчанию `max_events=500`, `max_bytes=1048576` (1 MiB полного JSON).
API позволяет уменьшать и возвращать эти значения в пределах `1…500` и
`1024…1048576` соответственно. Это целевой размер: одно большее событие
отправляется целиком отдельным батчем. Не задаётся максимальный размер
отдельного события посредством `max_bytes`.

Смена лимитов действует для следующего ещё не подготовленного диапазона.
Уже сохранённый pending не разделяется и не пересобирается. После HTTP 413
он остаётся на паузе: клиент увеличивает лимит endpoint или меняет URL на
подходящий приёмник, затем вызывает resume. Уменьшение max_bytes через PATCH
не уменьшит этот pending. Полные события не обрезаются, включая одиночное
событие сверх целевого размера. Перед выпуском проверяются технические
лимиты памяти/хранилища.

## 4. Клиентские методы

| Метод | Путь | Назначение |
|---|---|---|
| `POST` | `/subscriptions` | создать на паузе |
| `GET` | `/subscriptions` | список владельца |
| `GET` | `/subscriptions/{id}` | ресурс и диагностика |
| `PATCH` | `/subscriptions/{id}` | URL, описание, фильтры, лимиты |
| `DELETE` | `/subscriptions/{id}` | отменить подписку и удалить её pending |
| `POST` | `/subscriptions/{id}/pause` | прекратить новые попытки |
| `POST` | `/subscriptions/{id}/resume` | продолжить сохранённую доставку |
| `POST` | `/subscriptions/{id}/rotate-secret` | начать ротацию |
| `POST` | `/subscriptions/{id}/test` | тест endpoint и подписи |
| `GET` | `/subscriptions/{id}/logs` | диагностический журнал |
| `PATCH` | `/subscriptions/{id}/addresses` | атомарно изменить множество адресов на паузе |
| `GET` | `/subscriptions/{id}/addresses` | пагинация адресов |
| `GET` | `/subscriptions/{id}/addresses/{address}` | наличие адреса: 200/404 |

### 4.1. Создание и удаление

```http
POST /api/webhooks/v1/subscriptions
Idempotency-Key: create-deposits-1
Content-Type: application/json

{
  "url": "https://example.com/ton-webhook",
  "description": "депозиты",
  "event_type": "jetton_transfers",
  "filters": {"direction": "incoming", "jetton_masters": []},
  "addresses": [],
  "start_mc_seqno": "latest"
}
```

Ответ `201`:

```json
{"subscription":{"id":"0192f0c4-7d3e-7c4b-9a51-6f2d8e1b4a10","start_mc_seqno":51234568,"status":"paused","status_reason":"manual"},"secret":"whsec_..."}
```

В реальном ответе `subscription` — полный ресурс §2. Секрет возвращается
только при create/rotate и их идемпотентных повторах. При create не больше
1 000 адресов; остальное загружается отдельными PATCH. URL — https, до
2 048 символов, без userinfo, с запретом внутренних/зарезервированных адресов.
В dev-режиме http может быть разрешён явно. Проверка API не заменяет
проверку соединения Smokescreen.

DELETE отменяет доставку, удаляет набор и pending; уже отправленный
HTTP-запрос отозвать невозможно. Outbox об удалении и учёт прошлых ACK
сохраняются. Повтор удаления возвращает `204`, не восстанавливая ресурс.

### 4.2. Изменения подписки и адресов

PATCH допускает `description` в любом статусе. `url`, `filters`,
`delivery_limits` и PATCH адресов — только в paused. Тип, сеть и
`start_mc_seqno` неизменяемы.
`filters` заменяется целиком, а в `delivery_limits` меняются переданные поля.
Смена URL/лимитов/фильтра не вызывает автоматического resume.

```http
PATCH /api/webhooks/v1/subscriptions/{id}/addresses
Content-Type: application/json

{"add":["0:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF"],"remove":[]}
```

```json
{"address_count":1,"added":1,"removed":0,"config_version":2,"effective_after_cursor":"cursor-before-start"}
```

Не больше 10 000 адресов в add/remove суммарно. Запрос атомарный. Пересечение
нормализованных add/remove — `400`; существующее добавление и отсутствующее
удаление не ошибка. PATCH адресов в active или expired возвращает `409 conflict`.
Автоматической паузы или resume нет. GET списка
сортирует по байтам адреса, `limit` до 10 000; ответ включает версию набора.
Если набор меняется между страницами, полную сверку повторяют при одной версии.

Под блокировкой подписки API выбирает `effective_after_cursor`:

1. есть pending — конец pending;
2. иначе — сохранённый cursor, без пустого прогресса только в памяти worker.

Изменение фильтра/адресов действует строго после этой позиции; сохранённые
события не меняются. Ответ обычного PATCH содержит полный ресурс и
`effective_after_cursor` для фильтров/лимитов. Изменение может затронуть
исторические блоки, если подписка отстаёт. Каждое фактическое изменение
конфигурации или адресов увеличивает config_version под той же блокировкой;
для отсутствия изменений версия не увеличивается. Статусы, ACK и аренды
сами по себе config_version не меняют.

Worker при записи батча или продвижении пустого диапазона проверяет ту же
строку подписки, `config_version`, владельца/lease token и исходный cursor.
Если API успел первым, результат сканирования пересобирается. Если worker
успел первым, границей изменения становится сохранённый диапазон.

При реальной смене URL API в той же транзакции увеличивает lease_token и
отзывает аренду, обнуляя worker_id/lease_until. Значение token никогда не
сбрасывается. Pending, body и webhook-id не меняются. Старый ответ не
применяется даже при смене URL A → B → A. После resume worker получает новую
аренду и при резервировании попытки перечитывает текущий URL и секреты из БД;
параметры прежней аренды из кэша использовать нельзя.

Изменение фильтров/адресов не отзывает аренду уже начатой попытки: текущий
батч по прежнему набору остаётся действительным до ACK. Перед новой выборкой,
в том числе после resume, worker сверяет config_version с БД. При получении
аренды, отсутствии снимка или несовпадении версии он загружает полный
согласованный снимок адресов, фильтров и config_version; журнала дельт нет.
Resume без изменения конфигурации не требует повторной загрузки того же
набора. Финальная проверка версии остаётся: scan мог начаться до pause
и завершиться после PATCH/resume.

### 4.3. Статусы и возобновление

| Причина/операция | Результат |
|---|---|
| create | paused/manual |
| pause active | paused/manual; актуальная начатая попытка ещё может подтвердиться |
| pause paused | состояние не меняется |
| resume paused | active с тем же pending/cursor; новый цикл отказов |
| resume active | без изменений, включая failing_since |
| 24 часа отказов | paused/endpoint_failing |
| HTTP 410 | paused/endpoint_gone |
| HTTP 413 | paused/payload_too_large |
| установленная потеря обязательных исходных данных | paused/source_data_unavailable |
| следующий незаготовленный диапазон ушёл из hot | expired/history_unavailable |

Таймаут 15 секунд; retry через 5 секунд, 30 секунд, 2 минуты, 10 минут, затем
час с jitter 0.8…1.2. `Retry-After` для 429/502/503/504 учитывается по
DESIGN.md §8.2. `failing_since` сбрасывается только при ACK или явном
paused → active. При resume сбрасывается consecutive_failures и разрешается
немедленная попытка; общий attempt сохранённого батча не сбрасывается.
После pause без resume новых попыток нет.

Если сохранён pending, resume сначала разрешает доставить его даже
при уходе исходных блоков из hot. Только затем проверяется следующий
незаготовленный диапазон. Если сохранённых данных нет и позиция уже потеряна,
resume фиксирует expired и возвращает `409 start_too_old`. Для partial-курсора
нужен ещё текущий блок; для конца блока — следующий.

Expired терминален для доставки: pause/resume не возвращают его к
работе (`409`). Подписку можно прочитать, изменить описание или удалить;
изменение набора адресов в expired тоже запрещено.
Для восстановления клиент создаёт новую подписку и при необходимости
добирает старую историю через v3. Автоматического удаления expired нет.

`suspended` — запрет владельца поверх статуса. Клиент может настраивать,
ставить на паузу и вызывать resume, но HTTP и чтение новых диапазонов worker
не выполняет до unsuspend. Новая подписка такого владельца тоже suspended.
Монитор retention работает и для suspended-подписок. Pause/suspend допускают
сохранение ACK уже начатой актуальной попытки; смена URL — нет.

Временное ожидание источника оставляет active, выставляет
`delivery.blocked_reason=source_not_ready` и публикует управляющее событие.
Отсутствие ожидаемых данных не приравнивается к пустому диапазону.
Ожидание ещё не возникшего receiver — обычное следование за цепочкой и само
по себе не выставляет blocked_reason. Для уже найденного receiver отсутствие
записи `jetton_transfers` при найденном sender и проверенной готовности
источника означает исключение `unrecognized_transfer` с метрикой, а не паузу.
Отсутствующие sender, обязательное тело или проверочный контекст требуют
ожидания либо `paused/source_data_unavailable` при установленной потере,
как определено в DESIGN.md §5.2.

### 4.4. Ротация секрета

POST rotate-secret требует Idempotency-Key. Секрет — 32 случайных байта,
выдаётся как `whsec_` + base64. Предыдущий секрет действует 24 часа; запрос
подписывается обоими до `previous_secret_until`. Новый вызов с другим ключом
в этот период — `409`, повтор прежнего возвращает сохранённый результат.
Ротация не меняет body/webhook-id, cursor и lease_token. Заголовки вычисляются
на каждую попытку из актуальных секретов.

### 4.5. Test

Test разрешён только в paused и без suspended. Синхронная доставка
`webhook.test` через Smokescreen возвращает `webhook_id`, `result`,
`http_status`, `duration_ms`, `error`. Она не меняет cursor, failing_since,
pending и статус даже при 410/413; в журнал событий доставки не пишется.
Не чаще раза в 10 секунд. Диспетчер не запускает test одновременно с известной
незавершённой попыткой; запоздавший запрос старого worker всё равно возможен.

### 4.6. Журнал попыток

GET logs принимает `result=failed|succeeded`, `webhook_id`, `after`, `limit`
до 100. `failed` объединяет все неуспешные HTTP-результаты. Сортировка от
новых к старым, ключ `(attempted_at, id)`. Хранение — 7 дней.

```json
{
  "logs":[{
    "attempted_at":"2026-10-27T10:00:05Z",
    "webhook_id":"msg_example",
    "stream":"jetton_transfers",
    "attempt":3,
    "from_cursor":"cursor-a",
    "cursor":"cursor-b",
    "events":12,
    "bytes":182340,
    "result":"http_error",
    "http_status":503,
    "duration_ms":412,
    "error":"HTTP 503",
    "response_excerpt":"Service Unavailable",
    "applied_to_state":true
  }],
  "next":null
}
```

Результаты: succeeded, http_error, timeout, connection_error, blocked
(прокси запретил соединение). `attempt` резервируется до HTTP и растёт для
одного webhook-id; после падения возможны пропуски номеров. Если stale worker
получил ответ, запись может иметь `applied_to_state=false`: транспортный 2xx
ещё не означает, что его ACK был принят сервисом. При падении сам результат
попытки может быть неизвестен; журнал не является безошибочным аудитом сети.

Для ошибки сохраняются первые 512 байт ответа, декодированные безопасно в
текст; остальное тело не читается. Успешное тело не используется. Сводка
актуальной ошибки находится в `delivery.last_error`. Журнал не источник
биллинга и не архив полных payload.

## 5. Административный API и outbox

Отдельный порт только в приватной сети.

| Метод | Путь | Назначение |
|---|---|---|
| `POST` | `/admin/v1/owners/{owner}/suspend` | запретить доставки всех нынешних и новых подписок |
| `POST` | `/admin/v1/owners/{owner}/unsuspend` | снять запрет, сохранив статусы подписок |
| `DELETE` | `/admin/v1/owners/{owner}` | отменить существующие подписки владельца |
| `GET` | `/admin/v1/subscriptions` | диагностический список с owner/status и пагинацией |
| `POST` | `/admin/v1/outbox/claim` | получить неподтверждённые управляющие события и учёт ACK в аренду |
| `POST` | `/admin/v1/outbox/ack` | подтвердить экспорт конкретных событий с токеном аренды |

Outbox записывается в той же транзакции, что изменение состояния. События:
subscription_created, subscription_deleted, status_changed,
source_blocked/source_ready, owner_suspended/owner_unsuspended,
retention_at_risk, batch_acked. Предупреждение учитывает active, paused и suspended,
следующую незаготовленную позицию и текущую границу hot. Повторяющиеся
предупреждения ограничиваются по времени; порог задаётся конфигурацией.

Claim выбирает ограниченную пачку видимых неподтверждённых записей со
свободной или истёкшей арендой через `FOR UPDATE SKIP LOCKED`. Он выдаёт ID,
kind, owner/subscription_id, webhook_id при наличии, payload и новый claim token.
Один протокол применяется ко всем видам событий. Аренда —
60 секунд. ACK конкретных ID/token идемпотентен, чужой/устаревший token
не подтверждает запись. После истечения аренды событие будет выдано снова.
Потребитель дедуплицирует по ID; ACK делает после надёжного принятия.

Глобальный курсор `id > last_id` и `changed_after` не используются для
надёжной доставки: номер/время могут быть выделены до позднего коммита.
Периодический claim обнаружит такую строку независимо от её ID. Outbox
сохраняет owner/subscription_id и payload после удаления самой подписки.
Неподтверждённые события не имеют TTL; подтверждённые можно удалять через
7 дней после ACK. Диагностический список не заменяет outbox.

`batch_acked` — запись в том же outbox с уникальным webhook_id. Она создаётся
атомарно с продвижением cursor и удалением pending. Owner/subscription_id
берутся из подписки, payload содержит network, stream, events, bytes и
accepted_at — время принятия ACK сервисом. Количества относятся ко всему
подтверждённому батчу. Retry не увеличивает учёт; stale ACK и повтор ACK без
текущего pending не создают запись. Новая историческая подписка создаёт новые
доставки и учитывается отдельно.

Приватная часть имеет один логический потребитель outbox, при необходимости
с несколькими экземплярами. До ACK экспорта он надёжно сохраняет событие в
своём входящем журнале с дедупликацией по ID. Дальше независимые обработчики
учёта и уведомлений работают с принятыми событиями, а не конкурируют за
случайные части исходной очереди. Учёт дополнительно дедуплицируется по
webhook_id; тарифы и правила оплаты пересечений определяет приватная сторона.
Удаление диагностических HTTP-логов на учёт не влияет. Отставание outbox
измеряется отдельно для управляющих событий и batch_acked; пропускная
способность потребителя проверяется под нагрузкой доставок.

После восстановления бэкапа worker заблокированы внешним режимом
развёртывания до сверки конфигурации. Уведомление об инциденте не зависит от
восстановленного outbox: записи и целые подписки могли отсутствовать в бэкапе.
Процедура — DESIGN.md §11; числового epoch и автоматического seek нет.

## 6. Схема БД

PostgreSQL 18, отдельная база. Ниже целевая схема основного состояния;
это спецификация, не готовая миграция. Cursor хранится как внутренняя
версионированная структура jsonb, наружу выдаётся непрозрачный токен (§7).

```sql
create type subscription_status as enum ('active', 'paused', 'expired');

create table owners (
    id text primary key,
    suspended boolean not null default false
);

create table subscriptions (
    id uuid primary key default uuidv7(),
    owner text not null references owners(id),
    network text not null check (network in ('mainnet', 'testnet')),
    event_type text not null check (event_type in ('transactions', 'jetton_transfers')),
    url text not null,
    description text,
    filters jsonb not null default '{}',
    max_events integer not null default 500 check (max_events between 1 and 500),
    max_bytes integer not null default 1048576 check (max_bytes between 1024 and 1048576),
    start_mc_seqno integer not null,
    config_version bigint not null default 1,
    address_count bigint not null default 0,
    status subscription_status not null default 'paused',
    status_reason text default 'manual',
    status_changed_at timestamptz not null default now(),
    secret_enc bytea not null,
    previous_secret_enc bytea,
    previous_secret_until timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index subscriptions_owner_idx on subscriptions(owner, id);

create table subscription_addresses (
    subscription_id uuid not null references subscriptions(id) on delete cascade,
    address bytea not null check (octet_length(address) = 33),
    primary key (subscription_id, address)
);

create table delivery_state (
    subscription_id uuid primary key references subscriptions(id) on delete cascade,
    cursor jsonb not null,
    blocked_reason text,
    failing_since timestamptz,
    last_success_at timestamptz,
    last_error jsonb,
    last_retention_warning_at timestamptz,
    updated_at timestamptz not null default now()
);

create table leases (
    subscription_id uuid primary key references subscriptions(id) on delete cascade,
    worker_id text,
    lease_token bigint not null default 0,
    lease_until timestamptz
);
create index leases_until_idx on leases(lease_until);

create table pending_batches (
    subscription_id uuid primary key references subscriptions(id) on delete cascade,
    config_version bigint not null,
    webhook_id text not null unique,
    from_cursor jsonb not null,
    to_cursor jsonb not null,
    body bytea not null,
    event_count integer not null,
    body_bytes bigint not null,
    attempt integer not null default 0,
    consecutive_failures integer not null default 0,
    next_attempt_at timestamptz,
    created_at timestamptz not null default now()
);

create table delivery_log (
    id bigint generated always as identity primary key,
    subscription_id uuid not null,
    owner text not null,
    attempted_at timestamptz not null,
    webhook_id text not null,
    stream text not null,
    attempt integer not null,
    lease_token bigint not null,
    from_cursor jsonb not null,
    to_cursor jsonb not null,
    events integer not null,
    bytes bigint not null,
    result text not null,
    http_status smallint,
    duration_ms integer,
    error text,
    response_excerpt text,
    applied_to_state boolean not null
);
create index delivery_log_subscription_idx
    on delivery_log(subscription_id, attempted_at desc, id desc);
create index delivery_log_failed_idx
    on delivery_log(subscription_id, attempted_at desc, id desc)
    where result <> 'succeeded';
create index delivery_log_webhook_idx on delivery_log(subscription_id, webhook_id);
create index delivery_log_attempted_idx on delivery_log(attempted_at);

create table idempotency_keys (
    owner text not null,
    method text not null,
    path text not null,
    key text not null,
    request_hash bytea not null,
    response_status integer not null,
    response_enc bytea not null,
    created_at timestamptz not null default now(),
    expires_at timestamptz not null,
    primary key (owner, method, path, key)
);

create table outbox (
    id uuid primary key default uuidv7(),
    owner text not null,
    subscription_id uuid,
    kind text not null,
    webhook_id text unique,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    claim_token uuid,
    claim_until timestamptz,
    acked_at timestamptz,
    check (
        (kind = 'batch_acked' and webhook_id is not null and subscription_id is not null)
        or (kind <> 'batch_acked' and webhook_id is null)
    )
);
create index outbox_pending_idx on outbox(claim_until, id)
    where acked_at is null;
```

Секреты и идемпотентные ответы шифруются ключом конфигурации; ключ не хранится
в этой БД. Body — точные UTF-8 байты HTTP-запроса, не jsonb, которое может
изменить сериализацию. Подписка, delivery_state и постоянная строка leases
создаются атомарно. Lease token не сбрасывается при освобождении аренды.

У delivery_state нет второго ключа stream: строка одна на подписку.
Pending тоже один. Он сохраняется целиком до HTTP, удаляется при ACK и не
имеет TTL. Удаление подписки — единственная пользовательская отмена pending;
изменение лимитов, фильтров или URL не переписывает его байты.

Адреса меняются только на паузе. Worker сверяет config_version перед новой
выборкой, включая первую после resume. При получении аренды, отсутствии
снимка или несовпадении версии он загружает полный набор и фильтры вместе
с config_version в одном согласованном снимке. Синхронизации кэша через дельты
и отдельной таблицы изменений нет. Доставка сохранённого pending не требует
загрузки набора.

Outbox не имеет внешнего ключа на подписку или владельца: управляющие события
и batch_acked переживают их удаление. Уникальность webhook_id обеспечивает
один учёт принятого батча, ID outbox используется для доставки этого учёта
приватной стороне. После удаления экспортированной записи повтор ACK не
может создать её заново без соответствующего текущего pending.

Всё рабочее состояние, включая cursor, pending, retry, leases,
idempotency и outbox, записывается с `synchronous_commit=on`.
Диагностические логи также используют этот режим, хотя падение до записи
лога может оставить результат сетевой попытки неизвестным. Число синхронных
коммитов ограничено иначе: накопленный пустой прогресс в штатном режиме
сохраняется каждые 30 секунд либо покрывается следующим батчем. Checkpoints
и продление аренд выполняются ограниченными пачками (DESIGN.md §4, §8.1).
Условия Patroni, включая порядок включения режима и проверку фактического
`SHOW synchronous_commit` рабочих соединений, и восстановления — DESIGN.md
§10–11. Здесь нет таблицы meta с epoch.

## 7. Инварианты worker и курсора

Внутренняя позиция содержит version, network, event_type, mc_seqno и
`block_complete`. Для partial-позиции присутствуют lt и 32-байтный hash;
для полной они отсутствуют. Наружу — непрозрачное base64url-кодирование
версионированного формата; клиент не должен собирать его сам. Для jetton
используются lt/hash принимающей транзакции. API v1 не принимает cursor для
смены позиции подписки.

Claim, отзыв аренды при смене URL, фиксация сканирования, PATCH и ACK
сериализуются одной строкой подписки. Строка аренды при этих операциях
блокируется после подписки. Renew меняет только срок своей действующей аренды.
Операции, которым нужен владелец, сначала блокируют владельца, затем подписку;
внутри каждого уровня пачки сортируются по ID. Общий порядок блокировок
обязателен для избежания взаимных блокировок.

Продление аренды проверяет worker_id, ожидаемый lease_token и неистёкший срок.
RETURNING показывает фактически продлённые аренды; истёкшую или переданную
аренду нельзя оживить renew. Пачки имеют ограниченный размер и время ожидания
блокировок, чтобы одна задержка не остановила продление всех подписок.
Не продлённая вовремя аренда запрещает новые попытки и требует нового claim.

Перед сохранением нового диапазона worker проверяет active, отсутствие
suspended, отсутствие pending, сохранённый cursor, config_version,
владельца/token своей непросроченной аренды. Пустой диапазон проходит те же
проверки. Подписанный HTTP отправляется только после коммита pending.

`scan_cursor` в памяти может опережать сохранённый cursor только на проверенный
пустой диапазон. Для CAS и `pending.from_cursor` используется сохранённая
позиция; пустой префикс входит в интервал нового pending и подтверждается
вместе с ним. Без pending он фиксируется отдельным checkpoint. При смене
исходного cursor/config_version/lease_token прогресс в памяти
отбрасывается и состояние перечитывается. При сбоях checkpoint может
отставать больше 30 секунд; API не выдаёт позицию в памяти как сохранённую.

Ответ попытки применяется только при совпадении worker_id, lease_token,
webhook-id и действующей аренде. Новый worker продолжает тот же pending с
новым lease token. Pause/suspend не инвалидируют уже начатую попытку;
смена URL атомарно увеличивает lease_token и отзывает аренду, отсекая старый
ответ. Для продолжения после смены URL нужны resume, новый claim и чтение
текущего endpoint из БД.
Проверка только прежнего cursor недостаточна. БД не блокируется на время HTTP.

При ACK атомарно меняются cursor, pending и failing_since, а в outbox
создаётся batch_acked; при необходимости также меняется статус и записывается
управляющее событие. `webhook-id` — случайный устойчиво сохранённый ID, а не
хеш динамически пересобираемого тела. Retry сохраняет его и все байты тела.

## 8. Лимиты и оставшиеся проверки

Для v1 фиксируются: 1 000 адресов в create, 10 000 в PATCH, 100 opcode или
masters, 500 событий / целевые 1 MiB на батч, 15 секунд на HTTP, 24 часа до
автопаузы и на ротацию/идемпотентность, 7 дней диагностических логов, 512 байт
excerpt, 60 секунд аренды экспорта outbox. Числа для адресов и размера
батча нужно подтвердить нагрузочными измерениями до выпуска.

Отдельно проверяются стоимость поиска отправляющей транзакции через
messages, поведение на недоступном контексте jetton и отсутствие промежуточных
account state. Финальность и полнота payload не выводятся из успешного SQL
запроса без проверок общей библиотеки.

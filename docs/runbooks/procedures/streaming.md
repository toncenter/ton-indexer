# Процедура: сборка и развёртывание Streaming API

Streaming API состоит из `ton-trace-emulator` и `ton-streaming-go`, связанных
через Redis. Emulator читает локальную TON DB, эмулирует pending traces и
классифицирует actions встроенным MCH engine. Streaming API отправляет
обновления клиентам через SSE и WebSocket.

Если нужны только finalized-данные, вместо emulator можно использовать
`ton-finalized-streamer`. Он совместим с `ton-streaming-go` и поддерживает
несколько active-active instances с общим Redis; настройка и ограничения
описаны в [разделе 9](#9-ton-finalized-streamer-и-active-active-ha).

```text
TON-нода + ton-trace-emulator → Redis → ton-streaming-go → SSE / WebSocket
                                          ↑
                              Kvrocks работающего индексатора
                                 (address book и metadata)
```

Emulator запускается на машине синхронизированной TON-ноды. API можно вынести
на отдельную машину. Для address book и metadata нужен работающий индексатор:
развернуть его по [standard deployment runbook](../standard-deployment.md) и
указать его Kvrocks в параметрах streaming API. Без enrichment достаточно
TON-ноды, emulator, Redis и streaming API. Отдельный pending action classifier
не требуется.

Ниже используется запуск через systemd. Все `<PLACEHOLDERS>` нужно заменить
реальными значениями. Примеры предполагают TON DB в `/var/ton-work/db` и
OS-пользователя ноды `validator`.

## 1. Собрать и установить бинарники

Собирать оба компонента из одного checkout на машине с той же архитектурой и
совместимыми runtime libraries, что и целевые hosts. Установить зависимости
для Debian/Ubuntu:

```bash
sudo apt update
sudo apt install -y \
  build-essential cmake clang-21 \
  openssl libssl-dev zlib1g-dev libcurl4-openssl-dev \
  gperf git curl ccache libmicrohttpd-dev liblz4-dev \
  pkg-config libsecp256k1-dev libsodium-dev libhiredis-dev \
  python3-dev libpq-dev libjemalloc-dev automake autoconf libtool
```

В `PATH` должен быть Go версии из `ton-streaming-go/go.mod` или новее
(сейчас `1.26.3`). Для генерации MCH tables нужен Python 3.10 или новее и
директория `indexer` из этого же checkout. Первая сборка требует доступа в
интернет для загрузки зависимостей.

```bash
git clone --recursive https://github.com/toncenter/ton-indexer.git
cd ton-indexer
git checkout "<RELEASE_OR_COMMIT>"
git submodule update --init --recursive

CC=clang-21 CXX=clang++-21 cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DTON_USE_JEMALLOC=ON

cmake --build build --parallel 16 \
  --target ton-trace-emulator ton-streaming-go
```

Количество jobs подобрать под RAM и CPU build-машины. Использовать директорию
`build` в корне репозитория: Go CGO bindings ищут `ton-marker` по этому пути.
При уже существующем checkout начать с выбора версии и обновления submodules.

Результат сборки:

| Файл относительно корня checkout | Куда установить |
| --- | --- |
| `build/ton-index-worker/ton-trace-emulator/ton-trace-emulator` | TON node host: `/usr/local/bin/ton-trace-emulator` |
| `build/ton-streaming-go/ton-streaming-go` | API host: `/usr/local/bin/ton-streaming-go` |
| `build/ton-index-worker/ton-marker/libton-marker.so` | API host: `/usr/local/lib/libton-marker.so` |

На целевых hosts установить runtime dependencies. Для Ubuntu 24.04:

```bash
sudo apt update
sudo apt install -y \
  libpq5 libsecp256k1-1 libsodium23 libhiredis1.1.0 libcurl4 libjemalloc2
```

После переноса файлов на соответствующие hosts установить их. На TON node host:

```bash
sudo install -m 0755 ton-trace-emulator /usr/local/bin/ton-trace-emulator
```

На API host:

```bash
sudo install -m 0755 ton-streaming-go /usr/local/bin/ton-streaming-go
sudo install -m 0755 libton-marker.so /usr/local/lib/libton-marker.so
sudo ldconfig
```

Проверить `ldd /usr/local/bin/ton-trace-emulator` и
`ldd /usr/local/bin/ton-streaming-go` на соответствующих hosts: в выводе не
должно быть `not found`.

## 2. Подготовить TON-ноду

Нода должна быть синхронизирована. Emulator запускается от `validator`, чтобы
читать локальную DB. Его рабочая директория должна отличаться от директорий
index worker и других emulator instances:

```bash
sudo install -d -m 0750 -o validator -g validator /var/lib/ton-trace-emulator
sudo install -d -m 0750 -o root -g validator /etc/ton-trace-emulator
sudo install -m 0640 -o root -g validator \
  "<GLOBAL_CONFIG_PATH>" /etc/ton-trace-emulator/global.config.json
```

Global config должен соответствовать сети ноды. Для получения pending external
messages из overlay emulator использует `--global-config` и `--addr` вместе.
В примере ниже для него выделен отдельный UDP port `30303`: разрешить входящий
и исходящий UDP traffic и указать доступный извне IP. При NAT настроить
проброс этого порта. Не использовать уже занятый порт ноды.

Для `confirmed` событий нужен `validator-engine` с поддержкой
`--db-event-fifo`. Добавить к
существующим аргументам запуска ноды и перезапустить её штатным способом:

```text
--db-event-fifo /var/ton-work/streaming-events.fifo
```

Нода создаёт FIFO сама. Тот же путь передаётся emulator. У FIFO должен быть
один читатель: не подключать к нему одновременно index worker и emulator.
После запуска ноды проверить:

```bash
sudo -u validator test -p /var/ton-work/streaming-events.fifo
```

Без `--db-event-fifo` emulator использует polling и выдаёт только
pending/finalized. В таком режиме `/api/streaming/healthz` будет возвращать
`503`, поскольку проверяет также свежесть confirmed blocks.

## 3. Подготовить Redis

Использовать отдельный standalone Redis instance для streaming. Он хранит
временные snapshots и передаёт уведомления между emulator и API. Kvrocks
индексатора и Redis event-cache других сервисов не подходят для этой роли.

**При каждом старте `ton-trace-emulator` выполняет `FLUSHDB` выбранной Redis DB.** В ней
не должно быть данных других сервисов. На один streaming Redis instance
запускать один `ton-trace-emulator`; несколько streaming API instances могут
читать его одновременно. Для `ton-finalized-streamer` допустимы несколько
producers, и Redis при старте не очищается (см. раздел 9).
Redis Pub/Sub channels общие для всех логических DB instance,
поэтому разные номера DB не изолируют независимые streaming deployments.

На выделенном Redis host установить `redis-server` и `redis-tools`. В его
конфигурации задать:

```text
bind 127.0.0.1 <REDIS_PRIVATE_IP>
port 6379
protected-mode yes
requirepass <REDIS_PASSWORD>
save ""
appendonly no
```

Запустить Redis через systemd. Разрешить TCP `6379` только с TON node host и
API host. Проверить подключение с обоих hosts:

```bash
redis-cli -h "<REDIS_PRIVATE_IP>" -p 6379 --askpass ping
```

Ожидается `PONG`. Для примеров URI ниже использовать пароль без специальных
символов URI, например hex-строку. Emulator и API должны подключаться к одному
instance и одной DB.

## 4. Настроить компоненты

На TON node host создать `/etc/ton-trace-emulator/emulator.env`:

```dotenv
TON_DBROOT=/var/ton-work/db
TON_WORKDIR=/var/lib/ton-trace-emulator
TON_DB_EVENT_FIFO=/var/ton-work/streaming-events.fifo
TON_GLOBAL_CONFIG=/etc/ton-trace-emulator/global.config.json
TON_OVERLAY_ADDR=<NODE_PUBLIC_IP>:30303
REDIS_URI=tcp://:<REDIS_PASSWORD>@<REDIS_PRIVATE_IP>:6379/0
TON_NETWORK_ARGS=
```

На API host создать пользователя и директорию конфигурации:

```bash
sudo useradd --system --home /var/lib/ton-streaming --shell /usr/sbin/nologin ton-streaming
sudo install -d -m 0750 -o root -g ton-streaming /etc/ton-streaming
```

Создать `/etc/ton-streaming/streaming.env`:

```dotenv
REDIS_URI=redis://:<REDIS_PASSWORD>@<REDIS_PRIVATE_IP>:6379/0
STREAMING_PORT=8085
KVROCKS_ADDR=<KVROCKS_PRIVATE_IP>:6666
KVROCKS_PASSWORD=<KVROCKS_PASSWORD>
TON_NETWORK_ARGS=
```

`KVROCKS_ADDR` должен указывать на Kvrocks работающего индексатора той же
сети. API читает из него address book и metadata; эти данные должны уже
записываться индексатором. Разрешить API host доступ к TCP `6666`. Для запуска
без enrichment убрать `-kvrocks` и `-kvrocks-password` из API unit ниже.

В обоих env-файлах оставить `TON_NETWORK_ARGS=` для mainnet, а для testnet
указать `TON_NETWORK_ARGS=--testnet`. Также использовать global config и DB
соответствующей сети.

Ограничить доступ к env-файлам на соответствующих hosts:

```bash
sudo chown root:validator /etc/ton-trace-emulator/emulator.env
sudo chmod 0640 /etc/ton-trace-emulator/emulator.env
```

```bash
sudo chown root:ton-streaming /etc/ton-streaming/streaming.env
sudo chmod 0640 /etc/ton-streaming/streaming.env
```

Основные дополнительные параметры emulator:

| Параметр | Default | Назначение |
| --- | --- | --- |
| `--threads` | `7` | Scheduler threads. |
| `--mch-workers` | `1` | Workers встроенного классификатора actions. |
| `--trace-completed-ttl` | `30` | Хранение завершённого trace в Redis, секунды. |
| `--trace-root-pending-ttl` | `30` | Хранение trace с pending root, секунды. |
| `--trace-open-ttl` | `300` | Хранение незавершённого real trace с pending tail, секунды. |
| `--redis-channel` | Не задан | Дополнительный источник external messages: Redis Pub/Sub channel с base64 BOC. |

По умолчанию replay охватывает примерно последние 30 секунд событий;
фактическое окно зависит от retention и состояния trace. Это текущие
сохранённые snapshots, а не полная история обновлений.

Streaming API использует стандартные channels `streaming_transactions`,
`streaming_actions`, `streaming_account_states` и `invalidated_traces`.
Для этой конфигурации переопределять их не нужно.

## 5. Создать systemd services

На TON node host создать `/etc/systemd/system/ton-trace-emulator.service`:

```ini
[Unit]
Description=TON trace emulator
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=validator
Group=validator
WorkingDirectory=/var/lib/ton-trace-emulator
EnvironmentFile=/etc/ton-trace-emulator/emulator.env
ReadOnlyPaths=/var/ton-work/db
InaccessiblePaths=-/var/ton-work/keys -/var/ton-work/keys.old -/var/ton-work/db/keyring
ExecStart=/usr/local/bin/ton-trace-emulator \
  --db ${TON_DBROOT} \
  --working-dir ${TON_WORKDIR} \
  --db-event-fifo ${TON_DB_EVENT_FIFO} \
  --global-config ${TON_GLOBAL_CONFIG} \
  --addr ${TON_OVERLAY_ADDR} \
  --redis ${REDIS_URI} \
  $TON_NETWORK_ARGS
Restart=always
RestartSec=10
LimitNOFILE=1000000
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

На API host создать `/etc/systemd/system/ton-streaming-go.service`:

```ini
[Unit]
Description=TON Streaming API
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=ton-streaming
Group=ton-streaming
EnvironmentFile=/etc/ton-streaming/streaming.env
ExecStart=/usr/local/bin/ton-streaming-go \
  -redis ${REDIS_URI} \
  -port ${STREAMING_PORT} \
  -kvrocks ${KVROCKS_ADDR} \
  -kvrocks-password ${KVROCKS_PASSWORD} \
  $TON_NETWORK_ARGS
Restart=always
RestartSec=5
LimitNOFILE=100000
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

Сначала запустить TON-ноду и Redis, затем emulator на TON node host:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ton-trace-emulator
```

После этого запустить streaming API на API host:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ton-streaming-go
```

## 6. Настроить внешний доступ

API слушает TCP `8085` на всех интерфейсах. Разрешить доступ к этому порту
только с reverse proxy и monitoring hosts. На reverse proxy настроить TLS и
проксирование без изменения путей:

| Путь | Настройка |
| --- | --- |
| `/api/streaming/v2/sse` | Разрешить `POST`, отключить response buffering и caching. |
| `/api/streaming/v2/ws` | Разрешить WebSocket Upgrade. |
| `/api/streaming/healthz` | Использовать для проверки готовности backend. |

Установить idle timeout для streaming connections, например 60 секунд, и
сохранять keepalive traffic. WebSocket-клиенты должны регулярно отправлять
`ping`, как описано в [API-документации](../../toncenter_streaming_v2.md).


## 7. Проверить работу

На соответствующих hosts посмотреть состояние и логи:

```bash
systemctl status ton-trace-emulator
journalctl -u ton-trace-emulator -n 100 --no-pager
```

```bash
systemctl status ton-streaming-go
journalctl -u ton-streaming-go -n 100 --no-pager
curl -fsS http://127.0.0.1:8085/api/streaming/healthz
```

После выхода emulator в live mode ожидается HTTP `200` и `"ok": true`.
Healthcheck проверяет Redis и timestamps emulator: finalized masterchain block
и confirmed block должны быть не старше 15 секунд. `503` во время старта или
догоняющей обработки означает, что поток ещё не готов.
Для `ton-finalized-streamer` health содержит `mode=finalized`: проверяется
только свежесть finalized-блоков, отсутствие confirmed не вызывает `503`.

На API host открыть SSE-подписку на адрес с недавней активностью в выбранной
сети:

```bash
curl -N -X POST http://127.0.0.1:8085/api/streaming/v2/sse \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  --data '{
    "addresses": ["<ACCOUNT_ADDRESS>"],
    "types": ["transactions", "actions"],
    "min_finality": "pending",
    "replay_existing": true,
    "include_address_book": true,
    "include_metadata": true
  }'
```

Первым приходит `status: subscribed`, затем подходящие snapshots из Redis и
live events. При отсутствии событий соединение остаётся открытым и получает
keepalive comments. Пустой replay допустим, если подходящих данных в кеше нет.
Повторить проверку через внешний HTTPS endpoint, чтобы проверить proxy.

Для проверки WebSocket подключиться клиентом к
`wss://<STREAMING_HOST>/api/streaming/v2/ws` и отправить:

```json
{
  "operation": "subscribe",
  "id": "check-1",
  "addresses": ["<ACCOUNT_ADDRESS>"],
  "types": ["transactions"],
  "min_finality": "pending",
  "replay_existing": true
}
```

Ожидается `{"id":"check-1","status":"subscribed"}`, затем события.
Проверить наличие address book и metadata в SSE events для известного
индексатору адреса; отсутствие данных для неизвестного адреса допустимо.

## 8. Обновление и диагностика

Ниже описано обновление связки с обычным `ton-trace-emulator`. Ограничения
поочерёдного рестарта finalized producers приведены в разделе 9.

Для обновления собрать оба бинарника и `libton-marker.so` из одной версии.
Остановить streaming API, затем emulator на соответствующих hosts, заменить
файлы командами из пункта 1 и выполнить `ldconfig` на API host. Если менялись
units, выполнить `systemctl daemon-reload`. Запустить emulator, затем API и
повторить проверки из пункта 7.

Рестарт emulator очищает временный Redis state; replay накопится заново.
Клиенты должны переподключиться и при необходимости запросить
`replay_existing: true`. События за длительный перерыв replay не восстановит.
Рестарт только streaming API не очищает Redis, но разрывает его соединения.

| Симптом | Что проверить |
| --- | --- |
| `libton-marker.so: cannot open shared object file` | Установку shared library, `ldconfig` и вывод `ldd`. |
| Emulator не запускается с ошибкой Redis/FLUSHDB | URI, пароль, доступность выделенного Redis и права на выбранную DB. |
| Нет pending events | Global config, `--addr`, доступность UDP port или источник `--redis-channel`. |
| Нет confirmed events, healthcheck возвращает `503` | Поддержку DB events в ноде, одинаковый FIFO path, права и отсутствие второго читателя. |
| Finalized block в healthcheck устарел | Синхронизацию TON-ноды, чтение DB и отставание emulator. |
| Нет address book или metadata | Работу индексатора, его Kvrocks, сеть и флаги enrichment в подписке. |
| WebSocket закрывается с `1013` / `slow consumer`, SSE обрывается | Скорость чтения клиента и proxy buffering. Уменьшить подписку; после reconnect запросить replay. |

Emulator также сохраняет runtime statistics в
`/var/lib/ton-trace-emulator/stats`; по умолчанию interval равен 30 секундам.

## 9. ton-finalized-streamer и active-active HA

`ton-finalized-streamer` читает только finalized-блоки локальной TON-ноды,
собирает реальные трейсы и классифицирует actions тем же MCH engine.
Pending/confirmed и эмуляции будущих продолжений нет. Snapshots, actions,
account states, индексы и Pub/Sub-уведомления записываются в том же формате,
что у `ton-trace-emulator`, с дополнительными полями для finalized-режима.
`ton-streaming-go` из той же версии репозитория читает их через существующие
SSE/WebSocket API и `replay_existing`; отдельный streaming backend не нужен.

Обновления открытого трейса тоже сохраняются в Redis. Подписки на `trace`,
`actions` и `transactions` с `min_finality: "finalized"` получают только
завершённые трейсы: API учитывает поле `trace_complete`. Трейсы без известного
корня не публикуются. Account states не ждут завершения всего трейса.

### Запуск finalized-варианта

На build host из checkout, подготовленного в пункте 1, собрать:

```bash
cmake --build build --parallel 16 --target ton-finalized-streamer
```

Перенести `build/ton-index-worker/ton-trace-emulator/ton-finalized-streamer`
на TON node host и установить в `/usr/local/bin/ton-finalized-streamer`.
Использовать env-файл и systemd unit из пунктов 4–5, заменив `ExecStart`:

```ini
ExecStart=/usr/local/bin/ton-finalized-streamer \
  --db ${TON_DBROOT} \
  --working-dir ${TON_WORKDIR} \
  --redis ${REDIS_URI} \
  $TON_NETWORK_ARGS
```

Global config, overlay address и UDP port для этого бинаря не нужны.
`--db-event-fifo` можно добавить для пробуждения по событиям ноды; без него
работает polling. Правило одного читателя FIFO сохраняется. API unit менять
не требуется; в проверочных подписках указать `min_finality: "finalized"`.

Каждый запуск начинает с текущего последнего masterchain-блока и продолжает
работу без конечного seqno. История за время простоя и прежние открытые трейсы
не восстанавливаются; параметров `from`/`to` нет. Redis при старте сохраняется.
Replay TTL задаётся `--trace-completed-ttl`, по умолчанию 30 секунд.

### Active-active и ограничения

Запускать два или более `ton-finalized-streamer` на отдельных синхронизированных
TON node hosts, с одной версией кода, сетью и настройками. У каждого процесса
своя рабочая директория. Все producers и API instances подключаются к одной
Redis DB. Каждый producer обрабатывает весь доступный ему поток; Redis
дедуплицирует записи по ключу трейса и masterchain seqno. Выборов лидера нет.
Отказ одного producer сам по себе не разрывает SSE/WebSocket-соединения API.

- **Длинные трейсы при рестартах.** Риск касается трейсов, которые остаются
  открытыми между рестартами копий: это редкие, часто вырожденные случаи.
  Если все копии, знавшие корень, перезапустятся до завершения такого трейса,
  его завершённое событие может потеряться.
- **Очистка индексов пока зависит от памяти producer.** Если все знавшие трейс
  копии перезапустились до cleanup, snapshot истечёт, но ссылки в адресных и
  action-индексах могут остаться. Это ограничение текущей реализации.
- **Общий health не проверяет каждую копию.** Он отражает свежесть блоков,
  обработанных хотя бы одним producer. Отставание и ошибки каждого процесса
  нужно отслеживать отдельно.
- **HA producers не обеспечивает HA Redis.** Нативные Sentinel discovery и
  Redis Cluster в producer не поддержаны. Для переключения primary нужен
  стабильный endpoint, например HAProxy, направляющий подключения на master.
  Переключение Redis и возможная потеря кэша/уведомлений требуют отдельной проверки.
- **Не смешивать источники в одном Redis deployment.** Обычный
  `ton-trace-emulator` очищает DB при старте, а Pub/Sub channels общие даже для
  разных номеров DB. Также не смешивать версии finalized producer с разными
  протоколами записи.
- **Трейсы больше 1000 узлов пропускаются.** Их завершённый trace/actions
  результат через этот источник не гарантируется; остальные трейсы
  продолжают обрабатываться.

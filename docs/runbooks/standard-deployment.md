# Runbook: стандартное развёртывание через systemd

Этот runbook описывает развёртывание без Docker и без автоматического
failover. Все постоянно работающие процессы запускаются как systemd services.

Используются как минимум четыре машины:

- PostgreSQL host;
- Kvrocks host;
- TON node host, на котором вместе с нодой запускается
  `ton-index-postgres`;
- API host, на котором запускается `ton-index-go`.

Worker обязательно должен находиться на машине TON-ноды: он читает TON DB по
локальному пути. PostgreSQL и Kvrocks, наоборот, не следует размещать на этой
машине из-за конкуренции за RAM и disk I/O. API не использует TON DB и
запускается на отдельной машине. Action classifier опционален.

Ниже используются placeholders `<POSTGRES_PRIVATE_IP>`,
`<KVROCKS_PRIVATE_IP>`, `<NODE_PRIVATE_IP>`, `<API_PRIVATE_IP>` и `<S>`. Их
нужно заменить реальными значениями.

## 1. Подготовить сеть

Все соединения между компонентами должны проходить по private network.

На PostgreSQL host разрешить TCP `5432` только с TON node host, API host и, если
он используется, classifier host. На Kvrocks host аналогично разрешить TCP
`6666`. Не публиковать эти порты в интернет.

TON DB не должна экспортироваться по сети: её читает только локальный worker.

## 2. Собрать бинарники

Сборку проще всего выполнять на TON node host либо на отдельной build-машине с
той же архитектурой и совместимыми системными библиотеками.

Для сборки нужны CMake, `make`, `clang-21`, `clang++-21`, development packages
PostgreSQL, hiredis, jemalloc и остальные зависимости проекта. Поскольку
корневой CMake project также включает Go-компоненты, Go compiler должен быть
доступен в `PATH` уже на этапе конфигурации. Для сборки `ton-index-go` нужна
версия Go, указанная в `ton-index-go/go.mod`, или более новая. Дистрибутивный
пакет `golang-go` может оказаться старее.

Для Debian/Ubuntu основной набор пакетов выглядит так:

```bash
sudo apt update
sudo apt install -y \
  build-essential cmake clang-21 \
  openssl libssl-dev zlib1g-dev libcurl4-openssl-dev \
  gperf git curl ccache libmicrohttpd-dev liblz4-dev \
  pkg-config libsecp256k1-dev libsodium-dev libhiredis-dev \
  python3-dev libpq-dev postgresql-client libjemalloc-dev \
  automake autoconf libtool
```

Репозиторий нужно скачать вместе с submodules:

```bash
git clone --recursive https://github.com/toncenter/ton-indexer.git
cd ton-indexer
```

Если репозиторий уже скачан:

```bash
git submodule update --init --recursive
```

Создать директорию `build` и выполнить Release-сборку с jemalloc:

```bash
mkdir -p build
cd build

CC=clang-21 CXX=clang++-21 cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DTON_USE_JEMALLOC=ON \
  ..

make -j16 \
  ton-smc-scanner \
  ton-index-postgres \
  ton-index-postgres-migrate \
  ton-index-go
```

Количество jobs в `make -j16` нужно подобрать под build-машину. При первой
сборке `ton-index-go` CMake скачивает Go modules и устанавливает Swagger
generator, поэтому build-машине нужен доступ в интернет.

Установить C++ binaries на TON node host:

```bash
sudo install -m 0755 \
  ton-index-worker/ton-smc-scanner/ton-smc-scanner \
  ton-index-worker/ton-index-postgres/ton-index-postgres \
  ton-index-worker/ton-index-postgres/ton-index-postgres-migrate \
  /usr/local/bin/
```

Эта команда выполняется из директории `build`. Если сборка выполнялась на
другой машине, перенести эти три файла на TON node host и установить туда же.

## 3. Подготовить PostgreSQL host

Установить PostgreSQL штатным способом для выбранного Linux-дистрибутива и
запустить его через systemd. В `postgresql.conf` слушать только private address:

```conf
listen_addresses = '<POSTGRES_PRIVATE_IP>'
```

В `pg_hba.conf` разрешить нужную базу только конкретным hosts. Минимальное
правило для worker:

```conf
host  ton_index  ton_indexer  <NODE_PRIVATE_IP>/32  scram-sha-256
```

Создать пользователя и базу:

```sql
CREATE ROLE ton_indexer LOGIN PASSWORD '<POSTGRES_PASSWORD>';
CREATE DATABASE ton_index OWNER ton_indexer;
```

Для API и classifier в production лучше создать отдельные роли и выдать им
только необходимые права, а не использовать owner базы.

### Собрать и установить расширение pgton

Его нужно собрать против той же major-версии
PostgreSQL, которая запущена на этом host. Нельзя собирать расширение, например,
с headers PostgreSQL 18 и устанавливать его в PostgreSQL 17.

Установить build dependencies. В примере используется PostgreSQL 18; для другой
major-версии заменить `18` во всех командах ниже:

```bash
sudo apt update
sudo apt install -y build-essential cmake postgresql-server-dev-18
```

На PostgreSQL host должен быть доступен тот же checkout `ton-indexer`, из
которого собираются остальные компоненты. Из корня репозитория выполнить:

```bash
mkdir -p build-pgton
cd build-pgton

cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DPG_CONFIG=/usr/lib/postgresql/18/bin/pg_config \
  ../ton-index-worker/pgton

cmake --build . --target pgton -j16
sudo cmake --install . --verbose
```

`PG_CONFIG` указан явно, чтобы при наличии нескольких PostgreSQL versions CMake
не выбрал другую major-версию из `PATH`. Install rules расширения используют
пути, возвращённые этим `pg_config`, и устанавливают:

- `pgton.so` в PostgreSQL `pkglibdir`;
- `pgton.control` и `pgton--0.1.sql` в `sharedir/extension`.

После установки файлов создать extension в базе `ton_index` от имени
PostgreSQL superuser:

```bash
sudo -u postgres psql -d ton_index -c 'CREATE EXTENSION pgton;'
```

Проверить, что PostgreSQL видит расширение:

```bash
sudo -u postgres psql -d ton_index \
  -c "select extname, extversion from pg_extension where extname = 'pgton';"
```

`pgton` должен быть установлен и создан в базе до первого запуска миграций.
Установка extension поверх уже созданной domain-based схемы не переводит
существующие колонки на custom types автоматически.

## 4. Подготовить Kvrocks host

Для согласованности с Docker Compose рекомендуется зафиксировать Kvrocks
`2.14.0`. Apache Kvrocks распространяет эту версию как
[source release](https://kvrocks.apache.org/download/); согласно
[официальной инструкции](https://kvrocks.apache.org/docs/getting-started/),
сборка выполняется командой `./x.py build`, после чего binary находится в
`build/`.

```bash
git clone https://github.com/apache/kvrocks.git
cd kvrocks
git checkout 2.14.0
./x.py build
sudo install -m 0755 build/kvrocks /usr/local/bin/kvrocks
```

Создать отдельного системного пользователя и директории:

```bash
sudo useradd --system --home /var/lib/kvrocks --shell /usr/sbin/nologin kvrocks
sudo install -d -m 0750 -o kvrocks -g kvrocks /var/lib/kvrocks
sudo install -d -m 0750 -o root -g kvrocks /etc/kvrocks
```

Скопировать `kvrocks.conf` из исходников в `/etc/kvrocks/kvrocks.conf` и задать
как минимум:

```conf
bind <KVROCKS_PRIVATE_IP>
port 6666
requirepass <KVROCKS_PASSWORD>
db-name ton-index
dir /var/lib/kvrocks
```

Файл конфигурации содержит пароль, поэтому оставить его доступным только root и
группе `kvrocks`:

```bash
sudo chown root:kvrocks /etc/kvrocks/kvrocks.conf
sudo chmod 640 /etc/kvrocks/kvrocks.conf
```

Создать `/etc/systemd/system/kvrocks.service`:

```ini
[Unit]
Description=Apache Kvrocks
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=kvrocks
Group=kvrocks
WorkingDirectory=/var/lib/kvrocks
ExecStart=/usr/local/bin/kvrocks -c /etc/kvrocks/kvrocks.conf
Restart=always
RestartSec=5
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

Запустить Kvrocks:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now kvrocks
```

<a id="prepare-ton-node-host"></a>

## 5. Подготовить TON node host

TON-нода к этому моменту должна быть запущена и синхронизирована. Примеры ниже
предполагают, что её DB находится в `/var/ton-work/db`.

Создать конфигурационную и рабочую директории worker:

```bash
sudo install -d -m 0750 -o root -g validator /etc/ton-indexer
sudo install -d -m 0750 -o validator -g validator /var/lib/ton-indexer/worker
```

`/var/ton-work` и TON DB принадлежат пользователю `validator`. Поэтому
`ton-index-postgres`, `ton-smc-scanner` и другие процессы, которые читают
локальную TON DB, также запускаются от OS-пользователя `validator`.

Создать `/etc/ton-indexer/pgpass`:

```text
<POSTGRES_PRIVATE_IP>:5432:ton_index:ton_indexer:<POSTGRES_PASSWORD>
```

Host в этом файле должен совпадать с host в PostgreSQL DSN. Ограничить доступ к
файлу:

```bash
sudo chown validator:validator /etc/ton-indexer/pgpass
sudo chmod 600 /etc/ton-indexer/pgpass
```

## 6. Выполнить миграции

Миграции выполняются один раз после установки новой версии и до запуска
`ton-smc-scanner`, `ton-index-postgres`, API или classifier:

```bash
sudo -u validator env PGPASSFILE=/etc/ton-indexer/pgpass \
  /usr/local/bin/ton-index-postgres-migrate \
  --pg postgresql://ton_indexer@<POSTGRES_PRIVATE_IP>:5432/ton_index \
  --custom-types
```

Эта команда создаст необходимые таблицы и индексы.

## 7. Настроить worker service

Создать `/etc/ton-indexer/worker.env`:

```dotenv
TON_DBROOT=/var/ton-work/db
TON_WORKDIR=/var/lib/ton-indexer/worker
POSTGRES_DSN=postgresql://ton_indexer@<POSTGRES_PRIVATE_IP>:5432/ton_index
KVROCKS_DSN=tcp://<KVROCKS_PRIVATE_IP>:6666/0
KVROCKS_PASSWORD=<KVROCKS_PASSWORD>
TON_WORKER_FROM=<S>
TON_NETWORK_ARGS=--testnet
```

Для mainnet оставить `TON_NETWORK_ARGS=` пустым. Пароли лучше генерировать без
пробелов и shell metacharacters. Закрыть доступ к файлу:

```bash
sudo chown root:validator /etc/ton-indexer/worker.env
sudo chmod 640 /etc/ton-indexer/worker.env
```

Создать `/etc/systemd/system/ton-index-postgres.service`:

```ini
[Unit]
Description=TON Index PostgreSQL worker
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=validator
Group=validator
WorkingDirectory=/var/lib/ton-indexer
EnvironmentFile=/etc/ton-indexer/worker.env
Environment=PGPASSFILE=/etc/ton-indexer/pgpass
ReadOnlyPaths=/var/ton-work/db
InaccessiblePaths=-/var/ton-work/keys -/var/ton-work/keys.old -/var/ton-work/db/keyring
ExecStart=/usr/local/bin/ton-index-postgres \
  --db ${TON_DBROOT} \
  --working-dir ${TON_WORKDIR} \
  --pg ${POSTGRES_DSN} \
  --kvrocks ${KVROCKS_DSN} \
  --kvrocks-password ${KVROCKS_PASSWORD} \
  --from ${TON_WORKER_FROM} \
  $TON_NETWORK_ARGS
Restart=always
RestartSec=10
LimitNOFILE=1000000
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

Загрузить unit, но пока не запускать и не включать автозапуск worker:

```bash
sudo systemctl daemon-reload
```

`TON_WORKER_FROM` задаёт старт только для пустой базы. После первого запуска
worker продолжает с progress в PostgreSQL; изменение `TON_WORKER_FROM` не
перематывает существующий индекс.

`LimitNOFILE=1000000` — systemd-эквивалент `ulimit -n 1000000`. Системный
предел `fs.nr_open` на TON node host не должен быть ниже этого значения.
`ReadOnlyPaths` не позволяет worker изменять живую TON DB из своего mount
namespace, а `InaccessiblePaths` закрывает ему каталоги ключей ноды.

<a id="live-only"></a>

## 8. Вариант A: индексирование только вперёд

Этот вариант не запускает `ton-smc-scanner` и не загружает архивную историю.

1. Выбрать masterchain seqno `<S>` и записать его в `TON_WORKER_FROM`.
2. Включить автозапуск и запустить worker:

   ```bash
   sudo systemctl enable --now ton-index-postgres
   ```

3. После запуска не менять `<S>` для этого окружения. Ранних блоков до `<S>` в
   PostgreSQL не будет, а Kvrocks будет наполняться только состояниями
   аккаунтов, изменившихся после старта.

<a id="shard-state-bootstrap"></a>

## 9. Вариант B: предварительный shard state bootstrap

1. Оставить `ton-index-postgres` остановленным.
2. После миграций выполнить общую
   [процедуру shard state bootstrap](procedures/shard-state-bootstrap.md) для
   выбранного masterchain seqno `<S>`.
3. После успешного scanner run записать в `TON_WORKER_FROM` стартовую точку,
   указанную процедурой, и только затем включить автозапуск и запустить worker:

   ```bash
   sudo systemctl enable --now ton-index-postgres
   ```

Scanner запускается как одноразовая команда, а не как постоянно работающий
systemd service.

## 10. Развернуть API на отдельной машине

`ton-index-go` не читает TON DB. Ему нужны сетевой доступ к PostgreSQL и
Kvrocks, поэтому API следует запускать на отдельном host.

### Создать read-only пользователя PostgreSQL

После выполнения миграций создать отдельную роль API на PostgreSQL host:

```sql
CREATE ROLE ton_index_api
  LOGIN
  PASSWORD '<API_POSTGRES_PASSWORD>';

ALTER ROLE ton_index_api SET default_transaction_read_only = on;

GRANT CONNECT ON DATABASE ton_index TO ton_index_api;
GRANT USAGE ON SCHEMA public TO ton_index_api;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ton_index_api;

ALTER DEFAULT PRIVILEGES FOR ROLE ton_indexer IN SCHEMA public
  GRANT SELECT ON TABLES TO ton_index_api;
```

`GRANT SELECT ON ALL TABLES` открывает уже созданные миграциями таблицы, а
`ALTER DEFAULT PRIVILEGES` автоматически выдаёт `SELECT` на новые таблицы,
которые в следующих версиях создаст owner `ton_indexer`.

Добавить в `pg_hba.conf` доступ только с API host и перезагрузить конфигурацию
PostgreSQL:

```conf
host  ton_index  ton_index_api  <API_PRIVATE_IP>/32  scram-sha-256
```

```bash
sudo systemctl reload postgresql
```

### Установить ton-index-go

`ton-index-go` зависит от shared library `ton-marker`. Из директории `build`
нужно перенести на API host два файла:

```text
ton-index-go/ton-index-go
ton-index-worker/ton-marker/libton-marker.so
```

На API host установить runtime dependencies. Имена пакетов ниже соответствуют
Ubuntu 24.04 и могут отличаться в другом дистрибутиве:

```bash
sudo apt update
sudo apt install -y \
  libpq5 libsecp256k1-1 libsodium23 libhiredis1.1.0 libcurl4 libjemalloc2
```

Установить binary и shared library:

```bash
sudo install -m 0755 ton-index-go /usr/local/bin/ton-index-go
sudo install -m 0755 libton-marker.so /usr/local/lib/libton-marker.so
sudo ldconfig
```

Создать системного пользователя и директории:

```bash
sudo useradd --system --home /var/lib/ton-index-api --shell /usr/sbin/nologin ton-index-api
sudo install -d -m 0750 -o ton-index-api -g ton-index-api /var/lib/ton-index-api
sudo install -d -m 0750 -o root -g ton-index-api /etc/ton-index-api
```

Создать `/etc/ton-index-api/pgpass`:

```text
<POSTGRES_PRIVATE_IP>:5432:ton_index:ton_index_api:<API_POSTGRES_PASSWORD>
```

Ограничить доступ к файлу:

```bash
sudo chown ton-index-api:ton-index-api /etc/ton-index-api/pgpass
sudo chmod 600 /etc/ton-index-api/pgpass
```

Создать `/etc/ton-index-api/api.env`:

```dotenv
POSTGRES_DSN=postgresql://ton_index_api@<POSTGRES_PRIVATE_IP>:5432/ton_index
KVROCKS_ADDR=<KVROCKS_PRIVATE_IP>:6666
KVROCKS_PASSWORD=<KVROCKS_PASSWORD>
TON_API_BIND=<API_PRIVATE_IP>:8081
TON_API_MAX_CONNS=100
TON_NETWORK_ARGS=-testnet
```

Для mainnet оставить `TON_NETWORK_ARGS=` пустым. Ограничить доступ к файлу:

```bash
sudo chown root:ton-index-api /etc/ton-index-api/api.env
sudo chmod 640 /etc/ton-index-api/api.env
```

Создать `/etc/systemd/system/ton-index-go.service`:

```ini
[Unit]
Description=TON Index API
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=ton-index-api
Group=ton-index-api
WorkingDirectory=/var/lib/ton-index-api
EnvironmentFile=/etc/ton-index-api/api.env
Environment=PGPASSFILE=/etc/ton-index-api/pgpass
ExecStart=/usr/local/bin/ton-index-go \
  -pg ${POSTGRES_DSN} \
  -kvrocks ${KVROCKS_ADDR} \
  -kvrocks-password ${KVROCKS_PASSWORD} \
  -bind ${TON_API_BIND} \
  -maxconns ${TON_API_MAX_CONNS} \
  $TON_NETWORK_ARGS
Restart=always
RestartSec=5
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

Запустить API:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ton-index-go
```

## 11. Проверить работу

Посмотреть состояние и логи worker:

```bash
systemctl status ton-index-postgres
journalctl -u ton-index-postgres -f
```

Проверить progress в PostgreSQL:

```bash
sudo -u validator env PGPASSFILE=/etc/ton-indexer/pgpass \
  psql postgresql://ton_indexer@<POSTGRES_PRIVATE_IP>:5432/ton_index \
  -c 'select finalized_mc_seqno, updated_at from _ton_indexer_progress where id = 1;'
```

`finalized_mc_seqno` должен расти и приближаться к head TON-ноды. В логах не
должно быть повторяющихся ошибок подключения к PostgreSQL или Kvrocks.

Проверить API:

```bash
systemctl status ton-index-go
journalctl -u ton-index-go -f
curl -fsS http://<API_PRIVATE_IP>:8081/api/v3/masterchainInfo
```

Swagger доступен по адресу:

```text
http://<API_PRIVATE_IP>:8081/api/v3/index.html
```

Если нужны actions, отдельно выполнить
[процедуру запуска action classifier](procedures/action-classifier.md):
classifier является опциональным сервисом.

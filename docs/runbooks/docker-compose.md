# Runbook: Docker Compose без истории и shard state snapshot

Этот вариант предназначен для локальной разработки и тестовых окружений. На
одной машине запускаются:

- PostgreSQL;
- Kvrocks;
- Redis `event-cache`;
- `ton-index-postgres` (`index-worker`);
- `ton-index-go` (`index-api`);
- action classifier (`event-classifier`).

`ton-smc-scanner`, metadata и emulation services не запускаются. Индексация
начинается с `TON_WORKER_FROM` и продолжается только вперёд. Поэтому в
PostgreSQL не будет блоков до этой точки, а в Kvrocks не будет полного состояния
аккаунтов, которые не изменялись после неё.

## 1. Подготовить TON-ноду

TON-нода должна быть запущена и синхронизирована на той же машине, где будет
работать Docker Compose. Worker читает TON DB по локальному пути; подключение к
TON DB на другой машине не поддерживается.

По умолчанию Compose ожидает TON DB в `/var/ton-work/db`. Другой путь можно
задать через `TON_WORKER_DBROOT` в `.env`.

## 2. Скачать репозиторий

```bash
git clone --recursive https://github.com/toncenter/ton-indexer.git
cd ton-indexer
```

Если репозиторий уже скачан:

```bash
git submodule update --init --recursive
```

## 3. Настроить окружение

Скопировать пример конфигурации:

```bash
cp .env.example .env
mkdir -p private
```

Заполнить в `.env`:

- `TON_WORKER_FROM` — masterchain seqno, с которого начнётся индексация;
- `TON_WORKER_DBROOT` — локальный путь к TON DB;
- `TON_INDEXER_IS_TESTNET=1` для testnet или `0` для mainnet;
- `KVROCKS_PASSWORD` — пароль Kvrocks;
- `POSTGRES_PASSWORD_FILE` — путь к файлу с паролем PostgreSQL. По умолчанию
  используется `private/postgres_password`.

Записать пароль PostgreSQL в указанный файл без перевода строки:

```bash
printf '%s' 'CHANGE_ME' > private/postgres_password
chmod 600 .env private/postgres_password
```

> **Опционально.** По умолчанию PostgreSQL и Kvrocks публикуются на всех сетевых
> интерфейсах хоста. Если доступ к ним нужен только с этой машины, указать в
> `.env`:
>
> ```dotenv
> POSTGRES_PUBLISH_PORT=127.0.0.1:5432
> KVROCKS_PUBLISH_PORT=127.0.0.1:6666
> ```


`TON_WORKER_FROM` используется только при первом запуске на пустой базе. После
этого worker продолжает с progress, сохранённого в PostgreSQL; изменение
`TON_WORKER_FROM` в `.env` не меняет точку продолжения.

## 4. Собрать образы

```bash
docker compose build
```

## 5. Запустить сервисы

```bash
docker compose up -d
```

Compose самостоятельно:

1. Запустит PostgreSQL, Kvrocks и `event-cache`.
2. Выполнит `run-migrations`.
3. После успешных миграций запустит index worker и API.
4. Запустит `event-classifier` как часть стандартного stack.

## 6. Проверить запуск

Показать состояние контейнеров:

```bash
docker compose ps -a
```

`run-migrations` должен завершиться с exit code `0`. Остальные основные services
должны находиться в состоянии `Up`.

Показать логи worker и classifier:

```bash
docker compose logs --tail=200 index-worker event-classifier
```

Следить за ними в реальном времени:

```bash
docker compose logs -f index-worker event-classifier
```

Проверить API:

```bash
curl -fsS http://127.0.0.1:8081/api/v3/masterchainInfo
```

Swagger доступен по адресу:

```text
http://127.0.0.1:8081/api/v3/index.html
```

Если `TON_INDEXER_API_PORT` изменён, использовать настроенный порт вместо
`8081`.

## 7. Подключиться к PostgreSQL

Для интерактивных запросов использовать `psql` внутри PostgreSQL container:

```bash
docker compose exec postgres sh -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Если `psql` установлен непосредственно на хосте, подключиться через published
port можно так:

```bash
PGPASSWORD="$(tr -d '\r\n' < private/postgres_password)" \
  psql -h 127.0.0.1 -p 5432 -U postgres -d ton_index
```

Если в `.env` изменены `POSTGRES_USER`, `POSTGRES_DB` или локальная часть
`POSTGRES_PUBLISH_PORT`, использовать эти значения в команде подключения.

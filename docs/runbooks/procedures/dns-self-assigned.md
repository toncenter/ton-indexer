# Включение фильтрации self-assigned DNS в address book

По умолчанию `addressBook` показывает DNS-домен, которым адрес владеет и чей `wallet record` указывает на этот адрес, включая «подаренные» домены. Флаг `-dns-self-assigned-only` оставляет только домены, для которых адрес сам сделал последнюю запись `wallet record`. Последний автор записи хранится в Kvrocks (`ton-index:v1:dns_wallet_writers:<item>`).

## 1. Обновить воркеры

Развернуть актуальную версию `ton-index-postgres` (флаги запуска не меняются). Воркер начнёт записывать авторов новых записей в Kvrocks.

## 2. Запустить backfill истории

Утилита `dns-wallet-writers` переносит историю из PostgreSQL с полной историей (cold DB) в Kvrocks master. Запуск безопасен для работающего воркера и идемпотентен. В mainnet занимает около 20-30 минут.

```bash
cd ton-index-go
go build -o dns-wallet-writers ./cmd/dns-wallet-writers

./dns-wallet-writers \
  -pg postgresql://<USER>@<POSTGRES_HOST>:5432/ton_index \
  -kvrocks <KVROCKS_MASTER_HOST>:6666 \
  -kvrocks-password <KVROCKS_PASSWORD>
```

Опционально: `-dry-run` для прогона без записи, `-out <file>` для сохранения результата в JSONL.

Успешное завершение в логе: `writers <N>, applied <M>, target written true`.

## 3. Включить флаг в API

Добавить `-dns-self-assigned-only` во **все три** сервиса: `ton-index-go`, `ton-emulate-go` и `ton-streaming-go`.

> **Важно:** нужен `-kvrocks` или `-kvrocks-sentinels`. В `ton-index-go` флаг несовместим с `-cache-service-url`. Иначе сервис не запустится.
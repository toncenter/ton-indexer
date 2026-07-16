# Процедура: hot/cold PostgreSQL

Эта процедура разделяет данные индексатора между двумя PostgreSQL databases:

- **cold PostgreSQL** хранит всю историю начиная с masterchain seqno `1`;
- **hot PostgreSQL** хранит только последние `N` masterchain blocks;
- единственный Kvrocks остаётся общим для всей установки;
- `ton-index-postgres` и continuous action classifier пишут только в hot;
- logical replication переносит изменения из hot в cold;
- `ton-index-go` получает оба DSN и сам выбирает нужную database.

Worker создаёт в hot range partitions, а после истечения retention удаляет
старые partitions целиком. Их удаление не передаётся в cold: PostgreSQL logical
replication не реплицирует DDL. Поэтому cold продолжает хранить полную историю.

Ниже `S` — последний masterchain seqno, который уже полностью загружен в cold.
Если [процедура полной истории](full-history.md) закончилась на `NEW_S`, далее
следует считать `S = NEW_S`.

Процедура рассчитана на PostgreSQL 17 или новее. В примерах standard deployment
используется PostgreSQL 18.

## 1. Подготовить cold PostgreSQL до блока S

Заполнить cold PostgreSQL и общий Kvrocks по
[процедуре полной истории](full-history.md): выполнить shard state bootstrap,
проиндексировать диапазон `1..S`, при необходимости классифицировать историю и
создать финальные indexes.

Проверить верхнюю границу в cold:

```sql
SELECT max(seqno) AS cold_mc_seqno
FROM blocks
WHERE workchain = -1;
```

Результат должен быть равен `S`. До завершения переключения не запускать live
worker и continuous classifier. Archive workers и `ton-smc-scanner` после
подготовки cold больше не нужны.

## 2. Подготовить пустой hot PostgreSQL

На отдельной PostgreSQL машине выполнить разделы standard deployment по
подготовке PostgreSQL: установить ту же major-версию PostgreSQL, собрать и
установить `pgton`, создать database `ton_index` и роль `ton_indexer`.

В hot PostgreSQL включить logical replication:

```conf
wal_level = logical
```

`max_replication_slots` и `max_wal_senders` должны учитывать как минимум один
дополнительный logical slot `hot_to_cold`. Изменения параметров, требующих
restart PostgreSQL, применить до продолжения.

С подготовленного TON node или bootstrap host создать полную схему hot обычным
migration run:

```bash
sudo -u validator env PGPASSFILE=/etc/ton-indexer/pgpass \
  /usr/local/bin/ton-index-postgres-migrate \
  --pg postgresql://ton_indexer@<HOT_POSTGRES_HOST>:5432/ton_index \
  --custom-types
```

Hot должен быть новым и не содержать progress или blockchain data:

```sql
SELECT count(*) AS progress_rows FROM _ton_indexer_progress;

SELECT count(*) AS masterchain_blocks
FROM blocks
WHERE workchain = -1;
```

Оба запроса должны вернуть `0`. Не очищать уже использовавшуюся database для
этой процедуры: создать новую hot database.

Схемы hot и cold должны быть одной версии. Logical replication не переносит
schema migrations, extensions и DDL, поэтому при последующих обновлениях
миграции нужно выполнять на обеих databases.

## 3. Скопировать validator snapshots из cold в hot

Пять validator tables обновляются по мере live-индексации. Cold уже содержит их
состояние на блоке `S`, поэтому до запуска worker это состояние нужно один раз
скопировать в пустой hot:

```bash
sudo -u validator /bin/bash

export PGPASSFILE=/etc/ton-indexer/pgpass
export COLD_DSN='postgresql://ton_indexer@<COLD_POSTGRES_HOST>:5432/ton_index'
export HOT_DSN='postgresql://ton_indexer@<HOT_POSTGRES_HOST>:5432/ton_index'

pg_dump "$COLD_DSN" \
  --format=custom \
  --data-only \
  --no-owner \
  --no-privileges \
  -t public.validator_elections \
  -t public.validator_election_participants \
  -t public.validator_cycles \
  -t public.validator_cycle_members \
  -t public.validator_complaints \
  -f /tmp/validator_snapshots.dump

pg_restore \
  --dbname="$HOT_DSN" \
  --data-only \
  --single-transaction \
  --exit-on-error \
  --no-owner \
  --no-privileges \
  /tmp/validator_snapshots.dump

exit
```

В `PGPASSFILE` должна быть отдельная строка для каждого host. Формат и права
файла описаны в
[standard deployment](../standard-deployment.md#prepare-ton-node-host).

Следующий запрос выполнить отдельно в cold и hot. Количество строк для каждой
таблицы должно совпасть:

```sql
SELECT 'validator_elections' AS table_name, count(*) FROM validator_elections
UNION ALL
SELECT 'validator_election_participants', count(*) FROM validator_election_participants
UNION ALL
SELECT 'validator_cycles', count(*) FROM validator_cycles
UNION ALL
SELECT 'validator_cycle_members', count(*) FROM validator_cycle_members
UNION ALL
SELECT 'validator_complaints', count(*) FROM validator_complaints
ORDER BY table_name;
```

## 4. Настроить logical replication из hot в cold

Hot является publisher, cold — subscriber. Сначала настроить publication и
subscription, и только затем запускать live worker. Так logical slot существует
до появления первых блоков после `S`.

### 4.1. Создать replication role на hot

На hot создать отдельную роль:

```sql
CREATE ROLE hot_to_cold_repl
  LOGIN
  REPLICATION
  PASSWORD '<HOT_TO_COLD_PASSWORD>';

GRANT USAGE ON SCHEMA public TO hot_to_cold_repl;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO hot_to_cold_repl;

ALTER DEFAULT PRIVILEGES FOR ROLE ton_indexer IN SCHEMA public
  GRANT SELECT ON TABLES TO hot_to_cold_repl;
```

В `pg_hba.conf` hot PostgreSQL разрешить этой роли подключение только с cold
host:

```conf
host  ton_index  hot_to_cold_repl  <COLD_POSTGRES_IP>/32  scram-sha-256
```

После изменения перечитать PostgreSQL configuration.

### 4.2. Создать publication на hot

Выполнить на hot от owner таблиц или PostgreSQL superuser:

```sql
CREATE PUBLICATION hot_to_cold_pub
FOR TABLE
  blocks,
  shard_state,
  transactions,
  messages,
  traces,
  actions,
  action_accounts,
  jetton_transfers,
  jetton_burns,
  nft_transfers,
  nominator_pool_events,
  nominator_pool_validator_events,
  validator_events,
  validator_elections,
  validator_election_participants,
  validator_cycles,
  validator_cycle_members,
  validator_complaints
WITH (
  publish = 'insert, update, delete',
  publish_via_partition_root = true
);
```

`publish_via_partition_root = true` передаёт изменения из dynamically created
hot partitions как изменения корневых таблиц. В cold достаточно таблиц и
default partitions, созданных migrator; создавать там копии hot partitions не
нужно.

В publication намеренно нет current-state tables и служебных очередей. Current
state и исторические message BOCs остаются в единственном общем Kvrocks.
`_ton_indexer_progress` и очередь classifier принадлежат только hot.

### 4.3. Создать subscription на cold

Выполнить на cold от PostgreSQL superuser. `CREATE SUBSCRIPTION` с созданием
slot нельзя выполнять внутри явного transaction block:

```sql
CREATE SUBSCRIPTION hot_to_cold_sub
CONNECTION 'host=<HOT_POSTGRES_HOST> port=5432 dbname=ton_index user=hot_to_cold_repl password=<HOT_TO_COLD_PASSWORD> application_name=hot_to_cold_sub target_session_attrs=read-write connect_timeout=5'
PUBLICATION hot_to_cold_pub
WITH (
  slot_name = 'hot_to_cold',
  create_slot = true,
  enabled = true,
  copy_data = false,
  failover = true
);
```

`copy_data = false` обязателен: cold уже содержит всю историю до `S`, а пять
validator tables уже одинаковы в hot и cold. Initial copy привёл бы к
конфликтам primary key. Subscription передаёт только изменения, появившиеся в
hot после создания logical slot.

## 5. Настроить live worker на hot

В `/etc/ton-indexer/worker.env` из standard deployment заменить PostgreSQL DSN
и стартовую точку:

```dotenv
POSTGRES_DSN=postgresql://ton_indexer@<HOT_POSTGRES_HOST>:5432/ton_index
TON_WORKER_FROM=<S_PLUS_1>
```

`S_PLUS_1` означает ровно `S + 1`. Cold уже содержит блок `S`; повторно
индексировать его в пустой hot нельзя, иначе subscription получит дублирующий
`INSERT` для строки, которая уже есть в cold.

В `ExecStart` unit `ton-index-postgres.service` добавить partition flags:

```text
--pg-manage-partitions \
--pg-partition-size-mc-seqnos 216000 \
--pg-partition-retention-mc-seqnos 3024000
```

Итоговая часть команды должна выглядеть так:

```ini
ExecStart=/usr/local/bin/ton-index-postgres \
  --db ${TON_DBROOT} \
  --working-dir ${TON_WORKDIR} \
  --pg ${POSTGRES_DSN} \
  --kvrocks ${KVROCKS_DSN} \
  --kvrocks-password ${KVROCKS_PASSWORD} \
  --from ${TON_WORKER_FROM} \
  --pg-manage-partitions \
  --pg-partition-size-mc-seqnos 216000 \
  --pg-partition-retention-mc-seqnos 3024000 \
  $TON_NETWORK_ARGS
```

Флаги означают следующее:

- `--pg-manage-partitions` включает создание и удаление range partitions в
  hot PostgreSQL. Worker также обновляет таблицу `_ton_hot_cold_split`, из
  которой API читает безопасную границу маршрутизации.
- `--pg-partition-size-mc-seqnos` задаёт размер одной partition в masterchain
  seqnos. Чем меньше значение, тем точнее освобождается место, но тем больше
  partitions и DDL operations.
- `--pg-partition-retention-mc-seqnos` задаёт минимальную глубину hot history.
  Worker удаляет только целую partition, когда её правая граница уже старше
  retention. Поэтому фактическая глубина hot обычно находится между `retention`
  и `retention + partition_size`. Значение `0` отключает удаление и для
  hot/cold deployment не подходит.

Значения `216000` и `3024000` являются хорошей стартовой конфигурацией:
retention равен 14 partitions. При среднем интервале около 0.4 секунды между
masterchain blocks одна partition содержит примерно 1 день, а настроенный hot
window — примерно 14 дней. Из-за удаления только целых partitions фактически
hot может хранить до одной дополнительной partition, то есть около 15 дней.

При выборе других значений:

1. Определить желаемый hot window в днях.
2. Пересчитать его в masterchain seqnos по фактическому среднему интервалу
   между блоками.
3. Округлить retention вверх до кратного partition size.
4. Убедиться, что retention меньше текущего masterchain seqno. Иначе worker не
   сможет опубликовать границу hot/cold до тех пор, пока chain не станет старше
   retention.
5. Оставить retention больше максимального ожидаемого lag или периода
   недоступности logical replication. Перед уменьшением retention обязательно
   убедиться, что cold догнал hot.

После изменения unit запустить worker:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ton-index-postgres
```

Worker по-прежнему должен находиться на машине TON-ноды, работать от
`validator` и иметь `LimitNOFILE=1000000`, как в standard deployment. Kvrocks
configuration не меняется.

## 6. Настроить optional action classifier

Если actions нужны, historical backlog до `S` уже должен быть обработан в cold
во время full-history procedure. Continuous classifier теперь подключается
только к **hot PostgreSQL** и общему Kvrocks:

```env
TON_INDEXER_PG_DSN=postgresql+asyncpg://ton_indexer@<HOT_POSTGRES_HOST>:5432/ton_index
```

Новые `actions` и `action_accounts` будут перенесены в cold publication. Если
actions не нужны, classifier не запускать.

Остальная настройка описана в
[процедуре action classifier](action-classifier.md).

## 7. Подключить API к cold и hot

Создать read-only роль `ton_index_api` и выдать ей права из standard deployment
отдельно в cold и hot. PostgreSQL roles и grants logical replication не
переносит.

В `/etc/ton-index-api/pgpass` добавить обе строки:

```text
<COLD_POSTGRES_HOST>:5432:ton_index:ton_index_api:<API_POSTGRES_PASSWORD>
<HOT_POSTGRES_HOST>:5432:ton_index:ton_index_api:<API_POSTGRES_PASSWORD>
```

В `/etc/ton-index-api/api.env` задать оба DSN:

```dotenv
COLD_POSTGRES_DSN=postgresql://ton_index_api@<COLD_POSTGRES_HOST>:5432/ton_index
HOT_POSTGRES_DSN=postgresql://ton_index_api@<HOT_POSTGRES_HOST>:5432/ton_index
```

В `ton-index-go.service` передать cold через `-pg`, а hot через `-pg-hot`:

```ini
ExecStart=/usr/local/bin/ton-index-go \
  -pg ${COLD_POSTGRES_DSN} \
  -pg-hot ${HOT_POSTGRES_DSN} \
  -kvrocks ${KVROCKS_ADDR} \
  -kvrocks-password ${KVROCKS_PASSWORD} \
  -bind ${TON_API_BIND} \
  -maxconns ${TON_API_MAX_CONNS} \
  $TON_NETWORK_ARGS
```

Менять DSN местами нельзя. API читает `_ton_hot_cold_split` именно из
`-pg-hot` и использует её для маршрутизации запросов между databases.

Запускать API следует после того, как worker записал первые блоки, а проверки
logical replication из следующего раздела проходят успешно.

## 8. Проверить работу

На cold проверить состояние subscription:

```sql
SELECT subname,
       worker_type,
       pid,
       received_lsn,
       latest_end_lsn,
       latest_end_time
FROM pg_stat_subscription
WHERE subname = 'hot_to_cold_sub';
```

Должна быть строка apply worker с непустым `pid`; LSN и `latest_end_time`
должны обновляться по мере работы index worker.

Ошибки применения на cold:

```sql
SELECT subname,
       apply_error_count,
       sync_error_count,
       stats_reset
FROM pg_stat_subscription_stats
WHERE subname = 'hot_to_cold_sub';
```

`apply_error_count` не должен расти. `sync_error_count` должен оставаться `0`;
при `copy_data = false` initial table synchronization не выполняется.

На hot проверить logical slot:

```sql
SELECT slot_name,
       active,
       failover,
       restart_lsn,
       confirmed_flush_lsn,
       pg_size_pretty(
         pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
       ) AS pending_wal
FROM pg_replication_slots
WHERE slot_name = 'hot_to_cold';
```

Slot должен существовать, иметь `active = true` и `failover = true`.
`confirmed_flush_lsn` должен продвигаться. Если subscription долго не работает,
slot удерживает WAL на hot, поэтому `pending_wal` и свободное место необходимо
мониторить.

Следующий запрос регулярно выполнить отдельно на hot и cold:

```sql
SELECT max(seqno) AS mc_seqno
FROM blocks
WHERE workchain = -1;
```

Оба значения должны расти. Cold может ненадолго отставать, но при исправной
replication должен догонять hot. После остановки worker и исчезновения lag
значения должны совпасть.

На hot проверить созданные partitions:

```sql
SELECT parent.relname AS parent_table,
       child.relname AS partition_name,
       pg_get_expr(child.relpartbound, child.oid) AS partition_bound
FROM pg_inherits
JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
JOIN pg_class child ON child.oid = pg_inherits.inhrelid
JOIN pg_namespace n ON n.oid = parent.relnamespace
WHERE n.nspname = 'public'
  AND parent.relname IN ('blocks', 'transactions', 'messages', 'traces')
ORDER BY parent.relname, child.relname;
```

В hot должны появиться partitions с именами вида
`blocks_p_<from>_<to>`. В cold такие partitions не нужны: replicated rows
попадают в существующие cold tables.

Проверить опубликованную API границу на hot:

```sql
SELECT lt, seqno, utime
FROM _ton_hot_cold_split
WHERE id = 1;
```

Строка создаётся worker и со временем продвигается вперёд. Если её нет, API с
`-pg-hot` не сможет начать работу; проверить progress worker, partition flags и
его логи.

## 9. High availability вариант

Для HA используются два независимых Patroni clusters: cold и hot. Общий порядок
такой:

1. На одиночном cold PostgreSQL выполнить full-history procedure до `S`.
2. Из заполненного cold создать остальные members cold Patroni cluster и
   дождаться их синхронизации.
3. Подготовить пустой hot PostgreSQL, выполнить migrations и создать из него
   отдельный hot Patroni cluster.
4. Выполнить копирование validator snapshots, publication и subscription уже
   через primary соответствующих clusters.
5. Направить live workers и continuous classifier в hot HAProxy write endpoint.
6. Передать API cold и hot endpoints. Для минимального lag рекомендуется
   использовать endpoints текущих primary с read-only ролью API.

Hot и cold clusters должны быть построены по общим требованиям
[HA runbook](../high-availability.md). На всех их members должна быть одна
major-версия PostgreSQL и установлен совместимый `pgton`.

### 9.1. Использовать multihost connection в subscription

В HA варианте publication остаётся на hot, а subscription создаётся на cold
primary. В connection string перечислить все hot Patroni members:

```sql
CREATE SUBSCRIPTION hot_to_cold_sub
CONNECTION 'host=<HOT_PG_1>,<HOT_PG_2>,<HOT_PG_3> port=5432 dbname=ton_index user=hot_to_cold_repl password=<HOT_TO_COLD_PASSWORD> application_name=hot_to_cold_sub target_session_attrs=read-write connect_timeout=5'
PUBLICATION hot_to_cold_pub
WITH (
  slot_name = 'hot_to_cold',
  create_slot = true,
  enabled = true,
  copy_data = false,
  failover = true
);
```

`target_session_attrs=read-write` не позволяет subscription подключиться к hot
standby. После разрыва соединения libpq перебирает hosts и выбирает текущий hot
primary. На всех hot members в `pg_hba.conf` нужно разрешить подключения роли
`hot_to_cold_repl` со всех cold member IPs.

### 9.2. Синхронизировать logical slot на hot standbys

`failover = true` помечает logical slot для синхронизации, но одного этого
недостаточно. На каждом hot standby должны быть настроены:

```conf
wal_level = logical
hot_standby_feedback = on
sync_replication_slots = on
```

Каждый standby должен использовать собственный permanent physical replication
slot (`primary_slot_name`), а его `primary_conninfo` должен содержать valid
`dbname`. Patroni должен работать с `use_slots: true`.

На каждом потенциальном hot primary вручную задать
`synchronized_standby_slots`: это список **physical replication slot names**
остальных hot members, а не их DNS names. Например, для трёх members:

```text
hot-pg-1: synchronized_standby_slots = 'hot_pg_2,hot_pg_3'
hot-pg-2: synchronized_standby_slots = 'hot_pg_1,hot_pg_3'
hot-pg-3: synchronized_standby_slots = 'hot_pg_1,hot_pg_2'
```

Это per-member local configuration: общий список из Patroni DCS здесь не
подходит, потому что на разных members исключается разный собственный slot.
Имена заменить фактическими slot names из `pg_replication_slots`. Каждый
указанный slot должен существовать и принадлежать standby, который может быть
promoted. Если указанный physical slot отсутствует или invalidated, передача
из hot в cold остановится до исправления configuration.

На момент написания Patroni не поддерживает автоматическое ведение этого
параметра; это отслеживается в
[Patroni issue #3431](https://github.com/patroni/patroni/issues/3431). Поэтому
список нужно обновлять при добавлении, удалении или переименовании hot members.

### 9.3. Проверить готовность hot failover

На текущем hot primary проверить настройки и physical slots:

```sql
SHOW synchronized_standby_slots;

SELECT slot_name, slot_type, active, failover, synced, invalidation_reason
FROM pg_replication_slots
ORDER BY slot_type, slot_name;
```

На каждом hot standby проверить failover logical slot:

```sql
SELECT slot_name,
       synced,
       temporary,
       invalidation_reason,
       synced AND NOT temporary AND invalidation_reason IS NULL AS failover_ready
FROM pg_replication_slots
WHERE slot_name = 'hot_to_cold';
```

Перед разрешением production traffic `failover_ready` должен быть `true` на
каждом standby, который Patroni может promote. После тестового hot failover
повторить проверки из раздела 8: subscription на cold должна переподключиться к
новому hot primary, а masterchain seqno в cold снова должен продолжить расти.

Cold Patroni failover проверяется отдельно: после promotion нового cold primary
на нём должен автоматически запуститься apply worker `hot_to_cold_sub`.

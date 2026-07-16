# Процедура: загрузка полной истории

Эта процедура используется, если PostgreSQL должен содержать всю доступную
историю TON начиная с masterchain seqno `1`. Для неё нужна как минимум одна
архивная TON-нода, которая хранит blocks и shard states за весь индексируемый
диапазон.

Полная история требует большого объёма диска. По состоянию на июль 2026 года
mainnet PostgreSQL с архивными данными занимает около `18 TB`, а Kvrocks — около
`3 TB`. Эти значения продолжают расти. Дополнительное место требуется на время
создания индексов, а в HA deployment каждый PostgreSQL и Kvrocks replica хранит
собственную копию данных.

## 1. Создать схему без индексов

Создать PostgreSQL database, установить `pgton` и настроить Kvrocks по
[standard deployment](../standard-deployment.md). Постоянные workers, API и
classifier пока не запускать.

Выполнить миграции с `--no-create-indexes`:

```bash
sudo -u validator env PGPASSFILE=/etc/ton-indexer/pgpass \
  /usr/local/bin/ton-index-postgres-migrate \
  --pg postgresql://ton_indexer@<POSTGRES_PRIVATE_IP>:5432/ton_index \
  --custom-types \
  --no-create-indexes
```

Флаг откладывает создание тяжёлых secondary indexes до завершения массовой
загрузки. Primary keys, unique constraints и технические объекты, являющиеся
частью схемы, при этом создаются.

Сразу после создания пустой схемы создать единственный operational index,
необходимый очереди action classifier во время вставки masterchain blocks:

```sql
CREATE INDEX IF NOT EXISTS blocks_index_3
    ON blocks (seqno)
    WHERE workchain = -1;
```

Он создаётся на пустой таблице и затем поддерживается во время archive
backfill. Остальные secondary indexes на этой стадии не создавать.

## 2. Зафиксировать snapshot block

Выбрать свежий masterchain seqno `S` и больше не менять его в ходе первоначальной
загрузки. TON-нода должна хранить shard states блока `S` до полного завершения
scanner и возможных рестартов.

На одной из TON-нод выполнить
[shard state bootstrap](shard-state-bootstrap.md) для `S`. Scanner наполнит
current state в PostgreSQL и Kvrocks. Только после его успешного завершения
переходить к архивным workers.

<a id="archive-backfill"></a>

## 3. Проиндексировать диапазон от `1` до `S`

Каждый archive worker запускается на машине с архивной TON-нодой и читает её
локальную DB. На каждой такой машине предварительно выполнить раздел
[«Подготовить TON node host»](../standard-deployment.md#prepare-ton-node-host).
Для одного worker создать отдельный working directory:

```bash
sudo install -d -m 0750 -o validator -g validator \
  /var/lib/ton-indexer/archive-1-S
```

Запустить ограниченный диапазон:

```bash
sudo -u validator /bin/bash

export PGPASSFILE=/etc/ton-indexer/pgpass
ulimit -n 1000000

/usr/local/bin/ton-index-postgres \
  --db /var/ton-work/db \
  --working-dir /var/lib/ton-indexer/archive-1-S \
  --pg postgresql://ton_indexer@<POSTGRES_PRIVATE_IP>:5432/ton_index \
  --kvrocks tcp://<KVROCKS_PRIVATE_IP>:6666/0 \
  --kvrocks-password <KVROCKS_PASSWORD> \
  --kvrocks-skip-current-tables \
  --from 1 \
  --to <S> \
  --max-active-tasks "$(nproc)" \
  --threads "$(nproc)" \
  [--testnet]
```

`--kvrocks-skip-current-tables` запрещает архивному worker перезаписывать
current/upsert data, которые scanner уже зафиксировал на блоке `S`. Lookup data
в Kvrocks при этом продолжает записываться.

`ulimit` обязателен для каждого archive worker: процесс должен запускаться с
лимитом не меньше `1000000` file descriptors.

Для mainnet убрать `--testnet`.

Если worker завершился с ошибкой, повторить ту же команду с тем же working
directory. В bounded archive mode он проверит уже записанные masterchain seqnos
и продолжит с первого отсутствующего. `--force` для обычного продолжения не
нужен.

## 4. Распараллелить загрузку

Если доступно несколько архивных TON-нод, диапазон `1..S` можно разделить между
ними. Диапазоны включают обе границы, поэтому они не должны пересекаться:

```text
node-1: --from 1       --to <A>
node-2: --from <A + 1> --to <B>
node-3: --from <B + 1> --to <S>
```

На каждом host использовать локальный путь к TON DB, отдельный working
directory и добавить к команде:

```text
--no-leader
```

Все workers подключаются к одним PostgreSQL и Kvrocks. `--no-leader` допустим
только вместе с `--from` и `--to`; он отключает leader/standby coordination для
заранее разделённых архивных диапазонов.

После завершения всех workers проверить, что каждый диапазон закончился
успешно и вместе они непрерывно покрывают `1..S` без gaps.

Сначала заменить `<S>` на зафиксированный masterchain seqno и проверить общие
границы и количество masterchain blocks:

```sql
WITH params AS (
    SELECT <S>::integer AS target_seqno
),
stats AS (
    SELECT min(b.seqno) AS first_seqno,
           max(b.seqno) AS last_seqno,
           count(*)     AS indexed_blocks
    FROM blocks b
    CROSS JOIN params p
    WHERE b.workchain = -1
      AND b.mc_block_seqno BETWEEN 1 AND p.target_seqno
)
SELECT s.first_seqno,
       s.last_seqno,
       s.indexed_blocks,
       p.target_seqno AS expected_blocks,
       coalesce(
           s.first_seqno = 1
           AND s.last_seqno = p.target_seqno
           AND s.indexed_blocks = p.target_seqno::bigint,
           false
       ) AS complete
FROM stats s
CROSS JOIN params p;
```

Для полностью загруженного диапазона запрос должен вернуть `first_seqno = 1`,
`last_seqno = S`, `indexed_blocks = S` и `complete = true`.

Если `complete = false`, следующий запрос покажет все отсутствующие интервалы:

```sql
WITH params AS (
    SELECT <S>::integer AS target_seqno
),
seqnos AS (
    SELECT 0::bigint AS seqno
    UNION ALL
    SELECT b.seqno::bigint
    FROM blocks b
    CROSS JOIN params p
    WHERE b.workchain = -1
      AND b.mc_block_seqno BETWEEN 1 AND p.target_seqno
    UNION ALL
    SELECT (target_seqno + 1)::bigint
    FROM params
),
ordered AS (
    SELECT seqno,
           lead(seqno) OVER (ORDER BY seqno) AS next_seqno
    FROM seqnos
)
SELECT seqno + 1      AS gap_from,
       next_seqno - 1 AS gap_to
FROM ordered
WHERE next_seqno > seqno + 1
ORDER BY gap_from;
```

При отсутствии gaps второй запрос не вернёт ни одной строки. Проверка ведётся
по masterchain blocks: каждый успешно записанный masterchain block представляет
полностью обработанный индексатором masterchain round вместе с соответствующими
shard blocks.

## 5. Опционально классифицировать исторические traces

Если окружению нужны actions, после полной загрузки `1..S` выполнить
[процедуру action classifier](action-classifier.md) в historical backlog mode.
Перед classifier создаются только минимальные индексы, необходимые для его
`SELECT` и регулярных `DELETE`. Полный набор secondary indexes на этой стадии
по-прежнему не создаётся.

Если actions не нужны, этот шаг пропустить.

## 6. Догнать новый snapshot block

Scanner и первоначальная индексация могут занять несколько дней. Перед
созданием финальных индексов зафиксировать новый masterchain seqno `NEW_S` и
проиндексировать появившийся диапазон:

```text
--from <S> --to <NEW_S>
```

Для этого запуска использовать ту же bounded archive command, но другой
working directory и без `--kvrocks-skip-current-tables`. Тогда worker обновит
current data от состояния `S` до `NEW_S`. Уже существующий block `S` будет
пропущен, а обработка продолжится с первого отсутствующего seqno.

Если первоначальная загрузка закончилась быстро и дополнительного диапазона
нет, этот шаг пропустить и считать `NEW_S = S`.

После догоняющей индексации повторить обе SQL-проверки из пункта 4, заменив
`<S>` на `NEW_S`. Перед продолжением диапазон `1..NEW_S` должен вернуть
`complete = true`, а запрос gaps — ни одной строки.

Если окружению нужны actions и был проиндексирован дополнительный диапазон,
повторно запустить [action classifier](action-classifier.md) в historical
backlog mode. Он должен классифицировать traces, появившиеся между `S` и
`NEW_S`, до создания HA replicas.

## 7. Создать HA replicas

Этот шаг выполняется только для HA deployment. После завершения scanner,
архивных workers, догоняющей индексации и всех запусков historical classifier:

1. Инициализировать PostgreSQL replicas из наполненного PostgreSQL instance.
2. Инициализировать Kvrocks replicas из наполненного Kvrocks instance.
3. Дождаться синхронизации replicas и завершить настройку Patroni, HAProxy и
   Redis Sentinel по [HA runbook](../high-availability.md).

Пустые replicas перед массовой загрузкой создавать не нужно.

## 8. Создать финальные индексы

Повторно запустить migration binary, теперь без `--no-create-indexes`:

```bash
sudo -u validator env PGPASSFILE=/etc/ton-indexer/pgpass \
  /usr/local/bin/ton-index-postgres-migrate \
  --pg postgresql://ton_indexer@<POSTGRES_PRIVATE_IP>:5432/ton_index \
  --custom-types
```

Версия схемы уже будет актуальной, после чего binary перейдёт к созданию
отложенных indexes. Индексы создаются последовательно одним процессом. На базе
такого размера операция может занять несколько часов. Не нужно одновременно
запускать несколько migration processes.

## 9. Перейти в live mode

После создания индексов настроить постоянные workers с
`TON_WORKER_FROM=NEW_S`. В live command не должно быть `--to`, `--no-leader` и
`--kvrocks-skip-current-tables`.

Для standard deployment запустить один worker. Для HA deployment одновременно
запустить все workers в leader/standby mode. Они проиндексируют блоки после
`NEW_S` и догонят текущий network head.

После этого запустить API и, если нужны actions, action classifier в continuous
mode. По умолчанию тот же PostgreSQL cluster продолжает хранить полную историю
и принимать live data.

Если выбрано разделение на cold и hot, не запускать live services по этому
пункту. Вместо него выполнить [hot/cold procedure](hot-cold.md): она создаёт
отдельный пустой hot PostgreSQL и запускает worker с `NEW_S + 1`.

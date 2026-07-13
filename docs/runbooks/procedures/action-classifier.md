# Процедура: action classifier

Action classifier читает готовые traces из PostgreSQL, загружает необходимые
данные из Kvrocks и записывает распознанные actions обратно в PostgreSQL. Для
его работы также нужен отдельный Redis event cache. Доступ к TON DB ему не
требуется, поэтому classifier можно запускать на отдельной машине.

Classifier опционален. Если actions не нужны, эту процедуру можно полностью
пропустить. В Docker Compose classifier уже входит в стандартный stack и
запускается по умолчанию.

Есть два варианта использования одного и того же процесса:

1. **Historical backlog.** Обработать уже загруженную историю до блока `S` или
   `NEW_S`. Процесс сам не завершается после опустошения очереди: завершение
   проверяется SQL-запросом, после чего service останавливается вручную.
2. **Continuous mode.** Оставить classifier постоянно работающим вместе с live
   workers. Новые задания добавляются в общую PostgreSQL queue при записи
   masterchain blocks.

## 1. Подготовить Python environment

Использовать Python 3.12. На classifier host установить Python, venv и системные
библиотеки, необходимые зависимостям проекта. Для Debian/Ubuntu:

```bash
sudo apt update
sudo apt install -y \
  python3 python3-venv python3-pip \
  libpq-dev libsecp256k1-dev libsodium-dev libhiredis-dev rsync
```

Если classifier запускается на отдельной машине, создать системного пользователя
и директорию конфигурации:

```bash
sudo useradd --system --home /opt/ton-indexer --shell /usr/sbin/nologin ton-indexer
sudo install -d -m 0750 -o root -g ton-indexer /etc/ton-indexer
```

Из корня checkout скопировать директорию `indexer` и создать virtual
environment:

```bash
sudo install -d -m 0755 -o ton-indexer -g ton-indexer \
  /opt/ton-indexer/indexer

sudo rsync -a indexer/ /opt/ton-indexer/indexer/
sudo chown -R ton-indexer:ton-indexer /opt/ton-indexer

sudo -u ton-indexer python3 -m venv /opt/ton-indexer/venv
sudo -u ton-indexer /opt/ton-indexer/venv/bin/pip install \
  --no-cache-dir \
  -r /opt/ton-indexer/indexer/requirements.txt
```

При обновлении classifier повторно скопировать `indexer` из того же release,
что используется остальными компонентами, и ещё раз выполнить `pip install`.

## 2. Настроить подключения

Classifier требует read/write подключение к PostgreSQL. Read-only роль API для
него не подходит: процесс записывает `actions`, `action_accounts` и служебные
таблицы, а при повторной классификации удаляет старый результат.

Для standard deployment Redis event cache проще запустить локально на
classifier host и слушать только `127.0.0.1:6379`. В HA deployment все
classifier instances должны использовать общий отказоустойчивый event-cache
endpoint. Перед запуском classifier команда `redis-cli -u <REDIS_DSN> ping`
должна вернуть `PONG`.

Создать `/etc/ton-indexer/action-classifier.env`:

```env
TON_INDEXER_PG_DSN=postgresql+asyncpg://ton_indexer@<POSTGRES_WRITE_ENDPOINT>:5432/ton_index
TON_INDEXER_REDIS_DSN=redis://<EVENT_CACHE_PRIVATE_IP>:6379/0
TON_INDEXER_KVROCKS=<KVROCKS_PRIVATE_IP>:6666
TON_INDEXER_KVROCKS_PASSWORD=<KVROCKS_PASSWORD>
```

Для testnet добавить:

```env
TON_INDEXER_IS_TESTNET=1
```

PostgreSQL password хранить в `/etc/ton-indexer/pgpass`, как в
[standard deployment](../standard-deployment.md). Защитить оба файла:

```bash
sudo chown root:ton-indexer /etc/ton-indexer/action-classifier.env
sudo chown ton-indexer:ton-indexer /etc/ton-indexer/pgpass

sudo chmod 0640 /etc/ton-indexer/action-classifier.env
sudo chmod 0600 /etc/ton-indexer/pgpass
```

Redis event cache и Kvrocks — разные хранилища. Нельзя указывать Kvrocks в
`TON_INDEXER_REDIS_DSN`.

## 3. Создать минимальные индексы

Этот шаг нужен для классификации архивных данных, когда schema была создана с
`--no-create-indexes`, а classifier запускается до создания полного набора indexes.

```sql
CREATE INDEX IF NOT EXISTS blocks_index_3
    ON blocks (seqno)
    WHERE workchain = -1;

CREATE INDEX IF NOT EXISTS traces_index_2
    ON traces (mc_seqno_end);

CREATE INDEX IF NOT EXISTS transactions_index_7
    ON transactions (trace_id, lt);

CREATE INDEX IF NOT EXISTS action_accounts_index_2
    ON action_accounts (trace_id, action_id);
```

Дальнейший запуск `ton-index-postgres-migrate` без `--no-create-indexes` создаст
остальные индексы и пропустит эти четыре индекса.

Если полный набор indexes уже создан обычным migration run, отдельно выполнять
этот шаг не нужно.

## 4. Создать systemd service

Создать `/etc/systemd/system/ton-action-classifier.service`:

```ini
[Unit]
Description=TON action classifier
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ton-indexer
Group=ton-indexer
WorkingDirectory=/opt/ton-indexer/indexer
EnvironmentFile=/etc/ton-indexer/action-classifier.env
Environment=PGPASSFILE=/etc/ton-indexer/pgpass
ExecStart=/opt/ton-indexer/venv/bin/python3 /opt/ton-indexer/indexer/event_classifier.py --pool-size 8 --prefetch-size 10000 --batch-size 1000 --mc-seqno-batch-size 4
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Значения `pool-size`, `prefetch-size`, `batch-size` и `mc-seqno-batch-size`
являются стартовой конфигурацией и подбираются под ресурсы classifier host и
хранилищ.

Запустить classifier:

```bash
sudo systemctl daemon-reload
sudo systemctl start ton-action-classifier
```

Для большой historical queue опционально можно запустить два classifier
instances, обрабатывающих очередь с противоположных сторон.

Посмотреть логи:

```bash
journalctl -u ton-action-classifier -f
```

## 5. Проверить PostgreSQL queue

Очередь хранится в PostgreSQL в таблице `_classifier_tasks`. Если запустить
несколько classifier instances, они автоматически распределят задания между
собой.

Текущее состояние очереди показывает следующий запрос:

```sql
SELECT count(*) AS queued_total,
       min(mc_seqno) AS oldest_mc_seqno,
       max(mc_seqno) AS newest_mc_seqno
FROM _classifier_tasks;
```

`queued_total` должен уменьшаться. После рестарта classifier продолжает работу
с сохранённой PostgreSQL queue; вручную изменять или удалять её строки не нужно.

Частый `count(*)` по большой очереди создаёт дополнительную нагрузку, поэтому
этот запрос не следует запускать с коротким monitoring interval.

## 6. Проверить завершение historical backlog

Для проверки обработки истории до `S` заменить `<S>` на нужный masterchain
seqno:

```sql
WITH params AS (
    SELECT <S>::integer AS target_seqno
),
classified AS (
    SELECT count(*) AS classified_blocks
    FROM _blocks_classified b
    CROSS JOIN params p
    WHERE b.mc_seqno BETWEEN 1 AND p.target_seqno
),
queued AS (
    SELECT count(*) AS queued_blocks
    FROM _classifier_tasks t
    CROSS JOIN params p
    WHERE t.mc_seqno BETWEEN 1 AND p.target_seqno
)
SELECT p.target_seqno,
       c.classified_blocks,
       p.target_seqno AS expected_blocks,
       q.queued_blocks,
       c.classified_blocks = p.target_seqno::bigint
           AND q.queued_blocks = 0 AS complete
FROM params p
CROSS JOIN classified c
CROSS JOIN queued q;
```

Результат должен содержать `classified_blocks = S`, `queued_blocks = 0` и
`complete = true`. Поскольку `mc_seqno` является primary key таблицы
`_blocks_classified`, это одновременно подтверждает отсутствие gaps в
обработанном диапазоне `1..S`.

Критерием завершения historical run является `complete = true`, а не только
пустая queue.

Ошибки отдельных traces проверить отдельно:

```sql
SELECT count(*) AS failure_records,
       count(DISTINCT trace_id) AS failed_traces
FROM _classifier_failed_traces;
```

Наиболее частые ошибки:

```sql
SELECT error,
       count(DISTINCT trace_id) AS failed_traces
FROM _classifier_failed_traces
GROUP BY error
ORDER BY failed_traces DESC
LIMIT 20;
```

`_classifier_failed_traces` является историей ошибок: записи не удаляются
автоматически, если последующий повторный запуск обработал trace успешно.

## 7. Выполнить historical run

Для первоначальной загрузки полной истории:

1. Дождаться завершения archive workers до `S`.
2. Убедиться, что `blocks_index_3` был создан до archive backfill, и создать
   оставшиеся три минимальных индекса из пункта 3.
3. Запустить classifier. Для большой очереди опционально использовать два
   instances, обрабатывающих её с противоположных сторон.
4. Дождаться `complete = true` для `S` и проверить
   `_classifier_failed_traces`.
5. Остановить service:

```bash
sudo systemctl stop ton-action-classifier
```

После догоняющей индексации `S..NEW_S` снова запустить тот же service и
повторить проверку с `target_seqno = NEW_S`.

Повторный запуск безопасен: queue хранится в PostgreSQL, и classifier продолжит
с оставшихся заданий.

## 8. Перейти в continuous mode

После historical run и создания полного набора indexes включить classifier в
continuous mode:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ton-action-classifier
```

В continuous mode service работает постоянно. `_classifier_tasks` может быть
ненулевой во время обработки новых blocks, но queue не должна неограниченно
расти.

В HA deployment несколько classifier instances используют общую PostgreSQL
queue, один PostgreSQL write endpoint, общий Redis event cache и Kvrocks через
Sentinels. Instances автоматически распределяют нагрузку. Для Kvrocks вместо
`TON_INDEXER_KVROCKS` задаются:

```env
TON_INDEXER_KVROCKS_SENTINELS=<SENTINEL_1>:26379,<SENTINEL_2>:26379,<SENTINEL_3>:26379
TON_INDEXER_KVROCKS_SENTINEL_MASTER=<KVROCKS_MASTER_NAME>
TON_INDEXER_KVROCKS_SENTINEL_PASSWORD=<SENTINEL_PASSWORD>
TON_INDEXER_KVROCKS_PASSWORD=<KVROCKS_PASSWORD>
```

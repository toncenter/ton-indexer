# Процедура: shard state bootstrap

`ton-smc-scanner` загружает текущее состояние всех аккаунтов на момент
masterchain-блока `S`. Scanner читает shard states напрямую из локальной TON DB
и записывает current state в PostgreSQL и Kvrocks. При включённом
`--interfaces` он также определяет типы контрактов и наполняет связанные
current-таблицы и ключи.

В deployment без архивной истории после scanner `ton-index-postgres` индексирует
блоки начиная с `S` и продолжает работать вперёд. При загрузке полной истории
дальнейший порядок определяет отдельная full-history procedure.

Scanner должен быть запущен на машине с TON-нодой: удалённого режима чтения TON
DB у него нет. Полный проход занимает несколько часов. В одном из mainnet
развёртываний он занял около 12 часов, но фактическое время зависит от машины,
состояния node DB и настроек parallelism. Нода должна хранить shard states блока
`S` в течение всего запуска scanner и возможных рестартов, поэтому её
`--state-ttl` должен покрывать полную продолжительность процедуры с запасом.

## 1. Подготовить окружение

Перед запуском:

1. Выбрать masterchain seqno `S` и убедиться, что TON-нода полностью
   синхронизирована и хранит shard states для этого блока.
2. Выполнить миграции PostgreSQL и настроить Kvrocks.
3. Остановить `ton-index-postgres` и другие процессы, которые могут параллельно
   изменять current state в этих хранилищах.
4. Убедиться, что пользователь `ton-indexer` читает локальную TON DB и может
   подключиться к PostgreSQL и Kvrocks.
5. Для HA deployment оставить PostgreSQL и Kvrocks одиночными instances. В
   snapshot-only deployment replicas создаются после scanner, а при загрузке
   полной истории — после завершения всей full-history procedure.

Создать постоянный working directory на локальном диске машины с TON-нодой:

```bash
sudo install -d -m 0750 -o ton-indexer -g ton-indexer \
  /var/lib/ton-indexer/smc-scanner
```

Этот каталог хранит checkpoints. Его нельзя очищать или заменять до успешного
завершения scanner.

## 2. Запустить scanner

Для testnet выполнить:

```bash
sudo -u ton-indexer env PGPASSFILE=/etc/ton-indexer/pgpass \
  /usr/local/bin/ton-smc-scanner \
  --db /var/ton-work/db \
  --working-dir /var/lib/ton-indexer/smc-scanner \
  --seqno <S> \
  --pg postgresql://ton_indexer@<POSTGRES_PRIVATE_IP>:5432/ton_index \
  --kvrocks tcp://<KVROCKS_PRIVATE_IP>:6666/0 \
  --kvrocks-password <KVROCKS_PASSWORD> \
  --kvrocks-pool-size 256 \
  --max-parallel-batches 64 \
  -t "$(nproc)" \
  --account-states \
  --interfaces \
  [--testnet]
```

Для mainnet убрать `--testnet`. Значения PostgreSQL и Kvrocks должны указывать
на те же одиночные instances, из которых затем будет развёрнуто окружение.

`--account-states` включает запись всех account states. `--interfaces` нужен,
если окружение должно сразу получить current data для jettons, NFT и других
распознаваемых контрактов.

## 3. Дождаться завершения

Scanner должен завершиться с кодом `0`. В логах должны появиться сообщения о
завершении каждого shard без необработанных ошибок записи в PostgreSQL или
Kvrocks.

Во время работы нужно следить за потреблением памяти. В scanner всё ещё могут
проявляться утечки памяти, и длительный проход может быть остановлен OOM killer.
Приведённая команда использует все доступные CPU threads и запускает до `64`
параллельных batches на shard, поэтому потребление памяти нужно контролировать.

## 4. Продолжить после падения

После успешной вставки очередного диапазона scanner сохраняет checkpoint для
соответствующих `S` и shard в `--working-dir`. Если процесс завершился с ошибкой
или был убит из-за OOM:

1. Не очищать PostgreSQL, Kvrocks и working directory.
2. Убедиться, что PostgreSQL, Kvrocks и TON DB снова доступны.
3. Повторить ту же команду с теми же `--seqno`, `--working-dir` и флагами.

Scanner прочитает checkpoints и продолжит с последнего сохранённого диапазона.
Последние незавершённые ranges могут быть обработаны повторно. Запуск с другим
working directory приведёт к полному повторному проходу.

## 5. Продолжить развёртывание

Если scanner был запущен как часть
[загрузки полной истории](full-history.md#archive-backfill), не создавать
replicas и не запускать live workers. Вернуться в full-history procedure и
продолжить с archive backfill `1..S`.

Для [standard deployment](../standard-deployment.md#shard-state-bootstrap) без
архивной истории после успешного завершения scanner указать `S` в
`TON_WORKER_FROM` и запустить `ton-index-postgres`. Worker проиндексирует блок
`S`, а затем продолжит работу вперёд.

Для [HA deployment](../high-availability.md#shard-state-bootstrap) со snapshot,
но без архивной истории:

1. Полностью завершить scanner и проверить подготовленные PostgreSQL и Kvrocks.
2. Инициализировать PostgreSQL replicas из уже наполненного PostgreSQL instance.
3. Инициализировать Kvrocks replicas из уже наполненного Kvrocks instance.
4. После синхронизации replicas настроить Patroni, HAProxy и Redis Sentinel.
5. Запустить live workers с `S` в leader/standby mode.

Не следует сначала создавать пустые HA clusters, а затем запускать scanner в
них: bootstrap выполняется один раз до репликации, после чего уже готовые данные
распространяются на replicas.

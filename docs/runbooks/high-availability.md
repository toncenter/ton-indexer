# Runbook: high availability

Этот runbook описывает отличия HA deployment от
[стандартного развёртывания](standard-deployment.md). Сборка binaries, установка
`pgton`, базовая настройка PostgreSQL и Kvrocks, systemd units и создание
read-only пользователя API здесь не повторяются.

В HA deployment каждый постоянно работающий компонент имеет как минимум один
резервный instance и размещается в разных failure domains.

## 1. Общая топология

HA topology состоит из следующих частей:

1. PostgreSQL cluster под управлением Patroni. Для Patroni используется
   отказоустойчивый DCS, например cluster из трёх etcd nodes.
2. Несколько внутренних HAProxy instances перед PostgreSQL. Они предоставляют
   стабильный endpoint текущего primary и, при необходимости, отдельный
   endpoint реплик.
3. Kvrocks primary и несколько replicas. Выбор нового primary выполняет quorum
   из нескольких Redis Sentinel instances.
4. Несколько пар TON node + `ton-index-postgres`. Внутри каждой пары worker
   находится на машине своей ноды и читает только её локальную TON DB.
5. Несколько `ton-index-go` instances за отдельным API load balancer.
6. Опциональные action classifier instances и отказоустойчивый Redis
   event-cache, если окружению нужны actions.

Для компонентов, использующих quorum, обычно требуется нечётное число members.
Две копии процесса сами по себе не дают безопасного автоматического failover.

## 2. PostgreSQL, Patroni и HAProxy

На каждом PostgreSQL member должны быть установлены одинаковая major-версия
PostgreSQL и совместимая версия `pgton`. Physical replication переносит данные
extension, но не устанавливает `pgton.so`, control и SQL files на filesystem
нового member.

Patroni отвечает за выбор единственного PostgreSQL primary и promotion
replica. Patroni DCS также должен сохранять quorum при отказе одной машины.

Перед PostgreSQL размещается внутренний HAProxy:

- write endpoint направляет соединения только в Patroni primary;
- optional read endpoint распределяет соединения между healthy replicas;
- health checks используют роль member, которую сообщает Patroni, а не только
  доступность TCP `5432`.

HAProxy сам не должен становиться single point of failure. Его можно запускать
локально на каждом consumer host либо развернуть несколько instances за
внутренним VIP или load balancer.

Через write endpoint должны подключаться:

- `ton-index-postgres`;
- action classifier и другие сервисы, которым нужны записи.

Read-only DSN `ton-index-go` также можно направить в primary, если важнее всего
freshness. Использование replica endpoint допустимо только если API может
принимать replication lag.

## 3. Kvrocks и Redis Sentinel

Kvrocks replication переносит данные с primary на replicas. Redis Sentinel не
реплицирует данные сам: он наблюдает за Kvrocks members, достигает quorum и при
отказе primary выполняет promotion одной из replicas.

Рекомендуемая минимальная topology — один Kvrocks primary, две replicas и три
Sentinel instances в разных failure domains. Все Sentinels должны использовать
одно имя monitored master, например `kvrocks-main`.

Все workers подключаются к Kvrocks только через список Sentinels:

```text
--kvrocks-sentinels <SENTINEL_1>:26379,<SENTINEL_2>:26379,<SENTINEL_3>:26379
--kvrocks-master kvrocks-main
--kvrocks-password <KVROCKS_PASSWORD>
```

Все API instances используют тот же список и имя master, но API flags
называются иначе:

```text
-kvrocks-sentinels <SENTINEL_1>:26379,<SENTINEL_2>:26379,<SENTINEL_3>:26379
-kvrocks-sentinel-master kvrocks-main
-kvrocks-password <KVROCKS_PASSWORD>
```

В Sentinel mode не нужно одновременно указывать direct `--kvrocks` worker или
`-kvrocks` API endpoint. После promotion clients получают адрес нового primary
от Sentinel и переподключаются к нему.

## 4. Несколько TON nodes и workers

Каждый `ton-index-postgres` разворачивается по инструкции standard deployment,
но таких пар TON node + worker должно быть несколько. У каждой пары должны быть:

- собственная синхронизированная TON-нода;
- локальный путь к DB этой ноды;
- отдельный worker `working-dir`;
- запуск worker от локального OS-пользователя `validator`;
- `LimitNOFILE=1000000` в worker unit;
- одинаковые PostgreSQL HAProxy и Kvrocks Sentinel endpoints;
- одинаковая начальная точка и network configuration.

Все live workers запускаются одновременно. Они используют таблицу
`_ton_indexer_leader` в PostgreSQL и heartbeat lease: только leader записывает
очередную batch, остальные workers остаются standby и могут получить leadership
после отказа текущего leader.

PostgreSQL DSN каждого worker должен указывать на HAProxy write endpoint, а
Kvrocks configuration — на Sentinels. Встроенный failover watchdog worker нужно
оставить включённым, а systemd service должен перезапускать процесс после его
аварийного выхода.

PostgreSQL и Kvrocks обычно используют асинхронную репликацию. Поэтому после
failover новый primary может не содержать небольшой хвост уже подтверждённых
записей. Worker отслеживает сохранённый progress PostgreSQL и Kvrocks watermark
и обнаруживает такой rollback. При обнаружении потери данных worker завершает
работу, systemd запускает его снова, а при старте он выбирает более раннюю из
сохранившихся точек и повторно индексирует потерянный диапазон. Повторная запись
этого диапазона идемпотентна.

Несколько live workers не делят блоки между собой для ускорения индексации.
Они обеспечивают standby failover. Параллельная обработка непересекающихся
архивных диапазонов является отдельным режимом.

## 5. Несколько API instances

На каждой API машине устанавливаются `ton-index-go` и `libton-marker.so`, как
описано в standard deployment. Все instances используют одну read-only
PostgreSQL role и одинаковые HA endpoints.

Перед API размещается отдельный load balancer. Он должен направлять traffic
только в healthy instances и исключать instance при недоступности API,
PostgreSQL или Kvrocks. Локального persistent state у `ton-index-go` нет, поэтому
sticky sessions не требуются.

## 6. Action classifier

Action classifier остаётся опциональным. Если он нужен, запускаются несколько
instances, использующих общий PostgreSQL write endpoint, Kvrocks Sentinels и
отказоустойчивый Redis event-cache. Порядок подготовки индексов и запуска
описан в общей
[процедуре action classifier](procedures/action-classifier.md).

<a id="live-only"></a>

## 7. Вариант A: HA без архивной истории

1. Собрать и установить компоненты по standard deployment.
2. На одном bootstrap host подготовить схему PostgreSQL и пустой Kvrocks.
3. Из подготовленных хранилищ развернуть Patroni cluster, Kvrocks replicas,
   Redis Sentinels и внутренние HAProxy. На каждом PostgreSQL member установить
   `pgton`.
4. Подготовить несколько синхронизированных пар TON node + worker, но пока не
   запускать workers.
5. Указать всем workers одинаковую стартовую точку, PostgreSQL write endpoint и
   Sentinel configuration.
6. Запустить все workers и убедиться, что один получил leadership, а остальные
   работают как standby.
7. Запустить несколько API instances и только после проверки data pipeline
   разрешить traffic на API load balancer.
8. Если нужны actions, выполнить процедуру action classifier в continuous mode.

Этот вариант не запускает `ton-smc-scanner` и не загружает архивные данные.

<a id="shard-state-bootstrap"></a>

## 8. Вариант B: HA со shard state snapshot, но без истории

В этом варианте `ton-smc-scanner` запускается до создания HA clusters. Scanner
работает с одиночными PostgreSQL и Kvrocks instances и создаёт current state на
выбранном masterchain seqno `S`.

1. На одном bootstrap host подготовить схему PostgreSQL и Kvrocks.
2. Выполнить
   [процедуру shard state bootstrap](procedures/shard-state-bootstrap.md) для
   seqno `S`.
3. После успешной проверки snapshot развернуть из подготовленных instances
   Patroni cluster, Kvrocks replicas, Redis Sentinels и HAProxy.
4. Подготовить несколько пар TON node + worker и настроить им согласованную со
   snapshot стартовую точку.
5. Запустить workers в leader/standby mode.
6. Запустить API instances и разрешить traffic после проверки data pipeline.
7. Если нужны actions, запустить classifier в continuous mode.

Исторических blocks, transactions, messages и traces до `S` в этом варианте не
будет.

<a id="full-history"></a>

## 9. Вариант C: HA с полной историей и shard state snapshot

В этом варианте до запуска production-окружения загружается доступная архивная
история и выполняется `ton-smc-scanner` для выбранного masterchain seqno `S`.
После проверки подготовленных данных live workers продолжают индексацию с
согласованной точки.

В базовой конфигурации используется один PostgreSQL cluster. Он содержит всю
загруженную историю и продолжает принимать новые данные от live workers.
Отдельная база для live-данных не нужна, а `ton-index-go` подключается к одному
PostgreSQL endpoint через HAProxy.

Если единый cluster становится слишком большим, данные можно разделить между
двумя PostgreSQL clusters:

- cold cluster хранит архивную часть диапазона;
- hot cluster принимает новые данные от live workers;
- `ton-index-go` подключается к обоим clusters и выбирает нужный по границе
  диапазонов.

Такое разделение является дополнительной оптимизацией хранения, а не
обязательной частью этого варианта развёртывания. Полный порядок его настройки
описан в [hot/cold procedure](procedures/hot-cold.md).

1. До создания HA clusters подготовить на одном bootstrap host одиночные
   PostgreSQL и Kvrocks и выполнить
   [процедуру загрузки полной истории](procedures/full-history.md).
2. Процедура определяет архивные диапазоны, момент запуска `ton-smc-scanner` и
   порядок classifier.
3. Развернуть из подготовленной базы один PostgreSQL HA cluster. Если выбрано
   разделение истории и live-данных, вместо него развернуть отдельные cold и
   hot PostgreSQL clusters.
4. Развернуть Kvrocks HA cluster и запустить live workers в leader/standby mode.
5. Настроить `ton-index-go` на один PostgreSQL endpoint либо на hot и cold
   endpoints, после чего проверить запросы и разрешить production traffic.

## 10. Проверить failover

До production traffic нужно по отдельности проверить:

1. Patroni promotion после потери PostgreSQL primary, переключение HAProxy
   write endpoint и перезапуск worker при потере реплицированного хвоста.
2. Kvrocks promotion через Sentinel, переподключение workers и API и
   перезапуск worker при откате Kvrocks watermark.
3. Получение leadership standby worker после остановки текущего leader.
4. Продолжение индексации после полной потери одной пары TON node + worker.
5. Удаление остановленного API instance из балансировки.

После каждого failover нужно проверить не только доступность сервисов, но и
продвижение masterchain seqno, отсутствие gaps в PostgreSQL и согласованность
PostgreSQL progress с Kvrocks watermark.

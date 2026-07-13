# Варианты развёртывания

TON Indexer поддерживает три базовых варианта развёртывания. Дополнительные
возможности — shard state bootstrap, полная история, action classifier и
разделение hot/cold — выбираются отдельно и не образуют самостоятельные
топологии.

1. **Docker Compose.** Подходит для локальной разработки и тестовых окружений и
   не рекомендуется для production. PostgreSQL, Kvrocks, Redis event-cache,
   index worker, API и action classifier запускаются на одной машине с
   TON-нодой. Индексация начинается с текущей или явно выбранной точки и
   продолжается вперёд без архивной истории и shard state snapshot. Action
   classifier входит в стандартный Compose stack и запускается по умолчанию.
   См. [Docker Compose runbook](runbooks/docker-compose.md).

2. **Standard deployment.** Компоненты запускаются через systemd без
   автоматического failover. TON-нода и обслуживающий её worker находятся на
   одной машине, потому что worker читает локальную TON node DB. PostgreSQL,
   Kvrocks и API разворачиваются на отдельных machines. По умолчанию worker
   начинает индексировать вперёд с выбранного masterchain seqno. См.
   [standard deployment runbook](runbooks/standard-deployment.md).

3. **High availability deployment.** Используются несколько пар TON-нода +
   worker, отказоустойчивые PostgreSQL и Kvrocks, несколько API instances и
   внутренние load balancers. Live workers работают в leader/standby mode, а
   каждый компонент имеет резервный instance в другом failure domain. См.
   [HA runbook](runbooks/high-availability.md).

## Дополнительные возможности

### Shard state bootstrap

`ton-smc-scanner` однократно загружает полный current state аккаунтов на момент
выбранного masterchain seqno `S`. Эта возможность нужна, если окружение должно
сразу получить состояния аккаунтов, которые не изменялись после начала live
индексации.

Bootstrap можно добавить к standard или HA deployment. Общий порядок запуска
описан в [shard state bootstrap procedure](runbooks/procedures/shard-state-bootstrap.md),
а точки подключения — в разделах
[standard deployment](runbooks/standard-deployment.md#shard-state-bootstrap) и
[HA deployment](runbooks/high-availability.md#shard-state-bootstrap).

### Полная история

Если PostgreSQL должен содержать blockchain history начиная с masterchain seqno
`1`, до перехода в live mode выполняется archive backfill с одной или нескольких
архивных TON-нод. Процедура также включает shard state bootstrap, проверку gaps,
опциональную историческую классификацию actions и отложенное создание тяжёлых
indexes.

Порядок описан в
[full-history procedure](runbooks/procedures/full-history.md). Для HA topology
дополнительные шаги собраны в
[HA-варианте с полной историей](runbooks/high-availability.md#full-history).

### Action classifier

Action classifier преобразует traces в высокоуровневые actions. В standard и
HA deployment он опционален; в Docker Compose запускается по умолчанию. Для
исторических данных classifier можно запустить после archive backfill, а затем
оставить работать в continuous mode.

Подготовка Python environment, минимальных indexes и monitoring очереди описаны
в [action classifier procedure](runbooks/procedures/action-classifier.md).

### Hot/cold разделение

При большом объёме полной истории архивные и live-данные можно разделить между
cold и hot PostgreSQL clusters. Это дополнительная оптимизация хранения, а не
обязательная часть full-history или HA deployment. По умолчанию вся история и
новые данные могут храниться в одном PostgreSQL cluster.

# Процедура: бэкфил и развёртывание ton-index-firehose

`ton-index-firehose` читает локальную DB архивной TON-ноды и выдаёт Firehose
blocks типа `sf.ton.type.v1.Block`: по одному на masterchain seqno, вместе с
shard blocks, впервые зафиксированными этим masterchain block. Структура blocks
близка к `block.tlb`, дополнительно в блок попадают завершённые traces и новые
code, data и libraries аккаунтов, изменившихся в этом блоке (data больше
64 KiB не включается, вместо неё ставится `boc_omitted`). Proto-описание лежит
в `ton-index-worker/ton-index-firehose/proto`.

У tool два режима:

- **bounded range** (`--from`, `--to`, `--merged-blocks-dir`) пишет готовые
  merged-blocks bundles диапазона. Он используется для бэкфила истории;
- **live** (без `--to`) печатает Firehose protocol в stdout для firecore
  `reader-node` и продолжает с собственного cursor.

```text
архивная TON-нода                                    store host
ton-index-firehose (bounded) ── backfill/runner.py ─► S3 firehose/merged-blocks ◄── merger
                                                                                      ▲
firecore reader-node + ton-index-firehose (live) ───► S3 firehose/one-blocks ─────────┘
            └── live blocks, gRPC :10010 ───────────────────────────────────────► relayer
                                                                                      │
                                              firehose, substreams tier1/tier2 ◄──────┘
```

История конвертируется bounded-прогонами прямо в merged-blocks, live-часть
собирает merger из one-block files reader-node. Merger продолжает с последнего
bundle в merged-blocks, поэтому история должна доходить ровно до блока, с
которого стартует reader-node.

Все `<PLACEHOLDERS>` нужно заменить реальными значениями. Примеры предполагают
TON DB в `/var/ton-work/db` и OS-пользователя ноды `validator`.

## 1. Требования

**Архивная TON-нода.** Tool читает shard states каждого блока, поэтому нода
должна хранить blocks и states за весь конвертируемый диапазон (большие
`--state-ttl` и `--archive-ttl`). Для истории подходит и остановленная копия
DB: tool открывает её как RocksDB secondary и ничего в неё не пишет. Live
reader-node работает рядом с синхронизированной нодой; отставание ноды во время
бэкфила некритично, она догонит после.

**S3-совместимое хранилище.** Один bucket, например `firehose`, с префиксами
`merged-blocks`, `one-blocks` и `forked-blocks`. Bundles уже сжаты zstd, поэтому
компрессия на стороне хранилища не нужна. Нужны два ключа:

- ключ бэкфила с правами read/write на bucket — для runner;
- ключ firecore с правами read/write — для reader-node, merger и substreams
  (merger удаляет обработанные one-block files).

**Объём.** По состоянию на октябрь 2026 года mainnet merged-blocks
`1..99 999 999` занимают около `1.9 TiB` (1 000 000 bundles). Bundles весят
примерно `0.14–0.26` объёма archive packages ноды за тот же диапазон. Для
локальной копии кусков на время конвертации нужно `20–40 GB` на worker.

**Инструменты.** На хосте runner нужны `python3`, `rclone` и `zstd`; скрипты
бэкфила лежат в `ton-index-worker/ton-index-firehose/backfill`.

**Версии.** firecore `v1.20.1`; он собран с substreams `v1.23.0`, CLI
substreams должен быть той же версии.

## 2. Собрать ton-index-firehose

Собирать standalone-проект `ton-index-worker`: корневому проекту нужен Go,
этому — нет. Собирать только clang: gcc не собирает код ton (ошибка
`call of overloaded 'return_value(...)' is ambiguous`).

```bash
sudo apt update
sudo apt install -y \
  build-essential cmake clang ninja-build openssl libssl-dev zlib1g-dev \
  gperf git curl liblz4-dev pkg-config libsecp256k1-dev libsodium-dev \
  libhiredis-dev python3-dev libpq-dev automake autoconf libtool \
  libjemalloc-dev libmicrohttpd-dev libcurl4-openssl-dev \
  libprotobuf-dev protobuf-compiler libzstd-dev ccache

git clone --recursive https://github.com/toncenter/ton-indexer.git
cd ton-indexer
git checkout "<RELEASE_OR_COMMIT>"
git submodule update --init --recursive

CC=clang CXX=clang++ cmake -S ton-index-worker -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DTON_USE_JEMALLOC=ON \
  -DCMAKE_DISABLE_FIND_PACKAGE_GSL=ON \
  -DCMAKE_DISABLE_FIND_PACKAGE_BLAS=ON

cmake --build build --parallel "$(nproc)" --target ton-index-firehose
```

Результат: `build/ton-index-firehose/ton-index-firehose`. GSL и BLAS нужны только
`rldp2` из ton, который tool не использует; без них binary не требует BLAS на
хосте ноды. На целевом хосте нужны `libjemalloc2`, `curl` и `zstd`.

Сборка накладывает patch на `external/ton/third-party/rocksdb` прямо в
исходниках. Если checkout уже собирался с другой версией patch, сборка падает
с `Patch cannot be applied cleanly`: собирать из чистого checkout.

## 3. Подготовить хранилище

Ниже store называется `garage:` (так было в production: Garage, region
`garage`, path-style). Remote `garage:` для rclone задаётся через environment
скриптом `backfill/rclone-garage.sh`: он читает файл ключа с переменными
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` и `S3_ENDPOINT`.

```bash
sudo install -d -m 0750 -o root -g validator /etc/fh-backfill
sudo install -m 0640 -o root -g validator <KEY_FILE> /etc/fh-backfill/backfill.env
```

firecore обращается к store по URL вида:

```text
s3://<STORE_HOST>:3900/firehose/merged-blocks?region=garage&insecure=true
```

Ключи firecore берёт из `AWS_ACCESS_KEY_ID` и `AWS_SECRET_ACCESS_KEY`.

Bundles называются `%010d.dbin.zst` по первому блоку и содержат по 100 blocks.
Bundle `0000000000` содержит blocks `1..99`: block `0` — zerostate, а не block.

Не запускать rclone с `-vv`: debug log печатает ключи. На миллионе объектов
команды rclone, которые листают весь префикс (`cat`, `check` и `copy` без
`--no-traverse`), идут минутами; runner и `verify.py` обращаются к файлам по
одному через `--files-from-raw` и `--no-traverse`.

## 4. Нарезать очередь кусков

Бэкфил идёт кусками: один кусок — один bounded-прогон tool. Прогон нельзя
продолжить с середины, поэтому упавший кусок конвертируется заново целиком.
Работа на block растёт с объёмом archive packages, поэтому куски режутся по
объёму архива. Команды здесь и ниже выполняются из корня checkout:

```bash
B=ton-index-worker/ton-index-firehose/backfill
du -s --block-size=1M /var/ton-work/db/archive/packages/arch* > arch_sizes.txt

$B/make_chunks.py arch_sizes.txt 1 <S> 15000 2000000 > chunks.tsv
```

Аргументы: диапазон, максимум MB архива на кусок, максимум blocks на кусок.
Каждая строка — `from to prewarm`. Куски начинаются с кратного 100 и
заканчиваются перед кратным 100, как требуют bundles. `<S>` должен быть
`X99`: последний bundle тоже должен быть полным.

`prewarm` — сколько masterchain blocks перед началом куска прочитать для
TraceAssembler: каждый trace, пересекающий начало куска, должен начинаться
внутри этого окна, иначе tool завершается с `trace ... is broken`.
`make_chunks.py` ставит `2000` до seqno `59 000 000` и `3000` после (blocks
стали чаще).

## 5. Запустить runner

`backfill/runner.py` конвертирует куски, проверяет и заливает bundles и хранит
состояние каждого куска в JSON-файле, поэтому его можно остановить и запустить
снова. Кусок считается готовым, когда tool завершился с `rc 0` и строкой
`Finished bounded archive range`, на месте все bundles, они залиты и сверены
со store по MD5. После этого локальная копия удаляется.

```bash
B=ton-index-worker/ton-index-firehose/backfill
H=/var/lib/firehose-backfill
sudo install -d -o validator -g validator $H $H/state $H/work
sudo install -m 0755 build/ton-index-firehose/ton-index-firehose $H/ton-index-firehose
sudo install -m 0755 $B/runner.py $B/rclone-garage.sh $B/verify.py $H/

sudo -u validator python3 $H/runner.py init --state $H/state --chunks chunks.tsv
```

Runner запускается transient-сервисом: он переживает ssh session, перезапускается
при падении и продолжает со своего состояния. Tool, убитый за память, — это
неудачная попытка куска, а не конец сервиса (`OOMPolicy=continue`).

```bash
sudo systemd-run --unit=fh-backfill \
  -p Restart=on-failure -p RestartSec=60 -p OOMPolicy=continue \
  -p User=validator -p Group=validator \
  -p LimitNOFILE=1048576 -p MemoryMax=64G -p TimeoutStopSec=300 \
  -p StandardOutput=append:$H/runner.log -p StandardError=append:$H/runner.log \
  -E GARAGE_ENV=/etc/fh-backfill/backfill.env \
  python3 $H/runner.py run --state $H/state --work $H/work \
  --bin $H/ton-index-firehose --db /var/ton-work/db \
  --tool-args "--threads 96 --max-active-tasks 96 --celldb-cache-size 24576 --celldb-direct-io --stats-freq 60" \
  --workers 1 --rclone $H/rclone-garage.sh --dest garage:firehose/merged-blocks
```

Параметры tool:

- `--threads` и `--max-active-tasks` — больше числа ядер: заметная часть
  времени уходит на ожидание чтений из cell db;
- `--celldb-direct-io` с большим `--celldb-cache-size` — на XFS/ext4, когда
  много потоков читают cell db через page cache и упираются в ядро. На ZFS не
  работает: там без него и с `--celldb-cache-size 8192`;
- `--max-queue-messages` оставить по умолчанию (`1000000`): при `4000000` в
  тяжёлой эпохе процесс не уложился в `64G`.

Если binary собран без `TON_USE_JEMALLOC`, передать jemalloc через
`--env LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2`: с ним
конвертация примерно на треть быстрее.

`--workers N` запускает N кусков параллельно (с паузой `--stagger` секунд между
стартами), `--only <from>,...` и `--limit N` выбирают куски для пробного
прогона. Остановка: `sudo systemctl stop fh-backfill` — runner останавливает
tool и возвращает текущие куски в очередь.

Ориентиры по скорости (октябрь 2026, mainnet):

| Host | Настройка | Скорость |
| --- | --- | --- |
| 16 ядер, XFS, нода работает | 1 worker, 96 потоков, direct IO, cache 24 GiB | 40M: ~200 bl/s; 50–56M: ~550–650 bl/s; 68M+: ~1900 bl/s; ~265 GB архива в час |
| 96 vCPU, ZFS, нода остановлена | 4 workers по 64 потока, cache 8 GiB | 0–35M: ~1300 bl/s суммарно; 40–45M: ~280 bl/s; ~420–450 GB архива в час |

Самая тяжёлая часть — `40–45M` (~2.6 TB архива на 5M blocks). Вся история
`1..100M` на двух таких машинах заняла около 18 часов. Пик памяти — до ~30 GB
на worker в тяжёлой эпохе.

## 6. Распределить куски между машинами

Каждый кусок должен принадлежать ровно одной очереди. Удобнее сразу разделить
`chunks.tsv` по объёму архива пропорционально скорости машин и завести на
каждой своё состояние.

Перенести куски с работающего runner на другую машину можно без остановки:
runner берёт только куски в статусе `pending` и не трогает файлы чужих кусков.
В исходном состоянии поменять статус переносимых кусков, а на новой машине
завести их заново:

```bash
cd $H/state
python3 - <<'EOF'
import json, pathlib
for f in sorted(pathlib.Path("chunks").glob("*.json")):
    c = json.loads(f.read_text())
    if c["from"] >= <SPLIT> and c["status"] == "pending":
        c["status"] = "moved"
        tmp = f.with_suffix(".edit"); tmp.write_text(json.dumps(c)); tmp.replace(f)
        print(c["from"], c["to"], c["prewarm"])
EOF
```

Вывод — строки `from to prewarm` для `runner.py init` на новой машине. Брать
куски с конца очереди: runner берёт их по порядку с начала.

## 7. Следить за прогрессом и сбоями

```bash
sudo -u validator python3 $H/runner.py status --state $H/state
tail -5 $H/runner.log
```

`status` показывает счётчики кусков по статусам, готовые blocks, залитый объём
и упавшие куски. Лог каждого куска (и каждой неудачной попытки) остаётся в
`$H/state/logs`.

- **`trace ... is broken`.** Runner сам повторяет кусок, увеличивая `prewarm`
  в 4 раза до `20000`. Если и этого мало, поставить prewarm вручную: в JSON
  куска `status: "pending"`, `prewarm: <N>`, `attempts: 0`. Так, у
  `3 200 000..3 299 999` цепочка сообщений тянется больше суток, и куску
  понадобился `prewarm 1000000`.
- **Кусок упал 3 раза** — статус `failed`. После устранения причины
  `runner.py reset --state $H/state` возвращает упавшие куски в очередь.
- **Tool стоит, в его логе не меняется строка `Q[...]`.** Взаимоблокировка
  очереди на гигантских раундах (около seqno `3.29M`, до 90k транзакций на
  masterchain block) в binaries до коммита `41011e6d`. Собрать свежий binary и
  перезапустить кусок.

## 8. Проверить историю

После того как все очереди закончились, собрать состояния всех машин в одно
место и проверить их вместе со store:

```bash
GARAGE_ENV=/etc/fh-backfill/backfill.env $H/verify.py \
  --rclone $H/rclone-garage.sh --src garage:firehose/merged-blocks \
  --from 1 --to <S> --states <STATE_DIR> [<STATE_DIR>...] --jobs 16
```

Проверка занимает минуты:

- готовые куски всех состояний покрывают `1..S` ровно один раз;
- store содержит каждый bundle диапазона, а суммарный размер побайтно совпадает
  с тем, что залили runners;
- bundles по обе стороны каждой границы кусков связаны по `parent_num` и
  `parent_id`.

Внутренность куска к этому моменту уже проверена: tool пишет раунды строго
подряд, runner сверил имена bundles, upload — MD5. Без `--states` `verify.py`
скачивает и читает каждый bundle диапазона со скоростью сети.

Дополнительно, на store host:

```bash
firecore tools check merged-blocks "s3://<STORE_HOST>:3900/firehose/merged-blocks?region=garage&insecure=true" -r 0:<S + 1>
firecore tools check merged-blocks "s3://<STORE_HOST>:3900/firehose/merged-blocks?region=garage&insecure=true" -r <A>:<B> -b
```

Первая команда ищет дыры по именам файлов, вторая читает bundles и проверяет
их content на диапазоне. Предупреждение о 99 blocks в bundle `0000000000`
ожидаемо.

## 9. Запустить live reader-node

Reader-node стартует с блока, следующего за историей. Пока шла подготовка,
сеть уходит вперёд, поэтому перед стартом догнать историю runner'ом до
`X 099 999`, близкого к голове сети (bounded-прогон сам дождётся последнего
блока), и стартовать reader-node с `X 100 000`.

Reader-node разворачивается плейбуком `deploy-firehose.yaml` из корня
репозитория. Он собирает tool на хосте `builder`, скачивает firecore с
проверкой sha256 и ставит сервис `{{ stack_name }}__firehose_reader` на
хосты группы `{{ stack_name }}__firehose_reader`. Сервис работает от root.

Переменные (`ansible/config.yaml`):

| Переменная | Значение |
| --- | --- |
| `firecore_version`, `firecore_sha256` | релиз firecore и sha256 `firehose-core_linux_x86_64.tar.gz` из `checksums.txt` релиза |
| `firehose_store` | `s3://<STORE_HOST>:3900/firehose` |
| `firehose_s3_access_key_id`, `firehose_s3_secret_access_key` | ключ firecore; пишется в env file с правами `0600` |
| `services.firehose_reader.node_args` | аргументы tool: `--from <X 100 000>`, `--prewarm 3000`, потоки |
| `services.firehose_reader.additional_args` | флаги reader-node, в том числе `--reader-node-start-block-num <X 100 000>` |

В inventory для каждого хоста задать `firehose_reader_grpc_addr` — приватный
адрес, к которому подключается relayer, например
`<READER_PRIVATE_IP>:10010`. Остальные порты reader-node слушают только
localhost.

```bash
ansible-playbook -i <INVENTORY> deploy-firehose.yaml \
  -e @ansible/config.yaml [-e @<SECRETS>]
```

С `--tags deploy` плейбук только передеплоит сервис из уже собранного
`artifacts/ton-index-firehose`.

Особенности:

- `--from` действует только при первом старте. Дальше tool продолжает с
  cursor в `<working-dir>/firehose_cursor`, который отстаёт от выданных blocks
  на 11000: после перезапуска часть blocks выдаётся повторно, merger их
  отбрасывает. Чтобы начать заново, остановить сервис и удалить working
  directory tool.
- `--reader-node-one-block-suffix` — без дефисов: merger разбирает имена
  one-block files по `-` и падает с `panic: wrong filename format`. Плейбук
  берёт имя хоста, оставляя в нём только буквы и цифры.
- `--reader-node-blocks-chan-capacity 2000`: при заполнении канала на 90%
  reader-node завершается, а при догоне tool выдаёт blocks быстрее, чем они
  успевают уйти.
- `MemoryMax=32G`: самому процессу хватает ~7 GB, остальное — page cache DB,
  который вытесняется у лимита.
- После старта reader-node догоняет сеть за минуты: на этих эпохах tool выдаёт
  1–2 тысячи blocks в секунду, дальше отставание от сети — 1–2 секунды.

## 10. Запустить merger и relayer на store host

firecore ставится из release с проверкой sha256:

```bash
V=v1.20.1
curl -sSfLO https://github.com/streamingfast/firehose-core/releases/download/$V/firehose-core_linux_x86_64.tar.gz
curl -sSfLO https://github.com/streamingfast/firehose-core/releases/download/$V/checksums.txt
grep " firehose-core_linux_x86_64.tar.gz$" checksums.txt | sha256sum -c -
sudo install -d /opt/firehose-core/$V
sudo tar -C /opt/firehose-core/$V -xzf firehose-core_linux_x86_64.tar.gz
sudo ln -sfn /opt/firehose-core/$V/firecore /usr/local/bin/firecore

sudo useradd --system --home-dir /srv/scratch/firecore --shell /usr/sbin/nologin firecore
sudo install -d -o firecore -g firecore -m 0750 /srv/scratch/firecore
sudo install -d -m 0755 /etc/firecore
sudo install -o root -g firecore -m 0640 <FIRECORE_KEY_FILE> /etc/firecore/garage.env
```

Units лежат в `ton-index-worker/ton-index-firehose/deploy`. В
`firecore-relayer.service` указать в `--relayer-source` адрес reader-node,
затем:

```bash
sudo install -m 0644 firecore-merger.service firecore-relayer.service /etc/systemd/system/
sudo install -d -o firecore -g firecore /srv/scratch/firecore/merger /srv/scratch/firecore/relayer
sudo systemctl daemon-reload
sudo systemctl enable --now firecore-merger firecore-relayer
```

Merger при старте листает merged-blocks (на миллионе bundles около 30 секунд)
и пишет, с какого блока продолжает:

```text
resetting bundler base block num {"new_base_block_num": <X 100 000>, "lib": "#<X 099 999> ..."}
```

Если base меньше старта reader-node, история не дотянута до него: merger будет
ждать one-block files, которых нет. Relayer пишет `processing block` с `age`
порядка пары секунд. Healthz: merger — `127.0.0.1:10013/healthz`, relayer —
`127.0.0.1:10018/healthz`.

## 11. Проверить live

Цепочка bundles через стык истории и live:

```bash
GARAGE_ENV=/etc/fh-backfill/backfill.env $H/verify.py \
  --rclone $H/rclone-garage.sh --src garage:firehose/merged-blocks \
  --from <X 099 800> --to <LAST_MERGED_BUNDLE + 99>
```

Live и бэкфил дают одинаковые данные: bundle, собранный merger из one-block
files, побайтно совпадает (после распаковки zstd) с bounded-конвертацией того
же диапазона. Это стоит проверить на одном bundle после первого развёртывания.

## 12. Firehose и Substreams

Firehose и substreams tiers запускаются на store host тем же `firecore`, от
пользователя `firecore` с `EnvironmentFile=/etc/firecore/garage.env`. Пример
запуска:

```text
firecore start firehose substreams-tier1 substreams-tier2 --config-file= \
  --data-dir=/srv/scratch/firecore/data --log-to-file=false \
  --common-first-streamable-block=1 \
  "--common-merged-blocks-store-url=s3://localhost:3900/firehose/merged-blocks?region=garage&insecure=true" \
  --common-live-blocks-addr=127.0.0.1:10014 \
  --advertise-chain-name=ton --advertise-block-id-encoding=hex --ignore-advertise-validation \
  --substreams-tier1-block-type=sf.ton.type.v1.Block \
  --firehose-grpc-listen-addr=127.0.0.1:10015 \
  --substreams-tier1-grpc-listen-addr=127.0.0.1:10016 \
  --substreams-tier2-grpc-listen-addr=127.0.0.1:10017 \
  --substreams-tier1-subrequests-endpoint=127.0.0.1:10017 \
  --substreams-state-store-url=/srv/scratch/firecore/substreams-states \
  --metrics-listen-addr=127.0.0.1:9102
```

- Без `--config-file=` firecore ищет `./firehose.yaml` и не стартует.
- `--advertise-*`, `--ignore-advertise-validation` и
  `--substreams-tier1-block-type` обязательны: firecore не знает TON и не может
  вывести их из genesis block.
- `--common-live-blocks-addr` — адрес relayer, через него идут live blocks; с
  пустым значением отдаётся только история.
- По умолчанию порты слушают все интерфейсы: задать адреса явно.
- `firecore tools print` и `check -f` декодируют blocks, только если передать
  proto: `--proto-paths <DIR>` с содержимым `ton-index-firehose/proto`.

Smoke-модуль `ton-index-worker/ton-index-firehose/substreams/smoke` считает
shard blocks, транзакции и traces каждого блока и держит итоги в store, так что
его запуск проходит через оба tier. Модули на prost держать на
`substreams = "0.7"`: 0.8 перешёл на другую protobuf-библиотеку.

```bash
rustup target add wasm32-unknown-unknown
cd ton-index-worker/ton-index-firehose/substreams/smoke
cargo build --target wasm32-unknown-unknown --release
substreams pack substreams.yaml -o ton-smoke.spkg

substreams run -e 127.0.0.1:10016 --plaintext ton-smoke.spkg map_block_stats \
  -s 40000000 -t +3 -o jsonl
substreams run -e 127.0.0.1:10016 --plaintext ton-smoke.spkg map_totals \
  -s 200000 -t +1 --production-mode --limit-processed-blocks 0 -o jsonl
substreams run -e 127.0.0.1:10016 --plaintext ton-smoke.spkg map_block_stats \
  -s -5 -o jsonl
```

Последняя команда начинает за 5 blocks до головы сети и дальше идёт по live
blocks. Store-модуль нельзя вывести напрямую, поэтому итоги читает
`map_totals`. `--limit-processed-blocks 0` снимает ограничение CLI в 10 000
blocks на подготовку stores.

## 13. Частые ошибки

| Симптом | Причина и решение |
| --- | --- |
| `call of overloaded 'return_value(...)' is ambiguous` при сборке | сборка gcc; задать `CC=clang CXX=clang++` |
| `Patch cannot be applied cleanly: fix-secondary-open.patch` | rocksdb уже пропатчен другой версией patch; собирать из чистого checkout |
| `trace ... is broken; ... raise --prewarm` | trace начался раньше окна prewarm; см. раздел 7 |
| tool стоит, `Q[...]` в логе не меняется | binary до `41011e6d`; пересобрать |
| `panic: wrong filename format` в merger | дефисы в `--reader-node-one-block-suffix` |
| merger не мержит, base меньше старта reader-node | история не дотянута до старта reader-node; дозалить runner'ом |
| `unable to find config file "./firehose.yaml"` | добавить `--config-file=` |
| `failed to find message 'sf.ton.type.v1.Block'` в `tools print` | передать `--proto-paths` |
| reader-node завершается, канал заполнен на 90% | увеличить `--reader-node-blocks-chan-capacity` |
| rclone долго стартует на каждый файл | он листает весь префикс; `--files-from-raw` с `--no-traverse` |

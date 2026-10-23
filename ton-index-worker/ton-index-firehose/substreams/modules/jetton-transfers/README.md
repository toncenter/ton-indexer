# jetton-transfers: переводы джеттонов

`map_jetton_transfers` отдаёт на каждый masterchain-раунд переводы джеттонов:
сообщения `transfer#0f8a7ea5`, которые получили jetton-кошельки. Кошелёк
проверяется у своего мастера, как это делает индекс (`tondb-scanner`:
`JettonWalletDetectorR` и `ActionDetector`).

Как это работает, по раунду:

1. Кандидаты — транзакции, у входящего сообщения которых `opcode` равен
   `0x0f8a7ea5`; блок читается через lazy view, целиком декодируются только
   они.
2. Пачка `get_wallet_data` на различных кандидатах после раунда: успешный
   ответ (exit code 0 или 1) с `balance:int owner:slice master:slice
   code:cell` даёт владельца и мастера.
3. Пачка `get_wallet_address(owner)` на их мастерах после раунда: кошельком
   мастера считается только аккаунт, который мастер выводит для владельца.
4. Тело читается по TEP-74. Тело, которое не разбирается, — не перевод, как и
   транзакция, которая не ordinary.

Формат — `proto/jettons/v1/jetton_transfers.proto`, поля как у
`JettonTransfer` индекса: адреса в raw-виде `0:HEX`, `addr_none` строкой,
суммы десятичными строками, payload — BOC.

Отличия от индекса и API v3:

- forward payload, лежащий в самом теле (`Either` с битом 0), отдаётся
  отдельной ячейкой; индекс хранит только payload по ссылке;
- BOC payload кодируются не так, как у индекса (порядок ячеек, без crc32):
  сравнивать нужно хэши корней;
- переводы с `transaction_aborted` модуль отдаёт, как их хранит индекс, а
  API v3 (`/api/v3/jetton/transfers`) их не показывает.

Сверка с API v3 (2026-10-23): раунды 97 000 000–97 000 099 — 49 переводов из
49, 45 000 000–45 000 019 — 184 из 184, все поля совпали (payload — по хэшу
корня).

Стоимость — два вызова `ton_v1` на кошелёк в раунде; 20 раундов 2024 года
(45M) считаются ~6 с на одном воркере.

```bash
cargo build --release --target wasm32-unknown-unknown -p jetton_transfers
```

```bash
substreams pack substreams.yaml -o jetton-transfers.spkg
```

```bash
substreams run jetton-transfers.spkg map_jetton_transfers -s 97000000 -t +100 -o jsonl
```

# Toncenter Streaming API v2

The Toncenter Streaming API v2 delivers low-latency, real-time updates on transactions and actions observed on the TON blockchain.

Clients can subscribe to these updates by monitoring specific wallet or contract addresses, or by subscribing to a specific trace via **Server-Sent Events (SSE)** or **WebSocket** protocols.

Ideal for developers building wallets, explorers, monitoring systems, or automation tools, this API provides immediate insight into blockchain activity, while making the finality of each update explicit and easy to reason about.

---

## Supported Interfaces

The API offers two transport protocols with distinct connection semantics:

* **SSE** — `https://testnet.toncenter.com/api/streaming/v2/sse`
  Recommended for browser environments or clients that prefer HTTP streaming.

* **WebSocket** — `wss://testnet.toncenter.com/api/streaming/v2/ws`
  Preferred for persistent, bidirectional communication with dynamic subscribing/unsubscribing patterns.

---

## Finality Levels

All trace-based events (`transactions`, `actions`, `trace`) in v2 carry an explicit `finality` field:

```
"finality": "pending" | "confirmed" | "finalized"
```

The lifecycle is **monotonic** per trace:

```text
pending  →  confirmed  →  finalized
    \             ↘
     \-----> trace_invalidated
```

* **`pending`** — result of emulation or speculative execution; may be invalidated.
* **`confirmed`** — trace or transactions are included in a candidate shard block; small risk of rollback.
* **`finalized`** — committed in masterchain; not subject to update or invalidation.

For non-trace events:

* `account_state_change` and `jettons_change` are emitted only at `finality = "confirmed"` and `finality = "finalized"`.

Clients can choose how early they want to see updates via a `min_finality` filter in subscriptions.

---

## Using Server-Sent Events (SSE)

SSE uses a single `POST` request to establish the connection and specify the subscription. No further messages are sent by the client on this connection.

### SSE Request Fields

`POST https://toncenter.com/api/streaming/v2/sse`

| Field                    | Type     | Description                                                                                                                |
| ------------------------ | -------- | -------------------------------------------------------------------------------------------------------------------------- |
| `addresses`              | string[] | Wallet or contract addresses to monitor. Accepts any valid address format.                                                 |
| `trace_external_hash_norms` | string[] | *(Optional)* List of trace external hashes to monitor. Required when subscribing to `"trace"` events.                    |
| `types`                  | string[] | Event types to receive. See [Event Types](#event-types).                                                                   |
| `min_finality`           | string   | *(Optional)* Minimum finality level to receive: `"pending"`, `"confirmed"`, or `"finalized"`. Default is `"finalized"`.    |
| `replay_existing` | boolean *(optional)* | If `true`, send the latest matching snapshots still retained in Redis before continuing with live updates. Default: `false`. |
| `msg_body_hash` | string *(optional)* | Match the body cell hash of a transaction’s **incoming** message. Requires non-empty `addresses` and `"transactions"` in `types`. Accepts hex or base64 hash formats. Only filters `transactions`. |
| `include_address_book`   | boolean  | *(Optional)* If `true`, includes DNS-resolved names and friendly names for addresses.                                      |
| `include_metadata`       | boolean  | *(Optional)* If `true`, includes metadata for known token contracts (Jettons, NFTs, etc.).                                 |
| `action_types`           | string[] | *(Optional)* Filter actions by type (e.g. `["jetton_transfer","ton_transfer"]`). Only applies to `"actions"` events.       |
| `supported_action_types` | string[] | *(Advanced / Optional)* Advertise which action classification types the client understands. `"latest"` is used by default. |

> **Note:** In v2 there is no separate `"pending_transactions"` / `"pending_actions"` event type.
> You always subscribe to `"transactions"` / `"actions"` and use `min_finality` to control how early updates you receive.
>
> To subscribe to a trace, include `"trace"` in `types` and provide `trace_external_hash_norms`. Addresses are optional when subscribing only to `"trace"`.

#### Example: lowest-latency stream (pending → confirmed → finalized)

```http
POST https://toncenter.com/api/streaming/v2/sse
Content-Type: application/json
Accept: text/event-stream

{
  "addresses": ["EQC123...", "0:abc456..."],
  "types": ["transactions", "actions"],
  "min_finality": "pending",
  "include_address_book": true,
  "include_metadata": false,
  "action_types": ["jetton_transfer", "ton_transfer"]
}
```

#### Example: subscribe to a specific trace

```http
POST https://toncenter.com/api/streaming/v2/sse
Content-Type: application/json
Accept: text/event-stream

{
  "trace_external_hash_norms": ["E7im...+MjE="],
  "types": ["trace"],
  "min_finality": "pending",
  "include_address_book": true,
  "include_metadata": true
}
```

If the subscription is successful, the server returns as first message:

```json
{"status": "subscribed"}
```

Followed by events matching the subscription that are streamed as Server-Sent Events.
To keep the connection alive, the server sends the following line every 5 seconds:

```
: keepalive\n\n
```

Clients should ignore lines beginning with `:` per the SSE specification.

---

## Using WebSockets

Connect to the endpoint:

```bash
wss://toncenter.com/api/streaming/v2/ws
```

Once connected, clients use a small set of operations:

* `subscribe` — replace the subscription (addresses/types/finality) for this connection.
* `unsubscribe` — remove specific addresses or trace hashes from the current subscription.
* `ping` — keepalive / health check.

Each request may include an optional `id` field to correlate the server’s response.

### WebSocket Operations

#### `subscribe`

Replaces the entire subscription snapshot for this connection.

| Field                    | Type                  | Description                                                                               |
| ------------------------ | --------------------- | ----------------------------------------------------------------------------------------- |
| `operation`              | string                | Must be `"subscribe"`.                                                                    |
| `addresses`              | string[]              | Addresses to monitor.                                                                     |
| `trace_external_hash_norms` | string[] *(optional)* | Trace external hashes to monitor. Required when subscribing to `"trace"`.              |
| `types`                  | string[]              | Event types to receive. See [Event Types](#event-types).                                  |
| `min_finality`           | string *(optional)*   | Minimum finality: `"pending"`, `"confirmed"`, or `"finalized"`. Default is `"finalized"`. |
| `replay_existing` | boolean *(optional)* | If `true`, send the latest matching snapshots still retained in Redis before continuing with live updates. Default: `false`. |
| `msg_body_hash` | string *(optional)* | Match the body cell hash of a transaction’s **incoming** message. Requires non-empty `addresses` and `"transactions"` in `types`. Accepts hex or base64 hash formats. Only filters `transactions`. |
| `include_address_book`   | boolean *(optional)*  | If `true`, include address book info.                                                     |
| `include_metadata`       | boolean *(optional)*  | If `true`, include token metadata.                                                        |
| `action_types`           | string[] *(optional)* | Filter actions by type (e.g. `["jetton_transfer"]`). Only applies to `"actions"` events.  |
| `supported_action_types` | string[] *(optional)* | Advertise which action classification types client supports. `"latest"` by default.       |
| `id`                     | string *(optional)*   | Identifier for request/response correlation.                                              |

> **Note:** When subscribing only to `"trace"`, `addresses` may be empty. For any non-trace types, `addresses` are required.

Example:

```json
{
  "operation": "subscribe",
  "id": "1",
  "addresses": ["EQC123...", "0:abc456..."],
  "types": ["transactions", "actions", "account_state_change", "jettons_change", "trace"],
  "trace_external_hash_norms": ["E7im...+MjE="],
  "min_finality": "pending",
  "include_address_book": true,
  "include_metadata": false,
  "action_types": ["jetton_transfer", "ton_transfer"]
}
```

The server responds:

```json
{"id": "1", "status": "subscribed"}
```

Subsequent `subscribe` calls **replace** the current set of addresses and types (snapshot semantics).

#### `unsubscribe`

Removes one or more addresses or trace hashes from the subscription.

| Field                      | Type                | Description                                  |
| -------------------------- | ------------------- | -------------------------------------------- |
| `operation`                | string              | Must be `"unsubscribe"`.                     |
| `addresses`                | string[]            | Addresses to remove from subscription.       |
| `trace_external_hash_norms` | string[]           | Trace external hashes to remove.             |
| `id`                       | string *(optional)* | Identifier for request/response correlation. |

Example:

```json
{
  "operation": "unsubscribe",
  "id": "3",
  "addresses": ["EQC123..."],
  "trace_external_hash_norms": ["E7im...+MjE="]
}
```

Response:

```json
{"id": "3", "status": "unsubscribed"}
```

#### `ping`

Simple keepalive / connection health check. We recommend to send `ping` every 15 seconds to prevent closing connection.

| Field       | Type                | Description                                  |
| ----------- | ------------------- | -------------------------------------------- |
| `operation` | string              | Must be `"ping"`.                            |
| `id`        | string *(optional)* | Identifier for request/response correlation. |

Example:

```json
{
  "operation": "ping",
  "id": "ping-42"
}
```

Response:

```json
{"id": "ping-42", "status": "pong"}
```

---

## Replay and message body filtering

`replay_existing: true` sends current snapshots already retained by the service,
using the same filters, event formats, enrichment options, and invalidation rules
as live delivery. It does not replay earlier versions or guarantee an exact
30-second time window. Completed traces normally remain for about 30 seconds,
open traces can remain longer, and account states have a separate 60-second TTL.
If no matching data is retained, the subscription simply waits for live events.

For SSE, replay runs once when the connection is established. For WebSocket,
a `subscribe` request with `replay_existing: true` requests a read of retained data.
A subscription change does not cancel a read already in progress. Events are
filtered using the current subscription when they are prepared for sending;
omitting `msg_body_hash` removes that filter. Events already prepared or queued
can still arrive after the acknowledgement of a new `subscribe` or `unsubscribe`.

Only one replay runs per connection. A new replay request while it is busy schedules
another pass using the current subscription's lookup keys. Multiple requests while
busy are combined into that next pass. A subscription change without
`replay_existing: true` updates the filters but does not request another Redis read.

```json
{
  "operation": "subscribe",
  "addresses": ["<message destination address>"],
  "types": ["transactions"],
  "msg_body_hash": "<body cell hash in hex or base64>",
  "min_finality": "confirmed",
  "replay_existing": true
}
```

A transaction matches when **both** its account is in `addresses` and its incoming
message body hash equals `msg_body_hash`. Outgoing messages do not match this filter.
The body hash is the TON cell hash of the complete message body, including a signature
when the signature is part of that body. It is not the hash of the message envelope
or a SHA-256 digest of serialized BoC bytes. Multiple transactions may have the same
body hash; all matching transactions are eligible. Internal messages can match at
any depth of a trace, including gasless messages delivered by a relayer.

In subscriptions with several event types, `msg_body_hash` only filters
`transactions`. Other event types keep their own filters.

**`min_finality` keeps its existing meaning.** For trace-based events it filters
the finality of the trace snapshot. Address and body-hash filtering do not
recalculate that finality. An individual `transactions[].finality` can be higher
than the event's top-level `finality`, and updates within the same event finality
are possible. In particular, a confirmed matching transaction with a pending
trace tail still requires `min_finality: "pending"` for immediate delivery.

Account and jetton replay returns the latest retained finalized state and, if
permitted by `min_finality`, a confirmed state with a greater LT. If only the
confirmed state exists, it is returned when permitted. At equal LT, only the
finalized state is replayed. Replay reads account states only for explicitly
subscribed addresses. To replay jetton balances, include the jetton wallet
addresses. Live jetton updates also match subscriptions to the owner's address.

Updates arriving during replay are buffered and reconciled with the snapshots
by version. Replay data is sent only to the subscribing client. This is a best-effort
view of data still retained while it is read, not a database-wide atomic snapshot.
Trace invalidations are applied in delivery order and reset the remembered trace
version. Replay assumes a recreated trace with the same hash is delivered after
its previous invalidation; recreation while that invalidation is still pending
is not reconciled.
Connections that exceed the buffer while replay is active or cannot finish replay
within one minute are closed instead of silently dropping the initial events.
If the client's live output queue fills, the connection is closed instead of continuing
with silently dropped events. The server logs `slow consumer`. For WebSocket it
attempts a close frame with code `1013` and reason `slow consumer`; if the socket
cannot accept that frame promptly, it is closed directly. SSE ends the stream.
During replay, the sender waits for output capacity within its existing limits.
Replay switches to ordinary live delivery only after the sender has processed all
preceding events, including the SSE writer's flush. Live events arriving during
that wait remain buffered and are drained before the switch. For SSE, completion
means the SSE writer has flushed to the internal stream read by the HTTP server.
It does not acknowledge network delivery or processing by the client application.
Clients can reconnect with `replay_existing: true`, subject to the retention window.

---

## Event Format

Once a subscription is established, the server sends event messages matching the selected `types` and `min_finality`. Each message is a JSON object representing a trace, transaction, action, or account/jetton state change event.

Trace-related events are grouped by **trace**, which begins with an external message and includes all related transactions. The `trace_external_hash_norm` field is a normalized hash of the external message that initiated the trace. This value is consistent across all related events and can be used to correlate notifications.

Within a single trace, `transactions` and `actions` are sorted by logical time (LT) descending. The `trace` event includes the full trace tree and a map of all transactions in that trace.

### Event Types

The `type` field in each notification determines which structure is used:

* `transactions` — `TransactionsNotification`
* `actions` — `ActionsNotification`
* `trace` — `TraceNotification`
* `account_state_change` — `AccountStateNotification`
* `jettons_change` — `JettonsNotification`
* `trace_invalidated` — `TraceInvalidatedNotification` (special)

---

### TransactionsNotification

Triggered by subscriptions including `"transactions"`.

You may receive multiple notifications for the same `trace_external_hash_norm` as its finality increases (subject to your `min_finality` filter):

* `finality: "pending"` — speculative/emulated trace.
* `finality: "confirmed"` — trace is in a signed shard block.
* `finality: "finalized"` — trace is fully finalized on-chain.

#### Example

```json
{
  "type": "transactions",
  "finality": "pending",
  "trace_external_hash_norm": "E7im...+MjE=",
  "transactions": [ /* array of transaction objects */ ],
  "address_book": { /* optional mapping of known addresses */ },
  "metadata": { /* optional token metadata */ }
}
```

| Field                      | Type                | Description                                                                                    |
| -------------------------- | ------------------- | ---------------------------------------------------------------------------------------------- |
| `type`                     | string              | Always `"transactions"`.                                                                       |
| `finality`                 | string              | Finality level: `"pending"`, `"confirmed"`, or `"finalized"`.                                  |
| `trace_external_hash_norm` | string              | Normalized hash of the external message that produced this trace.                              |
| `transactions`             | Transaction[]       | List of transactions belonging to this trace (LT descending). Same schema as v3 `Transaction`. |
| `address_book`             | object *(optional)* | Mapping of addresses to user-friendly address and TON DNS domain.                              |
| `metadata`                 | object *(optional)* | Mapping of known token addresses to token metadata (Jettons, NFTs, etc.).                      |

---

### ActionsNotification

Triggered by subscriptions including `"actions"`.

Actions represent a higher-level interpretation of a trace (e.g. “TON transfer”, “Jetton transfer”). Like transactions, you may receive multiple notifications as finality increases.

#### Example

```json
{
  "type": "actions",
  "finality": "confirmed",
  "trace_external_hash_norm": "E7im...+MjE=",
  "actions": [ /* array of action objects */ ],
  "address_book": { /* optional mapping of known addresses */ },
  "metadata": { /* optional token metadata */ }
}
```

| Field                      | Type                | Description                                                                                      |
| -------------------------- | ------------------- | ------------------------------------------------------------------------------------------------ |
| `type`                     | string              | Always `"actions"`.                                                                              |
| `finality`                 | string              | Finality level: `"pending"`, `"confirmed"`, or `"finalized"`                                     |
| `trace_external_hash_norm` | string              | Normalized hash of the external message that produced this trace.                                |
| `actions`                  | Action[]            | List of classified actions for this trace.                                                       |
| `address_book`             | object *(optional)* | Mapping of addresses to user-friendly address and TON DNS domain.                                |
| `metadata`                 | object *(optional)* | Mapping of known token addresses to token metadata (Jettons, NFTs, etc.).                        |

`action_types` in subscriptions can be used to filter which actions are delivered (e.g. only `"jetton_transfer"`).

---

### TraceNotification

Triggered by subscriptions including `"trace"`. Unlike address-based subscriptions, trace events are not filtered by account and include **all** transactions and actions in the trace.

#### Example

```json
{
  "type": "trace",
  "finality": "confirmed",
  "trace_external_hash_norm": "E7im...+MjE=",
  "trace": { /* trace tree */ },
  "transactions": { /* map of tx hash -> transaction */ },
  "actions": [ /* array of action objects */ ],
  "address_book": { /* optional mapping of known addresses */ },
  "metadata": { /* optional token metadata */ }
}
```

| Field                      | Type                | Description                                                                                      |
| -------------------------- | ------------------- | ------------------------------------------------------------------------------------------------ |
| `type`                     | string              | Always `"trace"`.                                                                                |
| `finality`                 | string              | Finality level: `"pending"`, `"confirmed"`, or `"finalized"`.                                     |
| `trace_external_hash_norm` | string              | Normalized hash of the external message that produced this trace.                                |
| `trace`                    | TraceNode           | Trace tree with transaction + its children structure.                                                    |
| `transactions`             | object              | Map of transaction hash to transaction object.                                                    |
| `actions`                  | Action[] *(optional)* | Classified actions for this trace.    |
| `address_book`             | object *(optional)* | Mapping of addresses to user-friendly address and TON DNS domain.                                |
| `metadata`                 | object *(optional)* | Mapping of known token addresses to token metadata (Jettons, NFTs, etc.).                        |

---

### AccountStateNotification

Triggered by `"account_state_change"` — on each transaction executed on a subscribed address.
This event is sent for finality levels `confirmed` and `finalized` but not for `pending`.

#### Example

```json
{
  "type": "account_state_change",
  "finality": "finalized",
  "account": "0:18AA...",
  "state": {
    "hash": "POG0...73w=",
    "balance": "227746159040",
    "account_status": "active",
    "data_hash": "7qNe...XA4=",
    "code_hash": "ow8E...MG8="
  }
}
```

| Field      | Type         | Description                                                                                                           |
| ---------- | ------------ | --------------------------------------------------------------------------------------------------------------------- |
| `type`     | string       | Always `"account_state_change"`.                                                                                      |
| `finality` | string       | Finality level: `"confirmed"`, or `"finalized"`                                                                       |
| `account`  | string       | Account address in raw format.                                                                                        |
| `state`    | AccountState | New account state. Similar to [`API v3 AccountState`](https://toncenter.com/api/v3/) but without full code/data BoCs. |

---

### JettonsNotification

Triggered by `"jettons_change"` — on every transaction executed on jetton wallet contract where subscribed address is it's own address or is it's owner.
This event is sent for finality levels `confirmed` and `finalized` but not for `pending`.

#### Example

```json
{
  "type": "jettons_change",
  "finality": "finalized",
  "jetton": {
    "address": "0:88DA...",
    "balance": "1833889664971",
    "owner": "0:18AA...",
    "jetton": "0:B113...",
    "last_transaction_lt": "61664076000010"
  },
  "address_book": { /* optional mapping of known addresses */ },
  "metadata": { /* optional token metadata */ }
}
```

| Field          | Type                | Description                                                                                |
| -------------- | ------------------- | ------------------------------------------------------------------------------------------ |
| `type`         | string              | Always `"jettons_change"`.                                                                 |
| `finality`     | string              | Finality level: `"confirmed"` or `"finalized"`                                    |
| `jetton`       | JettonWallet        | Jetton wallet data. Same schema as [`API v3 JettonWallet`](https://toncenter.com/api/v3/). |
| `address_book` | object *(optional)* | Mapping of addresses to user-friendly address and TON DNS domain.                          |
| `metadata`     | object *(optional)* | Mapping of known token addresses to token metadata (Jettons, NFTs, etc.).                  |

---

### TraceInvalidatedNotification

In some cases, emulation or intermediate trace results are later determined to be invalid — for example:

* The external message was not ultimately accepted by the blockchain.
* A later state change invalidated a previously-emitted emulation result.
* Confirmed block was discarded by another block and the trace did not end up in finalized block.

To signal this, the API sends a `trace_invalidated` event for the corresponding `trace_external_hash_norm`.

#### Example

```json
{
  "type": "trace_invalidated",
  "trace_external_hash_norm": "vAa...uJE="
}
```

| Field                      | Type   | Description                                                          |
| -------------------------- | ------ | -------------------------------------------------------------------- |
| `type`                     | string | Always `"trace_invalidated"`.                                        |
| `trace_external_hash_norm` | string | Normalized hash of the external message whose trace was invalidated. |

Clients should treat this as a signal to remove or discard any previously stored `trace` / `transactions` / `actions` data for the affected trace.

Once a trace has been emitted at `finality = "finalized"`, no further `trace_invalidated` events will be sent for that `trace_external_hash_norm`.

---

## Event Delivery Semantics

Finality and delivery semantics for each event type:

| Type                   | Finality Levels Used                            | Description                                                                                                    |
| ---------------------- | ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `transactions`         | `pending`, `confirmed`, `finalized`             | Emitted per trace as it progresses. Clients may receive multiple snapshots per trace with increasing finality. |
| `actions`              | `pending`, `confirmed`, `finalized`             | Emitted per trace as actions are inferred and the trace finalizes. Some stages may be collapsed/omitted.       |
| `trace`                | `pending`, `confirmed`, `finalized`             | Emitted per trace; includes full trace tree and all transactions/actions for the trace.                        |
| `account_state_change` | `confirmed`, `finalized`                        | Emitted on every **confirmed**/**finalized** transaction executed on a subscribed address.                                   |
| `jettons_change`       | `confirmed`, `finalized`                        | Emitted on every **confirmed**/**finalized** transaction on a jetton wallet where the subscribed address is wallet/owner.    |
| `trace_invalidated`    | n/a                                             | Emitted when a previously-emitted `pending`/`confirmed` trace is deemed invalid; never after `finalized`.      |

### `min_finality` and delivery

For trace-based events (`transactions`, `actions`, `trace`):

* If `min_finality = "pending"`:

  * You receive all snapshots as finality increases (`pending` → `confirmed` → `finalized`).
* If `min_finality = "confirmed"`:

  * You **do not** receive pure `pending` emulation.
  * First event per trace will have `finality = "confirmed"` or later.
* If `min_finality = "finalized"`:

  * You receive only fully finalized events for traces.
  * No `trace_invalidated` events are expected in this mode.

For non-trace events:

* `account_state_change` and `jettons_change` are emitted with `finality = "confirmed"` and `finality = "finalized"` (no `pending` finality for these events).

---

## Event Invalidation

For subscriptions that see speculative states (`min_finality = "pending"` or `"confirmed"`), it is possible that a trace is later deemed invalid.

To signal this:

```json
{
  "type": "trace_invalidated",
  "trace_external_hash_norm": "vAa...uJE="
}
```

Clients **must**:

* Remove any locally cached `trace`, `transactions`, and `actions` associated with this `trace_external_hash_norm`.
* Treat the trace as if it never happened (until a new `pending`/`confirmed` event may appear with the same external hash).

The server:

* **May** emit `trace_invalidated` for traces whose last known finality was `pending` or `confirmed`.
* **Will not** emit `trace_invalidated` after a `finalized` event has been sent for that trace.

If you only subscribe with `min_finality = "finalized"`, you will not see speculative states and therefore typically will not see invalidation events.

* Note: Currently there's no way to get updated whether `account_state_change`/`jettons_change` with `finality = "confirmed"` was invalidated.

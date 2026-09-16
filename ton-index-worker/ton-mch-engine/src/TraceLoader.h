// The engine's trace data model (MsgContent / Message / Transaction / Trace),
// what every loader produces and the whole pipeline consumes.
//
// Loaders are elsewhere and are NOT interchangeable in kind: SchemaTraceLoader
// is the production one (schema::Transaction -> Trace); the .lz4 fixture reader
// is test-only and lives in fixtures/FixtureLoader.h, in the mch-fixtures
// target, so that lz4/msgpack never reach a product binary.
#pragma once

#include "Value.h"

#include "td/utils/Status.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mch {

struct Transaction;

struct MsgContent {
  std::string hash;
  std::string body;  // base64 BOC
};

struct Message {
  std::string msg_hash;
  std::string tx_hash;
  std::int64_t tx_lt{0};
  std::string direction;  // "in" | "out"
  std::optional<std::string> source;
  std::optional<std::string> destination;
  std::optional<std::int64_t> opcode;  // stored SIGNED (int32)
  std::optional<std::int64_t> value;
  std::optional<std::int64_t> created_lt;
  std::optional<std::int64_t> created_at;
  std::optional<bool> bounce;
  bool bounced{false};
  bool has_extra_currencies{false};  // value_extra_currencies map is non-empty
  std::optional<MsgContent> content;
  // The StateInit BOC of a deploying message. Separate from `content`,
  // which is the body: TL-B unpacks the two independently. Read only by
  // the GetGems sale/auction state-init parsers.
  std::optional<MsgContent> init_state;

  // Owning transaction, wired after decode.
  Transaction *tx{nullptr};

  // Opcode masked to 32 bits.
  std::optional<std::uint32_t> opcode32() const {
    if (!opcode) {
      return std::nullopt;
    }
    return static_cast<std::uint32_t>(*opcode & 0xFFFFFFFF);
  }
};

struct Transaction {
  std::string hash;
  std::int64_t lt{0};
  std::int64_t now{0};
  std::int64_t mc_block_seqno{0};  // Action.mc_seqno_end
  std::string account;
  std::string descr;  // "ord" | "tick_tock" | ...
  std::string orig_status;
  std::string end_status;
  std::optional<std::string> skipped_reason;  // "no_state" | "bad_state" | "no_gas" | "suspended"
  std::optional<std::int64_t> compute_exit_code;
  bool aborted{false};
  std::vector<std::unique_ptr<Message>> messages;
};

struct Trace {
  std::string trace_id;
  // Trace-level aggregates used by unknown-action serialization. Every loader
  // must populate them. Derived values are min/max transaction lt and time plus
  // maximum masterchain sequence number; a well-formed root has the minimum lt.
  std::int64_t start_lt{0}, end_lt{0}, start_utime{0}, end_utime{0}, mc_seqno_end{0};
  std::vector<std::unique_ptr<Transaction>> transactions;
  // Root `interfaces` section, decoded generically: account (raw uppercase
  // string, the msgpack key) -> Dict{InterfaceName -> Dict{field -> value}}.
  // Kept raw enough that a LookupSource can apply per-kind semantics on top
  // (B2). Msgpack scalars map: str->Str, int->Int, bool->Bool, nil->Null,
  // bin->Bytes, map->Dict, array->List; floats have no Value kind -> Null.
  std::map<std::string, Value> interfaces;
};

// Derives Trace's five aggregates from `trace.transactions`: min/max lt, min/max
// now, max mc_block_seqno. Used by loaders with no trace header, including the
// production SchemaTraceLoader.
//
// Fixture trace headers use the same aggregates.
void fill_trace_aggregates(Trace &trace);

}  // namespace mch

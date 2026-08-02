#include "SchemaTraceLoader.h"

#include "convert-utils.h"  // convert::to_raw_address (tondb-scanner)

#include "td/utils/base64.h"

#include <variant>

namespace mch {

namespace {

// td::Bits256 -> the base64 string form the fixtures/engine use for tx/msg hashes.
std::string b256(const td::Bits256 &h) { return td::base64_encode(h.as_slice()); }

// schema::AccountStatus enum -> the orig_status/end_status strings the engine
// reads (basic_blocks.py: "uninit"/"active"/... via TraceLoader).
std::string status_str(schema::AccountStatus s) {
  switch (s) {
    case schema::AccountStatus::uninit:
      return "uninit";
    case schema::AccountStatus::frozen:
      return "frozen";
    case schema::AccountStatus::active:
      return "active";
    case schema::AccountStatus::nonexist:
      return "nonexist";
  }
  return "";
}

struct DescrInfo {
  std::string descr;                   // "ord" | "tick_tock" | "other"
  std::optional<std::int64_t> exit_code;
  bool aborted{false};
};

// TransactionDescr variant -> descr string + compute exit code + aborted. The
// engine consumes the descr TYPE (tick_tock vs ord), the compute exit code (msg
// .exit_code envelope) AND aborted (BlockTree is_leaf_failed reads tx->aborted;
// also exposed as msg.transaction.aborted), aborted lives INSIDE the schema
// variant, unlike path A's flat top-level field. Non-ord/tick_tock descrs are
// rare (storage/split/merge) and map to "other" (never matched as tick_tock).
DescrInfo descr_info(const schema::TransactionDescr &d) {
  DescrInfo r;
  auto exit_of = [](const schema::TrComputePhase &cp) -> std::optional<std::int64_t> {
    if (const auto *vm = std::get_if<schema::TrComputePhase_vm>(&cp)) {
      return static_cast<std::int64_t>(vm->exit_code);
    }
    return std::nullopt;  // skipped compute phase -> no exit code
  };
  if (const auto *ord = std::get_if<schema::TransactionDescr_ord>(&d)) {
    r.descr = "ord";
    r.exit_code = exit_of(ord->compute_ph);
    r.aborted = ord->aborted;
  } else if (const auto *tt = std::get_if<schema::TransactionDescr_tick_tock>(&d)) {
    r.descr = "tick_tock";
    r.exit_code = exit_of(tt->compute_ph);
    r.aborted = tt->aborted;
  } else {
    r.descr = "other";
  }
  return r;
}

void map_message(Message *m, const schema::Message &s, const std::string &owner_tx_hash,
                 std::int64_t owner_tx_lt, const char *direction) {
  m->msg_hash = b256(s.hash);
  m->tx_hash = owner_tx_hash;
  m->tx_lt = owner_tx_lt;
  m->direction = direction;
  m->source = s.source;
  m->destination = s.destination;
  m->opcode = s.opcode ? std::optional<std::int64_t>(static_cast<std::int64_t>(*s.opcode))
                       : std::nullopt;
  if (s.value) {
    m->value = (s.value->grams.is_null())
                   ? std::nullopt
                   : std::optional<std::int64_t>(s.value->grams->to_long());
    m->has_extra_currencies = !s.value->extra_currencies.empty();
  }
  m->created_lt = s.created_lt ? std::optional<std::int64_t>(static_cast<std::int64_t>(*s.created_lt))
                               : std::nullopt;
  m->created_at = s.created_at ? std::optional<std::int64_t>(static_cast<std::int64_t>(*s.created_at))
                               : std::nullopt;
  m->bounce = s.bounce;
  m->bounced = s.bounced.value_or(false);
  // MsgContent: the engine carries the body as a base64 BOC string and decodes
  // it in MsgParse. Cell-hash rendering hides BOC writer-order differences, so
  // use the already-serialized body_boc directly. No cell
  // re-serialization, ClassifyCore untouched.
  MsgContent c;
  c.hash = b256(s.hash);
  c.body = s.body_boc;
  m->content = c;
  // Same treatment for the StateInit of a deploying message. This one read
  // covers BOTH the schema/prod path and the emulator path, which funnels its
  // re-parsed transactions through this same mapper.
  if (s.init_state_boc && s.init_state.not_null()) {
    MsgContent init;
    init.hash = b256(s.init_state->get_hash().bits());
    init.body = *s.init_state_boc;
    m->init_state = std::move(init);
  }
}

}  // namespace

td::Result<Trace> schema_to_trace(const std::string &trace_id,
                                  const std::vector<schema::Transaction> &txs) {
  Trace trace;
  trace.trace_id = trace_id;
  for (const auto &st : txs) {
    auto tx = std::make_unique<Transaction>();
    tx->hash = b256(st.hash);
    tx->lt = static_cast<std::int64_t>(st.lt);
    tx->now = static_cast<std::int64_t>(st.now);
    tx->mc_block_seqno = static_cast<std::int64_t>(st.mc_seqno);
    // to_raw_address emits "wc:lowerhex"; the engine's canonical account form is
    // uppercase hex (normalize_raw_address, applied wherever an account VALUE is
    // read) and the fixtures store uppercase too. Normalize so the raw-string
    // leaks (accounts assembly, msg.transaction.account expr) round-trip A==B.
    tx->account = convert::to_raw_address(st.account);
    if (auto norm = normalize_raw_address(tx->account)) {
      tx->account = *norm;
    }
    tx->orig_status = status_str(st.orig_status);
    tx->end_status = status_str(st.end_status);
    DescrInfo di = descr_info(st.description);
    tx->descr = di.descr;
    tx->compute_exit_code = di.exit_code;
    tx->aborted = di.aborted;
    // skipped_reason is absent from the schema (always null in fixtures too).

    Transaction *tx_raw = tx.get();
    if (st.in_msg) {
      auto m = std::make_unique<Message>();
      map_message(m.get(), *st.in_msg, tx_raw->hash, tx_raw->lt, "in");
      m->tx = tx_raw;
      tx->messages.push_back(std::move(m));
    }
    for (const auto &om : st.out_msgs) {
      auto m = std::make_unique<Message>();
      map_message(m.get(), om, tx_raw->hash, tx_raw->lt, "out");
      m->tx = tx_raw;
      tx->messages.push_back(std::move(m));
    }
    trace.transactions.push_back(std::move(tx));
  }
  // The emulator trace has no trace-level lt/utime header, so derive every
  // aggregate consumed by trace-level actions from its transactions.
  // See TraceLoader.h for the derivation contract.
  fill_trace_aggregates(trace);
  return trace;
}

}  // namespace mch

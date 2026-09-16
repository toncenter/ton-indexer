#include "SchemaTraceLoader.h"

#include "convert-utils.h"

#include "td/utils/base64.h"

#include <variant>

namespace mch {

namespace {

std::string b256(const td::Bits256 &h) { return td::base64_encode(h.as_slice()); }

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
  std::optional<std::string> skipped_reason;
  bool aborted{false};
};

// Extract descr tag, compute exit code, skipped reason and aborted from the
// schema variant. Non-ord/tick_tock descrs map to "other".
DescrInfo descr_info(const schema::TransactionDescr &d) {
  DescrInfo r;
  auto exit_of = [](const schema::TrComputePhase &cp) -> std::optional<std::int64_t> {
    if (const auto *vm = std::get_if<schema::TrComputePhase_vm>(&cp)) {
      return static_cast<std::int64_t>(vm->exit_code);
    }
    return std::nullopt;
  };
  auto skipped_reason_of = [](const schema::TrComputePhase &cp) -> std::optional<std::string> {
    const auto *skipped = std::get_if<schema::TrComputePhase_skipped>(&cp);
    if (skipped == nullptr) {
      return std::nullopt;
    }
    switch (skipped->reason) {
      case schema::cskip_no_state:
        return "no_state";
      case schema::cskip_bad_state:
        return "bad_state";
      case schema::cskip_no_gas:
        return "no_gas";
      case schema::cskip_suspended:
        return "suspended";
    }
    return std::nullopt;
  };
  if (const auto *ord = std::get_if<schema::TransactionDescr_ord>(&d)) {
    r.descr = "ord";
    r.exit_code = exit_of(ord->compute_ph);
    r.skipped_reason = skipped_reason_of(ord->compute_ph);
    r.aborted = ord->aborted;
  } else if (const auto *tt = std::get_if<schema::TransactionDescr_tick_tock>(&d)) {
    r.descr = "tick_tock";
    r.exit_code = exit_of(tt->compute_ph);
    r.skipped_reason = skipped_reason_of(tt->compute_ph);
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
  // Reuse the already-serialized body_boc; no cell re-serialization.
  MsgContent c;
  c.hash = b256(s.hash);
  c.body = s.body_boc;
  m->content = c;
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
    // Same canonical-or-original path as Account values.
    tx->account = canonicalize_or_passthrough(convert::to_raw_address(st.account));
    tx->orig_status = status_str(st.orig_status);
    tx->end_status = status_str(st.end_status);
    DescrInfo di = descr_info(st.description);
    tx->descr = di.descr;
    tx->compute_exit_code = di.exit_code;
    tx->skipped_reason = std::move(di.skipped_reason);
    tx->aborted = di.aborted;

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
  // Derive trace-level lt/utime aggregates from the transactions.
  fill_trace_aggregates(trace);
  return trace;
}

}  // namespace mch

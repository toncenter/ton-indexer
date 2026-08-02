// Vesting host fn (builders/vesting.py). See host/HostImpls.h for the internal
// registry surface and HostRegistry.h for the public one.
#include "host/HostImpls.h"

#include "host/HostCommon.h"

#include "BlockTree.h"
#include "TraceLoader.h"

#include "td/utils/base64.h"
#include "vm/boc.h"

#include <string>
#include <vector>

namespace mch {

namespace {

// The out-message body cell's hash, or an empty string when the body is absent
// or undecodable (Python `continue`s past both).
std::string out_body_hash(const Message &m) {
  if (!m.content || m.content->body.empty()) {
    return {};
  }
  auto r_raw = td::base64_decode(td::Slice(m.content->body));
  if (r_raw.is_error()) {
    return {};
  }
  auto r_cell = vm::std_boc_deserialize(r_raw.ok());
  if (r_cell.is_error() || r_cell.ok().is_null()) {
    return {};
  }
  auto h = r_cell.ok()->get_hash();
  td::Slice hs = h.as_slice();
  return std::string(hs.data(), hs.size());
}

}  // namespace

// blocks/vesting.py _vesting_message_was_sent(vesting_tx, request), reached
// through builders/vesting.py: scan the REQUEST transaction's out-messages for
// one addressed to the requested destination whose body hash matches the
// requested body. When the request body could not be hashed (message_body_hash
// null, Python's MessageAny fallback) the match falls back to the TON value.
// Args: (request block, its parsed VestingSendMessage body).
EvalResult vesting_message_was_sent(BuildEnv &, const std::vector<Value> &args) {
  if (args.size() != 2) {
    return rt_fault("vesting_message_was_sent: bad arguments");
  }
  const Block *send = as_block(args[0]);
  const Value &body = args[1];
  if (send == nullptr || (body.t != VType::Obj && body.t != VType::Dict)) {
    return rt_ok(Value::make_bool(false));
  }
  const Message *in = block_msg(send);
  if (in == nullptr || in->tx == nullptr) {
    return rt_ok(Value::make_bool(false));
  }
  const Value *want_dest = body.field("message_destination");
  const Value *want_value = body.field("message_value");
  const Value *want_hash = body.field("message_body_hash");
  if (want_dest == nullptr || want_dest->t != VType::Account) {
    return rt_ok(Value::make_bool(false));
  }
  bool have_hash = want_hash != nullptr && want_hash->t == VType::Bytes;

  for (const auto &m : in->tx->messages) {
    if (m->direction != "out" || !m->destination) {
      continue;
    }
    // AccountId(dest) == AccountId(requested): canonical-string equality, with
    // addr_none equal only to addr_none (Python compares as_str(), None==None).
    Value dest = account_from_opt(m->destination);
    if (dest.addr_none != want_dest->addr_none) {
      continue;
    }
    if (!dest.addr_none && dest.str != want_dest->str) {
      continue;
    }
    if (!have_hash) {
      // Could not hash the requested body - fall back to value matching.
      if (m->value && want_value != nullptr && !want_value->num.is_null() &&
          want_value->num->to_dec_string() == std::to_string(*m->value)) {
        return rt_ok(Value::make_bool(true));
      }
      continue;
    }
    std::string got = out_body_hash(*m);
    if (!got.empty() && got == want_hash->str) {
      return rt_ok(Value::make_bool(true));
    }
  }
  return rt_ok(Value::make_bool(false));
}

}  // namespace mch

#include "host/HostImpls.h"

#include "host/HostCommon.h"

#include "BlockTree.h"
#include "TraceLoader.h"

#include "common/refint.h"
#include "td/utils/base64.h"
#include "vm/boc.h"

#include <string>
#include <vector>

namespace mch {

namespace {

// Out-message body-cell hash, or empty when the body is absent or undecodable.
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

// Scan the request transaction's out-messages for one addressed to the
// requested destination whose body hash matches. When the request body could
// not be hashed, fall back to the TON value. Args: request block and its
// parsed VestingSendMessage body.
EvalResult vesting_message_was_sent(BuildEnv &, const std::vector<Value> &args) {
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
    Value dest = account_from_opt(m->destination);
    if (!same_account(dest, *want_dest)) {
      continue;
    }
    if (!have_hash) {
      // Could not hash the requested body - fall back to value matching.
      if (m->value && want_value != nullptr && !want_value->num.is_null() &&
          td::cmp(want_value->num, td::make_refint(*m->value)) == 0) {
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

// EVAA withdraw-family message parsers (messages/evaa.py). See parse/PSlice.h
// for shared machinery and MsgParse.cpp's header for the pytoniq-parity
// catalogue.
#include "parse/Parsers.h"

#include "parse/PSlice.h"

#include "common/refint.h"
#include "vm/cellslice.h"

#include <utility>

namespace mch {

// WithdrawMaster and WithdrawCollateralized use protocol ABI rows. The
// single-use fail_excess opcode enum is parsed here because it is not in the DSL.

// EvaaWithdrawFailExcess: opcode-tagged reason; an unknown opcode raises in
// Python (ValueError) -> soft-parse failure.
td::Result<Value> parse_evaa_withdraw_fail_excess(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  if (!cs.have(32)) {
    return td::Status::Error("evaa fail excess: underflow");
  }
  auto op = static_cast<td::uint32>(cs.fetch_ulong(32));
  const char *reason;
  if (op == 0x21e6) {
    reason = "withdraw_locked_excess";
  } else if (op == 0x21e7) {
    reason = "withdraw_not_collateralized_excess";
  } else if (op == 0x21e8) {
    reason = "withdraw_missing_prices_excess";
  } else if (op == 0x21ec) {
    reason = "withdraw_execution_crashed";
  } else {
    return td::Status::Error("evaa fail excess: unknown opcode");
  }
  Value::Fields f;
  f.emplace_back("opcode", Value::make_int64(static_cast<std::int64_t>(op)));
  f.emplace_back("reason", Value::make_str(reason));
  return Value::make_obj(std::move(f));
}

}  // namespace mch

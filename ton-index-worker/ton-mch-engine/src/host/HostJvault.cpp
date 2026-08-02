// JVault host fns (builders/jvault.py). See host/HostImpls.h for the internal
// registry surface and HostRegistry.h for the public one.
//
// Both functions take the composed
// jetton_transfer's `data` scalars (a b64 string / an already-parsed address
// list) rather than reaching the reference `.jetton_transfer_message` attribute the
// generic IR block does not expose.
#include "host/HostImpls.h"

#include "MsgParse.h"
#include "Value.h"

#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

#include <string>
#include <vector>

namespace mch {

// builders/jvault.py jvault_stake_period(forward_payload_b64): decode the b64
// BOC of the transfer forward_payload, skip the 32-bit op, read the 32-bit
// stake period. None -> null (an absent forward_payload). A malformed payload
// raises in Python -> EvalError -> match rejection; here rt_fault does the same.
EvalResult jvault_stake_period(BuildEnv &, const std::vector<Value> &args) {
  if (args.size() != 1) {
    return rt_fault("jvault_stake_period: bad arguments");
  }
  if (args[0].is_null()) {
    return rt_ok(Value::null());
  }
  if (args[0].t != VType::Str) {
    return rt_fault("jvault_stake_period: expected a b64 string");
  }
  auto raw = td::base64_decode(td::Slice(args[0].str));
  if (raw.is_error()) {
    return rt_fault("jvault_stake_period: base64 decode failed");
  }
  auto cell = vm::std_boc_deserialize(raw.move_as_ok());
  if (cell.is_error()) {
    return rt_fault("jvault_stake_period: BOC deserialize failed");
  }
  // Decode through the prefixless ABI-faithful JVaultStakePeriodPayload parser.
  auto r = parse_message_body("JVaultStakePeriodPayload", cell.move_as_ok());
  if (r.is_error()) {
    return rt_fault("jvault_stake_period: forward_payload underflow");
  }
  return rt_ok(*r.ok().field("stake_period"));
}

// The claim body's jetton-wallet list uses the expression-language account()
// constructor directly, so it needs no host function.

}  // namespace mch

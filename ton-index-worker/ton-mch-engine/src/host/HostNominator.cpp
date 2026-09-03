#include "host/HostImpls.h"

#include "host/HostCommon.h"

#include "BlockTree.h"
#include "HostRegistry.h"
#include "btypes_gen.h"

#include "common/refint.h"

#include <algorithm>

namespace mch {

namespace {

// Shared arity classifier for both the amount fn and payout-absorb shaper.
// Reference consumes every payout only for arity 1 or 2; 0 and >=3 consume none.
std::vector<Block *> classified_withdraw_payouts(const Block *request) {
  std::vector<Block *> payouts;
  if (request == nullptr) {
    return payouts;
  }
  for (Block *next : request->next_blocks) {
    if (next->btype == mch::btype::kTonTransfer) {
      payouts.push_back(next);
    }
  }
  std::stable_sort(payouts.begin(), payouts.end(),
                   [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
  if (payouts.size() != 1 && payouts.size() != 2) {
    payouts.clear();
  }
  return payouts;
}

}  // namespace

// Two payouts use the canonical first child; one is immediate only when it
// exceeds the request.
EvalResult nominator_withdraw_payout_amount(BuildEnv &, const std::vector<Value> &args) {
  const Block *request = as_block(args[0]);
  std::vector<Block *> payouts = classified_withdraw_payouts(request);
  if (payouts.size() == 2) {
    return rt_ok(data_field(payouts.front(), "value"));
  }
  if (payouts.size() == 1) {
    Value payout = data_field(payouts.front(), "value");
    Value requested = data_field(request, "value");
    if (!payout.num.is_null() && !requested.num.is_null() &&
        td::cmp(payout.num, requested.num) > 0) {
      return rt_ok(payout);
    }
  }
  return rt_ok(Value::null());
}

// Absorb exactly the same classified set used by the amount fn. This runs
// after the request and optional aux children have been wrapper-merged.
void nominator_withdraw_absorb_payouts(Block *produced, const ShaperMatch &m) {
  Block *request = m.capture("request");
  std::vector<Block *> payouts = classified_withdraw_payouts(request);
  if (payouts.empty()) {
    return;
  }

  // The wrapper merge made these siblings under produced. Temporarily expose
  // the connected downstream view merge_blocks requires, then restore the
  // reference child links after it has derived the combined frontier.
  Block *outer = produced->previous_block;
  std::vector<Block *> first_next = payouts.front()->next_blocks;
  payouts.front()->previous_block = outer;
  if (payouts.size() == 2) {
    payouts.front()->next_blocks.push_back(payouts.back());
    payouts.back()->previous_block = payouts.front();
  }
  produced->merge_blocks(payouts);
  payouts.front()->next_blocks = first_next;
  for (Block *payout : payouts) {
    payout->previous_block = request;
  }
  produced->previous_block = outer;
  produced->compact_connections();
}

}  // namespace mch

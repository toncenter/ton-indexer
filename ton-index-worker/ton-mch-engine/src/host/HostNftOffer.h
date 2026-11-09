#pragma once

#include "Value.h"

#include "td/utils/Status.h"
#include "td/utils/bits.h"
#include "vm/cells/Cell.h"

namespace mch {

// Only phase changes after deploy; the address commits to phase Funding.
td::Result<Value> parse_nft_offer(const td::Bits256 &address, const td::Ref<vm::Cell> &data);

}  // namespace mch

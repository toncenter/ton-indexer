#pragma once

#include "Value.h"

#include "td/utils/Status.h"
#include "td/utils/bits.h"
#include "vm/cells/Cell.h"

namespace mch {

// Only price and phase change after deploy; the address commits to phase Waiting.
td::Result<Value> parse_nft_fixed_sale(const td::Bits256 &address, const td::Ref<vm::Cell> &data);

}  // namespace mch

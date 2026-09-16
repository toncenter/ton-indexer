// Consumed-set decode helpers. The registry checks arity; these turn the
// single-List host-fn argument into Block pointers (or a fault / nullopt).
#pragma once

#include "ExprRuntime.h"
#include "Value.h"

#include <optional>
#include <vector>

namespace mch {

struct Block;  // BlockTree.h

// The consumed match set handed to a swap-style host fn: the single List arg
// decoded into its Block elements (non-Block elements dropped). The anchor is
// element 0; `others` is the tail.
struct ConsumedBlocks {
  std::vector<const Block *> blocks;

  const Block *anchor() const { return blocks.front(); }
  std::vector<const Block *> others() const {
    return blocks.empty() ? std::vector<const Block *>{}
                          : std::vector<const Block *>(blocks.begin() + 1, blocks.end());
  }
};

// Decode the single-List-arg calling convention after registry arity validation.
// The list must be non-empty and contain at least one Block.
EvalResult decode_consumed(const std::vector<Value> &args, const char *binding,
                           ConsumedBlocks &out);

// Same decode as decode_consumed, but bad arguments or an empty consumed set
// return nullopt for hosts that reject instead of faulting.
std::optional<ConsumedBlocks> decode_consumed_or_none(const std::vector<Value> &args);

}  // namespace mch

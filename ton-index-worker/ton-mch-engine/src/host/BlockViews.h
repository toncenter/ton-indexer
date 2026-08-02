// Narrow read-only views over blocks / consumed match sets for the host TUs
// Only operations required by current hosts
// are here, this is NOT a graph-query language. Each op below has a 2nd use in
// the DEX cores (Stonfi v2 collects calls-by-op, lt-sorts swaps, finds a
// next-block call child). HostCoffee consumes this view.
#pragma once

#include "Value.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mch {

struct Block;

// Is `b` present in `v` (pointer identity)?
bool block_in(const std::vector<const Block *> &v, const Block *b);

// The first call_contract block in `blocks` whose opcode == `op`, or nullptr.
const Block *first_call(const std::vector<const Block *> &blocks, std::uint32_t op);

// Every call_contract block in `blocks` whose opcode == `op`, in input order.
std::vector<const Block *> all_calls(const std::vector<const Block *> &blocks, std::uint32_t op);

// The first next_block of `b` that is a call_contract with opcode == `op`.
const Block *first_next_call(const Block *b, std::uint32_t op);

// De-duplicate by pointer identity, then stable-sort by min_lt ascending. This
// is the exact HostCoffee swap-ordering (sort-by-ptr + unique + stable min_lt
// sort), reproduced so the peer-swap order is byte-identical.
void unique_lt_sorted(std::vector<const Block *> &blocks);

// Truthiness of a block data field (Python `bool(block.data[field])` for the
// bool/int carriers the specs use): a true Bool or a non-zero Int. Absent /
// null / other types -> false. The has_internal_transfer test the stonfi cores
// repeat (v1 in/out legs, v2 out leg, no_internal_transfer, pton_self_transfer).
bool data_truthy(const Block *b, const char *field);

// AccountId(v).as_str(): the canonical "wc:HEX" string for a present Account,
// or nullopt for addr_none / a non-Account value. The stonfi + tonco cores both
// gate wallet lookups on this.
std::optional<std::string> acc_str(const Value &v);

}  // namespace mch

// Narrow read-only views over blocks and consumed match sets.
// Only operations required by current hosts; not a graph-query language.
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
const Block *first_call(const std::vector<Block *> &blocks, std::uint32_t op);

// Every call_contract block in `blocks` whose opcode == `op`, in input order.
std::vector<const Block *> all_calls(const std::vector<const Block *> &blocks, std::uint32_t op);

// The first next_block of `b` that is a call_contract with opcode == `op`.
const Block *first_next_call(const Block *b, std::uint32_t op);

// De-duplicate by pointer identity, then stable-sort by min_lt ascending so
// peer-swap order is byte-identical.
void unique_lt_sorted(std::vector<const Block *> &blocks);

// Truthiness of a Value: a true Bool or a non-zero Int.
bool value_truthy(const Value &v);

// True Bool or non-zero Int; absent, null, or other types are false.
bool data_truthy(const Block *b, const char *field);

// Canonical "wc:HEX" for a present Account, or nullopt for addr_none /
// a non-Account value.
std::optional<std::string> acc_str(const Value &v);

}  // namespace mch

#include "host/BlockViews.h"

#include "BlockTree.h"
#include "host/HostCommon.h"

#include <algorithm>
#include <unordered_set>
#include <utility>

namespace mch {

namespace {

template <typename BlockPtr>
const Block *first_call_impl(const std::vector<BlockPtr> &blocks, std::uint32_t op) {
  for (const Block *b : blocks) {
    if (is_call_op(b, op)) {
      return b;
    }
  }
  return nullptr;
}

}  // namespace

bool block_in(const std::vector<const Block *> &v, const Block *b) {
  return std::find(v.begin(), v.end(), b) != v.end();
}

const Block *first_call(const std::vector<const Block *> &blocks, std::uint32_t op) {
  return first_call_impl(blocks, op);
}

const Block *first_call(const std::vector<Block *> &blocks, std::uint32_t op) {
  return first_call_impl(blocks, op);
}

std::vector<const Block *> all_calls(const std::vector<const Block *> &blocks, std::uint32_t op) {
  std::vector<const Block *> out;
  for (const Block *b : blocks) {
    if (is_call_op(b, op)) {
      out.push_back(b);
    }
  }
  return out;
}

const Block *first_next_call(const Block *b, std::uint32_t op) {
  if (b == nullptr) {
    return nullptr;
  }
  for (const Block *n : b->next_blocks) {
    if (is_call_op(n, op)) {
      return n;
    }
  }
  return nullptr;
}

void unique_lt_sorted(std::vector<const Block *> &blocks) {
  std::unordered_set<const Block *> seen;
  std::vector<const Block *> out;
  out.reserve(blocks.size());
  for (const Block *b : blocks) {
    if (seen.insert(b).second) out.push_back(b);
  }
  std::stable_sort(out.begin(), out.end(),
                   [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
  blocks = std::move(out);
}

bool value_truthy(const Value &v) {
  return (v.t == VType::Bool && v.boolean) ||
         (v.t == VType::Int && !v.num.is_null() && v.num->sgn() != 0);
}

bool data_truthy(const Block *b, const char *field) {
  return value_truthy(data_field(b, field));
}

std::optional<std::string> acc_str(const Value &v) {
  if (v.t == VType::Account && !v.addr_none) {
    return v.str;
  }
  return std::nullopt;
}

}  // namespace mch

// Read-only block/consumed-set views (see host/BlockViews.h).
#include "host/BlockViews.h"

#include "BlockTree.h"
#include "host/HostCommon.h"

#include <algorithm>

namespace mch {

bool block_in(const std::vector<const Block *> &v, const Block *b) {
  return std::find(v.begin(), v.end(), b) != v.end();
}

const Block *first_call(const std::vector<const Block *> &blocks, std::uint32_t op) {
  for (const Block *b : blocks) {
    if (is_call_op(b, op)) {
      return b;
    }
  }
  return nullptr;
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
  std::sort(blocks.begin(), blocks.end());
  blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());
  std::stable_sort(blocks.begin(), blocks.end(),
                   [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
}

bool data_truthy(const Block *b, const char *field) {
  Value v = data_field(b, field);
  return (v.t == VType::Bool && v.boolean) ||
         (v.t == VType::Int && !v.num.is_null() && v.num->sgn() != 0);
}

std::optional<std::string> acc_str(const Value &v) {
  if (v.t == VType::Account && !v.addr_none) {
    return v.str;
  }
  return std::nullopt;
}

}  // namespace mch

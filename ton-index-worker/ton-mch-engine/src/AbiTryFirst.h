#pragma once

#include "vm/cells/Cell.h"
#include "vm/cellslice.h"

#include <optional>
#include <type_traits>
#include <variant>

namespace mch {

// Try ABI fragment candidates in declaration order, reopening the payload for
// every attempt. A successful variant tags the decoded candidate by C++ type;
// nullopt means that no candidate accepted the payload.
template <class... Candidates>
std::optional<std::variant<Candidates...>> try_parse_first(
    const td::Ref<vm::Cell> &payload) {
  std::optional<std::variant<Candidates...>> match;
  if (payload.is_null()) {
    return match;
  }

  auto attempt = [&](auto *tag) {
    if (match) {
      return;
    }
    using Candidate = std::remove_pointer_t<decltype(tag)>;
    bool special = false;
    vm::CellSlice cs = vm::load_cell_slice_special(payload, special);
    if (special) {
      return;
    }
    auto parsed = Candidate::from_slice(cs);
    if (parsed.is_ok()) {
      match.emplace(std::in_place_type<Candidate>, parsed.move_as_ok());
    }
  };
  (attempt(static_cast<Candidates *>(nullptr)), ...);
  return match;
}

}  // namespace mch

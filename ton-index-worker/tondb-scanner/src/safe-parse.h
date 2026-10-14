#pragma once
#include <cstddef>
#include <exception>
#include <utility>

#include "td/utils/logging.h"
#include "td/utils/Status.h"
#include "vm/dict.h"
#include "vm/excno.hpp"

// Parsing helpers for cells that come from contract get-methods or account data.
namespace safe_parse {

// Shared subtrees let a small dictionary hold a huge number of entries.
constexpr std::size_t kDefaultMaxDictEntries = 1024;

// Runs f() and converts any exception into an error.
template <class F>
auto guard(td::Slice what, F&& f) -> decltype(f()) {
  try {
    return f();
  } catch (const vm::VmError& e) {
    return td::Status::Error(PSLICE() << what << ": " << e.get_msg());
  } catch (const vm::VmVirtError& e) {
    return td::Status::Error(PSLICE() << what << ": " << e.get_msg());
  } catch (const std::exception& e) {
    return td::Status::Error(PSLICE() << what << ": " << e.what());
  } catch (...) {
    return td::Status::Error(PSLICE() << what << ": unknown exception");
  }
}

// Calls on_entry for each entry in key order, stops after max_entries entries.
template <class F>
td::Status for_each_dict_entry(td::Ref<vm::Cell> root, int key_bits, std::size_t max_entries, F&& on_entry) {
  return guard("dictionary", [&]() -> td::Status {
    vm::Dictionary dict{std::move(root), key_bits};
    td::Status status;
    std::size_t seen = 0;
    bool complete = dict.check_for_each([&](td::Ref<vm::CellSlice> value, td::ConstBitPtr key, int key_len) {
      if (++seen > max_entries) {
        status = td::Status::Error(PSLICE() << "more than " << max_entries << " entries");
        return false;
      }
      status = on_entry(key, key_len, std::move(value));
      return status.is_ok();
    });
    if (status.is_error()) {
      return status;
    }
    if (!complete) {
      return td::Status::Error("malformed dictionary");
    }
    return td::Status::OK();
  });
}

}  // namespace safe_parse

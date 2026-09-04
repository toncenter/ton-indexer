// TL-B message-parser registry and dispatch. Per-family adapters live under
// src/parse.
#include "MsgParse.h"

#include "AbiBridge.h"
#include "TraceLoader.h"
#include "parse/PSlice.h"
#include "parse/Parsers.h"

#include "common/refint.h"
#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"
#include "vm/excno.hpp"

#include <exception>

#include <cstdio>
#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace mch {

// Merge handwritten and ABI-generated parsers by key. Duplicate keys map to a
// null parser so dispatch fails closed; startup validation reports their names.
namespace {

using ParserSources = std::vector<const std::vector<std::pair<std::string, MsgParserFn>> *>;

std::vector<std::string> duplicate_parser_keys(const ParserSources &sources) {
  std::set<std::string> seen;
  std::set<std::string> dup_set;
  std::vector<std::string> dups;
  for (const auto *src : sources) {
    for (const auto &row : *src) {
      if (!seen.insert(row.first).second && dup_set.insert(row.first).second) {
        dups.push_back(row.first);
      }
    }
  }
  return dups;
}

// Hand parsers still without an ABI declaration; everything else lives in
// AbiBridge.cpp.
const std::vector<std::pair<std::string, MsgParserFn>> &hand_message_parsers() {
  static const std::vector<std::pair<std::string, MsgParserFn>> rows = {
      {"ChangeDnsRecordMessage", parse_change_dns},
      {"VestingSendMessage", parse_vesting_send_message},
  };
  return rows;
}

struct BuiltRegistry {
  std::map<std::string, MsgParserFn> map;
  std::vector<std::string> dups;
};

const BuiltRegistry &built_registry() {
  static const BuiltRegistry built = [] {
    // One enumeration of the two sources in first-wins order, shared by the
    // merge and the duplicate scan (they must never disagree on the source set
    // or its order).
    const ParserSources sources = {&hand_message_parsers(), &abi_message_parsers()};
    BuiltRegistry b;
    for (const auto *src : sources) {
      for (const auto &row : *src) {
        auto [it, inserted] = b.map.emplace(row.first, row.second);
        if (!inserted) {
          it->second = nullptr;  // claimed twice -> poisoned, parse fails closed
        }
      }
    }
    b.dups = duplicate_parser_keys(sources);
    return b;
  }();
  return built;
}

}  // namespace

const std::map<std::string, MsgParserFn> &message_parsers() {
  return built_registry().map;
}

td::Status validate_registries() {
  const auto &dups = built_registry().dups;
  if (dups.empty()) {
    return td::Status::OK();
  }
  std::string joined;
  for (std::size_t i = 0; i < dups.size(); ++i) {
    if (i) {
      joined += ", ";
    }
    joined += dups[i];
  }
  return td::Status::Error(PSLICE() << "duplicate message parser keys: " << joined);
}

td::Result<Value> parse_message_body(const std::string &type_name, const td::Ref<vm::Cell> &body) {
  const auto &built = built_registry();
  auto it = built.map.find(type_name);
  if (it == built.map.end()) {
    return td::Status::Error(PSLICE() << "message type " << type_name << " is not registered");
  }
  if (it->second == nullptr) {  // poisoned at merge time (multi-source key)
    return td::Status::Error(PSLICE() << "message type " << type_name << " is ambiguously registered");
  }
  // A malformed body must reduce to a clean parse failure,
  // but only for the exception kinds a parser legitimately raises, cell VM
  // errors and std::exception subclasses (out_of_range/logic_error from the
  // hand-written tails). Anything else (bad_alloc, unknown host faults) is an
  // infrastructure failure and propagates to the trace-level handler rather
  // than being misclassified as an ordinary parse mismatch.
  try {
    return it->second(body);
  } catch (const vm::VmError &e) {
    return e.as_status("parse threw: ");
  } catch (const vm::VmVirtError &) {
    return td::Status::Error("parse threw: vm virtualization error");
  } catch (const vm::VmNoGas &) {
    return td::Status::Error("parse threw: out of gas");
  } catch (const std::exception &e) {
    return td::Status::Error(PSLICE() << "parse threw: " << e.what());
  }
}

}  // namespace mch

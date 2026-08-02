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

// The registered parser set = the kept-hand parsers (13) + ABI-generated rows
// (73, under stable BARE declaration names).
// First-source-wins on a collision; a key claimed by more than one source is
// instead poisoned to a nullptr parser at merge time, so parse_message_body
// fails closed on an ambiguous name with the same map lookup it already does.
// The collision NAMES are kept alongside the map purely so validate_registries()
// can list them at startup. The total registered set and
// --surface / cpp_surface.json) is unchanged either way.
namespace {

// Kept hand parsers (irregular / single-use constructs).
const std::vector<std::pair<std::string, MsgParserFn>> &hand_message_parsers() {
  static const std::vector<std::pair<std::string, MsgParserFn>> rows = {
      {"JettonTransfer", parse_jetton_transfer},         // forward-payload tail hook
      {"MinterJettonMint", parse_minter_jetton_mint},    // nested ref-capture
      {"ChangeDnsRecordMessage", parse_change_dns},
      {"AuctionFillUpMessage", parse_auction_fill_up},
      {"DnsReleaseBalanceMessage", parse_dns_release_balance},
      {"VestingSendMessage", parse_vesting_send_message},
      {"VestingAddWhiteList", parse_vesting_add_whitelist},  // unbounded ref chain
      {"EvaaWithdrawFailExcess", parse_evaa_withdraw_fail_excess},  // opcode-enum
      {"LayerZeroOappExecuteCallback", parse_layerzero_oapp_execute_callback},
      {"LayerzeroChannelSendCallback", parse_layerzero_channel_send_callback},
      {"ChannelCommitPacket", parse_layerzero_channel_commit_packet},
      {"UlnConnectionVerifyCallbackParser", parse_uln_connection_verify_callback},
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

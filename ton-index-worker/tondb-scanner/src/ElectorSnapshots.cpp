#include "ElectorSnapshots.h"

#include "convert-utils.h"
#include "parse_contract_methods.h"
#include "safe-parse.h"
#include "smc-interfaces/FetchAccountFromShard.h"
#include "smc-interfaces/NominatorPool.h"
#include "smc-interfaces/execute-smc.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/base64.h"
#include "common/checksum.h"
#include <set>
#include <unordered_map>

namespace staking_history {
namespace {

template<class F> std::string json_object(F&& fill) {
  td::JsonBuilder json;
  { auto object = json.enter_object(); fill(object); }
  return json.string_builder().as_cslice().str();
}

template<class T, class F> std::string json_array(const std::vector<T>& rows, F&& fill) {
  td::JsonBuilder json;
  { auto array = json.enter_array();
    for (const auto& row : rows) array(td::JsonRaw(json_object([&](auto& o) { fill(o, row); })));
  }
  return json.string_builder().as_cslice().str();
}

td::Result<td::RefInt256> integer(const vm::StackEntry& value) {
  if (value.type() != vm::StackEntry::t_int || value.as_int().is_null() ||
      !value.as_int()->is_valid() || value.as_int()->sgn() < 0) {
    return td::Status::Error("Invalid nonnegative Elector integer");
  }
  return value.as_int();
}

td::Result<uint32_t> uint32(const vm::StackEntry& value) {
  TRY_RESULT(number, integer(value));
  if (number->bit_size(false) > 32) {
    return td::Status::Error("Elector uint32 overflow");
  }
  return static_cast<uint32_t>(number->to_long());
}

td::Result<std::vector<schema::FrozenElection>> parse_impl(const std::vector<vm::StackEntry>& stack) {
  if (stack.size() != 1) return td::Status::Error("past_elections must return one list");
  std::vector<schema::FrozenElection> result;
  std::set<uint32_t> ids;
  auto list = stack[0];
  while (list.type() != vm::StackEntry::t_null) {
    if (result.size() >= 4096 || list.type() != vm::StackEntry::t_tuple || list.as_tuple()->size() != 2) {
      return td::Status::Error("Invalid past_elections list");
    }
    auto cons = list.as_tuple();
    const auto& head = cons->at(0);
    if (head.type() != vm::StackEntry::t_tuple) return td::Status::Error("Invalid election tuple");
    auto tuple = head.as_tuple();
    if (tuple->size() < 2) return td::Status::Error("Short election tuple");
    schema::FrozenElection election{};
    TRY_RESULT_ASSIGN(election.election_id, uint32(tuple->at(0)));
    if (!ids.insert(election.election_id).second) return td::Status::Error("Duplicate election");
    // Both historical getter ABIs: (id, (...fields...)) and (id, fields...).
    size_t offset = 1;
    if (tuple->size() == 2 && tuple->at(1).type() == vm::StackEntry::t_tuple) {
      tuple = tuple->at(1).as_tuple();
      offset = 0;
    }
    if (tuple->size() != offset + 7) return td::Status::Error("Unsupported election tuple");
    TRY_RESULT_ASSIGN(election.unfreeze_at, uint32(tuple->at(offset)));
    TRY_RESULT_ASSIGN(election.stake_held, uint32(tuple->at(offset + 1)));
    TRY_RESULT(hash, integer(tuple->at(offset + 2)));
    if (hash->bit_size(false) > 256) return td::Status::Error("Invalid validator set hash");
    election.vset_hash = td::hex_string(hash, true, 64);
    TRY_RESULT_ASSIGN(election.total_stake, integer(tuple->at(offset + 4)));
    if (election.total_stake->bit_size(false) > 120) return td::Status::Error("Frozen total exceeds Coins range");
    const auto& dictionary = tuple->at(offset + 3);
    td::RefInt256 sum = td::make_refint(0);
    if (dictionary.type() != vm::StackEntry::t_null) {
      if (dictionary.type() != vm::StackEntry::t_cell) return td::Status::Error("Invalid frozen dictionary");
      vm::Dictionary frozen(dictionary.as_cell(), 256);
      for (auto it = frozen.begin(); !it.eof(); ++it) {
        if (election.positions.size() >= 65535) return td::Status::Error("Too many frozen positions");
        auto value = *it.cur_value();
        td::Bits256 holder;
        unsigned long long weight;
        int banned;
        if (!value.fetch_bits_to(holder) || !value.fetch_uint_to(64, weight)) {
          return td::Status::Error("Invalid frozen holder/weight");
        }
        auto stake = block::tlb::t_Grams.as_integer_skip(value);
        if (stake.is_null() || !value.fetch_int_to(1, banned) || !value.empty_ext()) {
          return td::Status::Error("Invalid frozen stake/banned");
        }
        election.positions.push_back({td::BitArray<256>(it.cur_pos()).to_hex(),
            "-1:" + holder.to_hex(), weight, stake, banned != 0});
        sum += stake;
      }
    }
    if (td::cmp(sum, election.total_stake) != 0) return td::Status::Error("Frozen stake sum mismatch");
    result.push_back(std::move(election));
    list = cons->at(1);
  }
  std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) { return a.election_id < b.election_id; });
  return result;
}

td::Result<schema::StakeHolder> detect_holder(const schema::AccountState& state, const ParsedBlock& parsed) {
  schema::StakeHolder holder;
  holder.address = convert::to_raw_address(state.account);
  if (state.code.is_null() || state.data.is_null()) return td::Status::Error("Holder has no active code/data");
  holder.code_hash = state.code->get_hash().to_hex();
  holder.data_hash = state.data->get_hash().to_hex();
  // Published Orbs v1.0/v1.1 hashes, not a heuristic based on a common getter.
  auto hash64 = td::base64_encode(state.code->get_hash().as_slice());
  if (hash64 == "pCrmnqx2/+DkUtPU8T04ehTkbAGlqtul/B2JPmxx9bo=" ||
      hash64 == "zA05WJ6ywM/g/eKEVmV6O909lTlVrj+Y8lZkqzyQT70=") {
    holder.kind = "single_nominator";
    return holder;
  }
  if (holder.code_hash == nominator_pool::CODE_HASH) {
    // Same code identity check as the normal NominatorPool detector. Its full
    // depositor dictionary is irrelevant to holder-level stake attribution.
    holder.kind = "nominator_pool";
    return holder;
  }
  TRY_RESULT(methods, parse_contract_methods(state.code, parsed.mc_block_.config_.get()));
  auto method_id = [](const std::string& name) { return (td::crc16(name) & 0xffff) | 0x10000; };
  for (const auto& probe : {std::pair<std::string, size_t>{"get_validator_controller_data", 12},
                            {"get_loan_state", 1}}) {
    if (std::find(methods.begin(), methods.end(), method_id(probe.first)) == methods.end()) continue;
    TRY_RESULT(stack, execute_smc_method(state.account, state.code, state.data,
        parsed.mc_block_.config_, probe.first, {}));
    if (stack.size() <= probe.second || stack[probe.second].type() != vm::StackEntry::t_slice) {
      return td::Status::Error("Invalid staking controller getter result");
    }
    if (probe.second == 1 && (stack.size() != 4 || stack[0].type() != vm::StackEntry::t_slice ||
        stack[2].type() != vm::StackEntry::t_slice || uint32(stack[3]).is_error())) {
      return td::Status::Error("Invalid Hipo loan result");
    }
    if (probe.second == 12 && stack[11].type() != vm::StackEntry::t_slice) {
      return td::Status::Error("Invalid controller validator address");
    }
    TRY_RESULT(pool, convert::to_raw_address(stack[probe.second].as_slice()));
    holder.kind = probe.second == 12 ? "liquid_staking_controller" : "hipo_controller";
    holder.pool = std::move(pool);
    return holder;
  }
  return holder;
}

}  // namespace

td::Result<std::vector<schema::FrozenElection>> parse_elections(const std::vector<vm::StackEntry>& stack) {
  return safe_parse::guard("Elector frozen positions", [&] { return parse_impl(stack); });
}

void collect_changed_holders(uint32_t mc_seqno, uint32_t gen_utime, ParsedBlock& parsed) {
  parsed.stake_holder_states_.clear();
  std::unordered_map<block::StdAddress, const schema::AccountState*> latest;
  for (const auto& state : parsed.account_states_) {
    if (state.account.workchain != ton::masterchainId) continue;
    auto& last = latest[state.account];
    if (!last || last->last_trans_lt < state.last_trans_lt) last = &state;
  }
  for (const auto& [address, state] : latest) {
    schema::StakeHolder holder;
    holder.address = convert::to_raw_address(address);
    if (state->code.not_null()) holder.code_hash = state->code->get_hash().to_hex();
    if (state->data.not_null()) holder.data_hash = state->data->get_hash().to_hex();
    auto result = safe_parse::guard("Stake holder", [&] { return detect_holder(*state, parsed); });
    if (result.is_ok()) holder = result.move_as_ok();
    else holder.error = result.move_as_error().message().str();
    holder.mc_seqno = mc_seqno;
    holder.gen_utime = gen_utime;
    parsed.stake_holder_states_.push_back(std::move(holder));
  }
}

void detect_holders(schema::ElectorSnapshot& snapshot, ParsedBlock& parsed) {
  AllShardStates roots;
  for (const auto& state : parsed.mc_block_.shard_blocks_) roots.push_back(state.block_state);
  std::set<std::string> holders;
  for (const auto& election : snapshot.elections) {
    for (const auto& position : election.positions) holders.insert(position.holder);
  }
  for (const auto& raw : holders) {
    auto cached = std::find_if(parsed.stake_holder_states_.begin(), parsed.stake_holder_states_.end(),
        [&](const auto& h) { return h.address == raw; });
    if (cached != parsed.stake_holder_states_.end()) {
      snapshot.holders.push_back(*cached);
      continue;
    }
    schema::StakeHolder holder;
    holder.address = raw;
    auto result = safe_parse::guard("Stake holder", [&]() -> td::Result<schema::StakeHolder> {
      block::StdAddress address;
      if (!address.parse_addr(raw)) return td::Status::Error("Invalid holder address");
      TRY_RESULT(state, lookup_account(roots, address));
      // Preserve provenance even if the probe fails.
      if (state.code.not_null()) holder.code_hash = state.code->get_hash().to_hex();
      if (state.data.not_null()) holder.data_hash = state.data->get_hash().to_hex();
      return detect_holder(state, parsed);
    });
    if (result.is_ok()) holder = result.move_as_ok();
    else holder.error = result.move_as_error().message().str();
    holder.mc_seqno = snapshot.mc_seqno;
    holder.gen_utime = snapshot.gen_utime;
    snapshot.holders.push_back(holder);
    parsed.stake_holder_states_.push_back(std::move(holder));
  }
}

std::string serialize(const schema::StakeHolder& h) {
  return json_object([&](auto& item) {
    item("address", h.address)("code_hash", h.code_hash)("data_hash", h.data_hash)
        ("kind", h.kind)("error", h.error);
    if (h.pool) item("pool", *h.pool);
  });
}

std::string serialize(const schema::ElectorSnapshot& snapshot) {
  return json_object([&](auto& root) {
    root("version", 1)("address", snapshot.address)("code_hash", snapshot.code_hash)("data_hash", snapshot.data_hash);
    root("previous_data_hash", snapshot.previous_data_hash);
    root("complete", td::JsonBool(snapshot.complete))("error", snapshot.error);
    root("penalty_activity", td::JsonBool(snapshot.penalty_activity));
    root("applications_complete", td::JsonBool(snapshot.applications_complete))("applications_error", snapshot.applications_error);
    if (snapshot.applications) {
      const auto& a = *snapshot.applications;
      root("applications", td::JsonRaw(json_object([&](auto& item) {
      item("election_id", int64_t(a.election_id))("finished", td::JsonBool(a.finished))("failed", td::JsonBool(a.failed))
          ("total_stake", a.total_stake->to_dec_string());
      item("participants", td::JsonRaw(json_array(a.participants, [](auto& member, const auto& p) {
        member("pubkey", p.validator_pubkey)("holder", p.stake_holder_address)
            ("adnl", p.adnl_addr)("stake", p.stake->to_dec_string());
      })));
      })));
    }
    root("elections", td::JsonRaw(json_array(snapshot.elections, [](auto& item, const auto& e) {
        item("election_id", int64_t(e.election_id))("unfreeze_at", int64_t(e.unfreeze_at))("stake_held", int64_t(e.stake_held))
            ("vset_hash", e.vset_hash)("total_stake", e.total_stake->to_dec_string());
        item("positions", td::JsonRaw(json_array(e.positions, [](auto& position, const auto& p) {
          position("pubkey", p.pubkey)("holder", p.holder)("weight", td::to_string(p.weight))
              ("stake", p.stake->to_dec_string())("banned", td::JsonBool(p.banned));
          if (p.adnl) position("adnl", *p.adnl);
        })));
    })));
    root("holders", td::JsonRaw(json_array(snapshot.holders, [](auto& item, const auto& h) {
      item("address", h.address)("code_hash", h.code_hash)("data_hash", h.data_hash)
          ("kind", h.kind)("error", h.error);
      if (h.pool) item("pool", *h.pool);
    })));
  });
}

}  // namespace staking_history

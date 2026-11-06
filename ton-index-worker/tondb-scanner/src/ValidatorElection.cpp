#include "ValidatorElection.h"

#include "ElectorRoundResults.h"
#include "Statistics.h"
#include "td/utils/base64.h"

namespace elector {
namespace {

using Election = std::optional<schema::ValidatorElection>;

struct Key {
  td::Bits256 code, election;
  int vm_version;
  bool has_election;
  bool operator==(const Key&) const = default;
};
struct KeyHash {
  size_t operator()(const Key& key) const {
    auto result = std::hash<td::Bits256>{}(key.code);
    result ^= std::hash<td::Bits256>{}(key.election) + (result << 6) + (result >> 2);
    return result ^ (static_cast<size_t>(key.vm_version) << 1) ^ key.has_election;
  }
};

memo::Cache<Key, Election, KeyHash>& cache() {
  static memo::Cache<Key, Election, KeyHash> instance(8 * 1024 * 1024, 16);
  return instance;
}

td::Result<std::optional<Key>> cache_key(
    const schema::AccountState& state, const std::shared_ptr<block::ConfigInfo>& config) {
  return safe_parse::guard("Elector election cache key", [&]() -> td::Result<std::optional<Key>> {
    if (state.code.is_null() || state.data.is_null() || !config) return std::optional<Key>{};
    // Audited deployed bytecode (fixture: test/fixtures/elector-code.b64).
    // Dispatcher: SETCP0; DICTPUSHCONST; DICTIGETJMPZ; THROWARG.
    // Method 86698 reads c4's first HashmapE reference, unpacks cur_elect and
    // walks members. Its entire CALLREF closure uses no c7/config/time/libraries.
    // Including an arbitrary code hash in a key would NOT establish that fact
    // for a future upgrade; unrecognized code must bypass this optimization.
    static const td::Bits256 known_code = [] {
      td::Bits256 hash;
      hash.as_slice().copy_from(td::base64_decode("Yhf4csmfr8uHDywRo2L1kzm+lQlfcNALnP8vbc1p090=").move_as_ok());
      return hash;
    }();
    td::Bits256 code_hash(state.code->get_hash().bits());
    if (code_hash != known_code) return std::optional<Key>{};
    TRY_RESULT(loaded, state.data->load_cell());
    vm::CellSlice data(std::move(loaded));
    if (data.special_type() != vm::Cell::SpecialType::Ordinary) return std::optional<Key>{};
    td::Ref<vm::Cell> election;
    if (!data.fetch_maybe_ref(election)) return td::Status::Error("Missing cur_elect reference");
    // get_hash() on the reference does not load/traverse its members dictionary.
    return std::optional<Key>{Key{code_hash,
        election.is_null() ? td::Bits256::zero() : td::Bits256(election->get_hash().bits()),
        config->get_global_version(), election.not_null()}};
  });
}

td::Result<td::RefInt256> integer(const vm::StackEntry& entry) {
  auto value = entry.as_int();
  if (value.is_null()) return td::Status::Error("Expected Elector integer stack entry");
  return value;
}
td::Result<uint32_t> u32(const vm::StackEntry& entry) {
  TRY_RESULT(value, integer(entry));
  auto n = value->to_long();
  if (n < 0 || n > std::numeric_limits<uint32_t>::max()) {
    return td::Status::Error("Elector integer out of uint32 range");
  }
  return static_cast<uint32_t>(n);
}
td::Result<bool> boolean(const vm::StackEntry& entry) {
  TRY_RESULT(value, integer(entry));
  return value->to_long() != 0;
}
td::Result<std::string> hex(const vm::StackEntry& entry) {
  TRY_RESULT(value, integer(entry));
  return td::hex_string(value, true, 64);
}

td::Result<Election> calculate(const schema::AccountState& state,
                              const std::shared_ptr<block::ConfigInfo>& config, uint32_t now) {
  return safe_parse::guard("Validator election", [&]() -> td::Result<Election> {
    g_statistics.record_count(ELECTOR_PARTICIPANT_GETTER);
    TRY_RESULT(stack, run_get_method(state, config, "participant_list_extended", {}, now));
    if (stack.size() < 7) return td::Status::Error("participant_list_extended returned too few stack entries");
    TRY_RESULT(id, u32(stack[0]));
    if (id == 0) return Election{};
    schema::ValidatorElection election{};
    election.election_id = id;
    TRY_RESULT_ASSIGN(election.elect_close, u32(stack[1]));
    TRY_RESULT_ASSIGN(election.min_stake, integer(stack[2]));
    TRY_RESULT_ASSIGN(election.total_stake, integer(stack[3]));
    TRY_RESULT_ASSIGN(election.failed, boolean(stack[5]));
    TRY_RESULT_ASSIGN(election.finished, boolean(stack[6]));
    auto list = stack[4];
    while (list.type() != vm::StackEntry::Type::t_null) {
      auto cons = list.as_tuple();
      if (cons.is_null() || cons->size() != 2) return td::Status::Error("Expected TVM cons list for participants");
      auto pair = cons->at(0).as_tuple();
      if (pair.is_null() || pair->size() < 2) return td::Status::Error("Invalid participant entry");
      auto data = pair->at(1).as_tuple();
      if (data.is_null() || data->size() < 4) return td::Status::Error("Invalid participant data");
      schema::ValidatorElectionParticipant participant{};
      participant.election_id = id;
      TRY_RESULT_ASSIGN(participant.validator_pubkey, hex(pair->at(0)));
      TRY_RESULT_ASSIGN(participant.stake, integer(data->at(0)));
      TRY_RESULT_ASSIGN(participant.max_factor, u32(data->at(1)));
      TRY_RESULT(address, hex(data->at(2)));
      participant.stake_holder_address = "-1:" + address;
      TRY_RESULT_ASSIGN(participant.adnl_addr, hex(data->at(3)));
      election.participants.push_back(std::move(participant));
      list = cons->at(1);
    }
    return Election{std::move(election)};
  });
}

size_t dynamic_bytes(const Election& value) {
  if (!value) return 0;
  constexpr size_t integer_bytes = sizeof(td::BigInt256) + 64;  // RefInt allocation + allowance
  size_t bytes = value->participants.capacity() * sizeof(schema::ValidatorElectionParticipant) + 2 * integer_bytes;
  for (const auto& p : value->participants) {
    bytes += integer_bytes + p.validator_pubkey.capacity() + p.adnl_addr.capacity() +
             p.stake_holder_address.capacity() + 3 * (1 + 16);  // terminators + string allocator allowance
  }
  return bytes;
}

Election with_source(Election election, uint32_t mc_seqno) {
  if (election) {
    election->source_mc_seqno = mc_seqno;
    for (auto& participant : election->participants) participant.source_mc_seqno = mc_seqno;
  }
  return election;
}

}  // namespace

td::Result<Election> parse_election(const schema::AccountState& state,
    const std::shared_ptr<block::ConfigInfo>& config, uint32_t now, uint32_t mc_seqno) {
  auto key = cache_key(state, config);
  auto load = [&] { return calculate(state, config, now); };
  // An unreadable/unrecognized layout is not a negative cache entry. Let the
  // original VM path report its own result, and try the key again on a retry.
  auto value = key.is_ok() && key.ok()
      ? cache().get(*key.ok(), load, dynamic_bytes) : cache().uncached(load);
  TRY_RESULT(result, std::move(value));
  return with_source(*result, mc_seqno);  // mutable per-block copy, outside the lock
}

td::Result<Election> parse_election_uncached(const schema::AccountState& state,
    const std::shared_ptr<block::ConfigInfo>& config, uint32_t now, uint32_t mc_seqno) {
  TRY_RESULT(result, calculate(state, config, now));
  return with_source(std::move(result), mc_seqno);
}

memo::Stats election_cache_stats() { return cache().stats(); }
std::string election_cache_report() { return cache().report("indexer.cache.elector_election"); }

}  // namespace elector

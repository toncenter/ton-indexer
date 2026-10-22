// Shared state, request validation and context preparation for TVM calls.
#include <algorithm>
#include <limits>
#include <unordered_set>
#include "Calls.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "td/utils/crypto.h"

namespace tvm_executor {
namespace {

namespace pbtvm = ton::tvm::v1;
using Cell = td::Ref<vm::Cell>;

// Validate the params interpreted while preparing the VM, before mixing
// request cells with onchain cells. Database read failures must stay retryable.
td::Status validate_config_override(int param, const Cell& cell, bool transaction) {
  switch (param) {
    case 5:
    case 9:
    case 12:
    case 14:
    case 31:
    case 44:
      if (!transaction) return td::Status::OK();
      break;
    case 0:
    case 8:
    case 18:
    case 19:
    case 20:
    case 21:
    case 24:
    case 25:
    case 43:
    case 45:
      break;
    default:
      // Other params remain opaque to the executor, including custom ones.
      return td::Status::OK();
  }
  try {
    // The same TL-B validation and cell budget as TON's check_one_config_param.
    // The budget also bounds traversal of dictionaries with shared subtrees.
    if (block::gen::ConfigParam{static_cast<unsigned>(param)}.validate_ref(cell)) {
      return td::Status::OK();
    }
  } catch (vm::VmVirtError&) {
    return td::Status::Error(PSLICE() << "config param " << param << " is pruned");
  } catch (vm::VmError&) {
    return td::Status::Error(PSLICE() << "config param " << param << " cannot be parsed");
  }
  return td::Status::Error(PSLICE() << "config param " << param << " is invalid");
}

// The address of the config contract, which is a special account: param 0,
// as TON reads it from the config of c7 (try_fetch_config_from_c7).
td::Bits256 config_address(const Cell& root) {
  td::Bits256 address = td::Bits256::zero();
  auto param = vm::Dictionary{root, 32}.lookup_ref(td::BitArray<32>::zero());
  if (param.not_null()) {
    auto cs = vm::load_cell_slice(param);
    if (cs.size() == 256) {
      cs.fetch_bits_to(address);
    }
  }
  return address;
}

}  // namespace

td::Status unreadable(td::Status error) {
  return td::Status::Error(static_cast<int>(Code::Unavailable), error.to_string());
}

td::Status malformed(td::Slice message) {
  return td::Status::Error(static_cast<int>(Code::Internal), message);
}

pbtvm::CallResult call_error(pbtvm::ErrorCode code, std::string message) {
  pbtvm::CallResult result;
  result.mutable_error()->set_code(code);
  result.mutable_error()->set_message(std::move(message));
  return result;
}

td::Result<Cell> request_boc(const std::string& boc, td::Slice what) {
  if (boc.size() > kMaxInputBocSize) {
    return td::Status::Error(PSLICE() << what << " of " << boc.size() << " bytes is over " << kMaxInputBocSize);
  }
  auto roots = vm::std_boc_deserialize_multi(td::Slice(boc));
  if (roots.is_error() || roots.ok().size() != 1) {
    return td::Status::Error(PSLICE() << what << " is not a BOC with one root");
  }
  return std::move(roots.move_as_ok()[0]);
}

td::Result<AccountState> request_account(const std::string& shard_account_boc) {
  TRY_RESULT(root, request_boc(shard_account_boc, "shard_account_boc"));
  block::gen::ShardAccount::Record shard_account;
  if (!tlb::unpack_cell(root, shard_account)) {
    return td::Status::Error("shard_account_boc is not a ShardAccount");
  }
  // Unpacking ShardAccount only extracts its reference. Validate Account
  // before address lookup: malformed request cells must not become retryable
  // database errors in RequestActor. Code and data remain opaque cells.
  try {
    if (!block::gen::t_Account.validate_ref(shard_account.account)) {
      return td::Status::Error("shard_account_boc: invalid Account");
    }
  } catch (vm::VmVirtError&) {
    return td::Status::Error("shard_account_boc: pruned Account");
  } catch (vm::VmError& e) {
    return td::Status::Error(PSLICE() << "shard_account_boc: invalid Account: " << e.get_msg());
  }
  return AccountState{shard_account.account, shard_account.last_trans_hash, shard_account.last_trans_lt};
}

std::optional<block::StdAddress> account_address(const Cell& account) {
  auto cs = vm::load_cell_slice_ref(account);
  block::gen::Account::Record_account record;
  block::StdAddress address;
  if (block::gen::t_Account.get_tag(*cs) != block::gen::Account::account || !tlb::csr_unpack(cs, record) ||
      !block::tlb::t_MsgAddressInt.extract_std_address(record.addr, address)) {
    return std::nullopt;
  }
  return address;
}

// Each distinct cell takes at least its two descriptor bytes, its data and a
// byte per reference.
td::Result<bool> boc_within(const Cell& root, std::size_t limit) {
  std::unordered_set<vm::CellHash> seen;
  std::vector<Cell> pending{root};
  std::size_t bytes = 0;
  while (!pending.empty()) {
    auto cell = std::move(pending.back());
    pending.pop_back();
    if (!seen.insert(cell->get_hash()).second) {
      continue;
    }
    auto loaded = cell->load_cell();
    if (loaded.is_error()) {
      return unreadable(loaded.move_as_error());
    }
    const auto& data = loaded.ok().data_cell;
    bytes += 2 + (data->get_bits() + 7) / 8 + data->get_refs_cnt();
    if (bytes > limit) {
      return false;
    }
    for (unsigned i = 0; i < data->get_refs_cnt(); i++) {
      pending.push_back(data->get_ref(i));
    }
  }
  return true;
}

td::Status load_tree(const Cell& root) {
  TRY_RESULT(within, boc_within(root, std::numeric_limits<std::size_t>::max()));
  CHECK(within);
  return td::Status::OK();
}

td::Result<td::Ref<vm::Tuple>> prev_blocks_info(const block::ConfigInfo& config) {
  if (config.get_global_version() < 4) {
    return td::Ref<vm::Tuple>{};
  }
  auto info = config.get_prev_blocks_info();
  if (info.is_error()) {
    return malformed(PSLICE() << "prev blocks info: " << info.error().message());
  }
  return info.move_as_ok();
}

Cell shard_account_cell(const AccountState& state) {
  return vm::CellBuilder{}
      .store_ref(state.account)
      .store_bits(state.last_trans_hash.cbits(), 256)
      .store_long(static_cast<td::int64>(state.last_trans_lt), 64)
      .finalize();
}

td::Result<std::optional<std::string>> shard_account_boc(const AccountState& state) {
  auto root = shard_account_cell(state);
  TRY_RESULT(within, boc_within(root, kMaxInputBocSize));
  if (!within) {
    return std::nullopt;
  }
  auto boc = vm::std_boc_serialize(root);
  if (boc.is_error()) {
    return malformed(PSLICE() << "cannot serialize a ShardAccount: " << boc.error().message());
  }
  if (boc.ok().size() > kMaxInputBocSize) {
    return std::nullopt;
  }
  return boc.ok().as_slice().str();
}

td::Result<block::Account> transaction_account(const AccountState& state, const block::StdAddress& address,
                                               const block::Config& config, td::uint32 now) {
  block::Account account{address.workchain, address.addr.cbits()};
  bool special = address.workchain == ton::masterchainId && config.is_special_smartcontract(address.addr);
  if (!account.unpack(vm::load_cell_slice_ref(shard_account_cell(state)), now, special)) {
    return td::Status::Error("cannot unpack the account for a transaction");
  }
  // Account::init_new resets these for account_none; keep the ShardAccount
  // as given, as the native emulator API does.
  account.last_trans_lt_ = state.last_trans_lt;
  account.last_trans_hash_ = state.last_trans_hash;
  return account;
}

AccountState account_state(const block::Account& account) {
  AccountState state{account.total_state, account.last_trans_hash_, account.last_trans_lt_};
  if (account.status == block::Account::acc_nonexist) {
    state.last_trans_hash.set_zero();
    state.last_trans_lt = 0;
  }
  return state;
}

td::Status check_transaction_config(const block::Config& config, const td::Ref<vm::Tuple>& prev_blocks_info,
                                    ton::WorkchainId workchain, td::uint32 now, td::Bits256 block_rand_seed) {
  td::Ref<vm::Cell> old_mparams;
  std::vector<block::StoragePrices> storage_prices;
  block::StoragePhaseConfig storage_phase_cfg{&storage_prices};
  block::ComputePhaseConfig compute_phase_cfg;
  block::ActionPhaseConfig action_phase_cfg;
  block::SerializeConfig serialize_config;
  td::RefInt256 masterchain_create_fee, basechain_create_fee;
  return block::FetchConfigParams::fetch_config_params(
      config, prev_blocks_info, &old_mparams, &storage_prices, &storage_phase_cfg, &block_rand_seed, &compute_phase_cfg,
      &action_phase_cfg, &serialize_config, &masterchain_create_fee, &basechain_create_fee, workchain, now);
}

td::Result<CallOverrides> parse_overrides(const pbtvm::ContextOverrides& overrides, bool transaction) {
  CallOverrides parsed;
  if ((overrides.has_now() && overrides.now() == 0) || (overrides.has_lt() && overrides.lt() == 0)) {
    return td::Status::Error("now and lt are not 0");
  }
  if (overrides.has_block_rand_seed() &&
      (overrides.block_rand_seed().size() != 32 || overrides.block_rand_seed() == std::string(32, '\0'))) {
    return td::Status::Error("block_rand_seed is 32 bytes, not all zero");
  }
  // Each param replaced by number, the lowest invalid one reported.
  std::vector<int> params;
  for (const auto& [param, boc] : overrides.config_params()) {
    params.push_back(param);
  }
  std::sort(params.begin(), params.end());
  for (int param : params) {
    auto cell = request_boc(overrides.config_params().at(param), PSLICE() << "config param " << param);
    if (cell.is_error()) {
      return td::Status::Error(cell.move_as_error().message().str());
    }
    auto status = validate_config_override(param, cell.ok(), transaction);
    if (status.is_error()) {
      return td::Status::Error(status.message().str());
    }
    parsed.config_params.emplace_back(param, cell.move_as_ok());
  }
  // Extra libraries, by the hash of their root cells.
  vm::Dictionary extra_libraries{256};
  for (int i = 0; i < overrides.libraries_size(); i++) {
    auto library = request_boc(overrides.libraries(i), PSLICE() << "library " << i);
    if (library.is_error()) {
      return td::Status::Error(library.move_as_error().message().str());
    }
    vm::CellBuilder cb;
    cb.store_ref(library.ok());
    extra_libraries.set_builder(library.ok()->get_hash().bits(), 256, cb);
  }

  if (overrides.has_now()) parsed.now = overrides.now();
  if (overrides.has_lt()) parsed.lt = overrides.lt();
  if (overrides.has_block_rand_seed()) {
    parsed.block_rand_seed.emplace();
    parsed.block_rand_seed->as_slice().copy_from(overrides.block_rand_seed());
  }
  parsed.libraries = extra_libraries.get_root_cell();
  return parsed;
}

td::Result<CallContext> resolve_context(const VmContext& context, CallOverrides overrides, int config_mode) {
  CallContext resolved{context, context.config, false, std::move(overrides.libraries)};
  if (!overrides.config_params.empty()) {
    vm::Dictionary dict{context.config->get_root_cell(), 32};
    for (const auto& [param, cell] : overrides.config_params) {
      dict.set_ref(td::BitArray<32>(param), cell);
    }
    auto root = dict.get_root_cell();
    auto config = std::make_shared<block::Config>(root, config_address(root), config_mode);
    auto status = config->unpack_wrapped();
    if (status.is_error()) {
      return td::Status::Error(PSLICE() << "config with the params given: " << status.message());
    }
    resolved.config = std::move(config);
    resolved.config_overridden = true;
  }
  if (overrides.now) {
    resolved.chain.now = *overrides.now;
  }
  if (overrides.lt) {
    resolved.chain.lt = *overrides.lt;
  }
  if (overrides.block_rand_seed) {
    resolved.chain.block_rand_seed = *overrides.block_rand_seed;
  }
  return resolved;
}

std::vector<Cell> CallContext::libraries(const Cell& account_library) const {
  std::vector<Cell> libraries;
  if (chain.config->get_libraries_root().not_null()) {
    libraries.push_back(chain.config->get_libraries_root());
  }
  if (account_library.not_null()) {
    libraries.push_back(account_library);
  }
  if (extra_libraries.not_null()) {
    libraries.push_back(extra_libraries);
  }
  return libraries;
}

td::Result<vm::Dictionary> CallContext::merged_libraries() const {
  vm::Dictionary libraries{chain.config->get_libraries_root(), 256};
  if (extra_libraries.not_null()) {
    vm::Dictionary extra{extra_libraries, 256};
    bool merged = extra.check_for_each([&](td::Ref<vm::CellSlice> value, td::ConstBitPtr key, int bits) {
      return libraries.set(key, bits, std::move(value), vm::Dictionary::SetMode::Set);
    });
    if (!merged) {
      return malformed("cannot merge the libraries of the call");
    }
  }
  return libraries;
}

td::Bits256 CallContext::vm_seed(const block::StdAddress& address, int global_version) const {
  td::BitArray<512> data;
  data.bits().copy_from(chain.block_rand_seed.cbits(), 256);
  // Transaction::prepare_rand_seed takes the first 32 bits of the rewritten
  // address, then the first 224 of the original one, before global version 8;
  // without anycast both are the account id.
  if (global_version >= 8) {
    (data.bits() + 256).copy_from(address.addr.cbits(), 256);
  } else {
    (data.bits() + 256).copy_from(address.addr.cbits(), 32);
    (data.bits() + 288).copy_from(address.addr.cbits(), 224);
  }
  td::Bits256 seed;
  td::sha256(data.as_slice(), seed.as_slice());
  return seed;
}

}  // namespace tvm_executor

#include "Stonfi.h"

#include "block/block-parse.h"
#include "convert-utils.h"
#include "DexConstants.h"
#include "DexPools.h"
#include "FetchAccountFromShard.h"
#include "Tokens.h"
#include "smc-interfaces/execute-smc.h"

namespace {

using T = vm::StackEntry::Type;

td::Result<block::StdAddress> wallet_master(const block::StdAddress& wallet, const block::StdAddress& router,
                                            const AllShardStates& shard_states,
                                            const std::shared_ptr<block::ConfigInfo>& config) {
  auto account_result = lookup_account(shard_states, wallet);
  if (account_result.is_error()) {
    return td::Status::Error(PSLICE() << "Pool wallet: " << account_result.error().message());
  }
  auto account = account_result.move_as_ok();
  if (account.code.is_null() || account.data.is_null()) {
    return td::Status::Error("Pool wallet not active");
  }
  auto wallet_data = JettonWalletDetectorR::detect(wallet, account.code, account.data, shard_states, config);
  if (wallet_data.is_error()) {
    return td::Status::Error(PSLICE() << "Pool wallet: " << wallet_data.error().message());
  }
  if (wallet_data.ok().owner != router) {
    return td::Status::Error("Pool wallet is not owned by the router");
  }
  return wallet_data.ok().jetton;
}

td::Result<std::string> stack_string(const td::Ref<vm::CellSlice>& cs) {
  if (cs->size() % 8 != 0 || cs->size_refs() != 0) {
    return td::Status::Error("Unexpected string slice");
  }
  std::string s(cs->size() / 8, '\0');
  if (!cs->prefetch_bytes(reinterpret_cast<unsigned char*>(s.data()), static_cast<int>(s.size()))) {
    return td::Status::Error("Unexpected string slice");
  }
  return s;
}

}

StonfiPoolDetector::StonfiPoolDetector(block::StdAddress address, td::Ref<vm::Cell> code_cell,
                                       td::Ref<vm::Cell> data_cell, AllShardStates shard_states,
                                       std::shared_ptr<block::ConfigInfo> config, td::Promise<Result> promise)
    : address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
      shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {
}

void StonfiPoolDetector::start_up() {
  promise_.set_result(detect(address_, code_cell_, data_cell_, shard_states_, config_));
  stop();
}

td::Result<StonfiPoolDetector::Result> StonfiPoolDetector::detect(
    const block::StdAddress& address, const td::Ref<vm::Cell>& code_cell, const td::Ref<vm::Cell>& data_cell,
    const AllShardStates& shard_states, const std::shared_ptr<block::ConfigInfo>& config) {
  if (!dex_snapshots_enabled()) {
    return td::Status::Error("DEX snapshots are off");
  }
  Result data;
  TRY_STATUS(read_pool(address, code_cell, data_cell, config, data));
  const td::Bits256 code_hash = code_cell->get_hash().bits();
  auto& cache = dex_pool_cache();
  auto known = cache.get(address);
  if (known && known->identity.code_hash == code_hash) {
    data.asset_0 = known->stonfi_asset_0;
    data.asset_1 = known->stonfi_asset_1;
  } else {
    TRY_STATUS(verify_identity(address, shard_states, config, data));
    cache.put(data, code_hash);
  }
  return data;
}

td::Status StonfiPoolDetector::read_pool(const block::StdAddress& address, const td::Ref<vm::Cell>& code_cell,
                                         const td::Ref<vm::Cell>& data_cell,
                                         const std::shared_ptr<block::ConfigInfo>& config, Result& data) {
  if (code_cell.is_null() || data_cell.is_null()) {
    return td::Status::Error("Code or data null");
  }
  const auto code_hash = code_cell->get_hash().to_hex();
  const bool is_v1 = dex_constants::stonfi_v1_pool_code_hashes.contains(code_hash);
  const bool is_v2 = dex_constants::stonfi_v2_pool_code_hashes.contains(code_hash);
  if (!is_v1 && !is_v2) {
    return td::Status::Error("Code hash mismatch");
  }

  data.address = address;
  if (is_v2) {
    TRY_RESULT(stack, execute_smc_method<12>(address, code_cell, data_cell, config, "get_pool_data", {}, {
      T::t_int, T::t_slice, T::t_int, T::t_int, T::t_int, T::t_slice,
      T::t_slice, T::t_int, T::t_int, T::t_slice, T::t_int, T::t_int
    }));
    data.version = 2;
    data.is_locked = stack[0].as_int()->sgn() != 0;
    TRY_RESULT_ASSIGN(data.router, convert::to_std_address(stack[1].as_slice()));
    data.total_supply_lp = stack[2].as_int();
    data.reserve_0 = stack[3].as_int();
    data.reserve_1 = stack[4].as_int();
    data.wallet_0 = stack[5].as_slice();
    data.wallet_1 = stack[6].as_slice();
    data.lp_fee = stack[7].as_int();
    data.protocol_fee = stack[8].as_int();

    TRY_RESULT(type_stack, execute_smc_method<1>(address, code_cell, data_cell, config, "get_pool_type", {},
                                                 {T::t_slice}));
    TRY_RESULT_ASSIGN(data.pool_type, stack_string(type_stack[0].as_slice()));
    auto tail_int = [&](size_t i) -> td::Result<td::RefInt256> {
      if (stack.size() <= i || stack[i].type() != T::t_int) {
        return td::Status::Error("Unexpected " + data.pool_type + " pool data");
      }
      return stack[i].as_int();
    };
    if (data.pool_type == "stableswap") {
      TRY_RESULT_ASSIGN(data.amp, tail_int(12));
    } else if (data.pool_type == "weighted_const_product") {
      TRY_RESULT_ASSIGN(data.w0, tail_int(12));
    } else if (data.pool_type == "weighted_stableswap") {
      TRY_RESULT_ASSIGN(data.amp, tail_int(12));
      TRY_RESULT_ASSIGN(data.rate, tail_int(13));
      TRY_RESULT_ASSIGN(data.w0, tail_int(14));
    } else if (data.pool_type != "constant_product" && data.pool_type != "constant_sum") {
      return td::Status::Error("Unknown pool type " + data.pool_type);
    }
  } else {
    TRY_RESULT(stack, execute_smc_method<7>(address, code_cell, data_cell, config, "get_pool_data", {}, {
      T::t_int, T::t_int, T::t_slice, T::t_slice, T::t_int, T::t_int, T::t_int
    }));
    data.version = 1;
    data.pool_type = "constant_product";
    data.reserve_0 = stack[0].as_int();
    data.reserve_1 = stack[1].as_int();
    data.wallet_0 = stack[2].as_slice();
    data.wallet_1 = stack[3].as_slice();
    data.lp_fee = stack[4].as_int();
    data.protocol_fee = stack[5].as_int();
    data.ref_fee = stack[6].as_int();
    // v1 get_pool_data has no router; it is the first field of the pool storage.
    try {
      auto storage = vm::load_cell_slice(data_cell);
      ton::WorkchainId workchain;
      ton::StdSmcAddress router;
      if (!block::tlb::t_MsgAddressInt.extract_std_address(storage, workchain, router)) {
        return td::Status::Error("No router in v1 pool storage");
      }
      data.router = block::StdAddress(workchain, router);
    } catch (vm::VmError& e) {
      return td::Status::Error("Failed to parse v1 pool storage: " + std::string(e.get_msg()));
    }
  }

  return td::Status::OK();
}

td::Status StonfiPoolDetector::verify_identity(const block::StdAddress& address, const AllShardStates& shard_states,
                                               const std::shared_ptr<block::ConfigInfo>& config, Result& data) {
  auto router_it = dex_constants::stonfi_routers.find(convert::to_raw_address(data.router));
  if (router_it == dex_constants::stonfi_routers.end() || router_it->second != data.version) {
    return td::Status::Error("Router is not an official STON.fi router");
  }
  TRY_RESULT(router, lookup_account(shard_states, data.router));
  if (router.code.is_null() || router.data.is_null()) {
    return td::Status::Error("Router account not found");
  }
  TRY_RESULT(derived, execute_smc_method<1>(data.router, router.code, router.data, config, "get_pool_address",
                                            {vm::StackEntry(data.wallet_0), vm::StackEntry(data.wallet_1)},
                                            {T::t_slice}));
  TRY_RESULT(derived_address, convert::to_std_address(derived[0].as_slice()));
  if (derived_address != address) {
    return td::Status::Error("Pool address mismatch with router");
  }

  TRY_RESULT(wallet_0, convert::to_std_address(data.wallet_0));
  TRY_RESULT(wallet_1, convert::to_std_address(data.wallet_1));
  TRY_RESULT_ASSIGN(data.asset_0, wallet_master(wallet_0, data.router, shard_states, config));
  TRY_RESULT_ASSIGN(data.asset_1, wallet_master(wallet_1, data.router, shard_states, config));
  return td::Status::OK();
}

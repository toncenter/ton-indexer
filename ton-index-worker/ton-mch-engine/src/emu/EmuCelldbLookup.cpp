#include "EmuCelldbLookup.h"

#include "EmuInterfaces.h"
#include "EmuJvaultChain.h"

#include "smc-interfaces/Multisig.h"
#include "smc-interfaces/NominatorPool.h"
#include "smc-interfaces/NftSale.h"
#include "smc-interfaces/NftSaleV4.h"
#include "smc-interfaces/Tokens.h"

#include <map>
#include <utility>

namespace mch {

namespace {

// The NUL separates lookup kind from the canonical raw address without aliasing.
std::string memo_key(const std::string &kind, const block::StdAddress &addr) {
  return kind + std::string(1, '\0') + std::to_string(addr.workchain) + ":" + addr.addr.to_hex();
}

}  // namespace

EmuCelldbTier2::EmuCelldbTier2(const AllShardStates *shard_states,
                               std::shared_ptr<block::ConfigInfo> config)
    : shard_states_(shard_states)
    , config_(std::move(config))
    // Capture the states pointer so the source survives moving this object.
    , account_source_([states = shard_states](const block::StdAddress &a) {
      return lookup_account(*states, a);
    }) {
}

const schema::AccountState *EmuCelldbTier2::account(const block::StdAddress &addr) {
  auto it = account_memo_.find(addr);
  if (it != account_memo_.end()) {
    return it->second ? &*it->second : nullptr;
  }
  stats_.fetched++;
  auto r = account_source_(addr);
  auto [pos, _] = account_memo_.emplace(
      addr, r.is_ok() ? std::optional<schema::AccountState>(r.move_as_ok()) : std::nullopt);
  return pos->second ? &*pos->second : nullptr;
}

Value EmuCelldbTier2::resolve(const std::string &kind, const block::StdAddress &addr) {
  enum class Handler {
    JvaultAssets,
    JettonWallet,
    DedustPool,
    NominatorPool,
    NftItem,
    NftAuction,
    NftSale,
    MultisigOrder,
  };
  static const std::map<std::string, Handler> handlers = {
      {"jvault_assets", Handler::JvaultAssets},
      {"jetton_wallet", Handler::JettonWallet},
      {"dedust_pool", Handler::DedustPool},
      {"nominator_pool", Handler::NominatorPool},
      {"nft_item", Handler::NftItem},
      {"nft_auction", Handler::NftAuction},
      {"nft_sale", Handler::NftSale},
      {"multisig_order", Handler::MultisigOrder},
  };
  auto handler = handlers.find(kind);
  if (handler == handlers.end()) {
    return Value::null();
  }
  if (handler->second == Handler::JvaultAssets) {
    return jvault_assets(addr);
  }

  const schema::AccountState *st = account(addr);
  if (st == nullptr) {
    return Value::null();
  }

  // Detectors that execute get-methods require config.
  std::optional<schema::BlockchainInterfaceV2> iface;
  switch (handler->second) {
    case Handler::NftItem: {
      if (config_ == nullptr) {
        return Value::null();
      }
      auto r = NftItemDetectorR::detect(addr, st->code, st->data, *shard_states_, config_);
      if (r.is_ok()) {
        iface = to_v2(r.move_as_ok());
      }
      break;
    }
    case Handler::NftAuction: {
      if (config_ == nullptr) {
        return Value::null();
      }
      auto r = GetGemsNftAuction::detect(addr, st->code, st->data, config_);
      if (r.is_ok()) {
        iface = to_v2(r.move_as_ok());
      }
      break;
    }
    case Handler::JettonWallet: {
      if (config_ == nullptr) {
        return Value::null();
      }
      auto r = JettonWalletDetectorR::detect(addr, st->code, st->data, *shard_states_, config_);
      if (r.is_ok()) {
        iface = to_v2(r.move_as_ok());
      }
      break;
    }
    case Handler::DedustPool: {
      if (config_ == nullptr) {
        return Value::null();
      }
      auto r = DedustPoolDetector::detect(addr, st->code, st->data, *shard_states_, config_);
      if (r.is_ok()) {
        iface = to_v2(r.move_as_ok());
      }
      break;
    }
    case Handler::NominatorPool: {
      auto r = NominatorPoolContract::detect(addr, st->code, st->data);
      if (r.is_ok()) {
        iface = to_v2(r.move_as_ok());
      }
      break;
    }
    case Handler::NftSale: {
      if (config_ == nullptr) {
        return Value::null();
      }
      auto r = GetGemsNftFixPriceSale::detect(addr, st->code, st->data, config_);
      if (r.is_ok()) {
        iface = to_v2(r.move_as_ok());
        break;
      }
      auto r4 = GetGemsNftFixPriceSaleV4::detect(addr, st->code, st->data, config_);
      if (r4.is_ok()) {
        iface = to_v2(r4.move_as_ok());
      }
      break;
    }
    case Handler::MultisigOrder: {
      if (config_ == nullptr) {
        return Value::null();
      }
      auto r = MultisigOrder::detect(addr, st->code, st->data, *shard_states_, config_);
      if (r.is_ok()) {
        iface = to_v2(r.move_as_ok());
      }
      break;
    }
    case Handler::JvaultAssets:
      break;
  }
  if (!iface) {
    return Value::null();
  }
  return ParsedBlockLookupSource::iface_value(kind, *iface);
}

Value EmuCelldbTier2::jvault_assets(const block::StdAddress &stake_wallet) {
  const schema::AccountState *wallet = account(stake_wallet);
  if (wallet == nullptr) {
    return Value::null();
  }
  auto r_wallet = parse_jvault_stake_wallet(wallet->data);
  if (r_wallet.is_error()) {
    return Value::null();
  }
  auto parts = r_wallet.move_as_ok();
  Value jvault_asset = jvault_asset_of(parts.minter);

  auto r_pool_addr = block::StdAddress::parse(td::Slice(parts.staking_pool_raw));
  if (r_pool_addr.is_error()) {
    return Value::null();
  }
  const schema::AccountState *pool = account(r_pool_addr.move_as_ok());
  if (pool == nullptr) {
    // A missing pool state yields a valid record with a null asset.
    return jvault_record(parts.staking_pool, Value::null(), std::move(jvault_asset));
  }

  auto r_lock = parse_jvault_pool_lock_wallet(pool->data);
  if (r_lock.is_error()) {
    return Value::null();
  }
  Value jw = fetch("jetton_wallet", {Value::make_str(r_lock.move_as_ok())});
  const Value *jetton = jw.field("jetton");
  if (jetton == nullptr || jetton->t != VType::Str) {
    return Value::null();
  }
  auto master = canonicalize_or_passthrough(jetton->str);
  return jvault_record(parts.staking_pool, Value::make_asset_jetton(master),
                       std::move(jvault_asset));
}

Value EmuCelldbTier2::fetch(const std::string &kind, const std::vector<Value> &args) {
  if (shard_states_ == nullptr || shard_states_->empty()) {
    return Value::null();
  }
  if (args.size() != 1 ||
      !(args[0].t == VType::Str ||
        (args[0].t == VType::Account && !args[0].addr_none))) {
    return Value::null();
  }
  auto r_addr = block::StdAddress::parse(td::Slice(args[0].str));
  if (r_addr.is_error()) {
    return Value::null();
  }
  block::StdAddress addr = r_addr.move_as_ok();

  const std::string key = memo_key(kind, addr);
  auto it = value_memo_.find(key);
  if (it != value_memo_.end()) {
    stats_.memo_hits++;
    return it->second;
  }
  // Any tier-2 parse failure is a miss and must not fail the whole trace.
  Value v;
  try {
    v = resolve(kind, addr);
  } catch (...) {
    v = Value::null();
  }
  value_memo_.emplace(key, v);
  return v;
}

}  // namespace mch

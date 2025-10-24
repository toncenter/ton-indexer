#include "td/actor/actor.h"
#include "vm/cells/Cell.h"
#include <unordered_set>
#include <string>
#include "IndexData.h"
#include "convert-utils.h"
#include "DataParser.h"
#include "Tokens.h"

#include "FetchAccountFromShard.h"
#include "parse_token_data.h"
#include "smc-interfaces/execute-smc.h"
#include "tokens-tlb.h"
#include "common/checksum.h"


JettonWalletDetectorR::JettonWalletDetectorR(block::StdAddress address, 
                                             td::Ref<vm::Cell> code_cell,
                                             td::Ref<vm::Cell> data_cell,
                                             AllShardStates shard_states,
                                             std::shared_ptr<block::ConfigInfo> config,
                                             td::Promise<Result> promise)
  : address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
    shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

void JettonWalletDetectorR::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  auto stack_r = execute_smc_method<4>(address_, code_cell_, data_cell_, config_, "get_wallet_data", {},
    {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell});
  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();
  Result data;
  data.address = address_;
  data.balance = stack[0].as_int();
  auto owner = convert::to_std_address(stack[1].as_slice());
  if (owner.is_error()) {
    promise_.set_error(owner.move_as_error());
    return;
  }
  data.owner = owner.move_as_ok();
  auto jetton = convert::to_std_address(stack[2].as_slice());
  if (jetton.is_error()) {
    promise_.set_error(jetton.move_as_error());
    return;
  }
  data.jetton = jetton.move_as_ok();
  
  auto is_claimed_stack_r = execute_smc_method<1>(address_, code_cell_, data_cell_, config_, "is_claimed", {},
    {vm::StackEntry::Type::t_int});
  if (is_claimed_stack_r.is_ok()) {
    auto is_claimed_stack = is_claimed_stack_r.move_as_ok();
    data.mintless_is_claimed = is_claimed_stack[0].as_int()->to_long() != 0;
  } else {
    data.mintless_is_claimed = std::nullopt;
  }

  auto R = td::PromiseCreator::lambda([=, this, SelfId = actor_id(this)](td::Result<schema::AccountState> account_state_r) mutable {
    if (account_state_r.is_error()) {
      promise_.set_error(account_state_r.move_as_error());
      stop();
      return;
    }
    auto account_state = account_state_r.move_as_ok();
    td::actor::send_closure(SelfId, &JettonWalletDetectorR::verify_with_master, account_state.code, account_state.data, data);
  });
  td::actor::create_actor<FetchAccountFromShardV2>("fetchaccountfromshard", shard_states_, data.jetton, std::move(R)).release();
}

void JettonWalletDetectorR::verify_with_master(td::Ref<vm::Cell> master_code, td::Ref<vm::Cell> master_data, Result jetton_wallet_data) {
  ton::SmartContract smc({master_code, master_data});
  ton::SmartContract::Args args;

  vm::CellBuilder anycast_cb;
  anycast_cb.store_bool_bool(false);
  auto anycast_cell = anycast_cb.finalize();
  td::Ref<vm::CellSlice> anycast_cs = vm::load_cell_slice_ref(anycast_cell);

  vm::CellBuilder cb;
  block::gen::t_MsgAddressInt.pack_addr_std(cb, anycast_cs, jetton_wallet_data.owner.workchain, jetton_wallet_data.owner.addr);
  auto owner_address_cell = cb.finalize();

  auto stack_r = execute_smc_method<1>(jetton_wallet_data.jetton, master_code, master_data, config_, "get_wallet_address", 
    {vm::StackEntry(vm::load_cell_slice_ref(owner_address_cell))}, {vm::StackEntry::Type::t_slice});
  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();

  auto wallet_address = convert::to_std_address(stack[0].as_slice());
  if (wallet_address.is_error()) {
    promise_.set_error(wallet_address.move_as_error());
    stop();
    return;
  }

  if (jetton_wallet_data.address == wallet_address.ok_ref()) {
    promise_.set_value(std::move(jetton_wallet_data));
  } else {
    promise_.set_error(td::Status::Error("Jetton Master returned wrong address"));
  }
  stop();
}


JettonMasterDetectorR::JettonMasterDetectorR(block::StdAddress address, 
                      td::Ref<vm::Cell> code_cell,
                      td::Ref<vm::Cell> data_cell, 
                      AllShardStates shard_states,
                      std::shared_ptr<block::ConfigInfo> config,
                      td::Promise<Result> promise)
  : address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
    shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}


void JettonMasterDetectorR::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  auto stack_r = execute_smc_method<5>(address_, code_cell_, data_cell_, config_, "get_jetton_data", {},
    {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell, vm::StackEntry::Type::t_cell});
  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();
  Result data;
  data.address = address_;
  data.total_supply = stack[0].as_int();
  data.mintable = stack[1].as_int()->to_long() != 0;
  auto admin_addr_cs = stack[2].as_slice();
  if (admin_addr_cs->size() == 2 && admin_addr_cs->prefetch_ulong(2) == 0) {
    // addr_none case
    data.admin_address = std::nullopt;
  } else {
    auto admin_address = convert::to_std_address(admin_addr_cs);
    if (admin_address.is_error()) {
      promise_.set_error(admin_address.move_as_error_prefix("jetton master admin address parsing failed: "));
      stop();
      return;
    }
    data.admin_address = admin_address.move_as_ok();
  }
  
  auto jetton_content = parse_token_data(stack[3].as_cell());
  if (jetton_content.is_error()) {
    promise_.set_error(jetton_content.move_as_error_prefix("get_jetton_data jetton_content parsing failed: "));
    stop();
    return;
  }
  data.jetton_content = jetton_content.move_as_ok();
  data.jetton_wallet_code_hash = stack[4].as_cell()->get_hash();

  promise_.set_value(std::move(data));
  stop();
}

NftItemDetectorR::NftItemDetectorR(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

void NftItemDetectorR::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  auto stack_r = execute_smc_method<5>(address_, code_cell_, data_cell_, config_, "get_nft_data", {},
        {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell});
  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();

  Result data;
  data.address = address_;
  data.init = stack[0].as_int()->to_long() != 0;
  data.index = stack[1].as_int();
  
  auto collection_addr_cs = stack[2].as_slice();
  if (collection_addr_cs->size() == 2 && collection_addr_cs->prefetch_ulong(2) == 0) {
    // addr_none case
    data.collection_address = std::nullopt;
  } else {
    auto collection_address = convert::to_std_address(stack[2].as_slice());
    if (collection_address.is_error()) {
      promise_.set_error(collection_address.move_as_error_prefix("nft collection address parsing failed: "));
      stop();
      return;
    }
    data.collection_address = collection_address.move_as_ok();
  }

  auto owner_addr_cs = stack[3].as_slice();
  if (owner_addr_cs->size() == 2 && owner_addr_cs->prefetch_ulong(2) == 0) {
    // addr_none case
    data.owner_address = std::nullopt;
  } else {
    auto owner_address = convert::to_std_address(owner_addr_cs);
    if (owner_address.is_error()) {
      promise_.set_error(owner_address.move_as_error_prefix("nft owner address parsing failed: "));
      stop();
      return;
    }
    data.owner_address = owner_address.move_as_ok();
  }

  if (!data.collection_address) {
    auto content = parse_token_data(stack[4].as_cell());
    if (content.is_error()) {
      promise_.set_error(content.move_as_error_prefix("nft content parsing failed: "));
      stop();
      return;
    }
    data.content = content.move_as_ok();
    promise_.set_value(std::move(data));
    stop();
  } else {
    auto ind_content = stack[4].as_cell();
    auto R = td::PromiseCreator::lambda([=, this, SelfId = actor_id(this)](td::Result<schema::AccountState> account_state_r) mutable {
      if (account_state_r.is_error()) {
        promise_.set_error(account_state_r.move_as_error());
        stop();
        return;
      }
      auto account_state = account_state_r.move_as_ok();
      td::actor::send_closure(SelfId, &NftItemDetectorR::got_collection, data, ind_content, account_state.code, account_state.data);
    });
    td::actor::create_actor<FetchAccountFromShardV2>("fetchaccountfromshard", shard_states_, data.collection_address.value(), std::move(R)).release();
  }
}

bool NftItemDetectorR::is_testnet = false;

const auto dot_ton_dns_root_addr_mainnet = block::StdAddress::parse("0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF").move_as_ok();
const auto dot_ton_dns_root_addr_testnet = block::StdAddress::parse("0:E33ED33A42EB2032059F97D90C706F8400BB256D32139CA707F1564AD699C7DD").move_as_ok();
const auto dot_t_dot_me_dns_root_addr_mainnet = block::StdAddress::parse("0:80D78A35F955A14B679FAA887FF4CD5BFC0F43B4A4EEA2A7E6927F3701B273C2").move_as_ok();

block::StdAddress NftItemDetectorR::get_dot_ton_dns_root_addr() {
  if (is_testnet) {
    return dot_ton_dns_root_addr_testnet;
  }
  return dot_ton_dns_root_addr_mainnet;
}

std::optional<block::StdAddress> NftItemDetectorR::dot_t_dot_me_dns_root_addr() {
  if (is_testnet) {
    return std::nullopt;  
  }
  return dot_t_dot_me_dns_root_addr_mainnet;
}

void NftItemDetectorR::got_collection(Result item_data, td::Ref<vm::Cell> ind_content, td::Ref<vm::Cell> collection_code, td::Ref<vm::Cell> collection_data) {
  auto verify = verify_with_collection(item_data.collection_address.value(), collection_code, collection_data, item_data.index);
  if (verify.is_error()) {
    promise_.set_error(verify.move_as_error());
    stop();
    return;
  }
  
  auto content = get_content(item_data.index, ind_content, item_data.collection_address.value(), collection_code, collection_data);
  if (content.is_error()) {
    promise_.set_error(content.move_as_error_prefix("failed to get nft item content: "));
    stop();
    return;
  }
  item_data.content = content.move_as_ok();

  process_domain_and_dns_data(get_dot_ton_dns_root_addr(), [this](){ return this->get_ton_domain(); }, item_data);
  auto t_me_root = dot_t_dot_me_dns_root_addr();
  if (t_me_root) {
      process_domain_and_dns_data(t_me_root.value(), [this](){ return this->get_t_me_domain(); }, item_data);
  }

  promise_.set_value(std::move(item_data));
  stop();
}

td::Status NftItemDetectorR::verify_with_collection(block::StdAddress collection_address, td::Ref<vm::Cell> collection_code, td::Ref<vm::Cell> collection_data, td::RefInt256 index) {
  auto stack_r = execute_smc_method<1>(collection_address, collection_code, collection_data, config_, "get_nft_address_by_index", 
    {vm::StackEntry(index)}, {vm::StackEntry::Type::t_slice});

  if (stack_r.is_error()) {
    return stack_r.move_as_error();
  }
  auto stack = stack_r.move_as_ok();

  auto nft_address = convert::to_std_address(stack[0].as_slice());
  if (nft_address.is_error()) {
    return td::Status::Error("get_nft_address_by_index parse address failed");
  }

  return nft_address.move_as_ok() == address_ ? td::Status::OK() : td::Status::Error("NFT Item doesn't belong to the referred collection");
}

td::Result<std::map<std::string, std::string>> NftItemDetectorR::get_content(td::RefInt256 index, td::Ref<vm::Cell> ind_content, block::StdAddress collection_address,
    td::Ref<vm::Cell> collection_code, td::Ref<vm::Cell> collection_data) {
  TRY_RESULT(stack, execute_smc_method<1>(collection_address, collection_code, collection_data, config_, "get_nft_content", 
    {vm::StackEntry(index), vm::StackEntry(ind_content)}, {vm::StackEntry::Type::t_cell}));

  return parse_token_data(stack[0].as_cell());
}

void NftItemDetectorR::process_domain_and_dns_data(const block::StdAddress& root_address, const std::function<td::Result<std::string>()>& get_domain_function, Result& item_data) {
  if (!item_data.collection_address.has_value() || !(item_data.collection_address.value() == root_address)) {
    return;
  }
  auto domain = get_domain_function();
  if (domain.is_error()) {
    LOG(ERROR) << "Failed to get domain for " << address_ << ": " << domain.move_as_error();
  } else {
    item_data.content.value()["domain"] = domain.ok();

    auto dns_data = get_dns_entry_data();
    if (dns_data.is_ok()) {
        dns_data.ok_ref().domain = domain.move_as_ok();
        item_data.dns_entry = dns_data.move_as_ok();
    }
  }
}

td::Result<NftItemDetectorR::Result::DNSEntry> NftItemDetectorR::get_dns_entry_data() {
  auto zero_byte_cell = vm::CellBuilder().store_bytes("\0").finalize();
  auto zero_byte_slice = vm::load_cell_slice_ref(zero_byte_cell);
  td::RefInt256 categories{true, 0};

  TRY_RESULT(stack, execute_smc_method(address_, code_cell_, data_cell_, config_, "dnsresolve", 
            {vm::StackEntry(zero_byte_slice), vm::StackEntry(categories)}));
  if (stack.size() != 2) {
    return td::Status::Error("dnsresolve returned unexpected stack size");
  }
  if (stack[0].type() != vm::StackEntry::Type::t_int) {
    return td::Status::Error("dnsresolve returned unexpected stack type at index 0");
  }

  auto resolved_bits_cnt = stack[0].as_int()->to_long();
  if (resolved_bits_cnt != 8) {
    return td::Status::Error("dnsresolve returned unexpected bits cnt resolved");
  }

  auto recordset_cell = stack[1].as_cell();
  if (recordset_cell.is_null()) {
    // recordset is null
    return Result::DNSEntry{};
  }

  vm::Dictionary records{recordset_cell, 256};
  Result::DNSEntry result;

  // TODO: support ProtoList
  auto site_cell = records.lookup_ref(td::sha256_bits256("site"));
  if (site_cell.not_null()) {
    tokens::gen::DNSRecord::Record_dns_adnl_address site_record;
    if (!tlb::unpack_cell(site_cell, site_record)) {
      LOG(ERROR) << "Failed to unpack DNSRecord site for " << address_;
    } else {
      result.site_adnl = site_record.adnl_addr;
    }
  }

  try {
    // TODO: support SmcCapList
    auto wallet_cell = records.lookup_ref(td::sha256_bits256("wallet"));
    if (wallet_cell.not_null()) {
      tokens::gen::DNSRecord::Record_dns_smc_address wallet_record;
      if (!tlb::unpack_cell(wallet_cell, wallet_record)) {
        LOG(ERROR) << "Failed to unpack DNSRecord wallet";
      } else {
        auto wallet = convert::to_std_address(wallet_record.smc_addr);
        if (wallet.is_error()) {
          LOG(ERROR) << "Failed to parse DNSRecord wallet address";
        } else {
          result.wallet = wallet.move_as_ok();
        }
      }
    }

    auto next_resolver_cell = records.lookup_ref(td::sha256_bits256("dns_next_resolver"));
    if (next_resolver_cell.not_null()) {
      tokens::gen::DNSRecord::Record_dns_next_resolver next_resolver_record;
      if (!tlb::unpack_cell(next_resolver_cell, next_resolver_record)) {
        LOG(ERROR) << "Failed to unpack DNSRecord next_resolver";
      } else {
        auto next_resolver = convert::to_std_address(next_resolver_record.resolver);
        if (next_resolver.is_error()) {
          LOG(ERROR) << "Failed to parse DNSRecord next_resolver address";
        } else {
          result.next_resolver = next_resolver.move_as_ok();
        }
      }
    }

    auto storage_bag_id_cell = records.lookup_ref(td::sha256_bits256("storage"));
    if (storage_bag_id_cell.not_null()) {
      tokens::gen::DNSRecord::Record_dns_storage_address dns_storage_record;
      if (!tlb::unpack_cell(storage_bag_id_cell, dns_storage_record)) {
        LOG(ERROR) << "Failed to unpack DNSRecord storage";
      } else {
        result.storage_bag_id = dns_storage_record.bag_id;
      }
    }
  } catch (vm::VmError& e) {
    return td::Status::Error("Failed to parse DNSRecord: " + std::string(e.get_msg()));
  }

  return result;
}

td::Result<std::string> NftItemDetectorR::get_ton_domain() {
  TRY_RESULT(stack, execute_smc_method<1>(address_, code_cell_, data_cell_, config_, "get_domain", {}, {vm::StackEntry::Type::t_slice}));
  auto cs = stack[0].as_slice();

  if (cs.not_null()) {
    auto size = cs->size();
    if (size % 8 == 0) {
      auto cnt = size / 8;
      unsigned char tmp[1024];
      cs.write().fetch_bytes(tmp, cnt);
      std::string s{tmp, tmp + cnt};
      
      return s + ".ton";
    }
  }
  return td::Status::Error("get_domain returned unexpected result");
}

td::Result<std::string> NftItemDetectorR::get_t_me_domain() {
  TRY_RESULT(stack, execute_smc_method<1>(address_, code_cell_, data_cell_, config_, "get_full_domain", {}, {vm::StackEntry::Type::t_slice}));
  auto cs = stack[0].as_slice();

  if (cs.not_null()) {
    auto size = cs->size();
    if (size % 8 == 0) {
      auto cnt = size / 8;
      unsigned char tmp[1024];
      cs.write().fetch_bytes(tmp, cnt);
      std::string s{tmp, tmp + cnt};
      
      // convert "me\0t\0username\0" to "username.t.me"
      std::string c;
      std::vector<std::string> parts;
      for (size_t i = 0; i < s.size(); i++) {
        if (s[i] == '\0') {
          parts.push_back(c);
          c = "";
        } else {
          c += s[i];
        }
      }
      std::string result;
      for (int16_t i = parts.size() - 1; i >= 0; i--) {
        result += parts[i];
        if (i != 0) {
          result += ".";
        }
      }
      return result;
    }
  }
  return td::Status::Error("get_full_domain returned unexpected result");
}

NftCollectionDetectorR::NftCollectionDetectorR(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)), 
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}
 
void NftCollectionDetectorR::start_up() {
  auto stack_r = execute_smc_method<3>(address_, code_cell_, data_cell_, config_, "get_collection_data", {},
    {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_cell, vm::StackEntry::Type::t_slice});
  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();
  Result data;
  data.address = address_;
  data.next_item_index = stack[0].as_int();
  auto owner_addr_cs = stack[2].as_slice();
  if (owner_addr_cs->size() == 2 && owner_addr_cs->prefetch_ulong(2) == 0) {
    // addr_none case
    data.owner_address = std::nullopt;
  } else {
    auto owner_address = convert::to_std_address(owner_addr_cs);
    if (owner_address.is_error()) {
      promise_.set_error(owner_address.move_as_error());
      stop();
      return;
    }
    data.owner_address = owner_address.move_as_ok();
  }
  auto collection_content = parse_token_data(stack[1].as_cell());
  if (collection_content.is_error()) {
    promise_.set_error(collection_content.move_as_error_prefix("get_collection_data collection_content parsing failed: "));
    stop();
    return;
  }
  data.collection_content = collection_content.move_as_ok();
  promise_.set_value(std::move(data));
  stop();
}

const std::unordered_set<std::string> DEDUST_POOL_CODE_HASHES {
  "1275095B6DA3911292406F4F4386F9E780099B854C6DEE9EE2895DDCE70927C1",
  "778F0D3FE6482C50888970DF5E787F40F3A4AB282170C035A5920877058C99D3",
  "C0F9D14FBC8E14F0D72CBA2214165EEE35836AB174130912BAF9DBFA43EAD562"
};

const auto DEDUST_FACTORY_ADDRESS = block::StdAddress::parse("EQBfBWT7X2BHg9tXAxzhz2aKiNTU1tpt5NsiK0uSDW_YAJ67").move_as_ok();

DedustPoolDetector::DedustPoolDetector(block::StdAddress address,
                                       td::Ref<vm::Cell> code_cell,
                                       td::Ref<vm::Cell> data_cell,
                                       AllShardStates shard_states,
                                       std::shared_ptr<block::ConfigInfo> config,
                                       td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

void DedustPoolDetector::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  if (!DEDUST_POOL_CODE_HASHES.contains(code_cell_->get_hash().to_hex())) {
    promise_.set_error(td::Status::Error("Code hash mismatch"));
    stop();
    return;
  }

  Result data;

  {
    auto stack_r = execute_smc_method<2>(address_, code_cell_, data_cell_, config_, "get_assets", {}, {
                                           vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice
                                         });
    if (stack_r.is_error()) {
      promise_.set_error(stack_r.move_as_error());
      stop();
      return;
    }

    auto stack = stack_r.move_as_ok();

    data.asset_1_slice = stack[0].as_slice();
    data.asset_2_slice = stack[1].as_slice();

    if (!get_asset(stack[0].as_slice(), data.asset_1) || !get_asset(stack[1].as_slice(), data.asset_2)) {
      promise_.set_error(td::Status::Error("Unsupported sum type"));
      stop();
      return;
    }
  }

  {
    auto stack_r = execute_smc_method<1>(address_, code_cell_, data_cell_, config_, "is_stable", {},
      { vm::StackEntry::Type::t_int });
    if (stack_r.is_error()) {
      promise_.set_error(stack_r.move_as_error());
      stop();
      return;
    }
    auto stack = stack_r.move_as_ok();
    data.is_stable = stack[0].as_int()->to_long();
  }

  {
    auto stack_r = execute_smc_method<2>(address_, code_cell_, data_cell_, config_,
      "get_reserves", {}, { vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int });
    if (stack_r.is_error()) {
      promise_.set_error(stack_r.move_as_error());
      stop();
      return;
    }
    auto stack = stack_r.move_as_ok();
    data.reserve_1 = stack[0].as_int();
    data.reserve_2 = stack[1].as_int();
  }

  {
    auto stack_r = execute_smc_method<2>(address_, code_cell_, data_cell_, config_,
      "get_trade_fee", {}, { vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int });
    if (stack_r.is_error()) {
      promise_.set_error(stack_r.move_as_error());
      stop();
      return;
    }
    auto stack = stack_r.move_as_ok();
    auto numerator = stack[0].as_int();
    auto denominator = stack[1].as_int();

    if (numerator->sgn() == 0 || denominator->sgn() == 0) {
      data.fee = 0.0;
    } else {
      data.fee = numerator->to_long() / static_cast<double>(denominator->to_long());
    }
  }

  // Fetch factory account and validate pool
  auto R = td::PromiseCreator::lambda([=, this, SelfId = actor_id(this)](td::Result<schema::AccountState> account_state_r) mutable {
    if (account_state_r.is_error()) {
      promise_.set_error(account_state_r.move_as_error());
      stop();
      return;
    }
    auto account_state = account_state_r.move_as_ok();
    td::actor::send_closure(SelfId, &DedustPoolDetector::verify_with_factory, account_state.code, account_state.data, data);
  });

  td::actor::create_actor<FetchAccountFromShardV2>("fetchaccountfromshard", shard_states_, DEDUST_FACTORY_ADDRESS, std::move(R)).release();
}

void DedustPoolDetector::verify_with_factory(td::Ref<vm::Cell> factory_code, td::Ref<vm::Cell> factory_data, Result pool_data) {
  auto pool_type = td::make_refint(pool_data.is_stable);
  auto stack_r = execute_smc_method<1>(
    DEDUST_FACTORY_ADDRESS,
    factory_code,
    factory_data,
    config_,
    "get_pool_address",
    {vm::StackEntry(pool_type), vm::StackEntry(pool_data.asset_1_slice), vm::StackEntry(pool_data.asset_2_slice)},
    { vm::StackEntry::Type::t_slice }
  );

  if (stack_r.is_error()) {
    auto as_error = stack_r.move_as_error();
    LOG(INFO) << as_error.to_string();
    promise_.set_error(std::move(as_error));
    stop();
    return;
  }

  auto stack = stack_r.move_as_ok();
  auto factory_pool_address_slice = stack[0].as_slice();

  block::StdAddress valid_pool_addr;
  block::tlb::MsgAddressInt address_int{};
  auto ok = address_int.extract_std_address(factory_pool_address_slice, valid_pool_addr, true);
  if (!ok) {
    promise_.set_error(td::Status::Error("Failed to unpack pool address from factory"));
    stop();
    return;
  }

  if (valid_pool_addr != address_) {
    promise_.set_error(td::Status::Error("Pool address mismatch with factory"));
    stop();
    return;
  }

  promise_.set_value(std::move(pool_data));
  stop();
}

bool DedustPoolDetector::get_asset(td::Ref<vm::CellSlice> slice, std::optional<block::StdAddress> &address) {
  auto cell_slice = slice->clone();
  int sum_type;
  cell_slice.fetch_int_to(4, sum_type);
  if (sum_type == 0) { // TON asset
    address = std::nullopt;
    return true;
  }
  if (sum_type == 1) {
    const auto remaining_bits = cell_slice.data_bits();
    const auto workchain_id = (ton::WorkchainId)remaining_bits.get_int(8);
    const td::ConstBitPtr addr_ptr = remaining_bits + 8;
    const ton::StdSmcAddress addr_bytes{addr_ptr};

    address = block::StdAddress(workchain_id, addr_bytes);
    return true;
  }
  return false;
}

// stonfi v2 pool code hashes for different pool types.
// they're immutable, so we can hardcode them.
// all been obtained by compiling ston-fi/dex-core-v2 contracts and wrapping them into libs
const std::unordered_map<std::string, std::string> STONFI_POOL_V2_CODE_HASHES {
  {"f04a14c3231221056c3499965e4604417e324f8e9121d840120d803288715594", "weighted_const_product"},
  {"063e559f6f6be7ca44b2e6bcb448650d06f73d5cb83cdcc3b3695543474aedcf", "weighted_stableswap"},
  {"ec614ea4aaea3f7768606f1c1632b3374d3de096a1e7c4ba43c8009c487fee9d", "constant_product"},
  {"2963cf56242fd0cd4da02fce037c10ec86056083a7264047fedac1c8fc534f7e", "constant_sum"},
  {"fbc7e8fcca72c2b9c078b359ffa936f46384491b895b6577b0a6cb3f569040bc", "stableswap"}
};

StonfiPoolV2Detector::StonfiPoolV2Detector(block::StdAddress address,
                                           td::Ref<vm::Cell> code_cell,
                                           td::Ref<vm::Cell> data_cell,
                                           AllShardStates shard_states,
                                           std::shared_ptr<block::ConfigInfo> config,
                                           td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

void StonfiPoolV2Detector::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  auto code_hash = code_cell_->get_hash().to_hex();
  auto pool_type_iter = STONFI_POOL_V2_CODE_HASHES.find(code_hash);
  if (pool_type_iter == STONFI_POOL_V2_CODE_HASHES.end()) {
    promise_.set_error(td::Status::Error("Code hash mismatch"));
    stop();
    return;
  }

  Result data;
  data.pool_type = pool_type_iter->second;  // get pool type by code hash

  // get pool data: reserves, token wallets, fees
  {
    auto stack_r = execute_smc_method<12>(address_, code_cell_, data_cell_, config_, "get_pool_data", {},
      { vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int,
        vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice,
        vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int,
        vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int });
    if (stack_r.is_error()) {
      promise_.set_error(stack_r.move_as_error());
      stop();
      return;
    }

    auto stack = stack_r.move_as_ok();
    // stack[0] = is_locked
    // stack[1] = router_address
    // stack[2] = total_supply
    data.reserve_1 = stack[3].as_int();
    data.reserve_2 = stack[4].as_int();
    auto token0_wallet_slice = stack[5].as_slice();
    auto token1_wallet_slice = stack[6].as_slice();
    auto lp_fee = stack[7].as_int(); 
    // stack[8] = protocol_fee
    // stack[9] = protocol_fee_address
    // stack[10] = collected_token0_protocol_fee
    // stack[11] = collected_token1_protocol_fee

    data.fee = lp_fee->to_long() / 10000.0;

    // parse token wallet addresses
    auto token0_wallet_r = convert::to_std_address(token0_wallet_slice);
    if (token0_wallet_r.is_error()) {
      promise_.set_error(token0_wallet_r.move_as_error());
      stop();
      return;
    }
    auto token1_wallet_r = convert::to_std_address(token1_wallet_slice);
    if (token1_wallet_r.is_error()) {
      promise_.set_error(token1_wallet_r.move_as_error());
      stop();
      return;
    }

    auto token0_wallet = token0_wallet_r.move_as_ok();
    auto token1_wallet = token1_wallet_r.move_as_ok();

    // resolve assets from token wallets
    resolve_assets(std::move(data), token0_wallet, token1_wallet);
  }
}

void StonfiPoolV2Detector::resolve_assets(Result pool_data, block::StdAddress token0_wallet, block::StdAddress token1_wallet) {
  auto R = td::PromiseCreator::lambda([=, this, SelfId = actor_id(this)](td::Result<schema::AccountState> account_state_r) mutable {
    if (account_state_r.is_error()) {
      promise_.set_error(account_state_r.move_as_error());
      stop();
      return;
    }
    auto account_state = account_state_r.move_as_ok();
    td::actor::send_closure(SelfId, &StonfiPoolV2Detector::got_token0_wallet_account, 
                           pool_data, token1_wallet, account_state.code, account_state.data);
  });

  td::actor::create_actor<FetchAccountFromShardV2>("fetchaccountfromshard", shard_states_, token0_wallet, std::move(R)).release();
}

void StonfiPoolV2Detector::got_token0_wallet_account(Result pool_data, block::StdAddress token1_wallet,
                                                     td::Ref<vm::Cell> token0_wallet_code, td::Ref<vm::Cell> token0_wallet_data) {
  try {
    pool_data.asset_1 = get_jetton_master(token0_wallet_code, token0_wallet_data, config_);
  } catch (const std::exception& e) {
    promise_.set_error(td::Status::Error(std::string("Failed to resolve asset_1: ") + e.what()));
    stop();
    return;
  }

  auto R = td::PromiseCreator::lambda([=, this, SelfId = actor_id(this)](td::Result<schema::AccountState> account_state_r) mutable {
    if (account_state_r.is_error()) {
      promise_.set_error(account_state_r.move_as_error());
      stop();
      return;
    }
    auto account_state = account_state_r.move_as_ok();
    td::actor::send_closure(SelfId, &StonfiPoolV2Detector::got_token1_wallet_account,
                           pool_data, account_state.code, account_state.data);
  });

  td::actor::create_actor<FetchAccountFromShardV2>("fetchaccountfromshard", shard_states_, token1_wallet, std::move(R)).release();
}

void StonfiPoolV2Detector::got_token1_wallet_account(Result pool_data, td::Ref<vm::Cell> token1_wallet_code,
                                                     td::Ref<vm::Cell> token1_wallet_data) {
  try {
    pool_data.asset_2 = get_jetton_master(token1_wallet_code, token1_wallet_data, config_);
  } catch (const std::exception& e) {
    promise_.set_error(td::Status::Error(std::string("Failed to resolve asset_2: ") + e.what()));
    stop();
    return;
  }
  promise_.set_value(std::move(pool_data));
  stop();
}

std::optional<block::StdAddress> StonfiPoolV2Detector::get_jetton_master(td::Ref<vm::Cell> wallet_code,
                                                                         td::Ref<vm::Cell> wallet_data,
                                                                         std::shared_ptr<block::ConfigInfo> config) {  
  if (wallet_code.is_null() || wallet_data.is_null()) {
    throw std::runtime_error("Wallet code or data is null");
  }

  auto stack_r = execute_smc_method<4>(block::StdAddress(), wallet_code, wallet_data, config, "get_wallet_data", {},
    {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell});
  
  if (stack_r.is_error()) {
    throw std::runtime_error("Failed to call get_wallet_data: " + stack_r.error().to_string());
  }

  auto stack = stack_r.move_as_ok();
  // stack[0] = balance
  // stack[1] = owner
  auto jetton_master_slice = stack[2].as_slice();
  // stack[3] = jetton_wallet_code

  auto jetton_master_r = convert::to_std_address(jetton_master_slice);
  if (jetton_master_r.is_error()) {
    throw std::runtime_error("Failed to parse jetton master address");
  }

  return jetton_master_r.move_as_ok();
}

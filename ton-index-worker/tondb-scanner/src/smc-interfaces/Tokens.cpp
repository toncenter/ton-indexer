#include "td/actor/actor.h"
#include "vm/cells/Cell.h"
#include "IndexData.h"
#include "convert-utils.h"
#include "DataParser.h"
#include "Tokens.h"
#include "parse_token_data.h"
#include "smc-interfaces/execute-smc.h"
#include "tokens-tlb.h"
#include "common/checksum.h"


class FetchAccountFromShardV2: public td::actor::Actor {
private:
  AllShardStates shard_states_;
  block::StdAddress address_;
  td::Promise<schema::AccountState> promise_;
public:
  FetchAccountFromShardV2(AllShardStates shard_states, block::StdAddress address, td::Promise<schema::AccountState> promise) 
    : shard_states_(shard_states), address_(address), promise_(std::move(promise)) {
  }

  void start_up() override {
    for (auto& root : shard_states_) {
      block::gen::ShardStateUnsplit::Record sstate;
      if (!tlb::unpack_cell(root, sstate)) {
        promise_.set_error(td::Status::Error("Failed to unpack ShardStateUnsplit"));
        stop();
        return;
      }
      if (!ton::shard_contains(ton::ShardIdFull(block::ShardId(sstate.shard_id)), ton::extract_addr_prefix(address_.workchain, address_.addr))) {
        continue;
      }

      vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts};
      
      auto shard_account_csr = accounts_dict.lookup(address_.addr);
      if (shard_account_csr.is_null()) {
        promise_.set_error(td::Status::Error("Account not found in accounts_dict"));
        stop();
        return;
      } 
      
      block::gen::ShardAccount::Record acc_info;
      if(!tlb::csr_unpack(std::move(shard_account_csr), acc_info)) {
        LOG(ERROR) << "Failed to unpack ShardAccount " << address_.addr;
        stop();
        return;
      }
      int account_tag = block::gen::t_Account.get_tag(vm::load_cell_slice(acc_info.account));
      switch (account_tag) {
      case block::gen::Account::account_none:
        promise_.set_error(td::Status::Error("Account is empty"));
        stop();
        return;
      case block::gen::Account::account: {
        auto account_r = ParseQuery::parse_account(acc_info.account, sstate.gen_utime, acc_info.last_trans_hash, acc_info.last_trans_lt);
        if (account_r.is_error()) {
          promise_.set_error(account_r.move_as_error());
          stop();
          return;
        }
        promise_.set_value(account_r.move_as_ok());
        stop();
        return;
      }
      default:
        promise_.set_error(td::Status::Error("Unknown account tag"));
        stop();
        return;
      }
    }
    promise_.set_error(td::Status::Error("Account not found in shards"));
    stop();
  }
};



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
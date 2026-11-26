#include "td/actor/actor.h"
#include "vm/cells/Cell.h"
#include <unordered_set>
#include <string>
#include "IndexData.h"
#include "convert-utils.h"
#include "DexConstants.h"
#include "DexPools.h"
#include "DataParser.h"
#include "Tokens.h"

#include "FetchAccountFromShard.h"
#include "parse_token_data.h"
#include "safe-parse.h"
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

namespace {

// The jetton master round-trip: ask the master which wallet it would derive for
// this owner and require it to be the account under detection.
td::Status verify_jetton_wallet_with_master(td::Ref<vm::Cell> master_code, td::Ref<vm::Cell> master_data,
                                            const std::shared_ptr<block::ConfigInfo>& config,
                                            const JettonWalletDetectorR::Result& jetton_wallet_data) {
  vm::CellBuilder anycast_cb;
  anycast_cb.store_bool_bool(false);
  auto anycast_cell = anycast_cb.finalize();
  td::Ref<vm::CellSlice> anycast_cs = vm::load_cell_slice_ref(anycast_cell);

  vm::CellBuilder cb;
  block::gen::t_MsgAddressInt.pack_addr_std(cb, anycast_cs, jetton_wallet_data.owner.workchain, jetton_wallet_data.owner.addr);
  auto owner_address_cell = cb.finalize();

  TRY_RESULT(stack, execute_smc_method<1>(jetton_wallet_data.jetton, master_code, master_data, config, "get_wallet_address",
    {vm::StackEntry(vm::load_cell_slice_ref(owner_address_cell))}, {vm::StackEntry::Type::t_slice}));

  TRY_RESULT(wallet_address, convert::to_std_address(stack[0].as_slice()));
  if (!(jetton_wallet_data.address == wallet_address)) {
    return td::Status::Error("Jetton Master returned wrong address");
  }
  return td::Status::OK();
}

}  // namespace

td::Result<JettonWalletDetectorR::Result> JettonWalletDetectorR::detect(
    const block::StdAddress& address, const td::Ref<vm::Cell>& code_cell, const td::Ref<vm::Cell>& data_cell,
    const AllShardStates& shard_states, const std::shared_ptr<block::ConfigInfo>& config) {
  if (code_cell.is_null() || data_cell.is_null()) {
    return td::Status::Error("Code or data null");
  }

  TRY_RESULT(stack, execute_smc_method<4>(address, code_cell, data_cell, config, "get_wallet_data", {},
    {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell}));
  Result data;
  data.address = address;
  data.balance = stack[0].as_int();
  TRY_RESULT_ASSIGN(data.owner, convert::to_std_address(stack[1].as_slice()));
  TRY_RESULT_ASSIGN(data.jetton, convert::to_std_address(stack[2].as_slice()));

  auto is_claimed_stack_r = execute_smc_method<1>(address, code_cell, data_cell, config, "is_claimed", {},
    {vm::StackEntry::Type::t_int});
  if (is_claimed_stack_r.is_ok()) {
    auto is_claimed_stack = is_claimed_stack_r.move_as_ok();
    data.mintless_is_claimed = is_claimed_stack[0].as_int()->to_long() != 0;
  } else {
    data.mintless_is_claimed = std::nullopt;
  }

  TRY_RESULT(master, lookup_account(shard_states, data.jetton));
  TRY_STATUS(verify_jetton_wallet_with_master(master.code, master.data, config, data));
  return data;
}

void JettonWalletDetectorR::start_up() {
  promise_.set_result(detect(address_, code_cell_, data_cell_, shard_states_, config_));
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

td::Result<NftItemDetectorR::Result> NftItemDetectorR::detect(
    const block::StdAddress& address, const td::Ref<vm::Cell>& code_cell,
    const td::Ref<vm::Cell>& data_cell, const AllShardStates& shard_states,
    const std::shared_ptr<block::ConfigInfo>& config) {
  if (code_cell.is_null() || data_cell.is_null()) {
    return td::Status::Error("Code or data null");
  }

  TRY_RESULT(stack, execute_smc_method<5>(address, code_cell, data_cell, config, "get_nft_data", {},
        {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell}));

  Result data;
  data.address = address;
  data.init = stack[0].as_int()->to_long() != 0;
  data.index = stack[1].as_int();

  auto collection_addr_cs = stack[2].as_slice();
  if (collection_addr_cs->size() == 2 && collection_addr_cs->prefetch_ulong(2) == 0) {
    data.collection_address = std::nullopt;
  } else {
    auto collection_address = convert::to_std_address(stack[2].as_slice());
    if (collection_address.is_error()) {
      return collection_address.move_as_error_prefix("nft collection address parsing failed: ");
    }
    data.collection_address = collection_address.move_as_ok();
  }

  auto owner_addr_cs = stack[3].as_slice();
  if (owner_addr_cs->size() == 2 && owner_addr_cs->prefetch_ulong(2) == 0) {
    data.owner_address = std::nullopt;
  } else {
    auto owner_address = convert::to_std_address(stack[3].as_slice());
    if (owner_address.is_error()) {
      return owner_address.move_as_error_prefix("nft owner address parsing failed: ");
    }
    data.owner_address = owner_address.move_as_ok();
  }

  if (!data.collection_address) {
    auto content = parse_token_data(stack[4].as_cell());
    if (content.is_error()) {
      return content.move_as_error_prefix("nft content parsing failed: ");
    }
    data.content = content.move_as_ok();
    return data;
  }

  auto ind_content = stack[4].as_cell();
  TRY_RESULT(collection, lookup_account(shard_states, *data.collection_address));

  TRY_RESULT(address_stack, execute_smc_method<1>(*data.collection_address, collection.code,
      collection.data, config, "get_nft_address_by_index", {vm::StackEntry(data.index)},
      {vm::StackEntry::Type::t_slice}));
  auto nft_address = convert::to_std_address(address_stack[0].as_slice());
  if (nft_address.is_error()) {
    return td::Status::Error("get_nft_address_by_index parse address failed");
  }
  if (!(nft_address.move_as_ok() == address)) {
    return td::Status::Error("NFT Item doesn't belong to the referred collection");
  }

  auto content_stack_r = execute_smc_method<1>(*data.collection_address, collection.code,
      collection.data, config, "get_nft_content",
      {vm::StackEntry(data.index), vm::StackEntry(ind_content)},
      {vm::StackEntry::Type::t_cell});
  if (content_stack_r.is_error()) {
    return content_stack_r.move_as_error_prefix("failed to get nft item content: ");
  }
  auto content_stack = content_stack_r.move_as_ok();
  auto content = parse_token_data(content_stack[0].as_cell());
  if (content.is_error()) {
    return content.move_as_error_prefix("failed to get nft item content: ");
  }
  data.content = content.move_as_ok();
  return data;
}

void NftItemDetectorR::start_up() {
  auto result = detect(address_, code_cell_, data_cell_, shard_states_, config_);
  if (result.is_error()) {
    promise_.set_error(result.move_as_error());
    stop();
    return;
  }
  auto data = result.move_as_ok();
  process_domain_and_dns_data(get_dot_ton_dns_root_addr(), [this](){ return this->get_ton_domain(); }, data);
  auto t_me_root = dot_t_dot_me_dns_root_addr();
  if (t_me_root) {
    process_domain_and_dns_data(t_me_root.value(), [this](){ return this->get_t_me_domain(); }, data);
  }
  promise_.set_value(std::move(data));
  stop();
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
        if (auto status = get_auction_info(dns_data.ok_ref()); status.is_error()) {
          LOG(DEBUG) << "Failed to get dns auction info for " << address_ << ": " << status;
        }
        if (auto status = get_last_fill_up_time(dns_data.ok_ref()); status.is_error()) {
          LOG(DEBUG) << "Failed to get dns last fill up time for " << address_ << ": " << status;
        }
        item_data.dns_entry = dns_data.move_as_ok();
    }
  }
}

td::Status NftItemDetectorR::get_auction_info(Result::DNSEntry& entry) {
  TRY_RESULT(stack, execute_smc_method<3>(address_, code_cell_, data_cell_, config_, "get_auction_info", {},
        {vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int}));

  auto max_bid_address_cs = stack[0].as_slice();
  if (max_bid_address_cs->size() == 2 && max_bid_address_cs->prefetch_ulong(2) == 0) {
    // No auction is active.
    return td::Status::OK();
  }

  auto max_bid_address = convert::to_std_address(max_bid_address_cs);
  if (max_bid_address.is_error()) {
    return max_bid_address.move_as_error_prefix("dns auction max_bid_address parsing failed: ");
  }

  auto auction_end_time = stack[2].as_int();
  if (auction_end_time->sgn() < 0 || auction_end_time->to_long() >= (static_cast<int64_t>(1) << 40)) {
    return td::Status::Error("get_auction_info returned out-of-range auction_end_time");
  }

  entry.max_bid_address = max_bid_address.move_as_ok();
  entry.max_bid_amount = stack[1].as_int();
  entry.auction_end_time = static_cast<uint64_t>(auction_end_time->to_long());
  return td::Status::OK();
}

td::Status NftItemDetectorR::get_last_fill_up_time(Result::DNSEntry& entry) {
  TRY_RESULT(stack, execute_smc_method<1>(address_, code_cell_, data_cell_, config_, "get_last_fill_up_time", {},
        {vm::StackEntry::Type::t_int}));

  auto value = stack[0].as_int();
  if (value->sgn() < 0 || value->to_long() >= (static_cast<int64_t>(1) << 40)) {
    return td::Status::Error("get_last_fill_up_time returned out-of-range value");
  }
  entry.last_fill_up_time = static_cast<uint64_t>(value->to_long());
  return td::Status::OK();
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

  return safe_parse::guard("Failed to parse DNSRecord", [&]() -> td::Result<Result::DNSEntry> {
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
  });
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

DedustPoolDetector::DedustPoolDetector(block::StdAddress address,
                                       td::Ref<vm::Cell> code_cell,
                                       td::Ref<vm::Cell> data_cell,
                                       AllShardStates shard_states,
                                       std::shared_ptr<block::ConfigInfo> config,
                                       td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

namespace {

bool dedust_get_asset(td::Ref<vm::CellSlice> slice, std::optional<block::StdAddress> &address) {
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

bool dedust_get_asset_v2(td::Ref<vm::CellSlice> slice, std::optional<block::StdAddress> &address) {
  // V2 asset is a standard MsgAddress; peek tag since extract_std_address fails on addr_none
  auto peek = slice->clone();
  unsigned tag = 0;
  if (!peek.fetch_uint_to(2, tag)) {
    return false;
  }
  if (tag == 0) {  // addr_none = native TON
    address = std::nullopt;
    return true;
  }
  block::StdAddress std_addr;
  block::tlb::MsgAddressInt addr_int{};
  if (addr_int.extract_std_address(slice, std_addr, true)) {
    address = std_addr;
    return true;
  }
  return false;
}

// Factory derivation rejects pools that only copy official code.
td::Status verify_dedust_with_factory(const block::StdAddress &factory_address,
                                      td::Ref<vm::Cell> factory_code, td::Ref<vm::Cell> factory_data,
                                      const std::shared_ptr<block::ConfigInfo> &config,
                                      const block::StdAddress &address,
                                      const DedustPoolDetector::Result &pool_data) {
  auto pool_type = td::make_refint(pool_data.is_stable);
  auto stack_r = execute_smc_method<1>(
    factory_address,
    factory_code,
    factory_data,
    config,
    "get_pool_address",
    {vm::StackEntry(pool_type), vm::StackEntry(pool_data.asset_1_slice), vm::StackEntry(pool_data.asset_2_slice)},
    { vm::StackEntry::Type::t_slice }
  );

  if (stack_r.is_error()) {
    auto as_error = stack_r.move_as_error();
    LOG(INFO) << as_error.to_string();
    return as_error;
  }

  auto stack = stack_r.move_as_ok();
  auto factory_pool_address_slice = stack[0].as_slice();

  block::StdAddress valid_pool_addr;
  block::tlb::MsgAddressInt address_int{};
  auto ok = address_int.extract_std_address(factory_pool_address_slice, valid_pool_addr, true);
  if (!ok) {
    return td::Status::Error("Failed to unpack pool address from factory");
  }

  if (valid_pool_addr != address) {
    return td::Status::Error("Pool address mismatch with factory");
  }
  return td::Status::OK();
}

// Check deploy-time code so upgraded V2 pools still match their original address.
td::Status verify_dedust_v2_pool(const block::StdAddress &address,
                                 const td::Ref<vm::Cell> &data_cell) {
  if (address.workchain != ton::basechainId) {
    return td::Status::Error("V2 pool is not in the basechain");
  }
  try {
    auto data = vm::load_cell_slice(data_cell);
    if (data.size_refs() < 3) {
      return td::Status::Error("Unexpected V2 pool data layout");
    }
    auto admin_cs = vm::load_cell_slice(data.prefetch_ref(2));
    ton::WorkchainId admin_workchain;
    ton::StdSmcAddress admin_addr;
    if (!block::tlb::t_MsgAddressInt.extract_std_address(admin_cs, admin_workchain, admin_addr) ||
        block::StdAddress(admin_workchain, admin_addr) != dex_constants::dedust_v2_admin) {
      return td::Status::Error("V2 pool admin is not DeDust");
    }

    vm::CellBuilder fees;
    fees.store_zeroes(26);  // Zero fee accumulators occupy 26 bits.
    vm::CellBuilder admin;
    block::tlb::t_MsgAddressInt.store_std_address(admin, admin_workchain, admin_addr);
    admin.store_zeroes(3);  // Three empty Maybe dictionaries.
    vm::CellBuilder init_data;
    init_data.store_zeroes(17);  // Empty Maybe dict, uninit status, two zero int1 and three zero Coins.
    init_data.store_ref(data.prefetch_ref(0));
    init_data.store_ref(fees.finalize());
    init_data.store_ref(admin.finalize());
    auto init_data_cell = init_data.finalize();
    for (const auto &code : dex_constants::dedust_v2_pool_library_cells) {
      vm::CellBuilder state_init;
      state_init.store_long(0b00110, 5);  // no split_depth, no special, code, data, no library
      state_init.store_ref(code);
      state_init.store_ref(init_data_cell);
      if (state_init.finalize()->get_hash().bits().compare(address.addr.bits(), 256) == 0) {
        return td::Status::OK();
      }
    }
    return td::Status::Error("V2 pool address does not match its initial state");
  } catch (vm::VmError &e) {
    return td::Status::Error("Failed to parse V2 pool data: " + std::string(e.get_msg()));
  }
}

td::Status read_dedust_v2(const block::StdAddress &address, const td::Ref<vm::Cell> &code_cell,
                          const td::Ref<vm::Cell> &data_cell, const std::shared_ptr<block::ConfigInfo> &config,
                          DedustPoolDetector::Result &data) {
  // get_pool_data has 19 entries; we read 11, with nullable wallets at indices 5 and 6.
  TRY_RESULT(stack, execute_smc_method_nullable<11>(address, code_cell, data_cell, config, "get_pool_data", {}, {
                                          vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int,
                                          vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice,
                                          nullable(vm::StackEntry::Type::t_cell), nullable(vm::StackEntry::Type::t_cell),
                                          nullable(vm::StackEntry::Type::t_cell),
                                          vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int
                                        }));

  data.asset_1_slice = stack[3].as_slice();
  data.asset_2_slice = stack[4].as_slice();
  if (!dedust_get_asset_v2(stack[3].as_slice(), data.asset_1) || !dedust_get_asset_v2(stack[4].as_slice(), data.asset_2)) {
    return td::Status::Error("Unsupported V2 asset encoding");
  }

  data.is_stable = false;
  // Status 0 = not initialised, 1 = setup (awaiting jetton wallets), 2 = ready. A ready pool swaps only while
  // swapActive (index 2) is set: activation rules can open swaps after deposits.
  data.is_locked = td::cmp(stack[0].as_int(), td::make_refint(2)) != 0 || stack[2].as_int()->sgn() == 0;
  auto base_fee_bps = stack[8].as_int()->to_long();
  data.fee = base_fee_bps / static_cast<double>(kDexFeeDivider);
  data.fee_numerator = stack[8].as_int();
  data.fee_denominator = td::make_refint(kDexFeeDivider);
  data.reserve_1 = stack[9].as_int();
  data.reserve_2 = stack[10].as_int();
  return td::Status::OK();
}

}  // namespace

td::Result<DedustPoolDetector::Result> DedustPoolDetector::detect(
    const block::StdAddress &address, const td::Ref<vm::Cell> &code_cell, const td::Ref<vm::Cell> &data_cell,
    const AllShardStates &shard_states, const std::shared_ptr<block::ConfigInfo> &config) {
  Result data;
  TRY_STATUS(read_pool(address, code_cell, data_cell, config, data));
  const td::Bits256 code_hash = code_cell->get_hash().bits();
  auto& cache = dex_pool_cache();
  auto known = cache.get(address);
  if (!known || known->identity.code_hash != code_hash) {
    TRY_STATUS(verify_identity(address, data_cell, shard_states, config, data));
    cache.put(data, code_hash);
  }
  return data;
}

td::Status DedustPoolDetector::read_pool(const block::StdAddress &address, const td::Ref<vm::Cell> &code_cell,
                                         const td::Ref<vm::Cell> &data_cell,
                                         const std::shared_ptr<block::ConfigInfo> &config, Result &data) {
  if (code_cell.is_null() || data_cell.is_null()) {
    return td::Status::Error("Code or data null");
  }

  const auto code_hash = code_cell->get_hash().to_hex();
  const bool is_v1 = dex_constants::dedust_v1_pool_code_hashes.contains(code_hash);
  const bool is_v2 = dex_constants::dedust_v2_pool_code_hashes.contains(code_hash);
  if (!is_v1 && !is_v2) {
    return td::Status::Error("Code hash mismatch");
  }

  data.address = address;
  data.version = is_v2 ? 2 : 1;
  if (is_v2) {
    return read_dedust_v2(address, code_cell, data_cell, config, data);
  }

  {
    TRY_RESULT(stack, execute_smc_method<2>(address, code_cell, data_cell, config, "get_assets", {}, {
                                           vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice
                                         }));

    data.asset_1_slice = stack[0].as_slice();
    data.asset_2_slice = stack[1].as_slice();

    if (!dedust_get_asset(stack[0].as_slice(), data.asset_1) || !dedust_get_asset(stack[1].as_slice(), data.asset_2)) {
      return td::Status::Error("Unsupported sum type");
    }
  }

  {
    TRY_RESULT(stack, execute_smc_method<1>(address, code_cell, data_cell, config, "is_stable", {},
      { vm::StackEntry::Type::t_int }));
    data.is_stable = stack[0].as_int()->to_long();
  }

  {
    TRY_RESULT(stack, execute_smc_method<2>(address, code_cell, data_cell, config,
      "get_reserves", {}, { vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int }));
    data.reserve_1 = stack[0].as_int();
    data.reserve_2 = stack[1].as_int();
  }

  if (data.is_stable) {
    read_stable_rates(data_cell, data);
  }

  {
    TRY_RESULT(stack, execute_smc_method<2>(address, code_cell, data_cell, config,
      "get_trade_fee", {}, { vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int }));
    auto numerator = stack[0].as_int();
    auto denominator = stack[1].as_int();
    data.fee_numerator = numerator;
    data.fee_denominator = denominator;

    if (numerator->sgn() == 0 || denominator->sgn() == 0) {
      data.fee = 0.0;
    } else {
      data.fee = numerator->to_long() / static_cast<double>(denominator->to_long());
    }
  }

  return td::Status::OK();
}

// DeDust v1 stable pools have no rate getter, so rates come from storage (4 refs, version, fee, 5 Coins: LP supply,
// reserves, 2 protocol fees). v4 stores decimals in ref 3 (rate = 10^(36 - decimals)); v5+ stores two u128 rates.
// Matching get_reserves guards against unknown layouts, leaving rates null if the check fails.
void DedustPoolDetector::read_stable_rates(const td::Ref<vm::Cell> &data_cell, Result &data) {
  try {
    auto cs = vm::load_cell_slice(data_cell);
    unsigned long long version = 0;
    td::RefInt256 coins[5];
    bool ok = cs.size_refs() == 4 && cs.fetch_ulong_bool(16, version) && cs.advance(16);
    for (auto &c : coins) {
      ok = ok && (c = block::tlb::t_Grams.as_integer_skip(cs)).not_null();
    }
    if (!ok || td::cmp(coins[1], data.reserve_1) != 0 || td::cmp(coins[2], data.reserve_2) != 0) {
      return;
    }
    td::RefInt256 rate_1, rate_2;
    if (version == 4 && cs.size() == 0) {
      auto settings = vm::load_cell_slice(cs.prefetch_ref(3));
      unsigned long long decimals_1 = 0, decimals_2 = 0;
      if (!settings.fetch_ulong_bool(8, decimals_1) || !settings.fetch_ulong_bool(8, decimals_2) ||
          decimals_1 > 36 || decimals_2 > 36) {
        return;
      }
      rate_1 = td::make_refint(1);
      rate_2 = td::make_refint(1);
      for (unsigned long long i = decimals_1; i < 36; i++) rate_1 = rate_1 * 10;
      for (unsigned long long i = decimals_2; i < 36; i++) rate_2 = rate_2 * 10;
    } else if (!cs.fetch_int256_to(128, rate_1, false) || !cs.fetch_int256_to(128, rate_2, false)) {
      return;
    }
    data.rate_1 = rate_1;
    data.rate_2 = rate_2;
  } catch (vm::VmError &) {
  }
}

td::Status DedustPoolDetector::verify_identity(const block::StdAddress &address, const td::Ref<vm::Cell> &data_cell,
                                               const AllShardStates &shard_states,
                                               const std::shared_ptr<block::ConfigInfo> &config, const Result &data) {
  if (data.version == 2) {
    return verify_dedust_v2_pool(address, data_cell);
  }
  TRY_RESULT(factory, lookup_account(shard_states, dex_constants::dedust_v1_factory));
  return verify_dedust_with_factory(dex_constants::dedust_v1_factory, factory.code, factory.data, config, address, data);
}

void DedustPoolDetector::start_up() {
  promise_.set_result(detect(address_, code_cell_, data_cell_, shard_states_, config_));
  stop();
}

#pragma once
#include <block/block.h>
#include <td/actor/actor.h>
#include <mc-config.h>

using AllShardStates = std::vector<td::Ref<vm::Cell>>;

class JettonWalletDetectorR: public td::actor::Actor {
public:
  struct Result {
    td::RefInt256 balance;
    block::StdAddress address;
    block::StdAddress owner;
    block::StdAddress jetton;
    std::optional<bool> mintless_is_claimed;
  };

  JettonWalletDetectorR(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise);

  void start_up() override;

private:
  void verify_with_master(td::Ref<vm::Cell> master_code, td::Ref<vm::Cell> master_data, Result jetton_wallet_data);

  block::StdAddress address_;
  td::Ref<vm::Cell> code_cell_;
  td::Ref<vm::Cell> data_cell_;
  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;
  td::Promise<Result> promise_;
};

class JettonMasterDetectorR: public td::actor::Actor {
public:
  struct Result {
    block::StdAddress address;
    td::RefInt256 total_supply;
    bool mintable;
    std::optional<block::StdAddress> admin_address;
    std::optional<std::map<std::string, std::string>> jetton_content;
    vm::CellHash jetton_wallet_code_hash;
  };

  JettonMasterDetectorR(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise);

  void start_up() override;

private:
  block::StdAddress address_;
  td::Ref<vm::Cell> code_cell_;
  td::Ref<vm::Cell> data_cell_;
  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;
  td::Promise<Result> promise_;
};

class NftItemDetectorR: public td::actor::Actor {
public:
  struct Result {
    struct DNSEntry {
      std::string domain;
      std::optional<block::StdAddress> wallet;
      std::optional<block::StdAddress> next_resolver;
      std::optional<td::Bits256> site_adnl;
      std::optional<td::Bits256> storage_bag_id;
    };
    block::StdAddress address;
    bool init;
    td::RefInt256 index;
    std::optional<block::StdAddress> collection_address;
    std::optional<block::StdAddress> owner_address;
    std::optional<std::map<std::string, std::string>> content;
    std::optional<DNSEntry> dns_entry;
  };

  static bool is_testnet;

  NftItemDetectorR(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise);

  void start_up() override;

private:
  void got_collection(Result item_data, td::Ref<vm::Cell> ind_content, td::Ref<vm::Cell> collection_code, td::Ref<vm::Cell> collection_data);
  td::Status verify_with_collection(block::StdAddress collection_address, td::Ref<vm::Cell> collection_code, td::Ref<vm::Cell> collection_data, td::RefInt256 index);
  td::Result<std::map<std::string, std::string>> get_content(td::RefInt256 index, td::Ref<vm::Cell> ind_content, block::StdAddress collection_address,
                                                            td::Ref<vm::Cell> collection_code, td::Ref<vm::Cell> collection_data);
  void process_domain_and_dns_data(const block::StdAddress& root_address, const std::function<td::Result<std::string>()>& get_domain_function, Result& item_data);
  td::Result<NftItemDetectorR::Result::DNSEntry> get_dns_entry_data();
  td::Result<std::string> get_ton_domain();
  td::Result<std::string> get_t_me_domain();
  static block::StdAddress get_dot_ton_dns_root_addr();
  static std::optional<block::StdAddress> dot_t_dot_me_dns_root_addr();

  block::StdAddress address_;
  td::Ref<vm::Cell> code_cell_;
  td::Ref<vm::Cell> data_cell_;
  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;
  td::Promise<Result> promise_;

  td::Ref<vm::Cell> ind_content_;
};

class NftCollectionDetectorR: public td::actor::Actor {
public:
  struct Result {
    block::StdAddress address;
    td::RefInt256 next_item_index;
    std::optional<block::StdAddress> owner_address;
    std::optional<std::map<std::string, std::string>> collection_content;
  };

  NftCollectionDetectorR(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise);

  void start_up() override;

private:
  block::StdAddress address_;
  td::Ref<vm::Cell> code_cell_;
  td::Ref<vm::Cell> data_cell_;
  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;
  td::Promise<Result> promise_;
};

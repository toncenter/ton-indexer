#pragma once
#include "td/actor/actor.h"
#include "DbScanner.h"
#include <PostgreSQLInserter.h>

using Detector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR,
                                     NftItemDetectorR, NftCollectionDetectorR,
                                     GetGemsNftAuction, GetGemsNftFixPriceSale,
                                     MultisigContract, MultisigOrder,
                                     VestingContract, DedustPoolDetector, StonfiPoolV2Detector>;

struct ShardStateData {
  AllShardStates shard_states_;
  block::gen::ShardStateUnsplit::Record sstate_;
  std::shared_ptr<block::ConfigInfo> config_;
};

struct Options {
  std::uint32_t seqno_{0};
  td::actor::ActorId<PostgreSQLInsertManager> insert_manager_;
  std::uint32_t batch_size_{8192};
  int max_parallel_batches_{8};
  bool index_interfaces_{false};
  bool index_account_states_{false};
  std::string working_dir_;
};

class ShardStateScanner;

class SmcScanner: public td::actor::Actor {
private:
  td::actor::ActorId<DbScanner> db_scanner_;
  Options options_;
public:
  SmcScanner(td::actor::ActorId<DbScanner> db_scanner, Options options) :
    db_scanner_(db_scanner), options_(options) {};

  void start_up() override;
  void got_block(DataContainerPtr block);
};

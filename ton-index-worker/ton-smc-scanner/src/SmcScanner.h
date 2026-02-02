#pragma once
#include "td/actor/actor.h"
#include "DbScanner.h"
#include <PostgreSQLInserter.h>

#ifndef INTERFACES_DETECTOR_TYPES
#   define INTERFACES_DETECTOR_TYPES                                  \
        JettonWalletDetectorR, JettonMasterDetectorR,                 \
        NftItemDetectorR,  NftCollectionDetectorR,                    \
        MultisigContract,  MultisigOrder,  VestingContract
#endif


using Detector = InterfacesDetector<INTERFACES_DETECTOR_TYPES>;

struct ShardStateData {
  AllShardStates shard_states_;
  block::gen::ShardStateUnsplit::Record sstate_;
  std::shared_ptr<block::ConfigInfo> config_;
};

struct Options {
  std::uint32_t seqno_{0};
  td::actor::ActorId<PostgreSQLInsertManager> insert_manager_;
  std::uint64_t basechain_batch_step_ = 0x0001'0000'0000'0000ULL;
  std::uint64_t masterchain_batch_step_ = 0x1000'0000'0000'0000ULL;
  std::uint32_t batch_size_{1000};
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
  void got_block(schema::MasterchainBlockDataState block);
};

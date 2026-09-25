#pragma once

#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "BlockParser.h"
#include "TraceAssembler.h"
#include "TraceInterfaceDetector.h"
#include "TraceMaterialization.h"
#include "emu/EmuClassifierActor.h"

struct ParsedFinalizedBlock {
  ton::BlockSeqno seqno{0};
  std::uint32_t unix_time{0};
  std::vector<TransactionInfo> transactions;
  AllShardStates shard_states;
  std::shared_ptr<block::ConfigInfo> config;
  // Interior transaction cells need both the block owners and their root anchors.
  std::vector<td::Ref<ton::validator::BlockData>> block_data_owners;
  std::vector<td::Ref<vm::Cell>> block_roots;
};

// One owner for finalized graphs and the message index joining consecutive blocks.
// Emits immutable full write plans; the scheduler owns Redis writes and retries.
class FinalizedTraceProcessor : public td::actor::Actor {
 public:
  FinalizedTraceProcessor(double completed_seconds, mch::EmuClassifierConfig classifier = {});
  void prepare_block(ParsedFinalizedBlock block, std::function<void(RedisWritePlan)> on_plan,
                     td::Promise<td::Unit> promise);

 private:
  struct Entry {
    ActiveTrace trace;
    std::uint32_t updated_at{0};
  };
  struct Work {
    TraceIds ids;
    bool oversized{false};
    std::unordered_set<block::StdAddress> addresses;
    ActiveTrace next_trace;
    DetectedAccounts accounts;
    RedisWritePlan cleanup;
    td::Timer classification_timer{true};
  };
  struct BlockWork {
    ParsedFinalizedBlock block;
    std::map<std::string, Work> traces;
    std::deque<std::string> ready;
    std::optional<td::Status> error;
    std::function<void(RedisWritePlan)> on_plan;
    td::Promise<td::Unit> promise;
  };

  double completed_seconds_;
  std::unordered_map<std::string, Entry> traces_;
  std::unordered_map<td::Bits256, TraceIds> awaiting_messages_;
  std::unordered_set<std::string> oversized_traces_;
  std::optional<BlockWork> block_;
  std::vector<td::actor::ActorOwn<mch::EmuClassifierActor>> classifiers_;
  std::deque<std::size_t> idle_classifiers_;

  td::Status assemble_block();
  void accounts_ready(std::string key, td::Result<DetectedAccounts> accounts);
  void classify_ready();
  void classified(std::string key, std::size_t worker, td::Result<mch::EmuClassifyResult> result);
  void emit_snapshot(const std::string& key, mch::EmuActionPayload payload);
  void finish_trace(const std::string& key, td::Status status = td::Status::OK());
  void finish_block();
  void tear_down() override;
};

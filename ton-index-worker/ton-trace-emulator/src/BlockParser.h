#pragma once

#include "IndexData.h"
#include "Measurement.h"
#include "td/actor/actor.h"

struct OutMsgInfo {
  td::Bits256 hash;
  td::Ref<vm::Cell> root;
};

struct TraceIds {
  td::Bits256 root_tx_hash;
  td::Bits256 ext_in_msg_hash;
  td::Bits256 ext_in_msg_hash_norm;
};

struct TransactionInfo {
  block::StdAddress account;
  td::Bits256 hash;
  ton::LogicalTime lt;
  td::Ref<vm::Cell> root;
  ton::BlockId block_id;
  ton::BlockSeqno mc_block_seqno;
  td::Bits256 in_msg_hash;
  std::vector<OutMsgInfo> out_msgs;
  std::optional<TraceIds> trace_ids{};
};

// Stateless decoding: trace_ids is populated only for an external-in root.
class BlockParser : public td::actor::Actor {
  td::Ref<ton::validator::BlockData> block_data_;
  ton::BlockSeqno mc_block_seqno_;
  td::Promise<std::vector<TransactionInfo>> promise_;
  void start_up() override;

 public:
  BlockParser(td::Ref<ton::validator::BlockData> data, ton::BlockSeqno seqno,
              td::Promise<std::vector<TransactionInfo>> promise, const MeasurementPtr& = {})
      : block_data_(std::move(data)), mc_block_seqno_(seqno), promise_(std::move(promise)) {}
};

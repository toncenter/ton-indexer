#pragma once
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include "td/actor/actor.h"
#include "ton/ton-types.h"
#include "IndexData.h"

struct BlockIdHasher {
    std::size_t operator()(const ton::BlockId& k) const {
        std::size_t seed = 0;
        seed ^= std::hash<ton::WorkchainId>()(k.workchain) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= std::hash<ton::ShardId>()(k.shard) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= std::hash<ton::BlockSeqno>()(k.seqno) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        return seed;
    }
};

struct BlockIdEq {
    bool operator()(const ton::BlockId& a, const ton::BlockId& b) const {
        return a.workchain == b.workchain && a.shard == b.shard && a.seqno == b.seqno;
    }
};

struct PendingConfirmedBlock {
    ton::BlockIdExt block_id_ext;
    std::unordered_set<td::Bits256> trace_hashes;
};

struct FinalizedBlockTraces {
    ton::BlockIdExt block_id_ext;
    std::unordered_set<td::Bits256> trace_hashes;
};

struct ShardIdHasher {
    std::size_t operator()(const ton::ShardIdFull& s) const {
        return std::hash<ton::WorkchainId>()(s.workchain) ^ (std::hash<ton::ShardId>()(s.shard) << 1);
    }
};

struct ShardIdEq {
    bool operator()(const ton::ShardIdFull& a, const ton::ShardIdFull& b) const {
        return a.workchain == b.workchain && a.shard == b.shard;
    }
};

struct PendingInvalidation {
    ton::BlockSeqno origin_seqno;
    std::unordered_set<td::Bits256> trace_hashes;
};

class InvalidatedTraceTracker : public td::actor::Actor {
  private:
    std::string redis_dsn_;
    std::unordered_map<ton::BlockId, std::vector<PendingConfirmedBlock>, BlockIdHasher, BlockIdEq> pending_confirmed_blocks_;
    std::unordered_map<ton::BlockId, FinalizedBlockTraces, BlockIdHasher, BlockIdEq> finalized_block_traces_;
    std::unordered_map<ton::ShardIdFull, PendingInvalidation, ShardIdHasher, ShardIdEq> deferred_invalidations_;

    void confirmed_block_discarded(const ton::BlockIdExt& pending_block, const ton::BlockIdExt& finalized_block);
    void publish_invalidated_traces(std::vector<td::Bits256> traces);
    void process_finalized_block(const ton::BlockId& block_id);

  public:
    explicit InvalidatedTraceTracker(std::string redis_dsn) : redis_dsn_(std::move(redis_dsn)) {}

    void register_pending_block(ton::BlockIdExt block_id_ext);
    void add_confirmed_trace(ton::BlockIdExt block_id_ext, td::Bits256 ext_in_hash_norm);
    void add_signed_trace(ton::BlockIdExt block_id_ext, td::Bits256 ext_in_hash_norm);
    void add_finalized_trace(ton::BlockIdExt block_id_ext, td::Bits256 ext_in_hash_norm);
    void finalized_mc_block_emulated(std::vector<ton::BlockId> block_ids);
};

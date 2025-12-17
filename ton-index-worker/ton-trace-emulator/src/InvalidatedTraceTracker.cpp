#include "InvalidatedTraceTracker.h"
#include <sw/redis++/redis++.h>
#include <algorithm>
#include "td/utils/base64.h"

namespace {
class InvalidatedTracePublisher : public td::actor::Actor {
private:
    std::string redis_dsn_;
    std::vector<td::Bits256> hashes_;
public:
    InvalidatedTracePublisher(std::string redis_dsn, std::vector<td::Bits256> hashes)
        : redis_dsn_(std::move(redis_dsn)), hashes_(std::move(hashes)) {}

    void start_up() override {
        try {
            sw::redis::Redis redis(redis_dsn_);
            for (const auto& hash : hashes_) {
                LOG(ERROR) << "Publishing invalidated trace: " << td::base64_encode(hash.as_slice());
                redis.publish("invalidated_traces", td::base64_encode(hash.as_slice()));
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to publish invalidated traces: " << e.what();
        }
        stop();
    }
};
} // namespace

void InvalidatedTraceTracker::register_pending_block(ton::BlockIdExt block_id_ext) {
    auto& entries = pending_confirmed_blocks_[block_id_ext.id];
    auto it = std::find_if(entries.begin(), entries.end(), [&](const PendingConfirmedBlock& blk) {
        return blk.block_id_ext.root_hash == block_id_ext.root_hash && blk.block_id_ext.file_hash == block_id_ext.file_hash;
    });
    if (it == entries.end()) {
        entries.push_back(PendingConfirmedBlock{block_id_ext, {}});
    }
}

void InvalidatedTraceTracker::add_confirmed_trace(ton::BlockIdExt block_id_ext, td::Bits256 ext_in_hash_norm) {
    auto it = pending_confirmed_blocks_.find(block_id_ext.id);
    if (it == pending_confirmed_blocks_.end()) {
        return;
    }
    for (auto& pending_block : it->second) {
        if (pending_block.block_id_ext.root_hash == block_id_ext.root_hash &&
            pending_block.block_id_ext.file_hash == block_id_ext.file_hash) {
            pending_block.trace_hashes.insert(ext_in_hash_norm);
            return;
        }
    }
}

void InvalidatedTraceTracker::add_signed_trace(ton::BlockIdExt block_id_ext, td::Bits256 ext_in_hash_norm) {
    // Signed traces are treated the same as confirmed traces for invalidation purposes.
    add_confirmed_trace(std::move(block_id_ext), ext_in_hash_norm);
}

void InvalidatedTraceTracker::add_finalized_trace(ton::BlockIdExt block_id_ext, td::Bits256 ext_in_hash_norm) {
    auto& finalized_entry = finalized_block_traces_[block_id_ext.id];
    finalized_entry.block_id_ext = block_id_ext;
    finalized_entry.trace_hashes.insert(ext_in_hash_norm);
}

void InvalidatedTraceTracker::finalized_mc_block_emulated(std::vector<ton::BlockId> block_ids) {
    for (auto& block_id : block_ids) {
        process_finalized_block(block_id);
    }
}

void InvalidatedTraceTracker::confirmed_block_discarded(const ton::BlockIdExt& pending_block, const ton::BlockIdExt& finalized_block) {
    LOG(WARNING) << "Confirmed block " << pending_block.to_str() << " was discarded by finalized block "
                 << finalized_block.to_str();
}

void InvalidatedTraceTracker::process_finalized_block(const ton::BlockId& block_id) {
    auto finalized_it = finalized_block_traces_.find(block_id);
    if (finalized_it == finalized_block_traces_.end()) {
        return;
    }
    auto shard_full = finalized_it->second.block_id_ext.shard_full();
    const auto& finalized_traces = finalized_it->second.trace_hashes;

    if (auto candidate_it = deferred_invalidations_.find(shard_full); candidate_it != deferred_invalidations_.end()) {
        std::vector<td::Bits256> to_publish;
        to_publish.reserve(candidate_it->second.trace_hashes.size());
        for (const auto& trace_hash : candidate_it->second.trace_hashes) {
            if (finalized_traces.find(trace_hash) == finalized_traces.end()) {
                to_publish.push_back(trace_hash);
            }
        }
        if (!to_publish.empty()) {
            td::actor::create_actor<InvalidatedTracePublisher>("InvalidatedTracePublisher", redis_dsn_, std::move(to_publish)).release();
        }
        deferred_invalidations_.erase(candidate_it);
    }

    auto pending_it = pending_confirmed_blocks_.find(block_id);
    if (pending_it == pending_confirmed_blocks_.end()) {
        finalized_block_traces_.erase(finalized_it);
        return;
    }

    std::unordered_set<td::Bits256> missing_traces;
    for (const auto& pending_block : pending_it->second) {
        if (pending_block.block_id_ext.root_hash == finalized_it->second.block_id_ext.root_hash &&
            pending_block.block_id_ext.file_hash == finalized_it->second.block_id_ext.file_hash) {
            continue;
        }
        for (const auto& trace_hash : pending_block.trace_hashes) {
            if (finalized_traces.find(trace_hash) == finalized_traces.end()) {
                missing_traces.insert(trace_hash);
            }
        }
        confirmed_block_discarded(pending_block.block_id_ext, finalized_it->second.block_id_ext);
    }

    if (!missing_traces.empty()) {
        deferred_invalidations_[shard_full] = PendingInvalidation{block_id.seqno, std::move(missing_traces)};
    }

    pending_confirmed_blocks_.erase(pending_it);
    finalized_block_traces_.erase(finalized_it);
}

void InvalidatedTraceTracker::publish_invalidated_traces(std::vector<td::Bits256> traces) {
    if (traces.empty()) {
        return;
    }
    td::actor::create_actor<InvalidatedTracePublisher>("InvalidatedTracePublisher", redis_dsn_, std::move(traces)).release();
}

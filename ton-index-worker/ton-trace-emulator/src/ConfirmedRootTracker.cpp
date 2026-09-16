#include "ConfirmedRootTracker.h"

#include <algorithm>
#include <iterator>

namespace {

bool same_block_variant(const ton::BlockIdExt& left,
                        const ton::BlockIdExt& right) {
    return left == right;
}

bool is_later_inclusion(const ton::BlockIdExt& candidate,
                        const ton::BlockIdExt& current) {
    return candidate.id.seqno > current.id.seqno;
}

}  // namespace

void ConfirmedRootTracker::add_confirmed_root(ton::BlockIdExt block_id,
                                              td::Bits256 trace_hash) {
    auto& variants = confirmed_blocks_[block_id.id];
    auto block = std::find_if(
        variants.begin(), variants.end(), [&](const ConfirmedBlock& candidate) {
            return same_block_variant(candidate.block_id, block_id);
        });
    if (block == variants.end()) {
        variants.push_back(ConfirmedBlock{block_id, {}});
        block = std::prev(variants.end());
    }
    block->roots.insert(trace_hash);
    auto& latest = latest_confirmed_roots_[trace_hash];
    if (latest.empty() || is_later_inclusion(block_id, *latest.begin())) {
        latest.clear();
        latest.insert(std::move(block_id));
    } else if (block_id.id.seqno == latest.begin()->id.seqno) {
        latest.insert(std::move(block_id));
    }
}

void ConfirmedRootTracker::add_finalized_root(ton::BlockIdExt block_id,
                                              td::Bits256 trace_hash) {
    finalized_roots_[block_id.id].insert(trace_hash);
    latest_confirmed_roots_.erase(trace_hash);
}

std::vector<td::Bits256> ConfirmedRootTracker::finalize_block(
    ton::BlockIdExt finalized_block) {
    std::set<td::Bits256> replaced_roots;
    auto finalized_roots_it = finalized_roots_.find(finalized_block.id);
    const std::set<td::Bits256> empty_roots;
    const auto& finalized_roots =
        finalized_roots_it == finalized_roots_.end()
            ? empty_roots
            : finalized_roots_it->second;

    auto confirmed_it = confirmed_blocks_.find(finalized_block.id);
    if (confirmed_it != confirmed_blocks_.end()) {
        std::set<td::Bits256> accepted_roots = finalized_roots;
        for (const auto& confirmed_block : confirmed_it->second) {
            if (same_block_variant(confirmed_block.block_id, finalized_block)) {
                accepted_roots.insert(
                    confirmed_block.roots.begin(), confirmed_block.roots.end());
            }
        }
        for (const auto& trace_hash : accepted_roots) {
            latest_confirmed_roots_.erase(trace_hash);
        }

        for (const auto& confirmed_block : confirmed_it->second) {
            if (same_block_variant(confirmed_block.block_id, finalized_block)) {
                continue;
            }

            for (const auto& trace_hash : confirmed_block.roots) {
                if (accepted_roots.count(trace_hash) != 0) {
                    continue;
                }
                auto latest = latest_confirmed_roots_.find(trace_hash);
                if (latest == latest_confirmed_roots_.end() ||
                    latest->second.erase(confirmed_block.block_id) == 0) {
                    continue;
                }
                if (latest->second.empty()) {
                    latest_confirmed_roots_.erase(latest);
                    replaced_roots.insert(trace_hash);
                }
            }
        }
        confirmed_blocks_.erase(confirmed_it);
    }
    if (finalized_roots_it != finalized_roots_.end()) {
        finalized_roots_.erase(finalized_roots_it);
    }

    return {replaced_roots.begin(), replaced_roots.end()};
}

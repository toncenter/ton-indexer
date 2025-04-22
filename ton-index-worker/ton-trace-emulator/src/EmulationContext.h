#pragma once
#include <vector>
#include <map>
#include <mutex>
#include <memory>
#include <crypto/block/mc-config.h>
#include <crypto/block/transaction.h>
#include <crypto/openssl/rand.hpp>
#include <crypto/block/block-parse.h>

struct AddrCmp {
    bool operator()(const block::StdAddress& lhs, const block::StdAddress& rhs) const {
        if (lhs.workchain != rhs.workchain) {
            return lhs.workchain < rhs.workchain;
        }
        return lhs.addr < rhs.addr;
    }
};

class EmulationContext {
private:
    struct ShardState {
        ton::BlockId blkid;
        uint32_t timestamp;
        ton::LogicalTime lt;
        td::Ref<vm::Cell> shard_state_cell;
    };

    ton::BlockSeqno mc_seqno_;
    std::vector<ShardState> shard_states_;
    std::multimap<block::StdAddress, block::Account, AddrCmp> emulated_accounts_;
    std::mutex emulated_accounts_mutex_;
    std::shared_ptr<block::ConfigInfo> config_;
    bool ignore_chksig_{false};
    td::Bits256 rand_seed_;
    const size_t txs_count_limit_{1000};
    std::atomic<size_t> txs_count_{0};

    const float block_rate = 0.37f;

public:
    EmulationContext(uint32_t mc_seqno, std::shared_ptr<block::ConfigInfo> config, bool ignore_chksig = false)
        : mc_seqno_(mc_seqno), config_(std::move(config)), ignore_chksig_(ignore_chksig) {
        prng::rand_gen().strong_rand_bytes(rand_seed_.data(), 32);
        std::srand(std::time(nullptr));
    }

    void add_shard_state(const ton::BlockId& blkid, uint32_t timestamp, ton::LogicalTime lt, td::Ref<vm::Cell> cell) {
        shard_states_.emplace_back(ShardState{blkid, timestamp, lt, std::move(cell)});
    }

    void increase_seqno(size_t count) {
        for (auto& shard_state : shard_states_) {
            shard_state.blkid.seqno += count;
            for (int i = 0; i < count; ++i) {
                shard_state.timestamp += randomized_rounding(1.f / block_rate);
            }
            shard_state.lt += count * block::ConfigInfo::get_lt_align();
        }
        mc_seqno_ += count;
    }

    uint32_t randomized_rounding(float num) {
        int integerPart = static_cast<uint32_t>(num);
        float fractionalPart = num - integerPart;
    
        float randomValue = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
    
        if (randomValue < fractionalPart) {
            return integerPart + 1;
        } else {
            return integerPart;
        }
    }    

    void increase_tx_count(size_t count) {
        txs_count_.fetch_add(count);
    }

    bool is_limit_exceeded() const {
        return txs_count_.load() >= txs_count_limit_;
    }

    void insert_account_state(block::Account accountState) {
        std::lock_guard<std::mutex> lock(emulated_accounts_mutex_);
        auto addr = block::StdAddress(accountState.workchain, accountState.addr);
        emulated_accounts_.insert({std::move(addr), std::move(accountState)});
    }

    td::Result<block::Account> get_account_state(const block::StdAddress& address) {
        {
            std::lock_guard<std::mutex> lock(emulated_accounts_mutex_);
            auto range = emulated_accounts_.equal_range(address);
            if (range.first != range.second) {
                auto it = std::prev(range.second);
                auto account = it->second;
                for (auto& shard_state : shard_states_) {
                    if (ton::shard_contains(shard_state.blkid.shard_full(), ton::extract_addr_prefix(address.workchain, address.addr))) {
                        account.now_ = shard_state.timestamp;
                        break;
                    }
                }
                return account;
            }
        }

        return search_in_shard_states(address);
    }

    std::multimap<block::StdAddress, block::Account, AddrCmp> release_account_states() {
        std::lock_guard<std::mutex> lock(emulated_accounts_mutex_);
        auto tmp = std::move(emulated_accounts_);
        emulated_accounts_.clear();
        return tmp;
    }

    ton::BlockSeqno get_mc_seqno() const {
        return mc_seqno_;
    }

    td::Bits256 get_rand_seed() const {
        return rand_seed_;
    }

    std::shared_ptr<block::ConfigInfo> get_config() const {
        return config_;
    }

    bool get_ignore_chksig() const {
        return ignore_chksig_;
    }

    void set_ignore_chksig(bool ignore_chksig) {
        ignore_chksig_ = ignore_chksig;
    }

    const std::vector<ShardState>& get_shard_states() const {
        return shard_states_;
    }

private:
    td::Result<block::Account> search_in_shard_states(const block::StdAddress& address) {
        for (const auto &shard_state : shard_states_) {
            if (!ton::shard_contains(shard_state.blkid.shard_full(), ton::extract_addr_prefix(address.workchain, address.addr))) {
                continue;
            }

            block::gen::ShardStateUnsplit::Record sstate;
            if (!tlb::unpack_cell(shard_state.shard_state_cell, sstate)) {
                return td::Status::Error("Failed to unpack ShardStateUnsplit");
            }

            vm::AugmentedDictionary accounts_dict(vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts);
            auto account_state = block::Account(address.workchain, address.addr.cbits());
            auto account = accounts_dict.lookup(address.addr);

            if (account.is_null()) {
                if (!account_state.init_new(shard_state.timestamp)) {
                    return td::Status::Error("Failed to init new account");
                }
            } else {
                if (!account_state.unpack(std::move(account), shard_state.timestamp, 
                            address.workchain == ton::masterchainId && config_->is_special_smartcontract(address.addr))) {
                    return td::Status::Error("Failed to unpack account");
                }
            }
            account_state.block_lt = shard_state.lt;

            std::lock_guard<std::mutex> lock(emulated_accounts_mutex_);
            auto it = emulated_accounts_.insert({address, std::move(account_state)});

            return it->second;
        }
        return td::Status::Error("Account not found in shard_states");
    }
};


#pragma once
#include <any>
#include <td/actor/actor.h>
#include <emulator/transaction-emulator.h>
#include "DbScanner.h"

#include "smc-interfaces/InterfacesDetector.h"

using TraceId = td::Bits256;

struct AddrCmp {
    bool operator()(const block::StdAddress& lhs, const block::StdAddress& rhs) const {
        if (lhs.workchain != rhs.workchain) {
            return lhs.workchain < rhs.workchain;
        }
        return lhs.addr < rhs.addr;
    }
};

struct TraceNode {
    td::Bits256 node_id; // hash of cur tx in msg
    block::StdAddress address;
    std::vector<std::unique_ptr<TraceNode>> children;
    td::Ref<vm::Cell> transaction_root;
    bool emulated;
    bool depth_limit_reached{false}; // true if due to depth limit children of the node were not emulated

    int depth() const {
        int res = 0;
        for (const auto& child : children) {
            res = std::max(res, child->depth());
        }
        return res + 1;
    }

    bool depth_limit_exceeded() const {
        if (depth_limit_reached) {
            return true;
        }
        for (const auto& child : children) {
            if (child->depth_limit_exceeded()) {
                return true;
            }
        }
        return false;
    }

    int transactions_count() const {
        int res = transaction_root.not_null() ? 1 : 0;
        for (const auto& child : children) {
            res += child->transactions_count();
        }
        return res;
    }

    std::string to_string(int tabs = 0) const {
        std::stringstream ss;
        ss << std::endl;
        for (int i = 0; i < tabs; i++) {
            ss << "--";
        }
        // print account, lt, status, bounced, in_msg opcode
        block::gen::Transaction::Record trans;
        block::gen::TransactionDescr::Record_trans_ord descr;
        tlb::unpack_cell(transaction_root, trans);
        tlb::unpack_cell(trans.description, descr);
        ss << "TX acc=" << trans.account_addr.to_hex() << " lt=" << trans.lt << " outmsg_cnt=" << trans.outmsg_cnt << " aborted=" << descr.aborted << std::endl;

        for (const auto& child : children) {
            ss << child->to_string(tabs + 1);
        }
        return ss.str();
    }
};

struct Trace {
    td::Bits256 root_tx_hash;
    td::Bits256 ext_in_msg_hash;
    td::Bits256 ext_in_msg_hash_norm;
    std::unique_ptr<TraceNode> root;
    td::Bits256 rand_seed;

    using Detector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR, 
                                        NftItemDetectorR, NftCollectionDetectorR,
                                        GetGemsNftFixPriceSale, GetGemsNftAuction>;

    std::multimap<block::StdAddress, block::Account, AddrCmp> emulated_accounts;
    std::unordered_map<block::StdAddress, std::vector<typename Detector::DetectedInterface>> interfaces;

    int depth() const {
        return root->depth();
    }

    bool depth_limit_exceeded() const {
        return root->depth_limit_exceeded();
    }

    int transactions_count() const {
        return root->transactions_count();
    }

    std::string to_string() const {
        return root->to_string();
    }
};

td::Result<block::StdAddress> fetch_msg_dest_address(td::Ref<vm::Cell> msg, int& type);

class TraceEmulatorImpl: public td::actor::Actor {
private:
    std::shared_ptr<emulator::TransactionEmulator> emulator_;
    std::vector<td::Ref<vm::Cell>> shard_states_;
    std::multimap<block::StdAddress, block::Account, AddrCmp>& emulated_accounts_;
    std::mutex& emulated_accounts_mutex_;
    std::unordered_map<block::StdAddress, td::actor::ActorOwn<TraceEmulatorImpl>>& emulator_actors_;
    std::unordered_map<TraceNode *, std::pair<std::unique_ptr<TraceNode>, td::Promise<std::unique_ptr<TraceNode>>>> result_promises_;

    uint32_t utime_{0};
public:
    TraceEmulatorImpl(std::shared_ptr<emulator::TransactionEmulator> emulator, std::vector<td::Ref<vm::Cell>> shard_states, 
                  std::multimap<block::StdAddress, block::Account, AddrCmp>& emulated_accounts, std::mutex& emulated_accounts_mutex,
                  std::unordered_map<block::StdAddress, td::actor::ActorOwn<TraceEmulatorImpl>>& emulator_actors)
        : emulator_(std::move(emulator)), shard_states_(std::move(shard_states)), 
          emulated_accounts_(emulated_accounts), emulated_accounts_mutex_(emulated_accounts_mutex), emulator_actors_(emulator_actors) {
    }

    void emulate(td::Ref<vm::Cell> in_msg, block::StdAddress address, size_t depth, td::Promise<std::unique_ptr<TraceNode>> promise);

private:
    td::Result<block::Account> unpack_account(vm::AugmentedDictionary& accounts_dict, const block::StdAddress& account_addr, uint32_t utime);
    void emulate_transaction(block::Account account, block::StdAddress address,
                            td::Ref<vm::Cell> in_msg, size_t depth, td::Promise<std::unique_ptr<TraceNode>> promise);
    void child_emulated(TraceNode *parent_node_raw, std::unique_ptr<TraceNode> child, size_t ind);
    void child_error(TraceNode *parent_node_raw, td::Status error);
};

// Emulates whole trace, in_msg is external inbound message
class TraceEmulator: public td::actor::Actor {
private:
    MasterchainBlockDataState mc_data_state_;
    td::Ref<vm::Cell> in_msg_;
    bool ignore_chksig_;
    td::Promise<Trace> promise_;
    td::Bits256 rand_seed_;

    std::shared_ptr<emulator::TransactionEmulator> emulator_;
    std::multimap<block::StdAddress, block::Account, AddrCmp> emulated_accounts_;
    std::mutex emulated_accounts_mutex_;
    std::unordered_map<block::StdAddress, td::actor::ActorOwn<TraceEmulatorImpl>> emulator_actors_;
public:
    TraceEmulator(MasterchainBlockDataState mc_data_state, td::Ref<vm::Cell> in_msg, bool ignore_chksig, td::Promise<Trace> promise);

    void start_up() override;
    void finish(td::Result<std::unique_ptr<TraceNode>> root);
};


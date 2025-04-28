#pragma once
#include <any>
#include <td/actor/actor.h>
#include <emulator/transaction-emulator.h>
#include "DbScanner.h"
#include "EmulationContext.h"

#include "smc-interfaces/InterfacesDetector.h"

using TraceId = td::Bits256;


struct TraceNode {
    td::Bits256 node_id; // hash of cur tx in msg
    block::StdAddress address;
    std::vector<std::unique_ptr<TraceNode>> children;
    td::Ref<vm::Cell> transaction_root;

    ton::BlockSeqno mc_block_seqno;
    ton::BlockId block_id;

    bool emulated;

    int depth() const {
        int res = 0;
        for (const auto& child : children) {
            res = std::max(res, child->depth());
        }
        return res + 1;
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
    bool tx_limit_exceeded{false};

    using Detector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR, 
                                        NftItemDetectorR, NftCollectionDetectorR,
                                        GetGemsNftFixPriceSale, GetGemsNftAuction>;

    std::multimap<block::StdAddress, block::Account, AddrCmp> emulated_accounts;
    std::unordered_map<block::StdAddress, std::vector<typename Detector::DetectedInterface>> interfaces;

    int depth() const {
        return root->depth();
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
    ton::BlockId block_id_;
    EmulationContext& context_;
    
    std::unordered_map<block::StdAddress, td::actor::ActorOwn<TraceEmulatorImpl>>& emulator_actors_;
    std::mutex& emulator_actors_mutex_;

    std::shared_ptr<emulator::TransactionEmulator> emulator_;

    std::unordered_map<TraceNode *, std::pair<std::unique_ptr<TraceNode>, td::Promise<std::unique_ptr<TraceNode>>>> result_promises_;
public:
    TraceEmulatorImpl(ton::BlockId block_id, EmulationContext& context,
        std::unordered_map<block::StdAddress, td::actor::ActorOwn<TraceEmulatorImpl>>& emulator_actors, std::mutex& emulator_actors_mutex)
        : block_id_(block_id), context_(context), emulator_actors_(emulator_actors), emulator_actors_mutex_(emulator_actors_mutex) {
            emulator_ = std::make_shared<emulator::TransactionEmulator>(context.get_config(), 0);
            auto libraries_root = context.get_config()->get_libraries_root();
            emulator_->set_libs(vm::Dictionary(libraries_root, 256));
            emulator_->set_ignore_chksig(context.get_ignore_chksig());
            context.set_ignore_chksig(false); // ignore chksig only on root tx
    }

    void emulate(td::Ref<vm::Cell> in_msg, block::StdAddress address, ton::LogicalTime lt, td::Promise<std::unique_ptr<TraceNode>> promise);

private:
    void child_emulated(TraceNode *parent_node_raw, std::unique_ptr<TraceNode> child, size_t ind);
    void child_error(TraceNode *parent_node_raw, td::Status error);
};

class ShardBlockEmulator: public td::actor::Actor {
private:
    ton::BlockId block_id_;
    EmulationContext& context_;
    std::vector<td::Ref<vm::Cell>> in_msgs_; // only msgs to accounts in current shard
    td::Promise<std::vector<std::unique_ptr<TraceNode>>> promise_;

    std::unordered_map<block::StdAddress, td::actor::ActorOwn<TraceEmulatorImpl>> emulator_actors_;
    std::mutex emulator_actors_mutex_;
    std::vector<std::unique_ptr<TraceNode>> result_{};
public:
    ShardBlockEmulator(ton::BlockId block_id, EmulationContext& context, std::vector<td::Ref<vm::Cell>> in_msgs,
                    td::Promise<std::vector<std::unique_ptr<TraceNode>>> promise)
        : block_id_(block_id), context_(context), in_msgs_(std::move(in_msgs)), promise_(std::move(promise)) {
    }

    void start_up() override;

private:
    void error(td::Status error);
    void child_emulated(std::unique_ptr<TraceNode> node, size_t child_ind);
};

class MasterchainBlockEmulator: public td::actor::Actor {
private:
    std::shared_ptr<emulator::TransactionEmulator> emulator_;
    EmulationContext& context_;
    std::vector<td::Ref<vm::Cell>> in_msgs_;
    td::Promise<std::vector<std::unique_ptr<TraceNode>>> promise_;
    std::vector<std::unique_ptr<TraceNode>> result_{};

public:
    MasterchainBlockEmulator(EmulationContext& context, std::vector<td::Ref<vm::Cell>> in_msgs,
                            td::Promise<std::vector<std::unique_ptr<TraceNode>>> promise)
        : context_(context), in_msgs_(std::move(in_msgs)), promise_(std::move(promise)) {
    }

    void start_up() override;

private:
    void shard_emulated(block::ShardId shard, std::vector<std::unique_ptr<TraceNode>> shard_traces, td::Promise<> promise);
    void all_shards_emulated();
    void error(td::Status error);
    void next_mc_emulated(std::vector<std::unique_ptr<TraceNode>> children);
};

// Emulates whole trace, in_msg is external inbound message
class TraceEmulator: public td::actor::Actor {
private:
    MasterchainBlockDataState mc_data_state_;
    td::Ref<vm::Cell> in_msg_;
    bool ignore_chksig_;
    td::Promise<Trace> promise_;
    td::Bits256 rand_seed_;

    std::unique_ptr<EmulationContext> context_;

    td::Timer timer_{false};
public:
    TraceEmulator(MasterchainBlockDataState mc_data_state, td::Ref<vm::Cell> in_msg, bool ignore_chksig, td::Promise<Trace> promise);

    void start_up() override;
    void finish(td::Result<std::vector<std::unique_ptr<TraceNode>>> root_r);
};


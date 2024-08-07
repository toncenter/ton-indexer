#pragma once
#include <any>
#include <td/actor/actor.h>
#include <emulator/transaction-emulator.h>
#include "DbScanner.h"

#include "smc-interfaces/InterfacesDetector.h"

using TraceId = td::Bits256;

struct Trace {
    TraceId id; // hash of initial in msg
    td::Bits256 node_id; // hash of cur tx in msg
    ton::WorkchainId workchain;
    std::vector<Trace *> children;
    td::Ref<vm::Cell> transaction_root;
    bool emulated;

    std::unique_ptr<block::Account> account;

    using Detector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR, 
                                        NftItemDetectorR, NftCollectionDetectorR,
                                        GetGemsNftFixPriceSale, GetGemsNftAuction>;
                                        
    std::vector<typename Detector::DetectedInterface> interfaces;

    ~Trace() {
        for (Trace* child : children) {
            if (child != nullptr) {
                delete child;
            }
        }
    }

    int depth() const {
        int res = 0;
        for (const auto child : children) {
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
        ss << "TX acc=" << trans.account_addr.to_hex() << " lt=" << trans.lt << " outmsg_cnt=" << trans.outmsg_cnt << " aborted=" << descr.aborted << " int_count: " << interfaces.size() << std::endl;

        for (const auto child : children) {
            ss << child->to_string(tabs + 1);
        }
        return ss.str();
    }
};

class TraceEmulator: public td::actor::Actor {
private:
    std::shared_ptr<emulator::TransactionEmulator> emulator_;
    std::vector<td::Ref<vm::Cell>> shard_states_;
    std::unordered_map<block::StdAddress, std::shared_ptr<block::Account>, AddressHasher> emulated_accounts_;
    block::StdAddress account_addr_;
    td::Ref<vm::Cell> in_msg_;
    bool is_external_{false};
    td::Promise<Trace *> promise_;
    size_t depth_{20};
    size_t pending_{0};
    Trace *result_{nullptr};

    void set_error(td::Status error);
    std::unique_ptr<block::Account> unpack_account(vm::AugmentedDictionary& accounts_dict);
    void emulate_transaction(std::unique_ptr<block::Account> account);
    void trace_emulated(Trace *trace, size_t ind);
public:
    TraceEmulator(std::shared_ptr<emulator::TransactionEmulator> emulator, std::vector<td::Ref<vm::Cell>> shard_states, 
                  std::unordered_map<block::StdAddress, std::shared_ptr<block::Account>, AddressHasher> emulated_accounts, td::Ref<vm::Cell> in_msg, size_t depth, td::Promise<Trace *> promise)
        : emulator_(std::move(emulator)), shard_states_(shard_states), emulated_accounts_(emulated_accounts), 
          in_msg_(std::move(in_msg)), depth_(depth), promise_(std::move(promise)) {
    }

    void start_up() override;
};

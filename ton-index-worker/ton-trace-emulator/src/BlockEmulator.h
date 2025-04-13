#include <td/actor/actor.h>
#include <ton/ton-types.h>
#include <emulator/transaction-emulator.h>
#include "IndexData.h"
#include "TraceEmulator.h"


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

class McBlockEmulator: public td::actor::Actor {
private:
    MasterchainBlockDataState mc_data_state_;
    std::function<void(Trace, td::Promise<td::Unit>)> trace_processor_;
    td::Promise<> promise_;
    size_t blocks_left_to_parse_;
    std::vector<TransactionInfo> txs_;

    std::vector<td::Ref<vm::Cell>> shard_states_;

    std::unordered_map<td::Bits256, TransactionInfo> tx_by_in_msg_hash_; // mapping from msg hash to tx that processed it as in_msg
    std::unordered_map<td::Bits256, TransactionInfo> tx_by_out_msg_hash_; // mapping from msg hash to tx that created it as out_msg

    std::unordered_set<TraceId> trace_ids_in_progress_;
    size_t in_progress_cnt_{0};

    int traces_cnt_{0};

    td::Timestamp start_time_;

    // static map for matching in-out msgs between blocks to propagate trace ids. TODO: clean up old entries.
    // TODO: care of thread safety
    inline static std::unordered_map<td::Bits256, TraceIds> interblock_trace_ids_;

    void parse_error(ton::BlockId blkid, td::Status error);
    void block_parsed(ton::BlockId blkid, std::vector<TransactionInfo> txs);
    void process_txs();
    void emulate_traces();
    std::unique_ptr<TraceNode> construct_commited_trace(const TransactionInfo& tx, std::vector<td::Ref<vm::Cell>>& msgs_to_emulate);
    void emulated_nodes_received(std::vector<std::unique_ptr<TraceNode>> commited_nodes,
        std::vector<std::unique_ptr<TraceNode>> emulated_nodes, std::unique_ptr<EmulationContext> context);
    void children_emulated(std::unique_ptr<TraceNode> parent_node, std::vector<std::unique_ptr<TraceNode>> child_nodes, 
        TraceIds trace_ids, std::unique_ptr<EmulationContext> context);
    void trace_error(td::Bits256 tx_hash, td::Bits256 trace_root_tx_hash, td::Status error);
    void trace_received(td::Bits256 tx_hash, Trace trace);
    void trace_interfaces_error(td::Bits256 trace_root_tx_hash, td::Status error);
    void trace_emulated(Trace trace);
    void trace_finished(td::Bits256 trace_root_tx_hash);

    td::Result<block::Account> fetch_account(const block::StdAddress& addr, ton::UnixTime now);

public:
    McBlockEmulator(MasterchainBlockDataState mc_data_state, std::function<void(Trace, td::Promise<td::Unit>)> trace_processor, td::Promise<> promise);

    virtual void start_up() override;
};
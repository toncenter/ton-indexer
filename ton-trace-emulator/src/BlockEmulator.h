#include <td/actor/actor.h>
#include <ton/ton-types.h>
#include <emulator/transaction-emulator.h>
#include "IndexData.h"
#include "TraceEmulator.h"


struct OutMsgInfo {
    td::Bits256 hash;
    block::StdAddress destination;
    td::Ref<vm::Cell> root;
};

struct TransactionInfo {
    block::StdAddress account;
    td::Bits256 hash;
    ton::LogicalTime lt;
    td::Ref<vm::Cell> root;
    td::Bits256 in_msg_hash;
    bool is_first;
    std::vector<OutMsgInfo> out_msgs;
    std::optional<td::Bits256> initial_msg_hash{}; // hash of initial transaction in block that caused this transaction. 
                                                 // This is not necessarily ext in message, because ext in could happen in prev block.
};

class McBlockEmulator: public td::actor::Actor {
private:
    MasterchainBlockDataState mc_data_state_;
    std::function<void(std::unique_ptr<Trace>)> trace_processor_;
    td::Promise<> promise_;
    size_t blocks_left_to_parse_;
    std::vector<TransactionInfo> txs_;

    std::shared_ptr<emulator::TransactionEmulator> emulator_;
    std::vector<td::Ref<vm::Cell>> shard_states_;

    std::unordered_map<td::Bits256, std::reference_wrapper<const TransactionInfo>, BitArrayHasher> tx_by_in_msg_hash_;

    std::unordered_set<TraceId, BitArrayHasher> trace_ids_in_progress_;

    int traces_cnt_{0};

    td::Timestamp start_time_;

    // static map for matching in-out msgs between blocks to propagate trace ids. TODO: clean up old entries.
    inline static std::unordered_map<td::Bits256, TraceId, BitArrayHasher> interblock_trace_ids_;

    void parse_error(ton::BlockId blkid, td::Status error);
    void block_parsed(ton::BlockId blkid, std::vector<TransactionInfo> txs);
    void process_txs();
    void db_error(td::Status error);
    void emulate_traces();
    void create_trace(const TransactionInfo& tx, td::Promise<Trace *> promise);
    void trace_error(td::Bits256 tx_hash, TraceId trace_id, td::Status error);
    void trace_received(td::Bits256 tx_hash, Trace *trace);
    void trace_interfaces_error(TraceId trace_id, td::Status error);
    void finish_processing(std::unique_ptr<Trace> trace);

    td::Result<block::Account> fetch_account(const block::StdAddress& addr, ton::UnixTime now);

public:
    McBlockEmulator(MasterchainBlockDataState mc_data_state, std::function<void(std::unique_ptr<Trace>)> trace_processor, td::Promise<> promise);

    virtual void start_up() override;
};
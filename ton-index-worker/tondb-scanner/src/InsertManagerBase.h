#pragma once
#include <string>
#include <queue>
#include "InsertManager.h"


struct InsertTaskStruct{
    std::uint32_t mc_seqno_;
    ParsedBlockPtr parsed_block_;
    td::Promise<td::Unit> promise_;

    QueueState get_queue_state();
};

class InsertManagerBase: public InsertManagerInterface {
public:
    void set_parallel_inserts_actors(int value) { max_parallel_insert_actors_ = value; }
    void set_insert_batch_size(QueueState batch_size) { batch_size_ = batch_size; }
    void set_insert_blocks(int value) { batch_size_.blocks_ = value; }
    void set_insert_txs(int value) { batch_size_.txs_ = value; }
    void set_insert_msgs(int value) { batch_size_.msgs_ = value; }
    
    void print_info();

    void start_up() override;
    void alarm() override;
    void insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, bool force, td::Promise<QueueState> queued_promise, td::Promise<td::Unit> inserted_promise) override;
    void get_insert_queue_state(td::Promise<QueueState> promise) override;

    virtual void create_insert_actor(std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) = 0;
private:
    std::queue<InsertTaskStruct> insert_queue_;
    QueueState queue_state_{0, 0, 0, 0};

    td::int32 max_parallel_insert_actors_{32};
    td::int32 parallel_insert_actors_{0};

    QueueState batch_size_{10000, 10000, 100000, 100000};

    bool check_batch_size(QueueState& batch_state);
    void schedule_next_insert_batches(bool full_batch);
    void insert_batch_finished();
};

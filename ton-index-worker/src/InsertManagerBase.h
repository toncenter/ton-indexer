#pragma once
#include <string>
#include <queue>
#include "InsertManager.h"


struct InsertTaskStruct{
    std::uint32_t mc_seqno_;
    ParsedBlockPtr parsed_block_;
    td::Promise<td::Unit> promise_;

    QueueStatus get_queue_status();
};

class InsertManagerBase: public InsertManagerInterface {
public:
    void set_batch_blocks_count(int value) { batch_blocks_count_ = value; }
    void set_parallel_inserts_actors(int value) { max_parallel_insert_actors_ = value; }
    void set_insert_blocks(int value) { max_insert_blocks_ = value; }
    void set_insert_txs(int value) { max_insert_txs_ = value; }
    void set_insert_msgs(int value) { max_insert_msgs_ = value; }
    
    void print_info();

    void start_up() override;
    void alarm() override;
    void insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, td::Promise<QueueStatus> queued_promise, td::Promise<td::Unit> inserted_promise) override;
    void get_insert_queue_status(td::Promise<QueueStatus> promise) override;

    virtual void create_insert_actor(std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) = 0;
private:
    std::queue<InsertTaskStruct> insert_queue_;
    QueueStatus queue_status_{0, 0, 0, 0};

    td::int32 batch_blocks_count_{512};
    td::int32 max_parallel_insert_actors_{32};
    td::int32 parallel_insert_actors_{0};

    td::int32 max_insert_mc_blocks_{1024};
    td::int32 max_insert_blocks_{2048};
    td::int32 max_insert_txs_{32768};
    td::int32 max_insert_msgs_{65536};

    bool check_batch_size(QueueStatus& batch_status);
    void schedule_next_insert_batches();
    void insert_batch_finished();
};

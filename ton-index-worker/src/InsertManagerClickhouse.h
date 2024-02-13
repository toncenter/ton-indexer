#pragma once
#include <string>
#include <queue>
#include "InsertManager.h"
#include <clickhouse/client.h>


class InsertBatchClickhouse;

class InsertManagerClickhouse: public InsertManagerInterface {
public:
    struct Credential {
        std::string host = "127.0.0.1";
        int port = 9000;
        std::string user = "default";
        std::string password = "";
        std::string dbname = "default";
    };
    InsertManagerClickhouse(Credential credential) : credential_(credential) {}

    void set_batch_blocks_count(int value) { batch_blocks_count_ = value; }
    void set_parallel_inserts_actors(int value) { max_parallel_insert_actors_ = value; }

    void start_up() override;
    void alarm() override;

    void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise) override;
    void insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, td::Promise<QueueStatus> queued_promise, td::Promise<td::Unit> inserted_promise) override;
    void get_insert_queue_status(td::Promise<QueueStatus> promise) override;
    
    void upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) override;
    void get_jetton_wallet(std::string address, td::Promise<JettonWalletData> promise) override;
    void upsert_jetton_master(JettonMasterData jetton_wallet, td::Promise<td::Unit> promise) override;
    void get_jetton_master(std::string address, td::Promise<JettonMasterData> promise) override;
    void upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) override;
    void get_nft_collection(std::string address, td::Promise<NFTCollectionData> promise) override;
    void upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) override;
    void get_nft_item(std::string address, td::Promise<NFTItemData> promise) override;
private:
    Credential credential_;

    std::queue<InsertTaskStruct> insert_queue_;
    QueueStatus queue_status_{0, 0, 0, 0};

    td::int32 batch_blocks_count_{512};
    td::int32 max_parallel_insert_actors_{32};
    td::int32 parallel_insert_actors_{0};

    td::int32 max_insert_mc_blocks_{1024};
    td::int32 max_insert_blocks_{2048};
    td::int32 max_insert_txs_{32768};
    td::int32 max_insert_msgs_{65536};

    
    clickhouse::ClientOptions get_clickhouse_options();

    bool check_batch_size(QueueStatus& batch_status);
    void schedule_next_insert_batches();
    void insert_batch_finished();
};


class InsertBatchClickhouse: public td::actor::Actor {
public:
    InsertBatchClickhouse(clickhouse::ClientOptions client_options, std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) 
        : client_options_(std::move(client_options)), insert_tasks_(std::move(insert_tasks)), promise_(std::move(promise)) {}

    void start_up() override;
private:
    clickhouse::ClientOptions client_options_;
    std::vector<InsertTaskStruct> insert_tasks_;
    td::Promise<td::Unit> promise_;

    void insert_blocks(clickhouse::Client& client);
};

#include "td/utils/StringBuilder.h"
#include "InsertManagerClickhouse.h"
#include "clickhouse/client.h"


void InsertManagerClickhouse::start_up() {
    LOG(INFO) << "Clickhouse start_up";
    try {
        auto options = get_clickhouse_options();
        clickhouse::Client client(options);

        td::StringBuilder builder;
        // TODO: create full tables
        builder << "CREATE TABLE IF NOT EXISTS blocks ("
                << "workchain Int32, "
                << "shard Int64, "
                << "seqno Int32, "
                << "root_hash FixedString(44), "
                << "file_hash FixedString(44), "
                << "mc_seqno Nullable(Int32), "
                << "gen_utime Int32, "
                << "start_lt UInt64, "
                << "end_lt UInt64, "
                << "transaction_count Int32"
                << ") ENGINE MergeTree PRIMARY KEY(workchain, shard, seqno)";
        client.Execute(builder.as_cslice().str());
        LOG(INFO) << "Clickhouse start_up finished";
    } catch (const std::exception& e) {
        LOG(FATAL) << "Clickhouse error: " << e.what();
        std::_Exit(2);
    }

    alarm_timestamp() = td::Timestamp::in(1.0);
}

void InsertManagerClickhouse::alarm() {
    alarm_timestamp() = td::Timestamp::in(1.0);
    td::actor::send_closure(actor_id(this), &InsertManagerClickhouse::schedule_next_insert_batches);
}

void InsertManagerClickhouse::get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise)
{
    LOG(INFO) << "Clickhouse get_existing_seqnos";
    try {
        auto options = get_clickhouse_options();
        clickhouse::Client client(options);
        
        std::vector<std::uint32_t> result;
        client.Select("SELECT seqno from blocks WHERE workchain = -1", [&result](const clickhouse::Block& block) {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                result.push_back(block[0]->As<clickhouse::ColumnInt32>()->At(i));
            }
        });
        promise.set_result(std::move(result));
    } catch (std::exception& e) {
       promise.set_error(td::Status::Error(DB_ERROR, PSLICE() << "Failed to fetch existing seqnos: " << e.what()));
    }
    return;
}

void InsertManagerClickhouse::insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, td::Promise<QueueStatus> queued_promise, td::Promise<td::Unit> inserted_promise) {    
    auto task = InsertTaskStruct{mc_seqno, std::move(block_ds), std::move(inserted_promise)};
    auto status_delta = task.get_queue_status();
    insert_queue_.push(std::move(task));
    queue_status_ += status_delta;
    queued_promise.set_result(queue_status_);
}

bool InsertManagerClickhouse::check_batch_size(QueueStatus &batch_status)
{
    return (
        (batch_status.mc_blocks_ < max_insert_mc_blocks_) &&
        (batch_status.blocks_ < max_insert_blocks_) &&
        (batch_status.txs_ < max_insert_txs_) &&
        (batch_status.msgs_ < max_insert_msgs_)
    );
}

void InsertManagerClickhouse::schedule_next_insert_batches()
{
    while(!insert_queue_.empty() && (parallel_insert_actors_ < max_parallel_insert_actors_)) {
        std::vector<InsertTaskStruct> batch;
        QueueStatus batch_status{0, 0, 0, 0};
        while(!insert_queue_.empty() && check_batch_size(batch_status)) {
            auto task = std::move(insert_queue_.front());
            insert_queue_.pop();

            QueueStatus loc_status = task.get_queue_status();
            batch_status += loc_status;
            queue_status_ -= loc_status;
            batch.push_back(std::move(task));
        }
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R){
            if(R.is_error()) {
                LOG(ERROR) << "Failed to insert batch: " << R.move_as_error();
            }
            td::actor::send_closure(SelfId, &InsertManagerClickhouse::insert_batch_finished);
        });
        
        ++parallel_insert_actors_;
        td::actor::create_actor<InsertBatchClickhouse>(PSLICE() << "insert_batch_clickhouse", std::move(get_clickhouse_options()), std::move(batch), std::move(P)).release();
    }
}

void InsertManagerClickhouse::insert_batch_finished() {
    --parallel_insert_actors_;
    td::actor::send_closure(actor_id(this), &InsertManagerClickhouse::schedule_next_insert_batches);
}

void InsertManagerClickhouse::get_insert_queue_status(td::Promise<QueueStatus> promise) {
    promise.set_result(queue_status_);
}

void InsertManagerClickhouse::upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) {
    promise.set_result(td::Unit());
}

void InsertManagerClickhouse::get_jetton_wallet(std::string address, td::Promise<JettonWalletData> promise) {
}

void InsertManagerClickhouse::upsert_jetton_master(JettonMasterData jetton_wallet, td::Promise<td::Unit> promise) {
    promise.set_result(td::Unit());
}

void InsertManagerClickhouse::get_jetton_master(std::string address, td::Promise<JettonMasterData> promise) {
}

void InsertManagerClickhouse::upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) {
    promise.set_result(td::Unit());
}

void InsertManagerClickhouse::get_nft_collection(std::string address, td::Promise<NFTCollectionData> promise) {
}

void InsertManagerClickhouse::upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) {
    promise.set_result(td::Unit());
}

void InsertManagerClickhouse::get_nft_item(std::string address, td::Promise<NFTItemData> promise) {
}

clickhouse::ClientOptions InsertManagerClickhouse::get_clickhouse_options()
{
    clickhouse::ClientOptions options;
    options.SetHost(credential_.host);
    options.SetPort(credential_.port);
    options.SetUser(credential_.user);
    options.SetPassword(credential_.password);
    options.SetDefaultDatabase(credential_.dbname);
    return std::move(options);
}

void InsertBatchClickhouse::start_up() {
    clickhouse::Client client(client_options_);
    try{
        // TODO: Insert more info
        insert_blocks(client);

        // all done
        for(auto& task_ : insert_tasks_) {
            task_.promise_.set_result(td::Unit());
        }
        promise_.set_result(td::Unit());
    } catch (const std::exception &e) {
        // something failed
        for(auto& task_ : insert_tasks_) {
            task_.promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting to PG: " << e.what()));
        }
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting to PG: " << e.what()));
    }
    stop();
}

void InsertBatchClickhouse::insert_blocks(clickhouse::Client &client){
    clickhouse::Block block;

    // using ColumnHash = clickhouse::ColumnFixedString(44);
    using ColumnOptionalInt32 = clickhouse::ColumnNullableT<clickhouse::ColumnInt32>;

    auto workchain = std::make_shared<clickhouse::ColumnInt32>();
    auto shard = std::make_shared<clickhouse::ColumnInt64>();
    auto seqno = std::make_shared<clickhouse::ColumnInt32>();
    auto root_hash = std::make_shared<clickhouse::ColumnFixedString>(44);
    auto file_hash = std::make_shared<clickhouse::ColumnFixedString>(44);
    auto mc_seqno = std::make_shared<ColumnOptionalInt32>();
    auto gen_utime = std::make_shared<clickhouse::ColumnInt32>();
    auto start_lt = std::make_shared<clickhouse::ColumnUInt64>();
    auto end_lt = std::make_shared<clickhouse::ColumnUInt64>();
    auto transaction_count = std::make_shared<clickhouse::ColumnInt32>();

    for(const auto& task_ : insert_tasks_) {
        for(const auto& blk_ :task_.parsed_block_->blocks_) {
            workchain->Append(blk_.workchain);
            shard->Append(blk_.shard);
            seqno->Append(blk_.seqno);
            root_hash->Append(blk_.root_hash);
            file_hash->Append(blk_.file_hash);
            if (blk_.mc_block_seqno) {
                mc_seqno->Append(blk_.mc_block_seqno.value());
            } else {
                mc_seqno->Append(std::nullopt);
            }
            gen_utime->Append(blk_.gen_utime);
            start_lt->Append(blk_.start_lt);
            end_lt->Append(blk_.end_lt);
            transaction_count->Append(blk_.transactions.size());
        }
    }
    block.AppendColumn("workchain", workchain);
    block.AppendColumn("shard", shard);
    block.AppendColumn("seqno", seqno);
    block.AppendColumn("root_hash", root_hash);
    block.AppendColumn("file_hash", file_hash);
    block.AppendColumn("mc_seqno", mc_seqno);
    block.AppendColumn("gen_utime", gen_utime);
    block.AppendColumn("start_lt", start_lt);
    block.AppendColumn("end_lt", end_lt);
    block.AppendColumn("transaction_count", transaction_count);

    client.Insert("blocks", block);

    for(auto& task_ : insert_tasks_) {
        task_.promise_.set_result(td::Unit());
    }

    stop();
}

QueueStatus InsertTaskStruct::get_queue_status() {
    QueueStatus status = {1, parsed_block_->blocks_.size(), 0, 0};
    for(const auto& blk : parsed_block_->blocks_) {
        status.txs_ += blk.transactions.size();
        for(const auto& tx : blk.transactions) {
            status.msgs_ += tx.out_msgs.size() + (tx.in_msg? 1 : 0);
        }
    }
    return status;
}

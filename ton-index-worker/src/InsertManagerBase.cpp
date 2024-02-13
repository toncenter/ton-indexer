#include "td/utils/StringBuilder.h"
#include "InsertManagerBase.h"


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


void InsertManagerBase::start_up() {
    LOG(INFO) << "InsertManagerBase::start_up called";
    alarm_timestamp() = td::Timestamp::in(1.0);
}

void InsertManagerBase::alarm() {
    alarm_timestamp() = td::Timestamp::in(1.0);
    td::actor::send_closure(actor_id(this), &InsertManagerBase::schedule_next_insert_batches);
}


void InsertManagerBase::print_info() {
  LOG(INFO) << "Insert manager(parallel=" << max_parallel_insert_actors_
            << ", batch_size=" << batch_blocks_count_
            << ", max_mc_blocks=" << max_insert_mc_blocks_
            << ", max_blocks=" << max_insert_blocks_
            << ", max_txs=" << max_insert_txs_
            << ", max_msgs=" << max_insert_msgs_
            << ")";
}



void InsertManagerBase::insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, td::Promise<QueueStatus> queued_promise, td::Promise<td::Unit> inserted_promise) {    
    auto task = InsertTaskStruct{mc_seqno, std::move(block_ds), std::move(inserted_promise)};
    auto status_delta = task.get_queue_status();
    insert_queue_.push(std::move(task));
    queue_status_ += status_delta;
    queued_promise.set_result(queue_status_);
}


void InsertManagerBase::get_insert_queue_status(td::Promise<QueueStatus> promise) {
    promise.set_result(queue_status_);
}


bool InsertManagerBase::check_batch_size(QueueStatus &batch_status)
{
    return (
        (batch_status.mc_blocks_ < max_insert_mc_blocks_) &&
        (batch_status.blocks_ < max_insert_blocks_) &&
        (batch_status.txs_ < max_insert_txs_) &&
        (batch_status.msgs_ < max_insert_msgs_)
    );
}

void InsertManagerBase::schedule_next_insert_batches()
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
            td::actor::send_closure(SelfId, &InsertManagerBase::insert_batch_finished);
        });
        
        ++parallel_insert_actors_;
        create_insert_actor(std::move(batch), std::move(P));
    }
}

void InsertManagerBase::insert_batch_finished() {
    --parallel_insert_actors_;
    td::actor::send_closure(actor_id(this), &InsertManagerBase::schedule_next_insert_batches);
}

#include "td/utils/StringBuilder.h"
#include "InsertManagerBase.h"


QueueState InsertTaskStruct::get_queue_state() {
    QueueState status = {1, static_cast<std::int32_t>(parsed_block_->blocks_.size()), 0, 0, static_cast<std::int32_t>(parsed_block_->traces_.size())};
    for(const auto& blk : parsed_block_->blocks_) {
        status.txs_ += blk.transactions.size();
        for(const auto& tx : blk.transactions) {
            status.msgs_ += tx.out_msgs.size() + (tx.in_msg ? 1 : 0);
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
    td::actor::send_closure(actor_id(this), &InsertManagerBase::schedule_next_insert_batches, false);
}

void InsertManagerBase::print_info() {
  LOG(INFO) << "Insert manager(parallel=" << max_parallel_insert_actors_
            << ", max_batch_mc_blocks=" << batch_size_.mc_blocks_
            << ", max_batch_blocks=" << batch_size_.blocks_
            << ", max_batch_txs=" << batch_size_.txs_
            << ", max_batch_msgs=" << batch_size_.msgs_
            << ")";
}


void InsertManagerBase::insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, bool force, td::Promise<QueueState> queued_promise, td::Promise<td::Unit> inserted_promise) {
    auto task = InsertTaskStruct{mc_seqno, std::move(block_ds), std::move(inserted_promise)};
    auto status_delta = task.get_queue_state();
    insert_queue_.push(std::move(task));
    queue_state_ += status_delta;
    queued_promise.set_result(queue_state_);

    if (force) {
        schedule_next_insert_batches(false);
    }
}


void InsertManagerBase::get_insert_queue_state(td::Promise<QueueState> promise) {
    promise.set_result(queue_state_);
}


bool InsertManagerBase::check_batch_size(QueueState &batch_state)
{
    return batch_state < batch_size_;
}

void InsertManagerBase::schedule_next_insert_batches(bool full_batch = false)
{
    while(!insert_queue_.empty() && (parallel_insert_actors_ < max_parallel_insert_actors_)) {
        if (check_batch_size(queue_state_) && full_batch)
            break;

        std::vector<InsertTaskStruct> batch;
        QueueState batch_state{0, 0, 0, 0};
        while(!insert_queue_.empty() && check_batch_size(batch_state)) {
            auto task = std::move(insert_queue_.front());
            insert_queue_.pop();

            QueueState loc_state = task.get_queue_state();
            batch_state += loc_state;
            queue_state_ -= loc_state;
            batch.push_back(std::move(task));
        }
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R){
            if(R.is_error()) {
                LOG(ERROR) << "Failed to insert batch: " << R.move_as_error();
            }
            td::actor::send_closure(SelfId, &InsertManagerBase::insert_batch_finished);
        });
        
        ++parallel_insert_actors_;
        LOG(INFO) << "Inserting batch[mb=" << batch_state.mc_blocks_ 
                  << ", b=" << batch_state.blocks_ 
                  << ", txs=" << batch_state.txs_
                  << ", msgs=" << batch_state.msgs_ 
                  << ", traces=" << batch_state.traces_ << "]";
        create_insert_actor(std::move(batch), std::move(P));
    }
}

void InsertManagerBase::insert_batch_finished() {
    --parallel_insert_actors_;
    td::actor::send_closure(actor_id(this), &InsertManagerBase::schedule_next_insert_batches, true);
}

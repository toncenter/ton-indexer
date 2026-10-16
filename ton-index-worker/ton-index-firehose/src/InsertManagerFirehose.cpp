#include "InsertManagerFirehose.h"

#include "td/utils/filesystem.h"
#include "td/utils/misc.h"
#include "td/utils/port/path.h"

namespace {

// Converts one round on whatever scheduler thread runs it.
class ConvertRound : public td::actor::Actor {
 public:
  ConvertRound(ParsedBlockPtr parsed_block, td::Promise<FirehoseBlock> promise)
      : parsed_block_(std::move(parsed_block)), promise_(std::move(promise)) {
  }

  void start_up() override {
    promise_.set_result(make_firehose_block(*parsed_block_));
    stop();
  }

 private:
  ParsedBlockPtr parsed_block_;
  td::Promise<FirehoseBlock> promise_;
};

}  // namespace

void InsertManagerFirehose::insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, bool force,
                                   td::Promise<QueueState> queued_promise, td::Promise<InsertResult> inserted_promise) {
  InsertTaskStruct task{mc_seqno, std::move(block_ds), std::move(inserted_promise)};
  if (mc_seqno < next_seqno_) {
    queued_promise.set_value(QueueState{queue_state_});
    task.promise_.set_value(InsertResult{});
    return;
  }
  queue_state_ += task.get_queue_state();
  if (auto it = pending_.find(mc_seqno); it != pending_.end()) {
    queue_state_ -= it->second.task.get_queue_state();
    it->second.task.promise_.set_error(td::Status::Error("superseded by a newer insert of the same seqno"));
    pending_.erase(it);
  }
  auto generation = next_generation_++;
  auto parsed_block = task.parsed_block_;
  pending_.emplace(mc_seqno, Round{std::move(task), generation, std::nullopt});
  queued_promise.set_value(QueueState{queue_state_});
  auto promise = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), mc_seqno, generation](td::Result<FirehoseBlock> block) {
        td::actor::send_closure(SelfId, &InsertManagerFirehose::converted, mc_seqno, generation, std::move(block));
      });
  td::actor::create_actor<ConvertRound>("convertround", std::move(parsed_block), std::move(promise)).release();
}

void InsertManagerFirehose::converted(std::uint32_t mc_seqno, std::uint64_t generation,
                                      td::Result<FirehoseBlock> block) {
  auto it = pending_.find(mc_seqno);
  if (it == pending_.end() || it->second.generation != generation) {
    return;  // superseded
  }
  it->second.block = std::move(block);
  write_ready();
}

void InsertManagerFirehose::write_ready() {
  while (!pending_.empty() && pending_.begin()->first == next_seqno_ && pending_.begin()->second.block) {
    auto task = std::move(pending_.begin()->second.task);
    auto block = std::move(*pending_.begin()->second.block);
    pending_.erase(pending_.begin());
    queue_state_ -= task.get_queue_state();

    if (block.is_error()) {
      // The scheduler refetches the round, nothing after it is written meanwhile.
      task.promise_.set_error(block.move_as_error_prefix(PSLICE() << "masterchain block " << task.mc_seqno_ << ": "));
      return;
    }
    for (auto& output : outputs_) {
      auto status = output->write(block.ok());
      if (status.is_error()) {
        // Blocks must not be skipped, so the process stops and resumes from the cursor.
        LOG(FATAL) << "Failed to write masterchain block " << task.mc_seqno_ << ": " << status;
      }
    }
    auto status = save_cursor(task.mc_seqno_);
    if (status.is_error()) {
      LOG(FATAL) << "Failed to save cursor " << task.mc_seqno_ << ": " << status;
    }
    task.promise_.set_value(InsertResult{});
    ++next_seqno_;
  }
}

td::Status InsertManagerFirehose::save_cursor(std::uint32_t seqno) {
  if (cursor_path_.empty()) {
    return td::Status::OK();
  }
  auto tmp_path = cursor_path_ + ".tmp";
  TRY_STATUS(td::write_file(tmp_path, std::to_string(seqno)));
  return td::rename(tmp_path, cursor_path_);
}

void InsertManagerFirehose::get_insert_queue_state(td::Promise<QueueState> promise) {
  promise.set_value(QueueState{queue_state_});
}

void InsertManagerFirehose::get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise,
                                                std::int32_t from_seqno, std::int32_t to_seqno) {
  // Bounded range: always written in full, starting from --from.
  next_seqno_ = static_cast<std::uint32_t>(from_seqno);
  promise.set_value({});
}

void InsertManagerFirehose::ensure_resume_state_initialized(td::Promise<bool> promise, std::int32_t from_seqno) {
  next_seqno_ = static_cast<std::uint32_t>(from_seqno);
  promise.set_value(true);
}

void InsertManagerFirehose::get_resume_seqno(td::Promise<ResumeState> promise, std::int32_t from_seqno,
                                             std::int32_t to_seqno) {
  auto cursor = cursor_path_.empty() ? td::Result<std::string>(td::Status::Error("no cursor"))
                                     : td::read_file_str(cursor_path_);
  if (cursor.is_ok()) {
    auto last_seqno = td::to_integer_safe<std::uint32_t>(td::trim(cursor.ok()));
    if (last_seqno.is_error()) {
      promise.set_error(last_seqno.move_as_error_prefix("bad cursor file: "));
      return;
    }
    next_seqno_ = last_seqno.ok() + 1;
    LOG(INFO) << "Resuming after masterchain block " << last_seqno.ok() << " from " << cursor_path_;
  } else if (from_seqno > 0) {
    next_seqno_ = static_cast<std::uint32_t>(from_seqno);
  } else {
    promise.set_error(td::Status::Error("no cursor yet, the first start needs --from"));
    return;
  }
  // Not initialized from the command line: the scheduler restores the
  // TraceAssembler state or prewarms it on the rounds before next_seqno_.
  promise.set_value(ResumeState{next_seqno_, false});
}

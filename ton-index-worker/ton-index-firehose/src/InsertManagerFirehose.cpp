#include "InsertManagerFirehose.h"

#include "td/actor/MultiPromise.h"
#include "td/utils/filesystem.h"
#include "td/utils/format.h"
#include "td/utils/misc.h"
#include "td/utils/port/path.h"
#include "td/utils/Time.h"
#include "rocksdb/perf_level.h"
#include "CellCache.h"

namespace {

// Converts one round on whatever scheduler thread runs it.
class ConvertRound : public td::actor::Actor {
 public:
  ConvertRound(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block, td::Promise<FirehoseBlock> promise)
      : mc_seqno_(mc_seqno), parsed_block_(std::move(parsed_block)), promise_(std::move(promise)) {
  }

  void start_up() override {
    // RocksDB counts into its thread-local perf context on every cell read by
    // default; nothing here reads the counts.
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    td::Timer timer;
    auto block = make_firehose_block(*parsed_block_);
    // Blocks are written in order, so a slow one holds up all after it.
    if (timer.elapsed() > 2.0) {
      std::size_t transactions = 0;
      for (const auto& blk : parsed_block_->blocks_) {
        transactions += blk.transactions.size();
      }
      LOG(WARNING) << "Converting masterchain block " << mc_seqno_ << " took " << td::format::as_time(timer.elapsed())
                   << ": " << parsed_block_->blocks_.size() << " blocks, " << transactions << " transactions";
    }
    promise_.set_result(std::move(block));
    stop();
  }

 private:
  std::uint32_t mc_seqno_;
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
  td::actor::create_actor<ConvertRound>("convertround", mc_seqno, std::move(parsed_block), std::move(promise)).release();
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

    if (block.is_error()) {
      // A block is not written without its account states or with a broken
      // trace. TraceAssembler cannot take a round again, so the process stops
      // and resumes from the cursor.
      LOG(FATAL) << "Failed to convert masterchain block " << task.mc_seqno_ << ": " << block.move_as_error();
    }
    // The round stays in the queue until every output has written it, which
    // bounds the bundles being compressed; its parsed block is not needed.
    auto queue_state = task.get_queue_state();
    task.parsed_block_.reset();
    {
      td::MultiPromise mp;
      auto ig = mp.init_guard();
      ig.add_promise(td::PromiseCreator::lambda(
          [SelfId = actor_id(this), mc_seqno = task.mc_seqno_, queue_state,
           promise = std::move(task.promise_)](td::Result<td::Unit> result) mutable {
            td::actor::send_closure(SelfId, &InsertManagerFirehose::written, mc_seqno, queue_state, std::move(result),
                                    std::move(promise));
          }));
      for (auto& output : outputs_) {
        output->write(block.ok(), ig.get_promise());
      }
    }
    // The cursor lags behind the output: reader-node may still hold the last
    // blocks written to stdout unstored, so after a crash they are written again.
    if (task.mc_seqno_ > kCursorLag && task.mc_seqno_ - kCursorLag > cursor_) {
      auto status = save_cursor(task.mc_seqno_ - kCursorLag);
      if (status.is_error()) {
        LOG(FATAL) << "Failed to save cursor " << task.mc_seqno_ - kCursorLag << ": " << status;
      }
    }
    ++next_seqno_;
    if (task.mc_seqno_ % 1000 == 0) {
      auto stats = firehose::cell_cache_stats();
      LOG(INFO) << "Cell cache after " << task.mc_seqno_ << ": " << stats.hits << " hits, " << stats.misses
                << " misses, " << td::format::as_size(stats.bytes) << "; " << firehose_account_stats();
    }
  }
}

void InsertManagerFirehose::written(std::uint32_t mc_seqno, QueueState queue_state, td::Result<td::Unit> result,
                                    td::Promise<InsertResult> promise) {
  if (result.is_error()) {
    // Blocks must not be skipped, so the process stops and resumes from the cursor.
    LOG(FATAL) << "Failed to write masterchain block " << mc_seqno << ": " << result.move_as_error();
  }
  queue_state_ -= queue_state;
  promise.set_value(InsertResult{});
}

td::Status InsertManagerFirehose::save_cursor(std::uint32_t seqno) {
  if (cursor_path_.empty()) {
    return td::Status::OK();
  }
  auto tmp_path = cursor_path_ + ".tmp";
  TRY_STATUS(td::write_file(tmp_path, std::to_string(seqno)));
  TRY_STATUS(td::rename(tmp_path, cursor_path_));
  cursor_ = seqno;
  return td::Status::OK();
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
    cursor_ = last_seqno.ok();
    next_seqno_ = cursor_ + 1;
    LOG(INFO) << "Resuming after masterchain block " << cursor_ << " from " << cursor_path_;
  } else if (from_seqno > 0) {
    // the cursor never goes below the first start
    cursor_ = static_cast<std::uint32_t>(from_seqno) - 1;
    next_seqno_ = static_cast<std::uint32_t>(from_seqno);
  } else {
    promise.set_error(td::Status::Error("no cursor yet, the first start needs --from"));
    return;
  }
  // Not initialized from the command line: the scheduler restores the
  // TraceAssembler state or prewarms it on the rounds before next_seqno_.
  promise.set_value(ResumeState{next_seqno_, false});
}

#pragma once
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "InsertManagerBase.h"
#include "FirehoseBlock.h"
#include "FirehoseOutput.h"

// Sink of the index pipeline that turns masterchain rounds into Firehose
// blocks. Rounds are converted in parallel, each by its own actor, and written
// strictly by seqno; rounds below the next one to write (TraceAssembler
// prewarm, rounds written before a restart) are acknowledged without output.
class InsertManagerFirehose : public InsertManagerInterface {
 public:
  // cursor_path keeps the last written seqno to resume after a restart; it is
  // empty for bounded ranges, which always start from --from.
  InsertManagerFirehose(std::vector<std::unique_ptr<FirehoseOutput>> outputs, std::string cursor_path)
      : outputs_(std::move(outputs)), cursor_path_(std::move(cursor_path)) {
  }

  void insert(std::uint32_t mc_seqno, ParsedBlockPtr block_ds, bool force, td::Promise<QueueState> queued_promise,
              td::Promise<InsertResult> inserted_promise) override;
  void get_insert_queue_state(td::Promise<QueueState> promise) override;
  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno = 0,
                           std::int32_t to_seqno = 0) override;
  void ensure_resume_state_initialized(td::Promise<bool> promise, std::int32_t from_seqno = 0) override;
  void get_resume_seqno(td::Promise<ResumeState> promise, std::int32_t from_seqno = 0, std::int32_t to_seqno = 0) override;

 private:
  struct Round {
    InsertTaskStruct task;
    std::uint64_t generation;
    std::optional<td::Result<FirehoseBlock>> block;  // set when converted
  };

  std::vector<std::unique_ptr<FirehoseOutput>> outputs_;
  std::string cursor_path_;
  std::uint32_t next_seqno_{0};
  std::uint64_t next_generation_{0};
  std::map<std::uint32_t, Round> pending_;
  QueueState queue_state_;

  void converted(std::uint32_t mc_seqno, std::uint64_t generation, td::Result<FirehoseBlock> block);
  void write_ready();
  td::Status save_cursor(std::uint32_t seqno);
};

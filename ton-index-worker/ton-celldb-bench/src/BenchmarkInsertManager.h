#pragma once

#include "BenchmarkStats.h"
#include "InsertManagerBase.h"

#include <cstdint>
#include <memory>
#include <vector>

class BenchmarkInsertManager final : public InsertManagerBase {
 public:
  BenchmarkInsertManager(std::shared_ptr<BenchmarkStats> stats, std::uint32_t resume_seqno);

  void set_max_data_depth(std::int32_t value);
  void set_latest_states_prepare_parallelism(std::int32_t value);
  void set_latest_states_prepare_chunk_size(std::int32_t value);

  void create_insert_actor(std::vector<InsertTaskStruct> insert_tasks,
                           td::Promise<InsertManagerInterface::InsertResult> promise) override;
  void get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno = 0,
                           std::int32_t to_seqno = 0) override;
  void ensure_resume_state_initialized(td::Promise<bool> promise, std::int32_t from_seqno = 0) override;
  void get_resume_seqno(td::Promise<InsertManagerInterface::ResumeState> promise, std::int32_t from_seqno = 0,
                        std::int32_t to_seqno = 0) override;

 private:
  std::shared_ptr<BenchmarkStats> stats_;
  std::uint32_t resume_seqno_{1};
  std::int32_t max_data_depth_{0};
  std::int32_t latest_states_prepare_parallelism_{4};
  std::int32_t latest_states_prepare_chunk_size_{128};
};

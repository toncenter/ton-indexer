#pragma once
#include <atomic>
#include <memory>
#include "td/actor/actor.h"
#include "td/utils/MpscPollableQueue.h"
#include "DbScanner.h"
#include "ExecutorServer.h"

namespace tvm_executor {

// The only entry point from foreign threads. MpscPollableQueue writers do
// not enter the scheduler or use TON thread IDs. The bound includes queued
// and executing requests; excess work is retried by fireton.
class RequestQueue : public std::enable_shared_from_this<RequestQueue> {
 public:
  explicit RequestQueue(std::size_t max_requests) : max_requests_(max_requests) {
    queue_.init();
  }
  void submit(Execution execution);
  void close() { closed_.store(true); }

 private:
  friend class Executor;
  td::MpscPollableQueue<Execution> queue_;
  const std::size_t max_requests_;
  std::atomic_size_t pending_{0};
  std::atomic_bool closed_{false};
};

// An IO actor drains the transport queue and starts CPU actors, one per
// RPC batch. All database access and account conversion stays on TON workers.
class Executor : public td::actor::Actor, public td::ObserverBase {
 public:
  Executor(std::shared_ptr<RequestQueue> queue, td::actor::ActorId<DbScanner> db_scanner)
      : queue_(std::move(queue)), db_scanner_(db_scanner) {}

 private:
  void start_up() override;
  void tear_down() override;
  void loop() override;
  void notify() override;

  std::shared_ptr<RequestQueue> queue_;
  td::actor::ActorId<DbScanner> db_scanner_;
  td::actor::ActorId<Executor> self_;
};

}  // namespace tvm_executor

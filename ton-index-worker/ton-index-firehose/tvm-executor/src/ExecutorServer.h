#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include "ton/tvm/v1/executor.pb.h"

// Only protobuf and standard-library types cross the gRPC / TON boundary:
// their different Abseil headers must not meet in one translation unit.
namespace tvm_executor {

inline constexpr std::size_t kMaxMessageSize = 256 << 20;

enum class Code { Ok = 0, Cancelled = 1, InvalidArgument = 3, ResourceExhausted = 8,
                  FailedPrecondition = 9, Unimplemented = 12, Internal = 13, Unavailable = 14 };

struct Answer {
  Code code{Code::Ok};
  std::string message;
  ton::tvm::v1::ExecuteResponse response;
};

struct Execution {
  ton::tvm::v1::BlockRequest request;
  std::shared_ptr<std::atomic_bool> cancelled;
  std::function<void(Answer)> complete;
};

// Enqueues the owned request; never waits for the database or runs TON code.
using Handler = std::function<void(Execution)>;

class ExecutorServer {
 public:
  virtual ~ExecutorServer() = default;
  // Called off the scheduler thread, while the scheduler keeps running.
  virtual void shutdown() = 0;
  static std::unique_ptr<ExecutorServer> start(const std::string& addr, Handler handler, std::string* error);
};

}  // namespace tvm_executor

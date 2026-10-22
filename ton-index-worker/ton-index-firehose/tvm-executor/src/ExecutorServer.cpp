#include "ExecutorServer.h"

#include <chrono>
#include <grpcpp/grpcpp.h>
#include "ton/tvm/v1/executor.grpc.pb.h"

namespace tvm_executor {
namespace {

class Call final : public grpc::ServerUnaryReactor {
 public:
  explicit Call(ton::tvm::v1::ExecuteResponse* response) : response_(response) {}

  std::shared_ptr<std::atomic_bool> cancelled = std::make_shared<std::atomic_bool>(false);

  void complete(Answer answer) {
    if (answer.code == Code::Ok) {
      response_->Swap(&answer.response);
    }
    // OnDone may delete this after Finish: do not access members afterwards.
    Finish(grpc::Status(static_cast<grpc::StatusCode>(answer.code), answer.message));
  }

 private:
  void OnCancel() override { cancelled->store(true); }
  void OnDone() override { delete this; }
  ton::tvm::v1::ExecuteResponse* response_;
};

class Service final : public ton::tvm::v1::Executor::CallbackService {
 public:
  explicit Service(Handler handler) : handler_(std::move(handler)) {}

  grpc::ServerUnaryReactor* Execute(grpc::CallbackServerContext*, const ton::tvm::v1::BlockRequest* request,
                                    ton::tvm::v1::ExecuteResponse* response) override {
    auto* call = new Call(response);
    handler_(Execution{*request, call->cancelled, [call](Answer answer) { call->complete(std::move(answer)); }});
    return call;
  }

 private:
  Handler handler_;
};

class Server final : public ExecutorServer {
 public:
  Server(std::unique_ptr<Service> service, std::unique_ptr<grpc::Server> server)
      : service_(std::move(service)), server_(std::move(server)) {
  }

  void shutdown() override {
    // Let active requests drain, then propagate cancellation to stragglers.
    server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
    server_->Wait();
  }

 private:
  std::unique_ptr<Service> service_;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace

std::unique_ptr<ExecutorServer> ExecutorServer::start(const std::string& addr, Handler handler, std::string* error) {
  auto service = std::make_unique<Service>(std::move(handler));
  grpc::ServerBuilder builder;
  int port = 0;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials(), &port);
  builder.SetMaxReceiveMessageSize(kMaxMessageSize);
  builder.SetMaxSendMessageSize(kMaxMessageSize);
  // The router pings every 30 s during a request, which has no deadline; by
  // default a server takes pings without data more often than every 5 min as
  // abuse and closes the connection.
  builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 10000);
  builder.RegisterService(service.get());
  auto server = builder.BuildAndStart();
  if (!server || port == 0) {
    *error = "cannot listen on " + addr;
    return nullptr;
  }
  return std::make_unique<Server>(std::move(service), std::move(server));
}

}  // namespace tvm_executor

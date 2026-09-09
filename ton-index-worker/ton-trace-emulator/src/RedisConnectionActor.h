#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "RedisTransport.h"
#include "td/actor/actor.h"
#include "td/utils/Observer.h"
#include "td/utils/Slice.h"
#include "td/utils/port/SocketFd.h"

struct redisReader;

// A persistent, single-batch connection. Create with ActorOptions::with_poll().
// Input is already encoded on a CPU actor. This actor only does bounded,
// nonblocking socket work and scalar reply parsing on the TD poll worker.
class RedisConnectionActor final : public td::actor::Actor, private td::ObserverBase {
 public:
  explicit RedisConnectionActor(RedisConnectionOptions options);
  ~RedisConnectionActor() override;
  void execute(RedisPipeline data, RedisPipeline publications, td::Promise<td::Unit> promise);

 private:
  enum class Phase { Setup, Data, Publications };
  struct ReaderDeleter {
    void operator()(redisReader* reader) const;
  };
  struct Request {
    RedisPipeline data;
    RedisPipeline publications;
    td::Promise<td::Unit> promise;
  };

  RedisConnectionOptions options_;
  td::actor::ActorId<RedisConnectionActor> self_;
  td::SocketFd socket_;
  std::unique_ptr<redisReader, ReaderDeleter> reader_;
  std::optional<Request> request_;
  RedisPipeline setup_;
  Phase phase_ = Phase::Setup;
  std::size_t written_ = 0;
  std::size_t remaining_replies_ = 0;
  std::optional<td::Status> first_error_;
  td::Timestamp deadline_;
  bool connecting_ = false;
  bool subscribed_ = false;

  void notify() override;
  void start_up() override;
  void loop() override;
  void alarm() override;
  void tear_down() override;
  td::Status connect();
  td::Status drive();
  const RedisPipeline& pipeline() const;
  void start_phase(Phase phase);
  void phase_finished();
  void finish(td::Status status);
  void close_connection();
};

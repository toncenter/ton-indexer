#pragma once

#include <functional>
#include <memory>

#include "td/actor/actor.h"
#include "td/utils/Observer.h"

#include "RedisTransport.h"

struct redisReader;
struct redisReply;

// RESP2 Pub/Sub connection on the TD poll worker. The callback must forward
// the batch to a CPU actor and acknowledge it after processing, even on bad BOC.
class ChannelListener final : public td::actor::Actor, private td::ObserverBase {
 public:
  using Handler = std::function<void(std::vector<std::string>, td::Promise<td::Unit>)>;
  ChannelListener(RedisConnectionOptions options, std::string channel, Handler handler);
  ~ChannelListener() override;

 private:
  enum class Phase { Setup, Subscribe, Listening };
  struct ReaderDeleter {
    void operator()(redisReader* reader) const;
  };

  RedisConnectionOptions options_;
  std::string channel_;
  Handler handler_;
  td::actor::ActorId<ChannelListener> self_;
  td::SocketFd socket_;
  std::unique_ptr<redisReader, ReaderDeleter> reader_;
  RedisPipeline output_;
  std::size_t written_ = 0;
  std::size_t setup_replies_ = 0;
  std::size_t reply_bytes_ = 0;
  Phase phase_ = Phase::Setup;
  td::Timestamp setup_deadline_;
  double retry_delay_ = 0.1;
  bool connecting_ = false;
  bool poll_subscribed_ = false;
  bool delivery_pending_ = false;

  void start_up() override;
  void notify() override;
  void loop() override;
  void alarm() override;
  void tear_down() override;
  td::Status connect();
  td::Status subscribe();
  td::Status drive(std::vector<std::string>& messages);
  td::Status process_reply(const redisReply& reply, std::vector<std::string>& messages);
  void delivery_finished(td::Result<td::Unit> result);
  void reconnect(td::Status error);
  void close_connection();
};

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <hiredis/hiredis.h>
#include <hiredis/read.h>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <sw/redis++/redis++.h>
#include <thread>
#include <vector>

#include "td/utils/port/detail/NativeFd.h"
#include "td/utils/tests.h"

#include "RedisMaterializer.h"
#include "ChannelListener.h"
#include "FinalizedTraceScheduler.h"
#include "ton/ton-tl.hpp"

#if TD_PORT_POSIX
#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

// Start after block parsing so the real scheduler/Redis path can be exercised
// with controlled replies, without opening a TON node database.
struct FinalizedTraceSchedulerTest : FinalizedTraceScheduler {
  std::vector<RedisWritePlan> plans;
  bool events_only;
  FinalizedTraceSchedulerTest(RedisConnectionOptions options, std::vector<RedisWritePlan> input, bool events = false)
      : FinalizedTraceScheduler({}, {}, std::move(options), 100, 100), plans(std::move(input)), events_only(events) {}
  void start_up() override {
    if (events_only) {
      check_db_events();
      td::actor::SchedulerContext::get().stop();
      return;
    }
    next_ = 100;
    block_time_ = 1234;
    block_active_ = busy_ = true;
    for (auto& plan : plans) trace_prepared(100, std::move(plan));
    prepared(td::Result<RedisWriteBatch>(RedisWriteBatch{}));
  }
  void check_db_events() {
    auto block = [](ton::WorkchainId workchain, ton::BlockSeqno seqno) {
      return ton::create_tl_block_id(ton::BlockIdExt{ton::BlockId{workchain, ton::shardIdAll, seqno},
          ton::RootHash::zero(), ton::FileHash::zero()});
    };
    head_ = 100;
    alarm_timestamp() = td::Timestamp::in(10);
    handle_db_event(ton::create_tl_object<ton::ton_api::db_event_blockSigned>(block(ton::masterchainId, 105)));
    handle_db_event(ton::create_tl_object<ton::ton_api::db_event_blockCandidateReceived>(block(ton::masterchainId, 105)));
    handle_db_event(ton::create_tl_object<ton::ton_api::db_event_blockApplied>(block(0, 105)));
    handle_db_event(ton::create_tl_object<ton::ton_api::db_event_blockApplied>(block(ton::masterchainId, 99)));
    ASSERT_EQ(0u, notified_head_);
    ASSERT_TRUE(!alarm_timestamp().is_in_past());

    handle_db_event(ton::create_tl_object<ton::ton_api::db_event_blockApplied>(block(ton::masterchainId, 102)));
    ASSERT_EQ(102u, notified_head_);
    ASSERT_TRUE(alarm_timestamp().is_in_past());

    // Events received while parsing/writing must survive and coalesce, without waking Redis retries early.
    busy_ = true;
    alarm_timestamp() = td::Timestamp::in(10);
    for (auto seqno : {106u, 104u, 106u}) {
      handle_db_event(ton::create_tl_object<ton::ton_api::db_event_blockApplied>(block(ton::masterchainId, seqno)));
    }
    ASSERT_EQ(106u, notified_head_);
    ASSERT_TRUE(!alarm_timestamp().is_in_past());
    got_head(td::Status::Error("temporary catch-up failure"));
    ASSERT_EQ(106u, notified_head_);
    ASSERT_TRUE(!busy_);
  }
};

namespace {
using namespace std::chrono_literals;
constexpr std::size_t kEncodeLimit = 64 * 1024 * 1024;

// A deliberately synchronous Redis peer on its own test thread. The real
// client and heartbeat share the scheduler's single poll worker.
class Peer {
 public:
  explicit Peer(int fd) : fd_(fd), reader_(redisReaderCreate(), redisReaderFree) {
    timeval timeout{3, 0};
    CHECK(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
    CHECK(setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);
  }
  std::vector<std::string> command() {
    while (true) {
      void* raw = nullptr;
      CHECK(redisReaderGetReply(reader_.get(), &raw) == REDIS_OK);
      std::unique_ptr<redisReply, decltype(&freeReplyObject)> reply(static_cast<redisReply*>(raw), freeReplyObject);
      if (reply) {
        CHECK(reply->type == REDIS_REPLY_ARRAY);
        std::vector<std::string> args;
        for (std::size_t i = 0; i < reply->elements; ++i) {
          CHECK(reply->element[i]->type == REDIS_REPLY_STRING);
          args.emplace_back(reply->element[i]->str, reply->element[i]->len);
        }
        return args;
      }
      char buffer[16384];
      auto size = recv(fd_.socket(), buffer, sizeof(buffer), 0);
      CHECK(size > 0);
      CHECK(redisReaderFeed(reader_.get(), buffer, static_cast<std::size_t>(size)) == REDIS_OK);
    }
  }
  void reply(const std::string& data) {
    std::size_t pos = 0;
    while (pos < data.size()) {
      auto size = send(fd_.socket(), data.data() + pos, data.size() - pos,
#ifdef MSG_NOSIGNAL
                       MSG_NOSIGNAL
#else
                       0
#endif
      );
      CHECK(size > 0);
      pos += static_cast<std::size_t>(size);
    }
  }
  void expect_no_command() {
    CHECK(reader_->len == reader_->pos);
    pollfd fd{fd_.socket(), POLLIN, 0};
    CHECK(poll(&fd, 1, 30) == 0);
  }
  void expect_closed() {
    char buffer[64];
    CHECK(recv(fd_.socket(), buffer, sizeof(buffer), 0) == 0);
  }

 private:
  td::NativeFd fd_;
  std::unique_ptr<redisReader, decltype(&redisReaderFree)> reader_;
};

class FakeRedis {
 public:
  FakeRedis() : listener_(::socket(AF_INET, SOCK_STREAM, 0)) {
    CHECK(listener_);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(bind(listener_.socket(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    CHECK(listen(listener_.socket(), 16) == 0);
    socklen_t size = sizeof(address);
    CHECK(getsockname(listener_.socket(), reinterpret_cast<sockaddr*>(&address), &size) == 0);
    port_ = ntohs(address.sin_port);
  }
  ~FakeRedis() {
    join();
  }
  RedisConnectionOptions options(std::string auth = {}, int db = 0) const {
    return parse_redis_connection_options("redis://" + auth + "127.0.0.1:" + std::to_string(port_) + "/" +
                                          std::to_string(db))
        .move_as_ok();
  }
  void run(std::function<void()> script) {
    thread_ = std::thread(std::move(script));
  }
  void join() {
    if (thread_.joinable())
      thread_.join();
  }
  Peer accept() {
    pollfd fd{listener_.socket(), POLLIN, 0};
    CHECK(poll(&fd, 1, 3000) == 1);
    auto accepted = ::accept(listener_.socket(), nullptr, nullptr);
    CHECK(accepted >= 0);
    ++accepted_;
    return Peer(accepted);
  }
  std::atomic<int> accepted_{0};

 private:
  td::NativeFd listener_;
  int port_ = 0;
  std::thread thread_;
};

class AlarmActor : public td::actor::Actor {
 public:
  AlarmActor(double seconds, std::function<void()> callback) : seconds_(seconds), callback_(std::move(callback)) {
  }

 private:
  double seconds_;
  std::function<void()> callback_;
  void start_up() override {
    alarm_timestamp() = td::Timestamp::in(seconds_);
  }
  void alarm() override {
    callback_();
    stop();
  }
};

void alarm_in(double seconds, std::function<void()> callback) {
  td::actor::create_actor<AlarmActor>(td::actor::ActorOptions().with_name("RedisTestAlarm").with_poll(), seconds,
                                      std::move(callback))
      .release();
}

RedisWriteBatch batch(std::string key = "trace", std::string value = "value") {
  RedisWritePlan plan;
  plan.trace_key = std::move(key);
  plan.fields_to_set = {{"node", std::move(value)}};
  plan.raw_external_message_hash = "external-hash";
  plan.publications = {{"streaming_transactions", "notice"}};
  return RedisWriteBatch{{std::move(plan)}};
}

void read_data(Peer& peer, const std::string& key = "trace") {
  ASSERT_EQ(std::vector<std::string>({"HSET", key, "node", "value"}), peer.command());
  ASSERT_EQ(std::vector<std::string>({"SETEX", "tr_in_msg:external-hash", "600", key}), peer.command());
}

void finish_data(Peer& peer) {
  peer.reply(":1\r\n+OK\r\n");
  ASSERT_EQ(std::vector<std::string>({"PUBLISH", "streaming_transactions", "notice"}), peer.command());
  peer.reply(":1\r\n");
}

// No shared external Redis, no FLUSHDB, and all peer waits have deadlines.
void with_materializer(RedisConnectionOptions options, std::size_t limit,
                       std::function<void(td::actor::ActorOwn<RedisMaterializer>&)> start) {
  td::actor::Scheduler scheduler({1});
  td::actor::ActorOwn<RedisMaterializer> materializer;
  scheduler.run_in_context([&] {
    materializer = td::actor::create_actor<RedisMaterializer>("RedisMaterializer", std::move(options), limit);
    alarm_in(5, [] { LOG(FATAL) << "Redis transport test timed out"; });
    start(materializer);
  });
  scheduler.run();
  scheduler.run_in_context([&] { materializer.reset(); });
}

void stop_scheduler() {
  td::actor::SchedulerContext::get().stop();
}

void commit_one_finalized(td::actor::ActorId<RedisMaterializer> id, std::uint32_t seqno,
                          RedisWritePlan plan,
                          td::Promise<std::int64_t> promise) {
  auto write = td::PromiseCreator::lambda(
      [id, seqno, promise = std::move(promise)](td::Result<std::int64_t> result) mutable {
        if (result.is_error()) { promise.set_error(result.move_as_error()); return; }
        td::actor::send_closure(id, &RedisMaterializer::finish_finalized, seqno, 1234, std::move(promise));
      });
  td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace, seqno, std::move(plan), std::move(write));
}
}  // namespace

namespace {
class SubscriptionConsumer : public td::actor::Actor {
 public:
  explicit SubscriptionConsumer(ChannelListener::Handler handler) : handler_(std::move(handler)) {
  }
  void receive(std::vector<std::string> messages, td::Promise<td::Unit> done) {
    handler_(std::move(messages), std::move(done));
  }

 private:
  ChannelListener::Handler handler_;
};

void cpu_alarm_in(double seconds, std::function<void()> callback) {
  td::actor::create_actor<AlarmActor>("SubscriberCpuAlarm", seconds, std::move(callback)).release();
}

void with_subscriber(RedisConnectionOptions options, ChannelListener::Handler handler,
                     std::function<void(td::actor::ActorOwn<ChannelListener>&)> start = {}) {
  td::actor::Scheduler scheduler({1});
  td::actor::ActorOwn<SubscriptionConsumer> consumer;
  td::actor::ActorOwn<ChannelListener> listener;
  scheduler.run_in_context([&] {
    consumer = td::actor::create_actor<SubscriptionConsumer>("SubscriptionConsumer", std::move(handler));
    listener = td::actor::create_actor<ChannelListener>(
        td::actor::ActorOptions().with_name("ChannelListener").with_poll(), std::move(options), "input",
        [self = consumer.get()](std::vector<std::string> messages, td::Promise<td::Unit> done) {
          td::actor::send_closure(self, &SubscriptionConsumer::receive, std::move(messages), std::move(done));
        });
    alarm_in(5, [] { LOG(FATAL) << "Redis subscriber test timed out"; });
    if (start)
      start(listener);
  });
  scheduler.run();
}

std::string subscription_ack() {
  return "*3\r\n$9\r\nsubscribe\r\n$5\r\ninput\r\n:1\r\n";
}

std::string publication(const std::string& value) {
  RedisPipeline encoded;
  encoded.append({"message", "input", value}, kEncodeLimit).ensure();
  return encoded.bytes();
}

void accept_subscription(Peer& peer) {
  ASSERT_EQ(std::vector<std::string>({"SUBSCRIBE", "input"}), peer.command());
  peer.reply(subscription_ack());
}
}  // namespace

TEST(RedisSubscriber, idle_keeps_cpu_and_poll_workers_responsive) {
  td::actor::set_debug(true);
  FakeRedis server;
  server.run([&] {
    auto peer = server.accept();
    accept_subscription(peer);
    // A quiet established subscription must outlive the setup timeout.
    std::this_thread::sleep_for(250ms);
    peer.expect_no_command();
    peer.reply(publication("after-idle"));
    peer.expect_closed();
  });
  std::atomic<bool> cpu_tick{false}, poll_tick{false};
  auto options = server.options();
  options.batch_timeout = 0.15;
  int received = 0;
  double idle_executions = 0;
  std::atomic<bool> idle_checked{false};
  with_subscriber(
      options,
      [&](auto messages, auto done) {
        ASSERT_TRUE(cpu_tick && poll_tick && idle_checked);
        ASSERT_EQ(std::vector<std::string>({"after-idle"}), messages);
        ++received;
        done.set_value(td::Unit());
        stop_scheduler();
      },
      [&](auto&) {
        cpu_alarm_in(0.03, [&] { cpu_tick = true; });
        alarm_in(0.03, [&] { poll_tick = true; });
        alarm_in(0.05, [&] {
          auto stats = td::actor::ActorTypeStatManager::get_stats(1);
          const auto& stat = stats.stats.at(typeid(ChannelListener));
          ASSERT_EQ(1, stat.alive);
          idle_executions = stat.executions;
        });
        alarm_in(0.20, [&] {
          auto stats = td::actor::ActorTypeStatManager::get_stats(1);
          // A quiet socket must not continuously reschedule itself.
          ASSERT_TRUE(stats.stats.at(typeid(ChannelListener)).executions - idle_executions <= 2);
          idle_checked = true;
        });
      });
  td::actor::set_debug(false);
  server.join();
  ASSERT_EQ(1, received);
  ASSERT_EQ(1, server.accepted_.load());
}

TEST(RedisSubscriber, partial_auth_select_and_subscription_with_messages) {
  FakeRedis server;
  std::vector<std::string> expected{"first", std::string("a\0b\r\n", 5), std::string(192 * 1024, 'x'), "last"};
  server.run([&] {
    auto peer = server.accept();
    ASSERT_EQ(std::vector<std::string>({"AUTH", "user", "secret"}), peer.command());
    ASSERT_EQ(std::vector<std::string>({"SELECT", "2"}), peer.command());
    peer.reply("+OK\r\n+O");
    peer.expect_no_command();
    peer.reply("K\r\n");
    ASSERT_EQ(std::vector<std::string>({"SUBSCRIBE", "input"}), peer.command());
    auto bytes = subscription_ack();
    for (const auto& value : expected)
      bytes += publication(value);
    peer.reply(bytes.substr(0, 12));
    peer.expect_no_command();
    peer.reply(bytes.substr(12));
    peer.expect_closed();
  });
  std::vector<std::string> received;
  with_subscriber(server.options("user:secret@", 2), [&](auto messages, auto done) {
    received.insert(received.end(), messages.begin(), messages.end());
    done.set_value(td::Unit());
    if (received.size() == expected.size())
      stop_scheduler();
  });
  server.join();
  ASSERT_EQ(expected, received);
  ASSERT_EQ(1, server.accepted_.load());
}

TEST(RedisSubscriber, burst_is_ordered_and_waits_for_consumer_before_next_batch) {
  FakeRedis server;
  constexpr int count = 1200;
  server.run([&] {
    auto peer = server.accept();
    accept_subscription(peer);
    std::string bytes;
    for (int i = 0; i < count; ++i)
      bytes += publication(std::to_string(i));
    peer.reply(bytes);
    // EOF with more than a turn's worth of replies must still drain in order.
  });
  int received = 0, deliveries = 0;
  std::atomic<bool> poll_tick{false};
  with_subscriber(
      server.options(),
      [&](auto messages, auto done) {
        ++deliveries;
        ASSERT_TRUE(messages.size() <= 256);
        for (const auto& message : messages)
          ASSERT_EQ(std::to_string(received++), message);
        if (deliveries == 1) {
          auto held = std::make_shared<td::Promise<td::Unit>>(std::move(done));
          cpu_alarm_in(0.08, [&, held] {
            ASSERT_EQ(1, deliveries);
            ASSERT_TRUE(poll_tick);
            held->set_value(td::Unit());
          });
        } else {
          done.set_value(td::Unit());
        }
        if (received == count)
          stop_scheduler();
      },
      [&](auto&) { alarm_in(0.02, [&] { poll_tick = true; }); });
  server.join();
  ASSERT_EQ(count, received);
  ASSERT_TRUE(deliveries >= 5);
}

TEST(RedisSubscriber, retries_auth_failure_and_subscription_timeout) {
  FakeRedis server;
  server.run([&] {
    {
      auto peer = server.accept();
      ASSERT_EQ(std::vector<std::string>({"AUTH", "secret"}), peer.command());
      peer.reply("-ERR invalid password\r\n");
      peer.expect_closed();
    }
    {
      auto peer = server.accept();
      ASSERT_EQ(std::vector<std::string>({"AUTH", "secret"}), peer.command());
      peer.reply("+OK\r\n");
      ASSERT_EQ(std::vector<std::string>({"SUBSCRIBE", "input"}), peer.command());
      // No acknowledgement: the setup deadline must reconnect this socket.
      peer.expect_closed();
    }
    auto peer = server.accept();
    ASSERT_EQ(std::vector<std::string>({"AUTH", "secret"}), peer.command());
    peer.reply("+OK\r\n");
    accept_subscription(peer);
    peer.reply(publication("recovered"));
    peer.expect_closed();
  });
  auto options = server.options("secret@");
  options.batch_timeout = 0.15;
  int received = 0;
  with_subscriber(options, [&](auto messages, auto done) {
    ASSERT_EQ(std::vector<std::string>({"recovered"}), messages);
    ++received;
    done.set_value(td::Unit());
    stop_scheduler();
  });
  server.join();
  ASSERT_EQ(1, received);
  ASSERT_EQ(3, server.accepted_.load());
}

TEST(RedisSubscriber, malformed_and_oversized_frames_reconnect_and_resubscribe) {
  FakeRedis server;
  std::vector<std::string> bad_frames{"?bad\r\n", "*1000000000\r\n", "*3\r\n*3\r\n",
                                      "*3\r\n$7\r\nmessage\r\n$5\r\ninput\r\n$2000000\r\n"};
  bad_frames.back().resize(1024 * 1024, 'x');
  server.run([&] {
    for (const auto& frame : bad_frames) {
      auto peer = server.accept();
      accept_subscription(peer);
      peer.reply(frame);
      peer.expect_closed();
    }
    auto peer = server.accept();
    accept_subscription(peer);
    peer.reply(publication("healthy"));
    peer.expect_closed();
  });
  int received = 0;
  with_subscriber(server.options(), [&](auto messages, auto done) {
    ASSERT_EQ(std::vector<std::string>({"healthy"}), messages);
    ++received;
    done.set_value(td::Unit());
    stop_scheduler();
  });
  server.join();
  ASSERT_EQ(1, received);
  ASSERT_EQ(bad_frames.size() + 1, static_cast<std::size_t>(server.accepted_.load()));
}

TEST(RedisSubscriber, shutdown_with_unacknowledged_delivery_closes_socket) {
  FakeRedis server;
  server.run([&] {
    auto peer = server.accept();
    accept_subscription(peer);
    peer.reply(publication("pending"));
    peer.expect_closed();
  });
  td::actor::ActorOwn<ChannelListener>* listener_ptr = nullptr;
  with_subscriber(
      server.options(),
      [&](auto messages, auto done) {
        ASSERT_EQ(std::vector<std::string>({"pending"}), messages);
        auto held = std::make_shared<td::Promise<td::Unit>>(std::move(done));
        listener_ptr->reset();
        cpu_alarm_in(0.03, [held] {
          // A completion after actor destruction, still in scheduler context.
          held->set_value(td::Unit());
          stop_scheduler();
        });
      },
      [&](auto& listener) { listener_ptr = &listener; });
  server.join();
}

TEST(RedisSubscriber, shutdown_during_setup_closes_socket) {
  FakeRedis server;
  std::atomic<bool> subscribed{false};
  server.run([&] {
    auto peer = server.accept();
    ASSERT_EQ(std::vector<std::string>({"SUBSCRIBE", "input"}), peer.command());
    subscribed = true;
    peer.expect_closed();
  });
  with_subscriber(
      server.options(), [](auto, auto) { CHECK(false); },
      [&](auto& listener) {
        alarm_in(0.1, [&] {
          ASSERT_TRUE(subscribed);
          listener.reset();
          alarm_in(0.02, [] { stop_scheduler(); });
        });
      });
  server.join();
  ASSERT_EQ(1, server.accepted_.load());
}

TEST(RedisTransport, binary_safe_encoding_and_limit) {
  RedisPipeline pipeline;
  auto value = std::string("a\0b\r\n$5\r\n", 10);
  pipeline.append({"HSET", "hash", "field", value}, kEncodeLimit).ensure();
  ASSERT_EQ(1u, pipeline.replies());
  auto* reader = redisReaderCreate();
  CHECK(redisReaderFeed(reader, pipeline.bytes().data(), pipeline.bytes().size()) == REDIS_OK);
  void* raw = nullptr;
  CHECK(redisReaderGetReply(reader, &raw) == REDIS_OK);
  auto* reply = static_cast<redisReply*>(raw);
  ASSERT_EQ(4u, reply->elements);
  ASSERT_EQ(value, std::string(reply->element[3]->str, reply->element[3]->len));
  freeReplyObject(reply);
  redisReaderFree(reader);
  ASSERT_TRUE(pipeline.append({"SET", "k", "v"}, 8).is_error());
  ASSERT_EQ(1u, pipeline.replies());
}

TEST(RedisTransport, partial_replies_auth_select_publish_barrier_and_reuse) {
  FakeRedis server;
  server.run([&] {
    auto peer = server.accept();
    ASSERT_EQ(std::vector<std::string>({"AUTH", "user", "secret"}), peer.command());
    ASSERT_EQ(std::vector<std::string>({"SELECT", "2"}), peer.command());
    peer.reply("+OK\r\n+O");
    peer.expect_no_command();
    peer.reply("K\r\n");
    read_data(peer);
    peer.reply(":1\r\n+O");
    peer.expect_no_command();
    peer.reply("K\r\n");
    ASSERT_EQ(std::vector<std::string>({"PUBLISH", "streaming_transactions", "notice"}), peer.command());
    peer.reply(":0\r\n");
    read_data(peer, "second");
    finish_data(peer);
  });
  int completions = 0;
  with_materializer(server.options("user:secret@", 2), 1, [&](auto& materializer) {
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch(),
        [&](td::Status status, RedisWriteBatch result) {
          status.ensure();
          ASSERT_EQ("trace", result.plans.front().trace_key);
          ++completions;
          td::actor::send_closure(
              materializer, &RedisMaterializer::write, batch("second"),
              [&](td::Status second, RedisWriteBatch) {
                second.ensure();
                ++completions;
                stop_scheduler();
              },
              td::Timer());
        },
        td::Timer());
  });
  server.join();
  ASSERT_EQ(2, completions);
  ASSERT_EQ(1, server.accepted_.load());
}

TEST(RedisTransport, command_error_suppresses_publications_and_next_batch_reconnects) {
  FakeRedis server;
  server.run([&] {
    {
      auto peer = server.accept();
      read_data(peer);
      peer.reply("-WRONGTYPE test\r\n+OK\r\n");
      peer.expect_closed();  // No PUBLISH after a failed data command.
    }
    auto peer = server.accept();
    read_data(peer, "second");
    finish_data(peer);
  });
  int completions = 0;
  with_materializer(server.options(), 1, [&](auto& materializer) {
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch(),
        [&](td::Status status, RedisWriteBatch original) {
          ASSERT_TRUE(status.is_error());
          ASSERT_EQ("trace", original.plans.front().trace_key);
          ++completions;
          td::actor::send_closure(
              materializer, &RedisMaterializer::write, batch("second"),
              [&](td::Status second, RedisWriteBatch) {
                second.ensure();
                ++completions;
                stop_scheduler();
              },
              td::Timer());
        },
        td::Timer());
  });
  server.join();
  ASSERT_EQ(2, completions);
  ASSERT_EQ(2, server.accepted_.load());
}

TEST(RedisTransport, new_root_and_version_are_written_before_obsolete_fields_are_deleted) {
  FakeRedis server;
  server.run([&] {
    auto peer = server.accept();
    ASSERT_EQ(std::vector<std::string>({"HSET", "trace", "root_node", "new-root", "new-root", "payload",
                                       "update_seq", "2"}), peer.command());
    ASSERT_EQ(std::vector<std::string>({"HDEL", "trace", "old-root"}), peer.command());
    ASSERT_EQ(std::vector<std::string>({"SETEX", "tr_in_msg:external-hash", "600", "trace"}), peer.command());
    peer.expect_no_command();
    peer.reply(":3\r\n:1\r\n+OK\r\n");
    ASSERT_EQ(std::vector<std::string>({"PUBLISH", "streaming_transactions", "notice"}), peer.command());
    peer.reply(":0\r\n");
  });
  auto replacement = batch();
  replacement.plans.front().fields_to_set = {{"root_node", "new-root"}, {"new-root", "payload"}, {"update_seq", "2"}};
  replacement.plans.front().node_fields_to_delete = {"old-root"};
  with_materializer(server.options(), 1, [&](auto& materializer) {
    td::actor::send_closure(materializer, &RedisMaterializer::write, std::move(replacement),
                            [](td::Status status, RedisWriteBatch) {
                              status.ensure();
                              stop_scheduler();
                            }, td::Timer());
  });
  server.join();
}

TEST(RedisTransport, disconnect_after_partial_reply_does_not_replay_batch) {
  FakeRedis server;
  server.run([&] {
    {
      auto peer = server.accept();
      read_data(peer);
      peer.reply(":1\r\n+O");
    }
    auto peer = server.accept();
    read_data(peer, "second");
    finish_data(peer);
  });
  int completions = 0;
  with_materializer(server.options(), 1, [&](auto& materializer) {
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch(),
        [&](td::Status status, RedisWriteBatch original) {
          ASSERT_TRUE(status.is_error());
          ASSERT_EQ(1u, original.plans.size());
          ++completions;
          td::actor::send_closure(
              materializer, &RedisMaterializer::write, batch("second"),
              [&](td::Status second, RedisWriteBatch) {
                second.ensure();
                ++completions;
                stop_scheduler();
              },
              td::Timer());
        },
        td::Timer());
  });
  server.join();
  ASSERT_EQ(2, completions);
}

TEST(RedisTransport, timeout_and_capacity_do_not_block_poll_worker) {
  FakeRedis server;
  std::atomic<bool> heartbeat{false};
  std::atomic<bool> overflow{false};
  server.run([&] {
    auto peer = server.accept();
    read_data(peer);
    auto until = std::chrono::steady_clock::now() + 1s;
    while ((!heartbeat || !overflow) && std::chrono::steady_clock::now() < until)
      std::this_thread::sleep_for(1ms);
    CHECK(heartbeat && overflow);
    // The only I/O worker must run the batch timeout despite this missing reply.
    peer.expect_closed();
  });
  auto options = server.options();
  options.batch_timeout = 0.2;
  int completions = 0;
  with_materializer(options, 1, [&](auto& materializer) {
    alarm_in(0.02, [&] { heartbeat = true; });
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch(),
        [&](td::Status status, RedisWriteBatch original) {
          ASSERT_TRUE(status.is_error());
          ASSERT_TRUE(status.message().str().find("timed out") != std::string::npos);
          ASSERT_EQ("trace", original.plans.front().trace_key);
          CHECK(heartbeat && overflow);
          ++completions;
          stop_scheduler();
        },
        td::Timer());
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch("overflow"),
        [&](td::Status status, RedisWriteBatch original) {
          ASSERT_TRUE(status.is_error());
          ASSERT_EQ("overflow", original.plans.front().trace_key);
          overflow = true;
          ++completions;
        },
        td::Timer());
  });
  server.join();
  ASSERT_EQ(2, completions);
  ASSERT_EQ(1, server.accepted_.load());
}

TEST(RedisTransport, shutdown_completes_active_batch_once) {
  FakeRedis server;
  std::atomic<bool> received{false};
  server.run([&] {
    auto peer = server.accept();
    read_data(peer);
    received = true;
    peer.expect_closed();
  });
  int completions = 0;
  with_materializer(server.options(), 1, [&](auto& materializer) {
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch(),
        [&](td::Status status, RedisWriteBatch original) {
          ASSERT_TRUE(status.is_error());
          ASSERT_EQ("trace", original.plans.front().trace_key);
          ++completions;
          stop_scheduler();
        },
        td::Timer());
    alarm_in(0.1, [&] {
      CHECK(received);
      materializer.reset();
    });
  });
  server.join();
  ASSERT_EQ(1, completions);
}

TEST(RedisTransport, invalid_auth_never_sends_batch) {
  FakeRedis server;
  server.run([&] {
    auto peer = server.accept();
    ASSERT_EQ(std::vector<std::string>({"AUTH", "secret"}), peer.command());
    peer.reply("-WRONGPASS secret should not be logged\r\n");
    peer.expect_closed();
  });
  with_materializer(server.options("secret@"), 1, [&](auto& materializer) {
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch(),
        [&](td::Status status, RedisWriteBatch) {
          ASSERT_TRUE(status.is_error());
          ASSERT_TRUE(status.message().str().find("secret") == std::string::npos);
          stop_scheduler();
        },
        td::Timer());
  });
  server.join();
}

TEST(RedisTransport, malformed_reply_fails_batch) {
  FakeRedis server;
  server.run([&] {
    auto peer = server.accept();
    read_data(peer);
    peer.reply("?not-resp\r\n");
    peer.expect_closed();
  });
  with_materializer(server.options(), 1, [&](auto& materializer) {
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch(),
        [&](td::Status status, RedisWriteBatch original) {
          ASSERT_TRUE(status.is_error());
          ASSERT_EQ("trace", original.plans.front().trace_key);
          stop_scheduler();
        },
        td::Timer());
  });
  server.join();
}

TEST(RedisTransport, large_request_handles_partial_writes_and_keeps_poll_worker_responsive) {
  FakeRedis server;
  std::atomic<bool> heartbeat{false};
  std::string value(4 * 1024 * 1024, 'x');
  value[777] = '\0';
  server.run([&] {
    auto peer = server.accept();
    auto until = std::chrono::steady_clock::now() + 1s;
    while (!heartbeat && std::chrono::steady_clock::now() < until)
      std::this_thread::sleep_for(1ms);
    CHECK(heartbeat);
    ASSERT_EQ(std::vector<std::string>({"HSET", "trace", "node", value}), peer.command());
    ASSERT_EQ("SETEX", peer.command().front());
    finish_data(peer);
  });
  with_materializer(server.options(), 1, [&](auto& materializer) {
    alarm_in(0.03, [&] { heartbeat = true; });
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch("trace", value),
        [&](td::Status status, RedisWriteBatch original) {
          status.ensure();
          ASSERT_EQ(value, original.plans.front().fields_to_set.front().second);
          stop_scheduler();
        },
        td::Timer());
  });
  server.join();
}

TEST(RedisTransport, many_replies_yield_without_losing_pipeline_order) {
  FakeRedis server;
  constexpr int count = 350;
  server.run([&] {
    auto peer = server.accept();
    for (int i = 0; i < count; ++i)
      read_data(peer, "trace-" + std::to_string(i));
    std::string replies;
    for (int i = 0; i < count; ++i)
      replies += ":1\r\n+OK\r\n";
    peer.reply(replies);
    for (int i = 0; i < count; ++i) {
      ASSERT_EQ(std::vector<std::string>({"PUBLISH", "streaming_transactions", "notice"}), peer.command());
    }
    replies.clear();
    for (int i = 0; i < count; ++i)
      replies += ":1\r\n";
    peer.reply(replies);
  });
  with_materializer(server.options(), 1, [&](auto& materializer) {
    RedisWriteBatch request;
    for (int i = 0; i < count; ++i)
      request.plans.push_back(std::move(batch("trace-" + std::to_string(i)).plans.front()));
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, std::move(request),
        [&](td::Status status, RedisWriteBatch result) {
          status.ensure();
          ASSERT_EQ(static_cast<std::size_t>(count), result.plans.size());
          stop_scheduler();
        },
        td::Timer());
  });
  server.join();
}

TEST(RedisTransport, sixteen_connections_progress_without_sixteen_worker_threads) {
  FakeRedis server;
  std::atomic<bool> heartbeat{false};
  server.run([&] {
    std::vector<Peer> peers;
    for (int i = 0; i < 16; ++i) {
      peers.push_back(server.accept());
      ASSERT_EQ("HSET", peers.back().command().front());
      ASSERT_EQ("SETEX", peers.back().command().front());
    }
    auto until = std::chrono::steady_clock::now() + 1s;
    while (!heartbeat && std::chrono::steady_clock::now() < until)
      std::this_thread::sleep_for(1ms);
    CHECK(heartbeat);
    for (auto& peer : peers)
      finish_data(peer);
  });
  int completions = 0;
  with_materializer(server.options(), 16, [&](auto& materializer) {
    alarm_in(0.03, [&] { heartbeat = true; });
    for (int i = 0; i < 16; ++i) {
      td::actor::send_closure(
          materializer, &RedisMaterializer::write, batch("trace-" + std::to_string(i)),
          [&](td::Status status, RedisWriteBatch) {
            status.ensure();
            if (++completions == 16)
              stop_scheduler();
          },
          td::Timer());
    }
  });
  server.join();
  ASSERT_EQ(16, completions);
  ASSERT_EQ(16, server.accepted_.load());
}

TEST(RedisTransport, oversized_batch_fails_before_opening_a_connection) {
  FakeRedis server;
  auto options = server.options();
  options.max_batch_bytes = 1024;
  with_materializer(options, 1, [&](auto& materializer) {
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, batch("trace", std::string(2048, 'x')),
        [&](td::Status status, RedisWriteBatch original) {
          ASSERT_TRUE(status.is_error());
          ASSERT_EQ(2048u, original.plans.front().fields_to_set.front().second.size());
          stop_scheduler();
        },
        td::Timer());
  });
  ASSERT_EQ(0, server.accepted_.load());
}

TEST(RedisTransport, real_redis_data_indexes_lua_publications_and_cleanup) {
  // Opt in with an isolated Redis URI. The normal suite uses only FakeRedis.
  const auto* uri = std::getenv("TON_REDIS_TRANSPORT_TEST_URI");
  if (!uri) {
    LOG(INFO) << "Skipping real Redis integration: TON_REDIS_TRANSPORT_TEST_URI is unset";
    return;
  }
  auto options = parse_redis_connection_options(uri).move_as_ok();
  sw::redis::ConnectionOptions client_options = sw::redis::Uri(uri).connection_options();
  client_options.socket_timeout = 2s;
  sw::redis::Redis redis(client_options);
  const auto prefix = "redis-transport-test:" + std::to_string(getpid()) + ":";
  const auto trace_key = prefix + "trace";
  const auto index_key = prefix + "index";
  const auto member = prefix + "member";
  const auto stale_member = prefix + "stale";
  const auto external_hash = prefix + "external";
  const auto account = prefix + "account";
  const auto channel = prefix + "events";
  const auto binary = std::string("state\0binary", 12);
  redis.hset(trace_key, "obsolete", "old");
  redis.zadd(index_key, stale_member, 1);

  std::vector<std::pair<std::string, std::string>> notices;
  auto subscriber = redis.subscriber();
  subscriber.on_message(
      [&](std::string chan, std::string message) { notices.emplace_back(std::move(chan), std::move(message)); });
  subscriber.subscribe(channel);
  subscriber.subscribe("streaming_account_states");
  subscriber.consume();
  subscriber.consume();

  RedisWritePlan first;
  first.trace_key = trace_key;
  first.raw_external_message_hash = external_hash;
  first.node_fields_to_delete = {"obsolete"};
  first.fields_to_set = {{"binary", binary}, {"marker", "first"}};
  first.indexes_to_remove = {{index_key, stale_member, 0}};
  first.indexes_to_add = {{index_key, member, 12345678901234567ULL}};
  first.account_states = {{account, 100, FinalityState::Finalized, binary, "interfaces"}};
  first.publications = {{channel, "first"}};
  auto second = first;
  second.fields_to_set = {{"marker", "second"}};
  second.account_states.front().lt = 99;  // The Lua guard must keep the newer state.
  second.account_states.front().state = "stale";
  second.publications = {{channel, "second"}};

  with_materializer(options, 1, [&](auto& materializer) {
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, RedisWriteBatch{{first, second}},
        [&](td::Status status, RedisWriteBatch) {
          status.ensure();
          stop_scheduler();
        },
        td::Timer());
  });
  ASSERT_EQ(binary, *redis.hget(trace_key, "binary"));
  ASSERT_EQ("second", *redis.hget(trace_key, "marker"));
  ASSERT_TRUE(!redis.hget(trace_key, "obsolete"));
  ASSERT_TRUE(!redis.zscore(index_key, stale_member));
  ASSERT_EQ(static_cast<double>(12345678901234567ULL), *redis.zscore(index_key, member));
  ASSERT_EQ(trace_key, *redis.get("tr_in_msg:" + external_hash));
  ASSERT_EQ(binary, *redis.hget("account_finalized:" + account, "state"));
  ASSERT_EQ("100", *redis.hget("account_finalized:" + account, "lt"));
  while (notices.size() < 3)
    subscriber.consume();
  ASSERT_EQ("streaming_account_states", notices[0].first);
  ASSERT_EQ(std::make_pair(channel, std::string("first")), notices[1]);
  ASSERT_EQ(std::make_pair(channel, std::string("second")), notices[2]);

  RedisWritePlan cleanup;
  cleanup.trace_key = trace_key;
  cleanup.erase_trace = true;
  cleanup.raw_external_message_hash = external_hash;
  cleanup.indexes_to_remove = {{index_key, member, 0}};
  cleanup.publications = {{channel, "deleted"}};
  with_materializer(options, 1, [&](auto& materializer) {
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, RedisWriteBatch{{cleanup}},
        [&](td::Status status, RedisWriteBatch) {
          status.ensure();
          stop_scheduler();
        },
        td::Timer());
  });
  ASSERT_EQ(0, redis.exists(trace_key));
  ASSERT_EQ(0, redis.exists("tr_in_msg:" + external_hash));
  ASSERT_TRUE(!redis.zscore(index_key, member));
  subscriber.consume();
  ASSERT_EQ(std::make_pair(channel, std::string("deleted")), notices.back());
}

TEST(RedisTransport, finalized_block_commit_deduplicates_and_rejects_gaps_and_type_errors) {
  const auto* uri = std::getenv("TON_FINALIZED_TEST_REDIS_URI");
  if (!uri) { LOG(INFO) << "Skipping finalized Redis integration: TON_FINALIZED_TEST_REDIS_URI is unset"; return; }
  auto options = parse_redis_connection_options(uri).move_as_ok();
  sw::redis::Redis redis(uri);
  // This opt-in test requires a dedicated DB. It never clears unknown data.
  ASSERT_EQ(0, redis.exists("finalized:progress"));
  ASSERT_EQ(0, redis.exists("health:ton-trace-emulator"));
  const auto trace = "finalized-test:" + std::to_string(getpid());
  const auto channel = trace + ":events";
  std::vector<std::string> messages;
  auto subscriber = redis.subscriber();
  subscriber.on_message([&](std::string, std::string msg) { messages.push_back(std::move(msg)); });
  subscriber.subscribe(channel);
  subscriber.consume();
  RedisWritePlan first;
  first.trace_key = trace;
  first.raw_external_message_hash = trace;
  first.replace_trace = true;
  first.expire_seconds = 30;
  first.fields_to_set = {{"update_seq", "100"}, {"old", "one"}};
  first.publications = {{channel, "one"}};
  auto second = first;
  second.fields_to_set = {{"update_seq", "101"}, {"new", "two"}};
  second.publications = {{channel, "two"}};
  auto wrong_type = second;
  wrong_type.indexes_to_add = {{trace + ":wrong-index", "member", 1}};
  redis.set(trace + ":wrong-index", "not a sorted set");
  struct Attempt { std::uint32_t seqno; RedisWritePlan plan; int result; std::string error; };
  std::vector<Attempt> attempts{
      {100, first, 1, {}},
      {100, first, 0, {}},
      {100, second, 0, {}}, // Completed blocks ignore even different payloads.
      {102, second, -1, "SEQNO_GAP"},
      {101, wrong_type, -1, "WRONGTYPE"},
      {101, wrong_type, 1, {}}, // Retry the same job after repairing its index key.
      {100, first, 0, {}},
  };
  with_materializer(std::move(options), 1, [&](auto& materializer) {
    auto id = materializer.get();
    auto run = std::make_shared<std::function<void(std::size_t)>>();
    std::weak_ptr<std::function<void(std::size_t)>> weak_run = run;
    *run = [&, id, weak_run](std::size_t index) {
      if (index == attempts.size()) { stop_scheduler(); return; }
      auto run = weak_run.lock();
      CHECK(run);
      const auto& attempt = attempts[index];
      auto done = td::PromiseCreator::lambda([&, index, run](td::Result<std::int64_t> result) {
        const auto& expected = attempts[index];
        if (expected.result < 0) {
          ASSERT_TRUE(result.is_error());
          ASSERT_TRUE(result.error().message().str().find(expected.error) != std::string::npos);
          ASSERT_EQ("100", redis.hget("finalized:progress", "last").value());
          ASSERT_EQ(0, redis.exists("finalized:written:101"));
          if (expected.error == "WRONGTYPE") {
            // Redis does not roll back preceding commands; the failed job stays unmarked.
            ASSERT_EQ("two", redis.hget(trace, "new").value());
            redis.del(trace + ":wrong-index");
          } else {
            ASSERT_EQ("one", redis.hget(trace, "old").value());
          }
        } else {
          ASSERT_TRUE(result.is_ok());
          ASSERT_EQ(expected.result, result.move_as_ok());
        }
        (*run)(index + 1);
      });
      commit_one_finalized(id, attempt.seqno, attempt.plan, std::move(done));
    };
    auto init = td::PromiseCreator::lambda([run](td::Result<std::int64_t> result) {
      ASSERT_TRUE(result.is_ok());
      ASSERT_EQ(100, result.move_as_ok());
      (*run)(0);
    });
    td::actor::send_closure(id, &RedisMaterializer::initialize_finalized, 100, std::move(init));
  });
  subscriber.consume();
  subscriber.consume();
  ASSERT_EQ(std::vector<std::string>({"one", "two"}), messages);
  ASSERT_EQ("101", redis.hget("finalized:progress", "last").value());
  ASSERT_EQ("101", redis.hget(trace, "update_seq").value());
  ASSERT_TRUE(!redis.hget(trace, "old").has_value());
  ASSERT_EQ("two", redis.hget(trace, "new").value());
  ASSERT_TRUE(redis.zscore(trace + ":wrong-index", "member").has_value());
  ASSERT_EQ("finalized", redis.hget("health:ton-trace-emulator", "mode").value());
  ASSERT_TRUE(redis.ttl(trace) > 0);
  ASSERT_TRUE(!redis.hget("finalized:progress", "configuration"));
  ASSERT_EQ(0, redis.exists("finalized:written:100"));
  ASSERT_EQ(0, redis.exists("finalized:written:101"));
  for (const auto& key : {trace, trace + ":wrong-index", "tr_in_msg:" + trace, std::string("finalized:progress"),
                          std::string("health:ton-trace-emulator")}) redis.del(key);
}

TEST(RedisTransport, finalized_trace_jobs_use_bounded_parallel_connections_and_reuse_them) {
  FakeRedis server;
  std::atomic<int> completed{0};
  bool overflow_rejected = false;
  server.run([&] {
    std::vector<Peer> peers;
    for (int i = 0; i < 3; ++i) peers.push_back(server.accept());
    std::set<std::string> received;
    for (auto& peer : peers) {
      const auto command = peer.command();
      ASSERT_EQ("EVAL", command[0]);
      received.insert(command[6]);
    }
    ASSERT_EQ(std::set<std::string>({"trace-0", "trace-1", "trace-2"}), received);
    ASSERT_EQ(0, completed.load()); // All three requests arrived before any response.
    peers[1].reply(":1\r\n");
    auto reused = peers[1].command();
    ASSERT_EQ("reused", reused[6]); // Only this slot is free; other requests still wait.
    peers[1].reply(":1\r\n");
    peers[2].reply(":1\r\n");
    peers[0].reply(":1\r\n");
  });
  with_materializer(server.options(), 3, [&](auto& writer) {
    auto id = writer.get();
    auto done = std::make_shared<std::function<void(td::Result<std::int64_t>)>>();
    std::weak_ptr<std::function<void(td::Result<std::int64_t>)>> weak = done;
    *done = [&, id, weak](td::Result<std::int64_t> result) {
      ASSERT_TRUE(result.is_ok());
      ASSERT_EQ(1, result.move_as_ok());
      const auto count = ++completed;
      if (count == 1) {
        auto plan = batch("reused").plans.front();
        plan.replace_trace = true;
        auto callback = weak.lock();
        CHECK(callback);
        td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace, 100,
            std::move(plan), td::PromiseCreator::lambda([callback](td::Result<std::int64_t> r) { (*callback)(std::move(r)); }));
      }
      if (count == 4) stop_scheduler();
    };
    for (int i = 0; i < 3; ++i) {
      auto plan = batch("trace-" + std::to_string(i)).plans.front();
      plan.replace_trace = true;
      td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace, 100,
          std::move(plan), td::PromiseCreator::lambda([done](td::Result<std::int64_t> r) { (*done)(std::move(r)); }));
    }
    auto overflow = batch("overflow").plans.front();
    overflow.replace_trace = true;
    td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace, 100,
        std::move(overflow), td::PromiseCreator::lambda([&](td::Result<std::int64_t> r) {
          ASSERT_TRUE(r.is_error());
          ASSERT_TRUE(r.error().message().str().find("capacity exhausted") != std::string::npos);
          overflow_rejected = true;
        }));
  });
  server.join();
  ASSERT_TRUE(overflow_rejected);
  ASSERT_EQ(4, completed.load());
  ASSERT_EQ(3, server.accepted_.load());
}

TEST(RedisTransport, another_producer_resumes_partial_block_without_republishing_or_stale_cleanup) {
  const auto* uri = std::getenv("TON_FINALIZED_TEST_REDIS_URI");
  if (!uri) { LOG(INFO) << "Skipping partial finalized block integration"; return; }
  auto options = parse_redis_connection_options(uri).move_as_ok();
  auto client_options = sw::redis::Uri(uri).connection_options();
  client_options.socket_timeout = 2s;
  sw::redis::Redis redis(client_options);
  ASSERT_EQ(0, redis.exists("finalized:progress"));
  ASSERT_EQ(0, redis.exists("health:ton-trace-emulator"));
  const auto prefix = "finalized-partial:" + std::to_string(getpid());
  auto make_plan = [&](std::string suffix, std::string value) {
    auto plan = batch(prefix + suffix, value).plans.front();
    plan.replace_trace = true;
    plan.expire_seconds = 30;
    plan.raw_external_message_hash = plan.trace_key;
    plan.publications = {{prefix, value}};
    return plan;
  };
  auto a = make_plan(":a", "A");
  auto c = make_plan(":c", "C");
  RedisWritePlan cleanup;
  cleanup.trace_key = prefix + ":b";
  cleanup.raw_external_message_hash = cleanup.trace_key;
  cleanup.erase_trace = true;
  auto recreated = make_plan(":b", "B-new");
  std::vector<std::string> messages;
  auto subscriber = redis.subscriber();
  subscriber.on_message([&](std::string, std::string message) { messages.push_back(std::move(message)); });
  subscriber.subscribe(prefix);
  subscriber.consume();

  // Producer A publishes just one trace, then disappears before completing the block.
  with_materializer(options, 2, [&](auto& writer) {
    const auto id = writer.get();
    auto initialized = td::PromiseCreator::lambda([&, id](td::Result<std::int64_t> result) {
      ASSERT_TRUE(result.is_ok());
      td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace, 200, a,
            td::PromiseCreator::lambda([](td::Result<std::int64_t> r) {
              ASSERT_TRUE(r.is_ok()); ASSERT_EQ(1, r.move_as_ok()); stop_scheduler();
            }));
    });
    td::actor::send_closure(id, &RedisMaterializer::initialize_finalized, 200, std::move(initialized));
  });
  subscriber.consume();
  ASSERT_EQ(std::vector<std::string>({"A"}), messages); // Live notice precedes block completion.
  ASSERT_EQ("199", redis.hget("finalized:progress", "last").value());
  ASSERT_EQ(1, redis.scard("finalized:written:200"));
  ASSERT_TRUE(redis.sismember("finalized:written:200", a.trace_key));
  ASSERT_EQ(-1, redis.ttl("finalized:written:200"));
  redis.del(a.trace_key); // Snapshot TTL may expire while the block is incomplete; dedup must survive.
  redis.set("health:ton-trace-emulator", "wrong type");

  using Step = std::function<void(td::actor::ActorId<RedisMaterializer>, td::Promise<std::int64_t>)>;
  struct Expected { Step step; std::int64_t result; std::string error; };
  std::vector<Expected> steps;
  // The shared bootstrap point wins even if a replacement starts at a newer node head.
  steps.push_back({[](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::initialize_finalized,
      250, std::move(p)); }, 200, {}});
  steps.push_back({[&](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace,
      200, a, std::move(p)); }, 0, {}});
  auto changed_a = make_plan(":a", "different-result");
  steps.push_back({[&](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace,
      200, changed_a, std::move(p)); }, 0, {}}); // First write wins; no result comparison.
  steps.push_back({[&](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace,
      200, cleanup, std::move(p)); }, 1, {}});
  steps.push_back({[&](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace,
      200, c, std::move(p)); }, 1, {}});
  steps.push_back({[](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::finish_finalized,
      202, 1234, std::move(p)); }, -1, "SEQNO_GAP"});
  steps.push_back({[](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::finish_finalized,
      200, 1234, std::move(p)); }, -1, "WRONGTYPE"});
  steps.push_back({[](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::finish_finalized,
      200, 1234, std::move(p)); }, 1, {}});
  steps.push_back({[&](auto id, auto p) { commit_one_finalized(id, 201, recreated, std::move(p)); }, 1, {}});
  steps.push_back({[&](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::write_finalized_trace,
      200, cleanup, std::move(p)); }, 0, {}});
  // Empty blocks need no metadata or written set.
  steps.push_back({[](auto id, auto p) { td::actor::send_closure(id, &RedisMaterializer::finish_finalized,
      202, 1234, std::move(p)); }, 1, {}});
  with_materializer(options, 2, [&](auto& writer) {
    auto id = writer.get();
    auto run = std::make_shared<std::function<void(std::size_t)>>();
    std::weak_ptr<std::function<void(std::size_t)>> weak = run;
    *run = [&, id, weak](std::size_t index) {
      if (index == steps.size()) { stop_scheduler(); return; }
      auto keep = weak.lock(); CHECK(keep);
      auto done = td::PromiseCreator::lambda([&, index, keep](td::Result<std::int64_t> r) {
        if (steps[index].result < 0) {
          ASSERT_TRUE(r.is_error());
          ASSERT_TRUE(r.error().message().str().find(steps[index].error) != std::string::npos);
          ASSERT_EQ("199", redis.hget("finalized:progress", "last").value());
          ASSERT_EQ(3, redis.scard("finalized:written:200"));
          if (steps[index].error == "WRONGTYPE") redis.del("health:ton-trace-emulator");
        } else { ASSERT_TRUE(r.is_ok()); ASSERT_EQ(steps[index].result, r.move_as_ok()); }
        (*keep)(index + 1);
      });
      steps[index].step(id, std::move(done));
    };
    (*run)(0);
  });
  subscriber.consume();
  subscriber.consume();
  ASSERT_EQ(std::vector<std::string>({"A", "C", "B-new"}), messages);
  ASSERT_EQ("202", redis.hget("finalized:progress", "last").value());
  ASSERT_EQ(0, redis.exists("finalized:written:200"));
  ASSERT_EQ(0, redis.exists("finalized:written:201"));
  ASSERT_EQ(0, redis.exists("finalized:written:202"));
  ASSERT_EQ(0, redis.exists(a.trace_key));
  ASSERT_EQ("B-new", redis.hget(cleanup.trace_key, "node").value());
  for (const auto& key : {a.trace_key, cleanup.trace_key, c.trace_key}) {
    redis.del(key); redis.del("tr_in_msg:" + key);
  }
  for (const auto* key : {"finalized:progress", "health:ton-trace-emulator"}) redis.del(key);
}

TEST(RedisTransport, finalized_scheduler_retries_all_redis_errors_without_advancing_early) {
  FakeRedis server;
  server.run([&] {
    auto a = server.accept();
    auto b = server.accept();
    const auto first = a.command();
    const auto failed = b.command();
    ASSERT_EQ("2", first[2]);
    ASSERT_EQ("2", failed[2]);
    b.reply("-ERR NOT_INITIALIZED\r\n");
    b.expect_closed();

    // Keep A in flight so B's retry must use its failed connection's slot.
    // No finish request is allowed while either job is still unacknowledged.
    for (const auto* error : {"SEQNO_GAP", "WRONGTYPE"}) {
      auto retry = server.accept();
      ASSERT_EQ(failed, retry.command());
      retry.reply(std::string("-ERR ") + error + "\r\n");
      retry.expect_closed();
    }
    auto retry = server.accept();
    const auto repeated = retry.command();
    ASSERT_EQ("2", repeated[2]);
    ASSERT_EQ(failed[6], repeated[6]);
    retry.reply(":0\r\n"); // Another producer may already have written it.
    a.reply(":1\r\n");

    for (const std::string error : {"SEQNO_GAP", "NOT_INITIALIZED", "WRONGTYPE", ""}) {
      auto control = server.accept();
      const auto finish = control.command();
      ASSERT_EQ("3", finish[2]);
      ASSERT_EQ("health:ton-trace-emulator", finish[5]);
      ASSERT_EQ("100", finish[6]);
      control.reply(error.empty() ? ":1\r\n" : "-ERR " + error + "\r\n");
      if (!error.empty()) control.expect_closed();
    }
  });
  td::actor::Scheduler scheduler({1});
  td::actor::ActorOwn<FinalizedTraceSchedulerTest> producer;
  scheduler.run_in_context([&] {
    auto a = batch("trace-a").plans.front();
    auto b = batch("trace-b").plans.front();
    a.replace_trace = b.replace_trace = true;
    producer = td::actor::create_actor<FinalizedTraceSchedulerTest>("FinalizedBarrierTest", server.options(),
        std::vector<RedisWritePlan>{std::move(a), std::move(b)});
    alarm_in(10, [] { LOG(FATAL) << "Finalized scheduler test timed out"; });
  });
  scheduler.run();
  scheduler.run_in_context([&] { producer.reset(); });
  server.join();
  ASSERT_EQ(9, server.accepted_.load());
}

TEST(FinalizedTraceScheduler, only_applied_masterchain_events_wake_processing_and_survive_busy_or_failed_catch_up) {
  td::actor::Scheduler scheduler({1});
  td::actor::ActorOwn<FinalizedTraceSchedulerTest> producer;
  scheduler.run_in_context([&] {
    producer = td::actor::create_actor<FinalizedTraceSchedulerTest>("FinalizedEventsTest", RedisConnectionOptions{},
        std::vector<RedisWritePlan>{}, true);
  });
  scheduler.run();
  scheduler.run_in_context([&] { producer.reset(); });
}
#endif

#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"
#include "crypto/vm/boc.h"

#include "td/utils/tests.h"
#include "td/actor/actor.h"
#include "td/utils/base64.h"
#include "crypto/block/block.h"
#include "vm/cells/Cell.h"
#include "convert-utils.h"
#include "ActionDetector.h"
#include "IndexData.h"


TEST(convert, to_raw_address) {
    auto address = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6ccgEBAQEAJAAAQ4ARhy+Ifz/haSyza6FGBWNSde+ZjHy+uBieS1O4PxaPVnA=")).move_as_ok()).move_as_ok();
    auto raw_address_serialized = convert::to_raw_address(vm::load_cell_slice_ref(address));
    CHECK(raw_address_serialized.is_ok());
    ASSERT_EQ("0:8C397C43F9FF0B49659B5D0A302B1A93AF7CCC63E5F5C0C4F25A9DC1F8B47AB3", raw_address_serialized.move_as_ok());
}

TEST(convert, to_raw_address_with_anycast) {
    // tx hash 692263ed0c02006a42c2570c1526dc0968e9ef36849086e7888599f5f7745f3b
    auto address = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6ccgEBAQEAKAAAS74kmhnMAhgIba/WWH4XFusx+cERuqOcvI+CfNEkWIYKL2ECy/pq")).move_as_ok()).move_as_ok();
    auto raw_address_serialized = convert::to_raw_address(vm::load_cell_slice_ref(address));
    CHECK(raw_address_serialized.is_ok());
    ASSERT_EQ("0:249A19CFF5961F85C5BACC7E70446EA8E72F23E09F34491621828BD840B2FE9A", raw_address_serialized.move_as_ok());
}

TEST(convert, to_raw_addr_var_with_anycast) {
    vm::CellBuilder builder;
    CHECK(builder.store_long_bool(3, 2));  // addr_var$11
    CHECK(builder.store_bool_bool(true));  // just$1
    CHECK(builder.store_uint_leq(30, 1));  // anycast depth
    CHECK(builder.store_bool_bool(true));  // rewrite prefix
    CHECK(builder.store_long_bool(1, 9));  // address length
    CHECK(builder.store_long_bool(0, 32)); // workchain
    CHECK(builder.store_bool_bool(false)); // address

    auto raw_address = convert::to_raw_address(vm::load_cell_slice_ref(builder.finalize()));
    CHECK(raw_address.is_ok());
    ASSERT_EQ("var$0:1:8", raw_address.move_as_ok());
}

TEST(convert, rejects_addr_var_with_anycast_deeper_than_address) {
    vm::CellBuilder builder;
    CHECK(builder.store_long_bool(3, 2));  // addr_var$11
    CHECK(builder.store_bool_bool(true));  // just$1
    CHECK(builder.store_uint_leq(30, 1));  // anycast depth
    CHECK(builder.store_bool_bool(true));  // rewrite prefix
    CHECK(builder.store_long_bool(0, 9));  // address length
    CHECK(builder.store_long_bool(0, 32)); // workchain

    auto raw_address = convert::to_raw_address(vm::load_cell_slice_ref(builder.finalize()));
    CHECK(raw_address.is_error());
}

TEST(convert, to_std_address) {
    auto address = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6ccgEBAQEAJAAAQ4ARhy+Ifz/haSyza6FGBWNSde+ZjHy+uBieS1O4PxaPVnA=")).move_as_ok()).move_as_ok();
    auto std_address_serialized = convert::to_std_address(vm::load_cell_slice_ref(address));
    CHECK(std_address_serialized.is_ok());
    td::Bits256 addr;
    addr.from_hex(td::Slice("8C397C43F9FF0B49659B5D0A302B1A93AF7CCC63E5F5C0C4F25A9DC1F8B47AB3"));
    ASSERT_EQ(block::StdAddress(0, addr), std_address_serialized.move_as_ok());
}

TEST(convert, to_std_address_with_anycast) {
    auto address = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6ccgEBAQEAKAAAS74kmhnMAhgIba/WWH4XFusx+cERuqOcvI+CfNEkWIYKL2ECy/pq")).move_as_ok()).move_as_ok();
    auto std_address_serialized = convert::to_std_address(vm::load_cell_slice_ref(address));
    CHECK(std_address_serialized.is_ok());
    td::Bits256 addr;
    addr.from_hex(td::Slice("249A19CFF5961F85C5BACC7E70446EA8E72F23E09F34491621828BD840B2FE9A"));
    ASSERT_EQ(block::StdAddress(0, addr), std_address_serialized.move_as_ok());
}

TEST(TonDbScanner, ActionDetector_parse_jetton_burn) {
  // message payload for tx xiOZW3mVbHkCtgLxQqXVAg4DIgxrTE3j9lHw6H3P/Yg=, correct layout
  auto message_payload = vm::load_cell_slice_ref(vm::std_boc_deserialize(td::base64_decode(
    td::Slice("te6cckEBAgEAOQABZllfB7xUbeTvz/ieq1AezMgZqAEPdvSWQKq0LY5UE5PZzQsh8ADqxh1H03XLREV/xoLz4QEAAaAxtO6I")).move_as_ok()).move_as_ok());

  auto transaction = schema::Transaction();
  transaction.account = block::StdAddress(std::string("EQCk6s76oduqQH_3Y3O7fxjWVoUXtD3Ev6-NbHjjDmfG1drE")); // jetton wallet
  transaction.in_msg = std::make_optional(schema::Message());
  transaction.in_msg->source = "0:87BB7A4B20555A16C72A09C9ECE68590F80075630EA3E9BAE5A222BFE34179F0"; // owner
  auto* descr = std::get_if<schema::TransactionDescr_ord>(&transaction.description);
  CHECK(descr != nullptr);
  descr->aborted = false;

  const std::string jetton_master_raw = "0:BDF3FA8098D129B54B4F73B5BAC5D1E1FD91EB054169C3916DFC8CCD536D1000";
  auto jetton_master_address = block::StdAddress::parse(jetton_master_raw);
  CHECK(jetton_master_address.is_ok());

  schema::JettonWalletDataV2 jetton_wallet;
  jetton_wallet.jetton = jetton_master_address.move_as_ok();

  ActionDetector detector(nullptr, td::Promise<ParsedBlockPtr>());
  auto result = detector.parse_jetton_burn(jetton_wallet, transaction, message_payload);
  CHECK(result.is_ok());
  auto burn = result.move_as_ok();
  ASSERT_EQ(transaction.in_msg->source.value(), burn.owner);
  ASSERT_EQ(convert::to_raw_address(transaction.account), burn.jetton_wallet);
  ASSERT_EQ(jetton_master_raw, burn.jetton_master);
  ASSERT_EQ(6083770390284902059, burn.query_id);
  ASSERT_TRUE(burn.custom_payload.not_null());
  ASSERT_EQ("A3FBB2B6DF19CEAA089D2794A26845822A2284D59B277469D167FFC19D00E44B", burn.custom_payload->get_hash().to_hex());
  CHECK(td::BigIntG<257>(8267792794) == **burn.amount.get());
}



#if TD_PORT_POSIX

#include "DbEventListener.h"
#include "ton/ton-tl.hpp"
#include "tl-utils/tl-utils.hpp"

#include <atomic>
#include <mutex>
#include <thread>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

td::BufferSlice make_applied_event(ton::BlockSeqno seqno) {
  ton::BlockIdExt block_id{ton::BlockId{ton::masterchainId, ton::shardIdAll, seqno}, ton::RootHash::zero(),
                           ton::FileHash::zero()};
  return ton::serialize_tl_object(
      ton::create_tl_object<ton::ton_api::db_event_blockApplied>(ton::create_tl_block_id(block_id)), true);
}

int open_fifo_writer(const std::string& path, double timeout_sec) {
  auto deadline = td::Timestamp::in(timeout_sec);
  while (!deadline.is_in_past()) {
    int fd = ::open(path.c_str(), O_WRONLY | O_NONBLOCK);
    if (fd >= 0) {
      return fd;
    }
    usleep(50'000);
  }
  return -1;
}

}  // namespace

TEST(db_event_listener, delivers_events_and_survives_fifo_recreation) {
  char tmp_dir[] = "/tmp/tondb-scanner-test-XXXXXX";
  CHECK(mkdtemp(tmp_dir) != nullptr);
  const std::string fifo_path = std::string(tmp_dir) + "/db-events.fifo";

  std::mutex mutex;
  std::vector<ton::BlockSeqno> received;
  auto received_count = [&] {
    std::lock_guard<std::mutex> guard(mutex);
    return received.size();
  };

  td::actor::Scheduler scheduler({td::actor::Scheduler::NodeInfo{1, 1}});
  td::actor::ActorOwn<DbEventListener> listener;
  scheduler.run_in_context([&] {
    listener = td::actor::create_actor<DbEventListener>("listener", fifo_path,
        [&](ton::tl_object_ptr<ton::ton_api::db_Event> event) {
          ton::ton_api::downcast_call(*event, td::overloaded(
              [&](ton::ton_api::db_event_blockApplied &ev) {
                std::lock_guard<std::mutex> guard(mutex);
                received.push_back(ton::create_block_id(ev.block_id_).seqno());
              },
              [](auto &) {}));
        });
  });
  auto run_until = [&](std::size_t expected_count) {
    auto deadline = td::Timestamp::in(10.0);
    while (received_count() < expected_count && !deadline.is_in_past()) {
      scheduler.run(0.05);
    }
    ASSERT_EQ(expected_count, received_count());
  };

  std::thread writer([&] {
    int fd = open_fifo_writer(fifo_path, 8.0);
    CHECK(fd >= 0);
    // two whole frames in one write, then one frame split in two writes
    auto ev1 = make_applied_event(101);
    auto ev2 = make_applied_event(102);
    std::string data(ev1.data(), ev1.size());
    data.append(ev2.data(), ev2.size());
    CHECK(::write(fd, data.data(), data.size()) == static_cast<ssize_t>(data.size()));
    auto ev3 = make_applied_event(103);
    CHECK(::write(fd, ev3.data(), 5) == 5);
    usleep(100'000);
    CHECK(::write(fd, ev3.data() + 5, ev3.size() - 5) == static_cast<ssize_t>(ev3.size() - 5));
    ::close(fd);
  });
  run_until(3);
  writer.join();
  {
    std::lock_guard<std::mutex> guard(mutex);
    ASSERT_EQ(101u, received[0]);
    ASSERT_EQ(102u, received[1]);
    ASSERT_EQ(103u, received[2]);
  }

  // the listener must hold an exclusive lock so a second reader backs off
  int second_reader = ::open(fifo_path.c_str(), O_RDONLY | O_NONBLOCK);
  CHECK(second_reader >= 0);
  ASSERT_TRUE(flock(second_reader, LOCK_EX | LOCK_NB) != 0 && errno == EWOULDBLOCK);
  ::close(second_reader);

  // recreated FIFO must be picked up
  CHECK(::unlink(fifo_path.c_str()) == 0);
  CHECK(::mkfifo(fifo_path.c_str(), 0660) == 0);
  std::thread writer2([&] {
    int fd = open_fifo_writer(fifo_path, 8.0);
    CHECK(fd >= 0);
    auto ev = make_applied_event(104);
    CHECK(::write(fd, ev.data(), ev.size()) == static_cast<ssize_t>(ev.size()));
    ::close(fd);
  });
  run_until(4);
  writer2.join();
  {
    std::lock_guard<std::mutex> guard(mutex);
    ASSERT_EQ(104u, received[3]);
  }

  scheduler.run_in_context([&] {
    listener.reset();
    td::actor::SchedulerContext::get().stop();
  });
  scheduler.run();
  ::unlink(fifo_path.c_str());
  ::rmdir(tmp_dir);
}

#endif

int main(int argc, char **argv) {
  td::set_default_failure_signal_handler().ensure();
    auto &runner = td::TestsRunner::get_default();
  runner.run_all();
  return 0;
}

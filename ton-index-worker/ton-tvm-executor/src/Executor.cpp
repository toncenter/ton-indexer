#include "Executor.h"

#include <map>
#include <optional>
#include <google/protobuf/io/coded_stream.h>
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "crypto/block/mc-config.h"
#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "crypto/vm/dict.h"
#include "CellCache.h"
#include "FirehoseBlock.h"

namespace tvm_executor {
namespace {

namespace pbstate = ton::state::v1;
namespace pbtvm = ton::tvm::v1;
using Handle = ton::validator::ConstBlockHandle;
using Cell = td::Ref<vm::Cell>;

// Same as the largest input BOC, so a returned state can be used as input.
constexpr std::size_t kMaxShardAccountBocSize = 1 << 20;

Answer error(Code code, std::string message) {
  return Answer{code, std::move(message), {}};
}

struct ShardState {
  ton::BlockIdExt block_id;
  Cell root;
};

struct McView {
  ShardState state;
  std::unique_ptr<block::ConfigInfo> shards;
  std::map<ton::BlockIdExt, ShardState> shard_states;
};

Cell account_none() {
  vm::CellBuilder cb;
  cb.store_long(0, 1);
  return cb.finalize();
}

// How a call fails its request tells fireton whether to wait. A cell db that
// could not be read may be read on another attempt, or by another backend:
// Unavailable, as are VmError exceptions, which cell loads throw. Cells that
// were read and cannot be parsed would fail the same way again: Internal.
td::Status unreadable(td::Status error) {
  return td::Status::Error(static_cast<int>(Code::Unavailable), error.to_string());
}

td::Status malformed(td::Slice message) {
  return td::Status::Error(static_cast<int>(Code::Internal), message);
}

// The BOC of the ShardAccount over a loaded account tree, none when it is
// over the limit.
td::Result<std::optional<std::string>> shard_account_boc(const td::Ref<vm::Cell>& tree, const td::Bits256& last_trans_hash,
                                                         td::uint64 last_trans_lt) {
  vm::CellBuilder cb;
  if (!cb.store_ref_bool(tree) || !cb.store_bits_bool(last_trans_hash.cbits(), 256) ||
      !cb.store_long_bool(static_cast<td::int64>(last_trans_lt), 64)) {
    return malformed("cannot build a ShardAccount");
  }
  auto boc = vm::std_boc_serialize(cb.finalize());
  if (boc.is_error()) {
    return malformed(PSLICE() << "cannot serialize a ShardAccount: " << boc.error().message());
  }
  if (boc.ok().size() > kMaxShardAccountBocSize) {
    return std::nullopt;
  }
  return boc.ok().as_slice().str();
}

// Synchronous cell operations run on a TON CPU worker, after the actor has
// obtained the shard root and a reader. They never wait on another actor.
// The error status carries the Code of the answer.
td::Result<pbtvm::CallResult> read_account(const pbstate::AccountRef& ref, const ShardState& shard,
                                         vm::CellDbReader& reader) {
  ton::StdSmcAddress addr;
  addr.as_slice().copy_from(td::Slice(ref.address().account_id()));
  block::gen::ShardStateUnsplit::Record state;
  if (!tlb::csr_unpack(vm::load_cell_slice_ref(shard.root), state)) {
    return malformed(PSLICE() << "cannot unpack the state of " << shard.block_id.to_str());
  }
  vm::AugmentedDictionary accounts{vm::load_cell_slice_ref(state.accounts), 256, block::tlb::aug_ShardAccounts};
  td::Ref<vm::Cell> account_root;
  td::Bits256 last_trans_hash = td::Bits256::zero();
  td::uint64 last_trans_lt = 0;
  auto value = accounts.lookup(addr.cbits(), 256);
  // The account tree as the BOC has it, null when it is over the limit, which
  // is found out before every cell of a large account is loaded.
  td::Ref<vm::Cell> tree;
  if (value.not_null()) {
    block::gen::ShardAccount::Record shard_account;
    if (!tlb::csr_unpack(value, shard_account)) {
      return malformed(PSLICE() << "cannot unpack the ShardAccount of " << ref.address().workchain() << ":" << addr.to_hex());
    }
    account_root = shard_account.account;
    last_trans_hash = shard_account.last_trans_hash;
    last_trans_lt = shard_account.last_trans_lt;
    // Read before the account is parsed, so that a failure to read it is
    // not taken for one to parse it.
    auto loaded = firehose::load_cell_tree_up_to(account_root->get_hash(), reader, kMaxShardAccountBocSize);
    if (loaded.is_error()) {
      return unreadable(loaded.move_as_error());
    }
    tree = loaded.move_as_ok();
    if (tree.is_null()) {
      vm::load_cell_slice_ref(account_root);
    }
  } else {
    account_root = account_none();
    tree = account_root;
  }

  pbtvm::CallResult result;
  auto* snapshot = result.mutable_account();
  *snapshot->mutable_source() = ref;
  block_id_to_proto(snapshot->mutable_block(), shard.block_id);
  snapshot->set_gen_utime(state.gen_utime);
  snapshot->set_gen_lt(state.gen_lt);
  auto* account_value = snapshot->mutable_value();
  auto account = account_to_proto(tree.not_null() ? tree : account_root);
  if (account.is_error()) {
    return malformed(account.error().message());
  }
  *account_value->mutable_account() = account.move_as_ok();
  account_value->set_last_trans_hash(last_trans_hash.as_slice().str());
  account_value->set_last_trans_lt(last_trans_lt);
  std::optional<std::string> boc;
  if (tree.not_null()) {
    TRY_RESULT_ASSIGN(boc, shard_account_boc(tree, last_trans_hash, last_trans_lt));
  }
  if (boc) {
    account_value->set_shard_account_boc(std::move(*boc));
  } else {
    account_value->set_boc_omitted(true);
  }
  return result;
}

class RequestActor final : public td::actor::Actor {
 public:
  RequestActor(td::actor::ActorId<DbScanner> db, Execution execution)
      : db_(db), request_(std::move(execution.request)), cancelled_(std::move(execution.cancelled)),
        promise_(td::PromiseCreator::lambda([complete = std::move(execution.complete)](td::Result<Answer> answer) mutable {
          complete(answer.is_ok() ? answer.move_as_ok() : error(Code::Unavailable, answer.move_as_error().to_string()));
        })) {}

 private:
  td::actor::ActorId<DbScanner> db_;
  pbtvm::BlockRequest request_;
  std::shared_ptr<std::atomic_bool> cancelled_;
  td::Promise<Answer> promise_;
  Handle block_;
  Handle loading_handle_;
  std::shared_ptr<vm::CellDbReader> reader_;
  std::map<ton::BlockSeqno, McView> views_;
  ton::BlockSeqno view_seqno_{0};
  Answer answer_;
  int call_index_{0};
  std::size_t response_size_{0};
  bool caught_up_{false};

  // Every database callback sends a message back to this actor. It never
  // accesses request state on a DbScanner/RootDb worker.
  template <class T>
  auto reply(void (RequestActor::*method)(td::Result<T>)) {
    return td::PromiseCreator::lambda([self = actor_id(this), method](td::Result<T> result) {
      td::actor::send_closure(self, method, std::move(result));
    });
  }

  void start_up() override {
    if (check_cancelled()) return;
    if (request_.mc_seqno() == 0 || request_.mc_root_hash().size() != 32 || !request_.has_request()) {
      return finish(error(Code::InvalidArgument, "a block request needs mc_seqno, a 32-byte mc_root_hash and a request"));
    }
    for (const auto& call : request_.request().calls()) {
      if (!call.has_get_account()) {
        return finish(error(Code::Unimplemented, "only get_account is implemented yet"));
      }
      auto point = call.get_account().account().at().point_case();
      if (point == pbstate::At::kBeforeTx || point == pbstate::At::kAfterTx) {
        return finish(error(Code::Unimplemented, "before_tx and after_tx are not implemented yet"));
      }
    }
    alarm_timestamp() = td::Timestamp::in(0.1);
    td::actor::send_closure(db_, &DbScanner::get_last_mc_seqno, reply(&RequestActor::got_head));
  }

  void got_head(td::Result<ton::BlockSeqno> result) {
    if (failed(result)) return;
    if (result.ok() < request_.mc_seqno()) {
      if (caught_up_) {
        return finish(error(Code::Unavailable, PSTRING() << "block " << request_.mc_seqno()
                                                        << " is not reached, head " << result.ok()));
      }
      caught_up_ = true;
      td::actor::send_closure(db_, &DbScanner::request_catch_up, reply(&RequestActor::caught_up),
                              ton::validator::CatchUpMode::Force);
      return;
    }
    td::actor::send_closure(db_, &DbScanner::get_mc_block_handle, request_.mc_seqno(), reply(&RequestActor::got_block));
  }

  void caught_up(td::Result<td::Unit> result) {
    if (failed(result)) return;
    td::actor::send_closure(db_, &DbScanner::get_last_mc_seqno, reply(&RequestActor::got_head));
  }

  void got_block(td::Result<Handle> result) {
    if (failed(result)) return;
    block_ = result.move_as_ok();
    if (request_.mc_root_hash() != block_->id().root_hash.as_slice().str()) {
      return finish(error(Code::FailedPrecondition, PSTRING() << "block " << request_.mc_seqno()
                            << " is " << block_->id().root_hash.to_hex() << " here"));
    }
    td::actor::send_closure(db_, &DbScanner::get_cell_db_reader, reply(&RequestActor::got_reader));
  }

  void got_reader(td::Result<std::shared_ptr<vm::CellDbReader>> result) {
    if (failed(result)) return;
    reader_ = result.move_as_ok();
    yield();
  }

  void got_view_handle(td::Result<Handle> result) {
    if (failed(result)) return;
    loading_handle_ = result.move_as_ok();
    td::actor::send_closure(db_, &DbScanner::get_block_state_root, loading_handle_, reply(&RequestActor::got_view_state));
  }

  void got_view_state(td::Result<Cell> result) {
    if (failed(result)) return;
    auto root = result.move_as_ok();
    auto shards = block::ConfigInfo::extract_config(root, loading_handle_->id(), block::ConfigInfo::needShardHashes);
    if (failed(shards)) return;
    views_.emplace(view_seqno_, McView{{loading_handle_->id(), std::move(root)}, shards.move_as_ok(), {}});
    loading_handle_.reset();
    yield();
  }

  void got_shard_handle(td::Result<Handle> result) {
    if (failed(result)) return;
    loading_handle_ = result.move_as_ok();
    td::actor::send_closure(db_, &DbScanner::get_block_state_root, loading_handle_, reply(&RequestActor::got_shard_state));
  }

  void got_shard_state(td::Result<Cell> result) {
    if (failed(result)) return;
    auto id = loading_handle_->id();
    views_.at(view_seqno_).shard_states.emplace(id, ShardState{id, result.move_as_ok()});
    loading_handle_.reset();
    yield();
  }

  void loop() override {
    if (check_cancelled()) return;
    if (call_index_ == request_.request().calls_size()) return finish(std::move(answer_));
    try {
      next_call();
    } catch (vm::VmVirtError&) {
      finish(error(Code::Unavailable, "pruned cell"));
    } catch (vm::VmError& e) {
      finish(error(Code::Unavailable, PSTRING() << "cell: " << e.get_msg()));
    }
  }

  void next_call() {
    const auto& ref = request_.request().calls(call_index_).get_account().account();
    const auto& address = ref.address();
    if (address.account_id().size() != 32) {
      return invalid(PSTRING() << "account_id of " << address.account_id().size() << " bytes");
    }
    bool before = ref.at().point_case() == pbstate::At::kBeforeBlock;
    view_seqno_ = request_.mc_seqno() - (before ? 1 : 0);
    auto it = views_.find(view_seqno_);
    if (it == views_.end()) {
      if (!before) return got_view_handle(block_);
      // Zerostate has a handle but no archive seqno index entry. Follow the
      // parent of the validated block for every before_block, including N=1.
      if (!block_->inited_prev_left()) {
        return finish(error(Code::Unavailable, "masterchain predecessor is missing"));
      }
      auto prev = block_->one_prev(true);
      if (!prev.is_masterchain() || prev.seqno() != view_seqno_) {
        return finish(error(Code::Unavailable, "invalid masterchain predecessor"));
      }
      td::actor::send_closure(db_, &DbScanner::get_block_handle, prev, reply(&RequestActor::got_view_handle));
      return;
    }
    auto& view = it->second;
    const ShardState* shard = &view.state;
    if (address.workchain() != ton::masterchainId) {
      ton::StdSmcAddress addr;
      addr.as_slice().copy_from(td::Slice(address.account_id()));
      auto hash = view.shards->get_shard_hash(ton::extract_addr_prefix(address.workchain(), addr).as_leaf_shard(), false);
      if (hash.is_null()) {
        return invalid(PSTRING() << "workchain " << address.workchain() << " has no shard at block " << view_seqno_);
      }
      auto id = hash->top_block_id();
      auto found = view.shard_states.find(id);
      if (found == view.shard_states.end()) {
        td::actor::send_closure(db_, &DbScanner::get_block_handle, id, reply(&RequestActor::got_shard_handle));
        return;
      }
      shard = &found->second;
    }
    auto result = read_account(ref, *shard, *reader_);
    if (result.is_error()) {
      auto e = result.move_as_error();
      auto code = e.code() == static_cast<int>(Code::Internal) ? Code::Internal : Code::Unavailable;
      return finish(error(code, PSTRING() << "block " << request_.mc_seqno() << ": " << e.message()));
    }
    append(result.move_as_ok());
  }

  void invalid(std::string message) {
    pbtvm::CallResult result;
    result.mutable_error()->set_code(pbtvm::ERROR_CODE_INVALID_REQUEST);
    result.mutable_error()->set_message(std::move(message));
    append(std::move(result));
  }

  void append(pbtvm::CallResult result) {
    auto size = result.ByteSizeLong();
    response_size_ += 1 + google::protobuf::io::CodedOutputStream::VarintSize64(size) + size;
    if (response_size_ > kMaxMessageSize) {
      // A transport error, never a per-call error: fireton retries smaller
      // batches. Stop before materializing the rest of an oversized reply.
      return finish(error(Code::ResourceExhausted, "batch response exceeds the gRPC message limit"));
    }
    *answer_.response.add_results() = std::move(result);
    ++call_index_;
    yield();
  }

  void alarm() override {
    if (!check_cancelled()) alarm_timestamp() = td::Timestamp::in(0.1);
  }

  bool check_cancelled() {
    if (!cancelled_->load()) return false;
    finish(error(Code::Cancelled, "cancelled by the client"));
    return true;
  }

  template <class T>
  bool failed(td::Result<T>& result) {
    if (check_cancelled()) return true;
    if (result.is_ok()) return false;
    finish(error(Code::Unavailable, PSTRING() << "block " << request_.mc_seqno() << ": " << result.move_as_error()));
    return true;
  }

  void finish(Answer answer) {
    promise_.set_value(std::move(answer));
    stop();
  }
};

}  // namespace

void RequestQueue::submit(Execution execution) {
  if (closed_.load()) {
    execution.complete(error(Code::Unavailable, "executor is shutting down"));
    return;
  }
  if (pending_.fetch_add(1) >= max_requests_) {
    pending_.fetch_sub(1);
    execution.complete(error(Code::Unavailable, "executor is busy"));
    return;
  }
  auto complete = std::move(execution.complete);
  execution.complete = [queue = shared_from_this(), complete = std::move(complete)](Answer answer) mutable {
    queue->pending_.fetch_sub(1);
    complete(std::move(answer));
  };
  queue_.writer_put(std::move(execution));
}

void Executor::start_up() {
  self_ = actor_id(this);
  auto& fd = queue_->queue_.reader_get_event_fd();
  td::actor::SchedulerContext::get().get_poll().subscribe(fd.get_poll_info().extract_pollable_fd(this), td::PollFlags::Read());
  loop();
}

void Executor::tear_down() {
  auto& fd = queue_->queue_.reader_get_event_fd();
  td::actor::SchedulerContext::get().get_poll().unsubscribe(fd.get_poll_info().get_pollable_fd_ref());
}

void Executor::notify() {
  td::actor::send_signals(self_, td::actor::ActorSignals::wakeup());
}

void Executor::loop() {
  auto& queue = queue_->queue_;
  queue.reader_get_event_fd().get_poll_info().sync_with_poll();
  while (auto ready = queue.reader_wait_nonblock()) {
    while (ready-- > 0) {
      td::actor::create_actor<RequestActor>("tvm-request", db_scanner_, queue.reader_get_unsafe()).release();
    }
  }
}

}  // namespace tvm_executor

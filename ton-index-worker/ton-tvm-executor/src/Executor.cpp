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
#include "Calls.h"
#include "FirehoseBlock.h"

namespace tvm_executor {
namespace {

namespace pbstate = ton::state::v1;
namespace pbtvm = ton::tvm::v1;
using Handle = ton::validator::ConstBlockHandle;
using Cell = td::Ref<vm::Cell>;

// Same as the largest input BOC, so a returned state can be used as input.
constexpr std::size_t kMaxShardAccountBocSize = kMaxInputBocSize;

// What a view of the masterchain state needs: the shards of accounts, and the
// config, libraries and prev blocks of VM contexts.
constexpr int kViewConfig = block::ConfigInfo::needShardHashes | block::ConfigInfo::needLibraries |
                            block::ConfigInfo::needCapabilities | block::ConfigInfo::needPrevBlocks;

Answer error(Code code, std::string message) {
  return Answer{code, std::move(message), {}};
}

// A block of the shard of an account, and the state after it.
struct Shard {
  Handle handle;
  Cell root;
  // Of the block, read for VM contexts.
  std::optional<td::Bits256> rand_seed;
};

// The masterchain state after a block.
struct McView {
  ton::BlockIdExt id;
  std::unique_ptr<block::ConfigInfo> config;
};

Cell account_none() {
  vm::CellBuilder cb;
  cb.store_long(0, 1);
  return cb.finalize();
}

td::ConstBitPtr account_id_bits(const pbstate::Address& address) {
  return td::ConstBitPtr(reinterpret_cast<const unsigned char*>(address.account_id().data()));
}

td::Result<block::gen::ShardStateUnsplit::Record> unpack_state(const Cell& root, const ton::BlockIdExt& id) {
  block::gen::ShardStateUnsplit::Record state;
  if (!tlb::csr_unpack(vm::load_cell_slice_ref(root), state)) {
    return malformed(PSLICE() << "cannot unpack the state of " << id.to_str());
  }
  return state;
}

// An account in a shard state.
struct Lookup {
  block::gen::ShardStateUnsplit::Record state;
  // account_none made here when the state has none.
  Cell account;
  bool stored{false};
  td::Bits256 last_trans_hash = td::Bits256::zero();
  td::uint64 last_trans_lt{0};
};

td::Result<Lookup> lookup_account(const Cell& root, const ton::BlockIdExt& id, const pbstate::Address& address) {
  Lookup found;
  TRY_RESULT_ASSIGN(found.state, unpack_state(root, id));
  vm::AugmentedDictionary accounts{vm::load_cell_slice_ref(found.state.accounts), 256, block::tlb::aug_ShardAccounts};
  auto value = accounts.lookup(account_id_bits(address), 256);
  if (value.is_null()) {
    found.account = account_none();
    return found;
  }
  block::gen::ShardAccount::Record shard_account;
  if (!tlb::csr_unpack(value, shard_account)) {
    return malformed(PSLICE() << "cannot unpack the ShardAccount of " << address.workchain() << ":"
                              << td::Bits256(account_id_bits(address)).to_hex());
  }
  found.account = shard_account.account;
  found.stored = true;
  found.last_trans_hash = shard_account.last_trans_hash;
  found.last_trans_lt = shard_account.last_trans_lt;
  return found;
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
td::Result<pbtvm::CallResult> read_account(const pbstate::AccountRef& ref, const ton::BlockIdExt& id, const Cell& root,
                                           vm::CellDbReader& reader) {
  TRY_RESULT(found, lookup_account(root, id, ref.address()));
  // The account tree as the BOC has it, null when it is over the limit, which
  // is found out before every cell of a large account is loaded.
  td::Ref<vm::Cell> tree = found.account;
  if (found.stored) {
    // Read before the account is parsed, so that a failure to read it is
    // not taken for one to parse it.
    auto loaded = firehose::load_cell_tree_up_to(found.account->get_hash(), reader, kMaxShardAccountBocSize);
    if (loaded.is_error()) {
      return unreadable(loaded.move_as_error());
    }
    tree = loaded.move_as_ok();
    if (tree.is_null()) {
      vm::load_cell_slice_ref(found.account);
    }
  }

  pbtvm::CallResult result;
  auto* snapshot = result.mutable_account();
  *snapshot->mutable_source() = ref;
  block_id_to_proto(snapshot->mutable_block(), id);
  snapshot->set_gen_utime(found.state.gen_utime);
  snapshot->set_gen_lt(found.state.gen_lt);
  auto* account_value = snapshot->mutable_value();
  auto account = account_to_proto(tree.not_null() ? tree : found.account);
  if (account.is_error()) {
    return malformed(account.error().message());
  }
  *account_value->mutable_account() = account.move_as_ok();
  account_value->set_last_trans_hash(found.last_trans_hash.as_slice().str());
  account_value->set_last_trans_lt(found.last_trans_lt);
  std::optional<std::string> boc;
  if (tree.not_null()) {
    TRY_RESULT_ASSIGN(boc, shard_account_boc(tree, found.last_trans_hash, found.last_trans_lt));
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
  std::shared_ptr<vm::CellDbReader> reader_;
  std::map<ton::BlockSeqno, McView> views_;
  std::map<ton::BlockIdExt, Shard> shards_;
  // What is being loaded: one thing at a time.
  ton::BlockSeqno loading_seqno_{0};
  Handle loading_;
  // The account of the shard_account_boc of the call at parsed_call_.
  int parsed_call_{-1};
  td::Result<Cell> request_account_;
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
      const pbstate::AccountRef* ref = nullptr;
      if (call.has_get_account()) {
        ref = &call.get_account().account();
      } else if (call.has_get_method()) {
        ref = &call.get_method().account().onchain();
      } else {
        return finish(error(Code::Unimplemented, "emulate is not implemented yet"));
      }
      auto point = ref->at().point_case();
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

  // The loaders give what is loaded already, or null after asking the
  // database for it: its reply yields to loop(), which makes the call again.

  // The masterchain state after block N or N-1.
  McView* view(ton::BlockSeqno seqno) {
    if (auto it = views_.find(seqno); it != views_.end()) {
      return &it->second;
    }
    loading_seqno_ = seqno;
    if (seqno == request_.mc_seqno()) {
      got_view_handle(block_);
      return nullptr;
    }
    // Zerostate has a handle but no archive seqno index entry. Follow the
    // parent of the validated block for every before_block, including N=1.
    if (!block_->inited_prev_left()) {
      finish(error(Code::Unavailable, "masterchain predecessor is missing"));
      return nullptr;
    }
    auto prev = block_->one_prev(true);
    if (!prev.is_masterchain() || prev.seqno() != seqno) {
      finish(error(Code::Unavailable, "invalid masterchain predecessor"));
      return nullptr;
    }
    td::actor::send_closure(db_, &DbScanner::get_block_handle, prev, reply(&RequestActor::got_view_handle));
    return nullptr;
  }

  void got_view_handle(td::Result<Handle> result) {
    if (failed(result)) return;
    loading_ = result.move_as_ok();
    td::actor::send_closure(db_, &DbScanner::get_block_state_root, loading_, reply(&RequestActor::got_view_state));
  }

  void got_view_state(td::Result<Cell> result) {
    if (failed(result)) return;
    auto root = result.move_as_ok();
    auto config = block::ConfigInfo::extract_config(root, loading_->id(), kViewConfig);
    if (failed(config)) return;
    views_.emplace(loading_seqno_, McView{loading_->id(), config.move_as_ok()});
    // Masterchain accounts are in the state of the masterchain block.
    shards_.emplace(loading_->id(), Shard{loading_, std::move(root), {}});
    loading_.reset();
    yield();
  }

  // The block of the shard of an address at a view; an error is the message
  // of INVALID_REQUEST.
  td::Result<ton::BlockIdExt> shard_of(const McView& view, ton::WorkchainId workchain, td::ConstBitPtr account_id) {
    if (workchain == ton::masterchainId) {
      return view.id;
    }
    ton::StdSmcAddress addr;
    addr.bits().copy_from(account_id, 256);
    auto hash = view.config->get_shard_hash(ton::extract_addr_prefix(workchain, addr).as_leaf_shard(), false);
    if (hash.is_null()) {
      return td::Status::Error(PSLICE() << "workchain " << workchain << " has no shard at block " << view.id.seqno());
    }
    return hash->top_block_id();
  }

  Shard* shard(const ton::BlockIdExt& id) {
    if (auto it = shards_.find(id); it != shards_.end()) {
      return &it->second;
    }
    td::actor::send_closure(db_, &DbScanner::get_block_handle, id, reply(&RequestActor::got_shard_handle));
    return nullptr;
  }

  void got_shard_handle(td::Result<Handle> result) {
    if (failed(result)) return;
    loading_ = result.move_as_ok();
    td::actor::send_closure(db_, &DbScanner::get_block_state_root, loading_, reply(&RequestActor::got_shard_state));
  }

  void got_shard_state(td::Result<Cell> result) {
    if (failed(result)) return;
    shards_.emplace(loading_->id(), Shard{loading_, result.move_as_ok(), {}});
    loading_.reset();
    yield();
  }

  const td::Bits256* rand_seed(Shard& shard) {
    if (shard.rand_seed) {
      return &*shard.rand_seed;
    }
    loading_ = shard.handle;
    td::actor::send_closure(db_, &DbScanner::get_block_data, loading_, reply(&RequestActor::got_block_data));
    return nullptr;
  }

  void got_block_data(td::Result<td::Ref<ton::validator::BlockData>> result) {
    if (failed(result)) return;
    block::gen::Block::Record block;
    block::gen::BlockExtra::Record extra;
    if (!tlb::unpack_cell(result.ok()->root_cell(), block) || !tlb::unpack_cell(block.extra, extra)) {
      return finish(error(Code::Internal, PSTRING() << "cannot unpack the extra of block " << loading_->id().to_str()));
    }
    shards_.at(loading_->id()).rand_seed = extra.rand_seed;
    loading_.reset();
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
    const auto& call = request_.request().calls(call_index_);
    if (call.has_get_account()) {
      return next_get_account(call.get_account().account());
    }
    next_get_method(call.get_method());
  }

  ton::BlockSeqno seqno_at(const pbstate::At& at) const {
    return request_.mc_seqno() - (at.point_case() == pbstate::At::kBeforeBlock ? 1 : 0);
  }

  void next_get_account(const pbstate::AccountRef& ref) {
    const auto& address = ref.address();
    if (address.account_id().size() != 32) {
      return invalid(PSTRING() << "account_id of " << address.account_id().size() << " bytes");
    }
    auto* view = this->view(seqno_at(ref.at()));
    if (view == nullptr) return;
    auto id = shard_of(*view, address.workchain(), account_id_bits(address));
    if (id.is_error()) return invalid(id.move_as_error().message().str());
    auto* shard = this->shard(id.ok());
    if (shard == nullptr) return;
    answer(read_account(ref, id.ok(), shard->root, *reader_));
  }

  void next_get_method(const pbtvm::GetMethod& call) {
    // The account, from the network at its point or from the call.
    Cell account;
    // Of an account to run, which its VM context is looked up by.
    std::optional<block::StdAddress> address;
    if (call.account().has_onchain()) {
      const auto& ref = call.account().onchain();
      if (ref.address().account_id().size() != 32) {
        return invalid(PSTRING() << "account_id of " << ref.address().account_id().size() << " bytes");
      }
      auto* view = this->view(seqno_at(ref.at()));
      if (view == nullptr) return;
      auto id = shard_of(*view, ref.address().workchain(), account_id_bits(ref.address()));
      if (id.is_error()) return invalid(id.move_as_error().message().str());
      auto* shard = this->shard(id.ok());
      if (shard == nullptr) return;
      auto found = lookup_account(shard->root, id.ok(), ref.address());
      if (found.is_error()) return answer(found.move_as_error());
      account = found.ok().account;
      if (found.ok().stored) {
        address = block::StdAddress{ref.address().workchain(), td::Bits256(account_id_bits(ref.address()))};
      }
    } else {
      if (parsed_call_ != call_index_) {
        parsed_call_ = call_index_;
        request_account_ = request_account(call.account().shard_account_boc());
      }
      if (request_account_.is_error()) return invalid(request_account_.error().message().str());
      account = request_account_.ok();
      address = account_address(account);
    }
    // The VM context is that of N, in the shard of the account at N, for
    // before_block too.
    std::optional<VmContext> context;
    if (address) {
      auto* view = this->view(request_.mc_seqno());
      if (view == nullptr) return;
      auto id = shard_of(*view, address->workchain, address->addr.cbits());
      if (id.is_error()) return invalid(id.move_as_error().message().str());
      auto* shard = this->shard(id.ok());
      if (shard == nullptr) return;
      auto* seed = rand_seed(*shard);
      if (seed == nullptr) return;
      auto state = unpack_state(shard->root, id.ok());
      if (state.is_error()) return answer(state.move_as_error());
      context = VmContext{view->config.get(), state.ok().gen_utime, state.ok().gen_lt, *seed};
    }
    answer(run_get_method(call, account, call.account().has_shard_account_boc(), context ? &*context : nullptr));
  }

  // The result of a call, or the end of the request with the Code its error carries.
  void answer(td::Result<pbtvm::CallResult> result) {
    if (result.is_error()) {
      auto e = result.move_as_error();
      auto code = e.code() == static_cast<int>(Code::Internal) ? Code::Internal : Code::Unavailable;
      return finish(error(code, PSTRING() << "block " << request_.mc_seqno() << ": " << e.message()));
    }
    append(result.move_as_ok());
  }

  void invalid(std::string message) {
    append(call_error(pbtvm::ERROR_CODE_INVALID_REQUEST, std::move(message)));
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

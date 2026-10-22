#include "Executor.h"

#include <algorithm>
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
#include "Replay.h"

namespace tvm_executor {
namespace {

namespace pbstate = ton::state::v1;
namespace pbtvm = ton::tvm::v1;
using Handle = ton::validator::ConstBlockHandle;
using Cell = td::Ref<vm::Cell>;

// Same as the largest input BOC, so a returned state can be used as input.
constexpr std::size_t kMaxShardAccountBocSize = kMaxInputBocSize;

// What a view of the masterchain state needs: the shards of accounts, and the
// config, libraries and prev blocks of VM contexts and transactions.
constexpr int kViewConfig = block::ConfigInfo::needShardHashes | block::ConfigInfo::needLibraries |
                            block::ConfigInfo::needPrevBlocks | kTransactionConfig;

Answer error(Code code, std::string message) {
  return Answer{code, std::move(message), {}};
}

// A block of the shard of an account, and the state after it.
struct Shard {
  Handle handle;
  Cell root;
  // Loaded lazily for VM contexts and transaction points.
  std::optional<ShardBlockData> data;
  // Lazy cells from an archive BOC hold a weak reference to its database.
  // Keep their BlockData owner alive for every replay using those cells.
  td::Ref<ton::validator::BlockData> data_owner;
};

// The masterchain state after a block.
struct McView {
  ton::BlockIdExt id;
  // Shared with the emulators of the calls.
  std::shared_ptr<block::ConfigInfo> config;
  // Every cell of the config is read: then parsing it meets no database error.
  bool config_loaded{false};
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
  AccountState value;
  bool stored{false};
};

// The state an AccountRef selects, and the block it belongs to.
struct AccountPoint {
  ton::BlockIdExt id;
  Lookup found;
};

// The way back along the shard chain of an account, from its block at N to
// its block at N-1, to the block of a transaction.
struct ChainWalk {
  ton::BlockIdExt current, boundary;
};

// The transactions of an account in a shard block of the round, and its
// states between them as they become known: states[i] is the state before
// transactions[i]. Kept for the batch, so that the points of one account
// replay its block once.
struct ReplayedAccount {
  struct State {
    AccountState value;
    bool stored;
  };
  // Of the state after the block, which snapshots name.
  block::gen::ShardStateUnsplit::Record header;
  std::vector<ReplayTransaction> transactions;
  std::vector<State> states;
  // After the last transaction, from the database.
  std::optional<State> after;
  // Goes on from states.back().
  std::unique_ptr<Replay> replay;
  // Once a transaction does not replay: the answer for the points after it.
  std::optional<std::string> mismatch;
};

td::Result<Lookup> lookup_account(const Cell& root, const ton::BlockIdExt& id, const pbstate::Address& address) {
  Lookup found;
  TRY_RESULT_ASSIGN(found.state, unpack_state(root, id));
  vm::AugmentedDictionary accounts{vm::load_cell_slice_ref(found.state.accounts), 256, block::tlb::aug_ShardAccounts};
  auto value = accounts.lookup(account_id_bits(address), 256);
  if (value.is_null()) {
    found.value.account = account_none();
    return found;
  }
  block::gen::ShardAccount::Record shard_account;
  if (!tlb::csr_unpack(value, shard_account)) {
    return malformed(PSLICE() << "cannot unpack the ShardAccount of " << address.workchain() << ":"
                              << td::Bits256(account_id_bits(address)).to_hex());
  }
  found.value = AccountState{shard_account.account, shard_account.last_trans_hash, shard_account.last_trans_lt};
  found.stored = true;
  return found;
}

// Synchronous cell operations run on a TON CPU worker, after the actor has
// obtained the shard root and a reader. They never wait on another actor.
// The error status carries the Code of the answer.
td::Result<pbtvm::CallResult> read_account(const pbstate::AccountRef& ref, const AccountPoint& point,
                                           vm::CellDbReader& reader) {
  const auto& found = point.found;
  // The account tree as the BOC has it, null when it is over the limit, which
  // is found out before every cell of a large account is loaded.
  td::Ref<vm::Cell> tree = found.value.account;
  if (found.stored) {
    // Read before the account is parsed, so that a failure to read it is
    // not taken for one to parse it.
    auto loaded = firehose::load_cell_tree_up_to(found.value.account->get_hash(), reader, kMaxShardAccountBocSize);
    if (loaded.is_error()) {
      return unreadable(loaded.move_as_error());
    }
    tree = loaded.move_as_ok();
    if (tree.is_null()) {
      vm::load_cell_slice_ref(found.value.account);
    }
  }

  pbtvm::CallResult result;
  auto* snapshot = result.mutable_account();
  *snapshot->mutable_source() = ref;
  block_id_to_proto(snapshot->mutable_block(), point.id);
  snapshot->set_gen_utime(found.state.gen_utime);
  snapshot->set_gen_lt(found.state.gen_lt);
  auto* account_value = snapshot->mutable_value();
  auto account = account_to_proto(tree.not_null() ? tree : found.value.account);
  if (account.is_error()) {
    return malformed(account.error().message());
  }
  *account_value->mutable_account() = account.move_as_ok();
  account_value->set_last_trans_hash(found.value.last_trans_hash.as_slice().str());
  account_value->set_last_trans_lt(found.value.last_trans_lt);
  std::optional<std::string> boc;
  if (tree.not_null()) {
    TRY_RESULT_ASSIGN(boc, shard_account_boc({tree, found.value.last_trans_hash, found.value.last_trans_lt}));
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
  // The points of this batch, by their serialized AccountRef.
  std::map<std::string, AccountPoint> points_;
  std::map<std::pair<ton::BlockIdExt, td::Bits256>, ReplayedAccount> replayed_;
  // Of the call being made, reset by append().
  std::optional<ChainWalk> walk_;
  // What is being loaded: one thing at a time.
  ton::BlockSeqno loading_seqno_{0};
  Handle loading_;
  // The account of the shard_account_boc of the call at parsed_call_.
  int parsed_call_{-1};
  td::Result<AccountState> request_account_;
  int parsed_message_call_{-1};
  td::Result<EmulationMessage> request_message_;
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

  // Reads the config of a view whole before a VM context or a replay uses it;
  // false once answered.
  bool load_config(McView& view) {
    if (!view.config_loaded) {
      auto status = load_tree(view.config->get_root_cell());
      if (status.is_error()) {
        answer(std::move(status));
        return false;
      }
      view.config_loaded = true;
    }
    return true;
  }

  // A historical context is pinned by the master_ref hash from its block.
  McView* historical_view(const ton::BlockIdExt& id) {
    if (auto it = views_.find(id.seqno()); it != views_.end()) {
      if (it->second.id != id) {
        finish(error(Code::Internal, "conflicting masterchain references"));
        return nullptr;
      }
      return &it->second;
    }
    loading_seqno_ = id.seqno();
    td::actor::send_closure(db_, &DbScanner::get_block_handle, id, reply(&RequestActor::got_view_handle));
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
    views_.emplace(loading_seqno_, McView{loading_->id(), std::shared_ptr<block::ConfigInfo>(config.move_as_ok())});
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

  const ShardBlockData* block_data(Shard& shard) {
    if (shard.data) {
      return &*shard.data;
    }
    loading_ = shard.handle;
    td::actor::send_closure(db_, &DbScanner::get_block_data, loading_, reply(&RequestActor::got_block_data));
    return nullptr;
  }

  void got_block_data(td::Result<td::Ref<ton::validator::BlockData>> result) {
    if (failed(result)) return;
    auto parsed = shard_block_data(result.ok()->root_cell(), loading_->id());
    if (parsed.is_error()) return finish(error(Code::Internal, parsed.error().message().str()));
    shards_.at(loading_->id()).data = parsed.move_as_ok();
    shards_.at(loading_->id()).data_owner = result.move_as_ok();
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
    if (call.has_get_method()) {
      return next_get_method(call.get_method());
    }
    if (call.has_emulate()) {
      return next_emulate(call.emulate());
    }
    invalid("call kind is missing");
  }

  ton::BlockSeqno seqno_at(const pbstate::At& at) const {
    return request_.mc_seqno() - (at.point_case() == pbstate::At::kBeforeBlock ? 1 : 0);
  }

  void next_get_account(const pbstate::AccountRef& ref) {
    if (auto* point = account_at(ref)) {
      answer(read_account(ref, *point, *reader_));
    }
  }

  AccountPoint* account_at(const pbstate::AccountRef& ref) {
    const auto key = ref.SerializeAsString();
    if (auto it = points_.find(key); it != points_.end()) {
      return &it->second;
    }
    const auto& address = ref.address();
    if (address.account_id().size() != 32) {
      invalid(PSTRING() << "account_id of " << address.account_id().size() << " bytes");
      return nullptr;
    }
    if (ref.at().has_before_tx() || ref.at().has_after_tx()) {
      return transaction_point(ref, key);
    }
    auto* view = this->view(seqno_at(ref.at()));
    if (view == nullptr) {
      return nullptr;
    }
    auto id = shard_of(*view, address.workchain(), account_id_bits(address));
    if (id.is_error()) {
      invalid(id.error().message().str());
      return nullptr;
    }
    auto* shard = this->shard(id.ok());
    if (shard == nullptr) {
      return nullptr;
    }
    auto found = lookup_account(shard->root, id.ok(), address);
    if (found.is_error()) {
      answer(found.move_as_error());
      return nullptr;
    }
    return &points_.emplace(key, AccountPoint{id.ok(), found.move_as_ok()}).first->second;
  }

  // before_tx and after_tx: the state between transactions of the account in
  // the round, read at the boundaries of its block and replayed within it.
  AccountPoint* transaction_point(const pbstate::AccountRef& ref, const std::string& key) {
    auto lt = ref.at().has_before_tx() ? ref.at().before_tx() : ref.at().after_tx();
    if (lt == 0) {
      invalid("transaction LT must not be zero");
      return nullptr;
    }
    const auto& addr = ref.address();
    block::StdAddress address{addr.workchain(), td::Bits256(account_id_bits(addr))};
    auto* id = transaction_block(address, lt);
    if (id == nullptr) {
      return nullptr;
    }
    auto* account = replayed_account(*id, address);
    if (account == nullptr) {
      return nullptr;
    }
    const auto& transactions = account->transactions;
    auto it = std::find_if(transactions.begin(), transactions.end(), [&](const auto& tx) { return tx.lt == lt; });
    if (it == transactions.end()) {
      invalid("transaction is not in this account's round");
      return nullptr;
    }
    auto index = static_cast<std::size_t>(it - transactions.begin()) + (ref.at().has_after_tx() ? 1 : 0);
    auto* state = state_at(*account, index, *id, address, addr);
    if (state == nullptr) {
      return nullptr;
    }
    AccountPoint point{*id, Lookup{account->header, state->value, state->stored}};
    return &points_.emplace(key, std::move(point)).first->second;
  }

  // The block of the round with the transaction at lt, on the shard chain of
  // the account from N back to N-1; null while blocks load, or once answered.
  const ton::BlockIdExt* transaction_block(const block::StdAddress& address, td::uint64 lt) {
    if (!walk_) {
      auto* current = view(request_.mc_seqno());
      if (current == nullptr) {
        return nullptr;
      }
      auto* previous = view(request_.mc_seqno() - 1);
      if (previous == nullptr) {
        return nullptr;
      }
      auto top = shard_of(*current, address.workchain, address.addr.cbits());
      auto boundary = shard_of(*previous, address.workchain, address.addr.cbits());
      if (top.is_error() || boundary.is_error()) {
        invalid((top.is_error() ? top.error() : boundary.error()).message().str());
        return nullptr;
      }
      walk_ = ChainWalk{top.move_as_ok(), boundary.move_as_ok()};
    }
    while (walk_->current != walk_->boundary) {
      auto* shard = this->shard(walk_->current);
      if (shard == nullptr) {
        return nullptr;
      }
      auto* data = block_data(*shard);
      if (data == nullptr) {
        return nullptr;
      }
      if (lt >= data->start_lt && lt < data->end_lt) {
        return &walk_->current;
      }
      auto predecessor = account_predecessor(*data, address);
      if (predecessor.is_error()) {
        answer(predecessor.move_as_error());
        return nullptr;
      }
      walk_->current = predecessor.move_as_ok();
    }
    invalid("transaction is not in this account's round");
    return nullptr;
  }

  // The block is loaded by transaction_block().
  ReplayedAccount* replayed_account(const ton::BlockIdExt& id, const block::StdAddress& address) {
    auto key = std::make_pair(id, address.addr);
    if (auto it = replayed_.find(key); it != replayed_.end()) {
      return &it->second;
    }
    const auto& shard = shards_.at(id);
    auto transactions = account_transactions(*shard.data, address.addr);
    if (transactions.is_error()) {
      answer(transactions.move_as_error());
      return nullptr;
    }
    if (transactions.ok().empty()) {
      invalid("transaction is not in this account's round");
      return nullptr;
    }
    auto header = unpack_state(shard.root, id);
    if (header.is_error()) {
      answer(header.move_as_error());
      return nullptr;
    }
    ReplayedAccount account;
    account.header = header.move_as_ok();
    account.transactions = transactions.move_as_ok();
    return &replayed_.emplace(key, std::move(account)).first->second;
  }

  // The state before transactions[index], or after the last one for index n;
  // null while blocks load or the replay goes on, one transaction a turn, or
  // once answered. A point after a transaction that does not replay is
  // REPLAY_MISMATCH; the states at the boundaries are read anyway.
  const ReplayedAccount::State* state_at(ReplayedAccount& account, std::size_t index, const ton::BlockIdExt& id,
                                         const block::StdAddress& address, const pbstate::Address& addr) {
    const auto& transactions = account.transactions;
    if (index == transactions.size()) {
      if (!account.after) {
        auto found = lookup_account(shards_.at(id).root, id, addr);
        if (found.is_error()) {
          answer(found.move_as_error());
          return nullptr;
        }
        if (!block_state_matches(found.ok().value, transactions.back().after)) {
          return nullptr;
        }
        account.after = ReplayedAccount::State{found.ok().value, found.ok().stored};
      }
      return &*account.after;
    }
    const auto& data = *shards_.at(id).data;
    if (account.states.empty()) {
      auto predecessor = account_predecessor(data, address);
      if (predecessor.is_error()) {
        answer(predecessor.move_as_error());
        return nullptr;
      }
      auto* before = shard(predecessor.ok());
      if (before == nullptr) {
        return nullptr;
      }
      auto found = lookup_account(before->root, predecessor.ok(), addr);
      if (found.is_error()) {
        answer(found.move_as_error());
        return nullptr;
      }
      if (!block_state_matches(found.ok().value, transactions.front().before)) {
        return nullptr;
      }
      account.states.push_back({found.ok().value, found.ok().stored});
    }
    if (index < account.states.size()) {
      return &account.states[index];
    }
    if (account.mismatch) {
      append(call_error(pbtvm::ERROR_CODE_REPLAY_MISMATCH, *account.mismatch));
      return nullptr;
    }
    if (!account.replay) {
      auto* historical = historical_view(data.master);
      if (historical == nullptr || !load_config(*historical)) {
        return nullptr;
      }
      auto replay = Replay::create(account.states.back().value, address, data, historical->config);
      if (replay.is_error()) {
        answer(replay.move_as_error());
        return nullptr;
      }
      account.replay = replay.move_as_ok();
    }
    auto result = account.replay->advance(transactions[account.states.size() - 1]);
    if (result.is_error()) {
      answer(result.move_as_error());
      return nullptr;
    }
    if (result.ok().empty()) {
      account.states.push_back({account.replay->state(), false});
    } else {
      account.mismatch = result.move_as_ok();
      account.replay.reset();
    }
    yield();
    return nullptr;
  }

  // A state at a block boundary that is not that of the transactions of the
  // block is a database out of order, not a mismatch of a replay: Internal.
  bool block_state_matches(const AccountState& state, const td::Bits256& expected) {
    if (expected == state.account->get_hash().bits()) {
      return true;
    }
    answer(malformed("the account state at a block boundary differs from its transactions"));
    return false;
  }

  void next_get_method(const pbtvm::GetMethod& call) {
    // The account, from the network at its point or from the call.
    AccountState account;
    // Of an account to run, which its VM context is looked up by.
    std::optional<block::StdAddress> address;
    if (call.account().has_onchain()) {
      const auto& ref = call.account().onchain();
      auto* point = account_at(ref);
      if (!point) return;
      account = point->found.value;
      address = account_address(account.account);
    } else {
      if (parsed_call_ != call_index_) {
        parsed_call_ = call_index_;
        request_account_ = request_account(call.account().shard_account_boc());
      }
      if (request_account_.is_error()) return invalid(request_account_.error().message().str());
      account = request_account_.ok();
      address = account_address(account.account);
    }
    auto context = address ? context_for(*address) : std::nullopt;
    if (address && !context) return;
    answer(run_get_method(call, account.account, call.account().has_shard_account_boc(), context ? &*context : nullptr));
  }

  // Public calls always execute in N, even when their account is before N.
  std::optional<VmContext> context_for(const block::StdAddress& address) {
    auto* view = this->view(request_.mc_seqno());
    if (view == nullptr || !load_config(*view)) {
      return std::nullopt;
    }
    auto id = shard_of(*view, address.workchain, address.addr.cbits());
    if (id.is_error()) {
      invalid(id.error().message().str());
      return std::nullopt;
    }
    auto* shard = this->shard(id.ok());
    if (shard == nullptr) {
      return std::nullopt;
    }
    auto* data = block_data(*shard);
    if (data == nullptr) {
      return std::nullopt;
    }
    auto state = unpack_state(shard->root, id.ok());
    if (state.is_error()) {
      answer(state.move_as_error());
      return std::nullopt;
    }
    if (data->rand_seed.is_zero()) {
      answer(malformed("zero block_rand_seed in block context"));
      return std::nullopt;
    }
    return VmContext{view->config, state.ok().gen_utime, state.ok().gen_lt, data->rand_seed};
  }

  void next_emulate(const pbtvm::Emulate& call) {
    if (parsed_message_call_ != call_index_) {
      parsed_message_call_ = call_index_;
      request_message_ = request_message(call.message_boc());
    }
    if (request_message_.is_error()) return invalid(request_message_.error().message().str());
    const auto& message = request_message_.ok();
    AccountState account;
    if (call.account().has_shard_account_boc()) {
      if (parsed_call_ != call_index_) {
        parsed_call_ = call_index_;
        request_account_ = request_account(call.account().shard_account_boc());
      }
      if (request_account_.is_error()) return invalid(request_account_.error().message().str());
      account = request_account_.ok();
    } else {
      if (call.has_account() && !call.account().has_onchain()) return invalid("account source is missing");
      auto ref = call.account().onchain();
      if (ref.has_address() && (ref.address().workchain() != message.destination.workchain ||
          ref.address().account_id() != message.destination.addr.as_slice().str())) {
        return invalid("account address differs from message destination");
      }
      ref.mutable_address()->set_workchain(message.destination.workchain);
      ref.mutable_address()->set_account_id(message.destination.addr.as_slice().str());
      auto* point = account_at(ref);
      if (!point) return;
      account = point->found.value;
    }
    auto context = context_for(message.destination);
    if (!context) return;
    answer(run_emulate(call, message, account, call.account().has_shard_account_boc(), *context));
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
    walk_.reset();
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

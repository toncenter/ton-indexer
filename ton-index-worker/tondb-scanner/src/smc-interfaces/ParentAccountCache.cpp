#include "ParentAccountCache.h"

#include "ParentCellCache.h"

namespace {
struct ParentState {
  schema::AccountState state; // code/data are detached descriptors below
  ParentCellCache::Root code, data;
  uint32_t valid_from = 0;
};

ParentState detach(const schema::AccountState& state, uint32_t seqno) {
  ParentState value{state, ParentCellCache::describe(state.code), ParentCellCache::describe(state.data), seqno};
  value.state.code.clear();
  value.state.data.clear();
  return value;
}

size_t state_bytes(const ParentState& value) {
  constexpr size_t integer_bytes = sizeof(td::BigInt256) + 64;
  return value.state.account_status.capacity() + 1 + integer_bytes +
      value.state.balance.extra_currencies.size() *
      (sizeof(std::pair<const uint32_t, td::RefInt256>) + 4 * sizeof(void*) + integer_bytes);
}

// Bounded, conservative change journal for loads completing behind the head.
// False positives only deny admission. No false negatives: every transaction
// address is inserted, even when the final state failed to load.
struct Changes {
  uint32_t seqno = 0;
  td::Bits256 root_hash{};
  std::array<uint64_t, 128> bits{};
  static std::array<size_t, 3> positions(const block::StdAddress& address) {
    uint64_t h = std::hash<block::StdAddress>{}(address);
    std::array<size_t, 3> result;
    for (auto& position : result) {
      h += 0x9e3779b97f4a7c15ULL;
      auto z = h;
      z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
      z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
      position = (z ^ (z >> 31)) % (128 * 64);
    }
    return result;
  }
  void add(const block::StdAddress& address) {
    for (auto p : positions(address)) bits[p / 64] |= uint64_t(1) << (p % 64);
  }
  bool contains(const block::StdAddress& address) const {
    for (auto p : positions(address)) if (!(bits[p / 64] & (uint64_t(1) << (p % 64)))) return false;
    return true;
  }
};
}  // namespace

struct ParentAccountCache::Impl {
  mutable std::mutex mutex;
  // 32 MiB of the default budget is sufficient for many thousands of parents;
  // the remainder stores shared warmed cell images. Both caches use SLRU.
  memo::Cache<block::StdAddress, ParentState> states;
  ParentCellCache cells;
  std::array<Changes, 128> history;
  uint64_t epoch = 0;
  uint32_t head = 0;
  td::Bits256 head_hash{};
  uint64_t resets = 0, updates = 0, invalidations = 0, bypasses = 0, stale = 0;
  std::array<uint64_t, 5> reported{};

  static size_t available(size_t bytes) {
    return bytes > sizeof(Impl) + 128 ? bytes - sizeof(Impl) - 128 : 0;
  }
  explicit Impl(size_t bytes)
      : states(available(bytes) / 16, std::numeric_limits<size_t>::max(), memo::Policy::segmented),
        cells(available(bytes) - available(bytes) / 16) {}

  bool unchanged_since(uint32_t seqno, const block::StdAddress& address) const {
    if (seqno > head || head - seqno >= history.size()) return false;
    for (uint64_t n = uint64_t(seqno) + 1; n <= head; ++n) {
      const auto& changes = history[n % history.size()];
      if (changes.seqno != n || changes.contains(address)) return false;
    }
    return true;
  }
};

ParentAccountCache::ParentAccountCache(size_t max_bytes) {
  if (max_bytes < MIN_BYTES) throw std::invalid_argument("Parent cache budget must be at least 256 KiB");
  impl_ = std::make_unique<Impl>(max_bytes);
}
ParentAccountCache::~ParentAccountCache() = default;

ParentAccountView ParentAccountCache::advance(uint32_t seqno, td::Bits256 root_hash, td::Bits256 prev_root_hash,
                                             const std::vector<Update>& changes) {
  if (!seqno) return {};
  std::lock_guard lock(impl_->mutex);
  auto& p = *impl_;
  if (p.epoch && seqno <= p.head) {
    const auto& old = p.history[seqno % p.history.size()];
    if (old.seqno == seqno && old.root_hash == root_hash) return {this, p.epoch, seqno};
    if (old.seqno != seqno) return {}; // an old retry outside the bounded journal
    // A conflicting identity, including a retry on another branch, starts a
    // new epoch. Previously launched detectors can finish but cannot publish.
  }
  if (!p.epoch || uint64_t(seqno) != uint64_t(p.head) + 1 || prev_root_hash != p.head_hash) {
    p.states.clear();
    ++p.epoch;
    ++p.resets;
    p.history = {};
  }
  auto& journal = p.history[seqno % p.history.size()];
  journal = Changes{seqno, root_hash, {}};
  try {
    for (const auto& change : changes) {
      journal.add(change.address);
      p.states.update(change.address, [&](const ParentState&) -> std::shared_ptr<const ParentState> {
        if (!change.state || change.state->account_status != "active" || change.state->code.is_null() ||
            change.state->data.is_null()) {
          ++p.invalidations;
          return {};
        }
        ++p.updates;
        return std::make_shared<const ParentState>(detach(*change.state, seqno));
      }, state_bytes);
    }
  } catch (...) {
    // An optimization failure cannot leave partially advanced cached states.
    p.states.clear();
    ++p.epoch;
    ++p.resets;
    p.history = {};
    p.history[seqno % p.history.size()] = Changes{seqno, root_hash, {}};
  }
  p.head = seqno;
  p.head_hash = root_hash;
  return {this, p.epoch, seqno};
}

ParentAccountView ParentAccountCache::observe(uint32_t seqno, const ParsedBlock& block) {
  // The canonical chain and complete shard diff are supplied by DbScanner.
  const schema::BlockDataState* mc = nullptr;
  for (const auto& item : block.mc_block_.shard_blocks_diff_) {
    if (item.block_data.not_null() && item.block_data->block_id().is_masterchain()) {
      if (mc) return {};
      mc = &item;
    }
  }
  if (!mc || mc->block_data->block_id().seqno() != seqno || !mc->handle || !mc->handle->inited_prev()) return {};
  auto prev = mc->handle->prev();
  if (prev.size() != 1 || !prev[0].is_masterchain() || uint64_t(prev[0].seqno()) + 1 != seqno) return {};

  std::unordered_map<block::StdAddress, const schema::Transaction*> touched;
  for (const auto& b : block.blocks_) {
    if (b.mc_block_seqno != seqno) return {};
    for (const auto& tx : b.transactions) {
      if (tx.mc_seqno != seqno) return {};
      auto& last = touched[tx.account];
      if (!last || last->lt < tx.lt) last = &tx;
    }
  }
  std::unordered_map<block::StdAddress, const schema::AccountState*> final_states;
  for (const auto& state : block.account_states_) {
    auto& last = final_states[state.account];
    if (!last || last->last_trans_lt < state.last_trans_lt) last = &state;
  }
  std::vector<Update> changes;
  changes.reserve(touched.size());
  for (const auto& [address, tx] : touched) {
    auto it = final_states.find(address);
    const schema::AccountState* state = it == final_states.end() ? nullptr : it->second;
    // A read failure may leave an EARLIER state of this account in another
    // shard block. It must invalidate too, rather than publishing that state.
    if (state && (state->last_trans_lt != tx->lt || state->hash != tx->account_state_hash_after)) state = nullptr;
    changes.push_back({address, state});
  }
  return advance(seqno, mc->block_data->block_id().root_hash, prev[0].root_hash, changes);
}

td::Result<schema::AccountState> ParentAccountCache::lookup(
    const ParentAccountView& view, const block::StdAddress& address,
    const std::shared_ptr<vm::CellDbReader>& reader,
    const std::function<td::Result<schema::AccountState>()>& load) {
  std::shared_ptr<const ParentState> found;
  bool eligible;
  {
    std::lock_guard lock(impl_->mutex);
    eligible = reader && view.owner == this && view.epoch == impl_->epoch && view.mc_seqno <= impl_->head;
    if (eligible) {
      found = impl_->states.find(address, [&](const ParentState& value) { return value.valid_from <= view.mc_seqno; });
    } else {
      ++impl_->bypasses;
    }
  }
  if (!found) {
    TRY_RESULT(state, load());
    if (!eligible || state.account_status != "active" || state.code.is_null() || state.data.is_null()) return state;
    auto detached = safe_parse::guard("Detach parent state", [&]() -> td::Result<ParentState> {
      return detach(state, view.mc_seqno);
    });
    if (detached.is_error()) return state;
    found = std::make_shared<const ParentState>(detached.move_as_ok());
    {
      std::lock_guard lock(impl_->mutex);
      if (view.epoch == impl_->epoch && impl_->unchanged_since(view.mc_seqno, address)) {
        try {
          impl_->states.put(address, found, state_bytes);
        } catch (const std::bad_alloc&) {
          ++impl_->bypasses;
        }
      } else {
        ++impl_->stale;
      }
    }
  }
  auto state = found->state;
  state.code = impl_->cells.wrap(found->code, reader);
  state.data = impl_->cells.wrap(found->data, reader);
  return state;
}

size_t ParentAccountCache::retained_bytes() const {
  return sizeof(Impl) + 128 + impl_->states.stats().bytes + impl_->cells.stats().bytes;
}
memo::Stats ParentAccountCache::state_stats() const { return impl_->states.stats(); }
memo::Stats ParentAccountCache::cell_stats() const { return impl_->cells.stats(); }

std::string ParentAccountCache::report() {
  std::ostringstream out;
  {
    std::lock_guard lock(impl_->mutex);
    std::array<uint64_t, 5> values{impl_->resets, impl_->updates, impl_->invalidations, impl_->bypasses, impl_->stale};
    const char* names[] = {"reset", "update", "invalidate", "version_bypass", "stale_publish"};
    for (size_t i = 0; i < values.size(); ++i) {
      out << "indexer.cache.parents." << names[i] << " COUNT : " << values[i] - impl_->reported[i] << '\n';
    }
    impl_->reported = values;
    out << "indexer.cache.parents.verified_mc GAUGE : " << impl_->head << '\n';
  }
  out << impl_->states.report("indexer.cache.parents") << impl_->cells.report();
  out << "indexer.cache.parents.total_bytes GAUGE : " << retained_bytes() << '\n';
  return out.str();
}

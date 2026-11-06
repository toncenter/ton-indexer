#pragma once

#include <chrono>
#include <condition_variable>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>

#include "safe-parse.h"

namespace memo {

enum class Access { hit, miss, coalesced };
enum class Policy { lru, segmented };

struct Stats {
  uint64_t hits = 0, misses = 0, coalesced = 0, evictions = 0;
  uint64_t computations = 0, errors = 0, bypasses = 0, oversized = 0;
  uint64_t compute_micros = 0, wait_micros = 0;
  size_t entries = 0, bytes = 0, inflight = 0;
};

// Successful immutable values only. The first caller computes synchronously;
// followers wait without holding the cache lock. Loaders must not depend on
// queued actor work (or recursively request their own key).
// Only parsed values are retained, never CellDB readers or lazy cell graphs.
template<class Key, class Value, class Hash = std::hash<Key>>
class Cache {
 public:
  using Ref = std::shared_ptr<const Value>;

  explicit Cache(size_t max_bytes, size_t max_entries = std::numeric_limits<size_t>::max(),
                 Policy policy = Policy::lru)
      : max_bytes_(max_bytes), max_entries_(max_entries), policy_(policy) {}

  // Predicates/updates execute under the cache lock: no I/O or actor work.
  template<class Predicate>
  Ref find(const Key& key, Predicate&& predicate) {
    std::lock_guard lock(mutex_);
    auto it = index_.find(key);
    if (it == index_.end() || !predicate(*it->second->value)) {
      ++stats_.misses;
      return {};
    }
    ++stats_.hits;
    touch(it->second);
    return it->second->value;
  }

  template<class DynamicBytes>
  void put(const Key& key, Ref value, DynamicBytes&& dynamic_bytes) {
    auto charge = entry_overhead() + dynamic_bytes(*value);
    std::list<Node> retired;
    Index retired_index;
    std::lock_guard lock(mutex_);
    cancel_flight(key);
    publish(key, std::move(value), charge, retired, retired_index);
  }

  template<class Update, class DynamicBytes>
  void update(const Key& key, Update&& update, DynamicBytes&& dynamic_bytes) {
    std::list<Node> retired;
    Index retired_index;
    std::lock_guard lock(mutex_);
    auto it = index_.find(key);
    if (it == index_.end()) return;
    auto value = update(*it->second->value);
    cancel_flight(key);
    if (!value) {
      retire(it, retired);
    } else {
      auto charge = entry_overhead() + dynamic_bytes(*value);
      publish(key, std::move(value), charge, retired, retired_index);
    }
  }

  void erase(const Key& key) {
    std::list<Node> retired;
    std::lock_guard lock(mutex_);
    cancel_flight(key);
    auto it = index_.find(key);
    if (it != index_.end()) retire(it, retired);
  }

  void clear() {
    std::list<Node> retired;
    Index retired_index;
    std::lock_guard lock(mutex_);
    for (auto& [_, flight] : flights_) flight->cancelled = true;
    flights_.clear();
    retired.splice(retired.end(), lru_);
    retired.splice(retired.end(), protected_);
    index_.swap(retired_index);
    bytes_ = protected_bytes_ = 0;
  }

  template<class Load, class DynamicBytes>
  td::Result<Ref> get(const Key& key, Load&& load, DynamicBytes&& dynamic_bytes, Access* access = nullptr) {
    std::shared_ptr<Flight> flight;
    {
      std::unique_lock lock(mutex_);
      if (auto it = index_.find(key); it != index_.end()) {
        if (access) *access = Access::hit;
        ++stats_.hits;
        touch(it->second);
        return it->second->value;
      }
      if (auto it = flights_.find(key); it != flights_.end()) {
        if (access) *access = Access::coalesced;
        ++stats_.coalesced;
        flight = it->second;
        const auto start = Clock::now();
        flight->ready.wait(lock, [&] { return flight->done; });
        stats_.wait_micros += micros(start);
        if (flight->error.is_error()) return flight->error.clone();
        return flight->value;
      }
      ++stats_.misses;
      if (access) *access = Access::miss;
      flight = std::make_shared<Flight>();
      flights_.emplace(key, flight);
    }

    auto result = compute(std::forward<Load>(load));
    const size_t charge = result.is_ok() ? entry_overhead() + dynamic_bytes(*result.ok()) : 0;
    // Destruction of potentially large evicted values happens after unlocking.
    std::list<Node> retired;
    Index retired_index;
    {
      std::lock_guard lock(mutex_);
      if (result.is_ok()) {
        flight->value = result.ok();
        if (!flight->cancelled) {
          // A cache allocation failure must not strand followers or turn a
          // successfully computed value into a parsing failure.
          try {
            publish(key, result.ok(), charge, retired, retired_index);
          } catch (const std::bad_alloc&) {
            ++stats_.bypasses;
          }
        }
      } else {
        flight->error = result.error().clone();
      }
      flight->done = true;
      auto it = flights_.find(key);
      if (it != flights_.end() && it->second == flight) flights_.erase(it);
    }
    flight->ready.notify_all();
    return result;
  }

  template<class Load>
  td::Result<Ref> uncached(Load&& load) {
    {
      std::lock_guard lock(mutex_);
      ++stats_.bypasses;
    }
    return compute(std::forward<Load>(load));
  }

  Stats stats() const {
    std::lock_guard lock(mutex_);
    return snapshot();
  }

  // Counts/times are interval deltas; gauges describe current retained data.
  // Keep cumulative counters internally so tests/diagnostics can inspect them
  // without racing or resetting the periodic reporter.
  std::string report(const std::string& prefix) {
    Stats current, previous;
    {
      std::lock_guard lock(mutex_);
      current = snapshot();
      previous = reported_;
      reported_ = current;
    }
    std::ostringstream out;
    auto count = [&](const char* name, uint64_t Stats::*field) {
      out << prefix << '.' << name << " COUNT : " << current.*field - previous.*field << '\n';
    };
    count("hit", &Stats::hits);
    count("miss", &Stats::misses);
    count("coalesced", &Stats::coalesced);
    count("eviction", &Stats::evictions);
    count("compute", &Stats::computations);
    count("error", &Stats::errors);
    count("bypass", &Stats::bypasses);
    count("oversized", &Stats::oversized);
    out << prefix << ".compute.micros SUM : " << current.compute_micros - previous.compute_micros << '\n'
        << prefix << ".wait.micros SUM : " << current.wait_micros - previous.wait_micros << '\n'
        << prefix << ".entries GAUGE : " << current.entries << '\n'
        << prefix << ".bytes GAUGE : " << current.bytes << '\n'
        << prefix << ".inflight GAUGE : " << current.inflight << '\n';
    return out.str();
  }

 private:
  using Clock = std::chrono::steady_clock;
  struct Flight {
    std::condition_variable ready;
    bool done = false;
    bool cancelled = false;
    Ref value;
    td::Status error;
  };
  struct Node {
    Key key;
    Ref value;
    size_t bytes;
    bool protected_entry = false;
  };
  using Index = std::unordered_map<Key, typename std::list<Node>::iterator, Hash>;

  void cancel_flight(const Key& key) {
    auto it = flights_.find(key);
    if (it != flights_.end()) {
      it->second->cancelled = true;
      flights_.erase(it);
    }
  }
  void trim_protected() {
    const auto limit = max_bytes_ - max_bytes_ / 5;
    while (!protected_.empty() && protected_bytes_ > limit) {
      auto it = std::prev(protected_.end());
      protected_bytes_ -= it->bytes;
      it->protected_entry = false;
      lru_.splice(lru_.begin(), protected_, it);
    }
  }
  void touch(typename std::list<Node>::iterator it) {
    if (policy_ == Policy::lru) {
      lru_.splice(lru_.begin(), lru_, it);
    } else if (it->protected_entry) {
      protected_.splice(protected_.begin(), protected_, it);
    } else {
      it->protected_entry = true;
      protected_bytes_ += it->bytes;
      protected_.splice(protected_.begin(), lru_, it);
      trim_protected();
    }
  }
  void retire(typename Index::iterator it, std::list<Node>& retired) {
    auto node = it->second;
    bytes_ -= node->bytes;
    if (node->protected_entry) protected_bytes_ -= node->bytes;
    retired.splice(retired.end(), node->protected_entry ? protected_ : lru_, node);
    index_.erase(it);
  }
  void publish(const Key& key, Ref value, size_t charge, std::list<Node>& retired, Index& retired_index) {
    auto existing = index_.find(key);
    if (charge > max_bytes_ || max_entries_ == 0) {
      if (existing != index_.end()) retire(existing, retired);
      ++stats_.oversized;
      return;
    }
    if (existing != index_.end()) {
      // Replace without making transaction-driven updates count as accesses.
      auto node = existing->second;
      retired.push_back(Node{key, node->value, node->bytes});
      bytes_ -= node->bytes;
      if (node->protected_entry) protected_bytes_ -= node->bytes;
      node->value = std::move(value);
      node->bytes = charge;
      if (node->protected_entry) protected_bytes_ += charge;
    } else {
      lru_.push_front(Node{key, std::move(value), charge});
      try {
        index_.emplace(key, lru_.begin());
      } catch (...) {
        retired.splice(retired.end(), lru_, lru_.begin());
        throw;
      }
    }
    bytes_ += charge;
    trim_protected();
    while (!index_.empty() && (resident_bytes() > max_bytes_ || index_.size() > max_entries_)) {
      auto node = lru_.empty() ? std::prev(protected_.end()) : std::prev(lru_.end());
      retire(index_.find(node->key), retired);
      ++stats_.evictions;
    }
    if (index_.empty() && resident_bytes() > max_bytes_) index_.swap(retired_index);
  }

  static uint64_t micros(Clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
  }
  static constexpr size_t entry_overhead() {
    // List links, hash node links, shared_ptr control block and allocator
    // allowance, in addition to the actual objects. Bucket arrays are counted
    // separately, even after eviction. This is accounted memory, not RSS.
    return sizeof(Node) + sizeof(typename Index::value_type) + sizeof(Value) + 12 * sizeof(void*);
  }
  size_t resident_bytes() const { return bytes_ + index_.bucket_count() * sizeof(void*); }
  Stats snapshot() const {
    auto result = stats_;
    result.entries = index_.size();
    result.bytes = resident_bytes();
    result.inflight = flights_.size();
    return result;
  }
  template<class Load>
  td::Result<Ref> compute(Load&& load) {
    {
      std::lock_guard lock(mutex_);
      ++stats_.computations;
    }
    const auto start = Clock::now();
    auto result = safe_parse::guard("Memoized computation", [&]() -> td::Result<Ref> {
      TRY_RESULT(value, load());
      return Ref(std::make_shared<const Value>(std::move(value)));
    });
    {
      std::lock_guard lock(mutex_);
      stats_.compute_micros += micros(start);
      if (result.is_error()) ++stats_.errors;
    }
    return result;
  }

  const size_t max_bytes_, max_entries_;
  const Policy policy_;
  mutable std::mutex mutex_;
  std::list<Node> lru_;
  std::list<Node> protected_;
  Index index_;
  // Transient entries are bounded by synchronous caller concurrency, and are
  // removed on success AND failure. They never retain negative cache entries.
  std::unordered_map<Key, std::shared_ptr<Flight>, Hash> flights_;
  size_t bytes_ = 0;
  size_t protected_bytes_ = 0;
  Stats stats_, reported_;
};

}  // namespace memo

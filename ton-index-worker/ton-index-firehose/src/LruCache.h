#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace firehose {

// A thread-safe LRU map bounded by the sum of the charges of its entries,
// sharded by the first byte of the key (a hash). Values are copied out, so
// they should be cheap to copy, such as Ref or shared_ptr.
template <class Key, class Value>
class LruCache {
 public:
  explicit LruCache(std::size_t max_bytes) : shard_max_bytes_(max_bytes / kShards) {
  }

  std::optional<Value> get(const Key& key) {
    auto& shard = shard_of(key);
    std::lock_guard<std::mutex> guard(shard.mutex);
    auto it = shard.map.find(key);
    if (it == shard.map.end()) {
      misses_.fetch_add(1, std::memory_order_relaxed);
      return std::nullopt;
    }
    shard.lru.splice(shard.lru.begin(), shard.lru, it->second.lru_it);
    hits_.fetch_add(1, std::memory_order_relaxed);
    return it->second.value;
  }

  void put(const Key& key, Value value, std::size_t charge) {
    // Evicted values are released after the lock: freeing one may take time.
    std::vector<Value> evicted;
    auto& shard = shard_of(key);
    std::lock_guard<std::mutex> guard(shard.mutex);
    if (shard.map.count(key) != 0) {
      return;
    }
    shard.lru.push_front(key);
    shard.map.emplace(key, Entry{std::move(value), charge, shard.lru.begin()});
    shard.bytes += charge;
    while (shard.bytes > shard_max_bytes_ && !shard.lru.empty()) {
      auto victim = shard.map.find(shard.lru.back());
      shard.bytes -= victim->second.charge;
      evicted.push_back(std::move(victim->second.value));
      shard.map.erase(victim);
      shard.lru.pop_back();
    }
  }

  std::uint64_t hits() const {
    return hits_.load(std::memory_order_relaxed);
  }
  std::uint64_t misses() const {
    return misses_.load(std::memory_order_relaxed);
  }
  std::uint64_t bytes() {
    std::uint64_t total = 0;
    for (auto& shard : shards_) {
      std::lock_guard<std::mutex> guard(shard.mutex);
      total += shard.bytes;
    }
    return total;
  }

 private:
  static constexpr std::size_t kShards = 64;

  struct Entry {
    Value value;
    std::size_t charge;
    typename std::list<Key>::iterator lru_it;
  };
  struct Shard {
    std::mutex mutex;
    std::unordered_map<Key, Entry> map;
    std::list<Key> lru;
    std::size_t bytes{0};
  };

  Shard& shard_of(const Key& key) {
    return shards_[static_cast<unsigned char>(key.as_slice()[0]) % kShards];
  }

  std::size_t shard_max_bytes_;
  std::array<Shard, kShards> shards_;
  std::atomic<std::uint64_t> hits_{0};
  std::atomic<std::uint64_t> misses_{0};
};

}  // namespace firehose

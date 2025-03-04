#pragma once
#include <cstdint>

struct QueueState {
  std::int32_t mc_blocks_{0};
  std::int32_t blocks_{0};
  std::int32_t txs_{0};
  std::int32_t msgs_{0};
  std::int32_t traces_{0};

  QueueState& operator+=(const QueueState& r);
  QueueState& operator-=(const QueueState& r);

  friend QueueState operator+(QueueState l, const QueueState& r);
  friend QueueState operator-(QueueState l, const QueueState& r);

  friend inline bool operator==(const QueueState& l, const QueueState& r) {
    return (l.mc_blocks_ == r.mc_blocks_) && (l.blocks_ == r.blocks_) && (l.txs_ == r.txs_) && (l.msgs_ == r.msgs_);
  }
  friend inline bool operator<(const QueueState& l, const QueueState& r) {
    return (l.mc_blocks_ <= r.mc_blocks_) && (l.blocks_ <= r.blocks_) && (l.txs_ <= r.txs_) && (l.msgs_ <= r.msgs_);
  }
};

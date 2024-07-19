#include "queue_state.h"


QueueState operator+(QueueState l, const QueueState &r) {
  return QueueState{l.mc_blocks_ + r.mc_blocks_, l.blocks_ + r.blocks_, l.txs_ + r.txs_, l.msgs_ + r.msgs_, l.traces_ + r.traces_};
}

QueueState operator-(QueueState l, const QueueState &r) {
  return QueueState{l.mc_blocks_ - r.mc_blocks_, l.blocks_ - r.blocks_, l.txs_ - r.txs_, l.msgs_ - r.msgs_, l.traces_ - r.traces_};
}

QueueState& QueueState::operator+=(const QueueState& r) {
  mc_blocks_ += r.mc_blocks_;
  blocks_ += r.blocks_;
  txs_ += r.txs_;
  msgs_ += r.msgs_;
  traces_ += r.traces_;
  return *this;
}

QueueState& QueueState::operator-=(const QueueState& r) {
  mc_blocks_ -= r.mc_blocks_;
  blocks_ -= r.blocks_;
  txs_ -= r.txs_;
  msgs_ -= r.msgs_;
  traces_ -= r.traces_;
  return *this;
}

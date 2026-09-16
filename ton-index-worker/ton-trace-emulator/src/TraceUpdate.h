#pragma once

#include <cstddef>
#include <utility>
#include <vector>

#include "Measurement.h"
#include "TraceEmulator.h"

// All trace fragments produced for one logical trace by one block update.
// Fragments remain separate trees because their parents may live in earlier
// blocks; every processing stage and its telemetry operate on the update.
struct TraceUpdate {
  std::vector<Trace> fragments;
  MeasurementPtr measurement;

  std::size_t size() const {
    return fragments.size();
  }

  bool empty() const {
    return fragments.empty();
  }
};

inline TraceUpdate make_trace_update(Trace trace, MeasurementPtr measurement) {
  TraceUpdate update;
  update.fragments.push_back(std::move(trace));
  update.measurement = std::move(measurement);
  return update;
}

// Keeps one authoritative interface view per account. An emulated final state
// always wins over a committed state, regardless of fragment order.
void normalize_trace_update_interfaces(TraceUpdate& update);

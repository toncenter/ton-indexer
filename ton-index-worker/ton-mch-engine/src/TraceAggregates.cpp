// Trace-level aggregate derivation shared by schema and fixture loaders.
// Keeping it separate prevents the schema path from depending on lz4/msgpack.
#include "TraceLoader.h"

#include <algorithm>

namespace mch {

void fill_trace_aggregates(Trace &trace) {
  bool first = true;
  for (const auto &tx : trace.transactions) {
    trace.start_lt = first ? tx->lt : std::min(trace.start_lt, tx->lt);
    trace.end_lt = first ? tx->lt : std::max(trace.end_lt, tx->lt);
    trace.start_utime = first ? tx->now : std::min(trace.start_utime, tx->now);
    trace.end_utime = first ? tx->now : std::max(trace.end_utime, tx->now);
    trace.mc_seqno_end =
        first ? tx->mc_block_seqno : std::max(trace.mc_seqno_end, tx->mc_block_seqno);
    first = false;
  }
}

}  // namespace mch

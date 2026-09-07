#pragma once

#include <memory>

struct ConfirmedTraceSnapshotData;

// The write may succeed while its shared trace view belongs to another fork.
// This requests normal finalized emulation, not an insertion-error alert.
inline constexpr int kConfirmedSnapshotRootMismatch = -1001;
// Promotion cannot modify the graph. Re-run ordinary finalized emulation.
inline constexpr int kConfirmedPromotionUnavailable = -1002;

// Immutable, cell-free confirmed nodes and root identity from a successful
// insert. No full trace, pending tails or actions are retained for restoration.
using ConfirmedTraceSnapshot =
    std::shared_ptr<const ConfirmedTraceSnapshotData>;

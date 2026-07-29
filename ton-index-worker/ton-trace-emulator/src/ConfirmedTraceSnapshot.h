#pragma once

#include <memory>

struct ConfirmedTraceSnapshotData;

// An immutable, cell-free trace state produced by a successful confirmed
// insert. The scheduler only groups these opaque snapshots by block.
using ConfirmedTraceSnapshot =
    std::shared_ptr<const ConfirmedTraceSnapshotData>;

// Converts emulator transaction cells with the indexing pipeline's ParseQuery
// parsers into the schema::Transaction classifier input.
#pragma once

#include "EmuTypes.h"

#include "IndexData.h"  // schema::Transaction

#include "td/utils/Status.h"

#include <vector>

namespace mch {

// A node parse failure rejects the whole trace; partial trees are not classified.
td::Result<std::vector<schema::Transaction>> emu_to_schema_txs(const EmuTraceView &view,
                                                               int global_version);

}  // namespace mch

// Fallback for a failed, childless wallet external. It decodes unsent messages
// into ghost children and serializes them instead of emitting an unknown action.
// This cannot be a normal matcher because it creates tree nodes and must run only
// after the main serialization path produces nothing.
#pragma once

#include "BlockTree.h"

#include <cstddef>

namespace mch {

class LookupSource;  // BuildRuntime.h

// Forces `root` failed, parses its message body as a tg-wallet/v3/v4/v5r1
// external and appends one ghost child per decoded payload. Returns the
// number of children appended; 0 means the body is not a wallet external,
// carries no messages, or does not parse. Never throws.
std::size_t synthesize_ghost_children(EventTree &tree, EventNode *root);

// A jetton-transfer call becomes a jetton_transfer block whose asset comes
// from the sender's wallet interface (there is no internal-transfer leg to
// read the receiver's from). Returns the produced block, or nullptr when
// `block` is not a jetton-transfer call or the build would have raised.
Block *fallback_jetton_transfer(Block *block, BlockArena &arena, const LookupSource &src);

}  // namespace mch

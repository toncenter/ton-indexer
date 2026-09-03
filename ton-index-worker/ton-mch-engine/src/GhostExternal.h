// Fallback for a failed, childless wallet external. It decodes unsent messages
// into ghost children and serializes them instead of emitting an unknown action.
// This cannot be a normal matcher because it creates tree nodes and must run only
// after the main serialization path produces nothing.
#pragma once

#include "BlockTree.h"

#include <cstddef>

namespace mch {

class LookupSource;  // BuildRuntime.h

// init_from_external (event_processing.py:159-196). Forces `root` failed, parses
// its message body as a tg-wallet/v3/v4/v5r1 external and appends one ghost
// child per decoded payload. Returns the number of children appended; 0 means
// the body is not a wallet external, carries no messages, or does not parse
// (Python: extract_payload_from_wallet_message returns []). Never throws.
std::size_t synthesize_ghost_children(EventTree &tree, EventNode *root);

// FallbackJettonTransferBlockMatcher (blocks/jettons.py:282-330), the sole entry
// of matchers_for_failed_externals: a jetton-transfer call becomes a
// jetton_transfer block whose asset comes from the SENDER's wallet interface
// (there is no internal-transfer leg to read the receiver's from). Returns the
// produced block, or nullptr when `block` is not a jetton-transfer call or the
// build would have raised (Python: try_build catches and returns None).
Block *fallback_jetton_transfer(Block *block, BlockArena &arena, const LookupSource &src);

}  // namespace mch

#pragma once

#include "crypto/tl/tlblib.hpp"

namespace ton_marker {

// Tolk arrays are stored as a length and a chain of chunks. A chunk's item
// count is implicit: it is the number of refs left after reading the next link.
// This reader handles fixed-ref items (such as MessageToSend, with one ref).
class TolkArray {
public:
    TolkArray(const tlb::TLB& item, unsigned refs_per_item) : item_(item), refs_per_item_(refs_per_item) {}

    bool print_skip(tlb::JsonPrinter& pp, vm::CellSlice& cs) const;

private:
    const tlb::TLB& item_;
    unsigned refs_per_item_;
};

} // namespace ton_marker

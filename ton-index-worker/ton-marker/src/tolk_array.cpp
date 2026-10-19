#include "tolk_array.h"

#include <string>

namespace ton_marker {

bool TolkArray::print_skip(tlb::JsonPrinter& pp, vm::CellSlice& cs) const {
    if (!refs_per_item_ || !cs.have(8 + 1)) return false;

    const unsigned declared_count = cs.fetch_ulong(8);
    if (!declared_count || !cs.fetch_ulong(1) || !cs.size_refs()) return false;
    auto chunk = cs.fetch_ref();

    std::string json = "[";
    unsigned count = 0;
    while (chunk.not_null()) {
        auto slice = vm::load_cell_slice(chunk);
        if (!slice.have(1)) return false;
        const bool has_next = slice.fetch_ulong(1);
        vm::Ref<vm::Cell> next;
        if (has_next) {
            if (!slice.size_refs()) return false;
            next = slice.fetch_ref();
        }

        const unsigned refs = slice.size_refs();
        if (!refs || refs % refs_per_item_ || refs / refs_per_item_ > declared_count - count) return false;
        const unsigned chunk_items = refs / refs_per_item_;
        for (unsigned i = 0; i < chunk_items; ++i) {
            const auto before_refs = slice.size_refs();
            tlb::JsonPrinter item_printer;
            if (!item_.print_skip(item_printer, slice) || !item_printer.ok() ||
                before_refs - slice.size_refs() != refs_per_item_) {
                return false;
            }
            if (count) json += ',';
            json += item_printer.get_json();
            ++count;
        }
        if (!slice.empty_ext()) return false;
        chunk = std::move(next);
    }
    if (count != declared_count) return false;
    json += ']';
    return pp.write_raw(json);
}

} // namespace ton_marker

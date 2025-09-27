#include "special.h"
#include "schemes.h"

namespace ton_marker {

int count_actions_depth(vm::Ref<vm::Cell> list) {
    if (list.is_null()) {
        return 0;
    }
    int i = -1;
    do {
        ++i;
        bool special = true;
        auto cs = vm::load_cell_slice_special(std::move(list), special);
        if (special) {
            break;
        }
        list = cs.prefetch_ref();
    } while (list.not_null());
    return i;
}

bool try_parse_special(unsigned opcode, vm::CellSlice& cs, tlb::JsonPrinter& pp) {
    if (opcode == OPCODE_W5_EXTERNAL_SIGNED_REQUEST || opcode == OPCODE_W5_INTERNAL_SIGNED_REQUEST || opcode == OPCODE_W5_EXTENSION_ACTION_REQUEST) {        
        // we can't use W5MsgBody just as is. it has snake cells for actions,
        // and tlb-generated code is not capable of detecting how many cells are in the snake.
        // so we first just count actions depth on refs,
        // and then parsing with W5MsgBody using the counts.
        auto cs_copy = cs;
        int out_actions_count = 0;
        int extended_actions_count = 0;

        // skip opcode
        cs_copy.advance(32);

        // skip fields before actions
        if (opcode == OPCODE_W5_EXTENSION_ACTION_REQUEST) {
            cs_copy.advance(64); // skip query_id
        } else {
        cs_copy.advance(32); // skip wallet_id
        cs_copy.advance(32); // skip valid_until
        cs_copy.advance(32); // skip msg_seqno
        }

        // check flags for actions
        bool has_out_actions = cs_copy.fetch_ulong(1);
        bool has_extended_actions = cs_copy.fetch_ulong(1);

        // get out_actions depth from first ref if present
        if (has_out_actions && cs_copy.size_refs() > 0) {
            auto first_ref = cs_copy.fetch_ref();
            if (first_ref.not_null()) {
                out_actions_count = count_actions_depth(first_ref);
            }
        }

        if (has_extended_actions) {
            extended_actions_count = 1; // extended_actions are starting in the cell itself
            while (cs_copy.size_refs() > 0) {
                extended_actions_count++;
                cs_copy = vm::load_cell_slice(cs_copy.fetch_ref());
            }
        }
        schemes::W5MsgBody w5_parser(out_actions_count, extended_actions_count-1); // last ref is not empty cell, that's why -1
        auto cs_copy2 = cs;
        return w5_parser.print_skip(pp, cs_copy2);
    }
    
    return false;
}

} // namespace ton_marker

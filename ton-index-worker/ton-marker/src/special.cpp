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
        int out_actions_count = 0;
        int other_actions_count = 0;
        if (cs.size_refs() > 0) {
            auto first_ref = cs.prefetch_ref(0);
            if (first_ref.not_null()) {
                out_actions_count = count_actions_depth(first_ref);
            }
        }

        if (cs.size_refs() > 1) {
            auto second_ref = cs.prefetch_ref(1);
            if (second_ref.not_null()) {
                other_actions_count = count_actions_depth(second_ref);
            }
        }
        std::cout << "out_actions_count: " << out_actions_count << std::endl;
        std::cout << "other_actions_count: " << other_actions_count << std::endl;
        schemes::W5MsgBody w5_parser(out_actions_count, other_actions_count);
        auto cs_copy = cs;
        return w5_parser.print_skip(pp, cs_copy);
    }
    
    return false;
}

} // namespace ton_marker

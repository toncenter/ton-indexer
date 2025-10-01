#pragma once

#include "crypto/tl/tlblib.hpp"

namespace ton_marker {

const unsigned OPCODE_W5_INTERNAL_SIGNED_REQUEST = 0x73696e74;
const unsigned OPCODE_W5_EXTERNAL_SIGNED_REQUEST = 0x7369676e;
const unsigned OPCODE_W5_EXTENSION_ACTION_REQUEST = 0x6578746e;
const unsigned OPCODE_HIGHLOAD_V3_INTERNAL_REQUEST = 0xae42e5a4;
    
int count_actions_depth(vm::Ref<vm::Cell> list);

bool try_parse_special(unsigned opcode, vm::CellSlice& cs, tlb::JsonPrinter& pp);

} // namespace ton_marker

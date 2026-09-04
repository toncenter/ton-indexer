// Internal per-family message-body parse fns for MsgParse.cpp's
// message_parsers() map. Not part of the public MsgParse.h surface.
#pragma once

#include "../Value.h"

#include "td/utils/Status.h"
#include "vm/cells/Cell.h"

namespace mch {

td::Result<Value> parse_jetton_transfer(const td::Ref<vm::Cell> &body);

td::Result<Value> parse_change_dns(const td::Ref<vm::Cell> &body);

td::Result<Value> parse_vesting_send_message(const td::Ref<vm::Cell> &body);

}  // namespace mch

#include "td/actor/actor.h"
#include "vm/cells/Cell.h"
#include "vm/stack.hpp"
#include "common/refcnt.hpp"
#include "smc-envelope/SmartContract.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "td/utils/base64.h"
#include "convert-utils.h"


td::Result<std::string> convert::to_raw_address(td::Ref<vm::CellSlice> cs) {
  auto tag = block::gen::MsgAddress().get_tag(*cs);
  switch (tag) {
    case block::gen::MsgAddress::cons1:
      switch (block::gen::MsgAddressInt().get_tag(*cs)) {
        case block::gen::MsgAddressInt::addr_var:
          return "addr_var";
        case block::gen::MsgAddressInt::addr_std: {
          block::gen::MsgAddressInt::Record_addr_std addr;
          if (!tlb::csr_unpack(cs, addr)) {
            return td::Status::Error("Failed to unpack MsgAddressInt");
          }
          return std::to_string(addr.workchain_id) + ":" + addr.address.to_hex();
        }
        default:
          return td::Status::Error("Failed to unpack MsgAddressInt");
      }
    case block::gen::MsgAddress::cons2:
      switch (block::gen::MsgAddressExt().get_tag(*cs)) {
        case block::gen::MsgAddressExt::addr_none:
          return "addr_none";
        case block::gen::MsgAddressExt::addr_extern:
          return "addr_extern";
        default:
          return td::Status::Error("Failed to unpack MsgAddressExt");
      }
    default:
      return td::Status::Error("Failed to unpack MsgAddress");
  }
}

td::Result<block::StdAddress> convert::to_std_address(td::Ref<vm::CellSlice> cs) {
  auto tag = block::gen::MsgAddress().get_tag(*cs);
  switch (tag) {
    case block::gen::MsgAddress::cons1:
      switch (block::gen::MsgAddressInt().get_tag(*cs)) {
        case block::gen::MsgAddressInt::addr_var:
          return td::Status::Error("addr_var is not std address");
        case block::gen::MsgAddressInt::addr_std: {
          block::gen::MsgAddressInt::Record_addr_std addr;
          if (!tlb::csr_unpack(cs, addr)) {
            return td::Status::Error("Failed to unpack addr_std");
          }
          return block::StdAddress(addr.workchain_id, addr.address);
        }
        default:
          return td::Status::Error("Failed to unpack MsgAddressInt");
      }
    case block::gen::MsgAddress::cons2:
      return td::Status::Error("MsgAddressExt is not std address");
    default:
      return td::Status::Error("Failed to unpack MsgAddress");
  }
}

std::string convert::to_raw_address(block::StdAddress address) {
  return std::to_string(address.workchain) + ":" + address.addr.to_hex();
}

td::Result<td::int64> convert::to_balance(vm::CellSlice& balance_slice) {
  auto balance = block::tlb::t_Grams.as_integer_skip(balance_slice);
  if (balance.is_null()) {
      return td::Status::Error("Failed to unpack balance");
  }
  auto res = balance->to_long();
  if (res == td::int64(~0ULL << 63)) {
      return td::Status::Error("Failed to unpack balance (2)");
  }
  return res;
}

td::Result<td::int64> convert::to_balance(td::Ref<vm::CellSlice> balance_ref) {
  vm::CellSlice balance_slice = *balance_ref;
  return to_balance(balance_slice);
}

// td::Result<td::RefInt256> convert::to_balance256(vm::CellSlice& balance_slice) {
//   auto balance = block::tlb::t_Grams.as_integer_skip(balance_slice);
//   if (balance.is_null()) {
//       return td::Status::Error("Failed to unpack balance");
//   }
//   auto res = balance->to_long();
//   if (res == td::int64(~0ULL << 63)) {
//       return td::Status::Error("Failed to unpack balance (2)");
//   }
//   return balance;
// }

// td::Result<td::RefInt256> convert::to_balance256(td::Ref<vm::CellSlice> balance_ref) {
//   vm::CellSlice balance_slice = *balance_ref;
//   return to_balance(balance_slice);
// }

td::Result<td::optional<std::string>> convert::to_bytes(td::Ref<vm::Cell> cell) {
  if (cell.is_null()) {
    return td::optional<std::string>();
  }
  TRY_RESULT(boc, vm::std_boc_serialize(cell, vm::BagOfCells::Mode::WithCRC32C));
  return td::base64_encode(boc.as_slice().str());
}

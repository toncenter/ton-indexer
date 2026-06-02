#include "td/actor/actor.h"
#include "vm/cells/Cell.h"
#include "vm/cells/CellSlice.h"
#include "vm/stack.hpp"
#include "common/refcnt.hpp"
#include "smc-envelope/SmartContract.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "td/utils/base64.h"
#include "convert-utils.h"


namespace {

std::string bits_to_padded_hex(td::ConstBitPtr bits, int bit_len) {
  static const char digits[] = "0123456789ABCDEF";
  std::string result;
  result.reserve((bit_len + 3) / 4);
  for (int i = 0; i < bit_len; i += 4) {
    int value = 0;
    int chunk = bit_len - i < 4 ? bit_len - i : 4;
    for (int j = 0; j < chunk; ++j) {
      value <<= 1;
      if (bits[i + j]) {
        value |= 1;
      }
    }
    value <<= 4 - chunk;
    result.push_back(digits[value]);
  }
  return result;
}

td::Status apply_anycast(td::Ref<vm::CellSlice> anycast_ref, td::BitPtr address_bits) {
  if (anycast_ref.not_null() && anycast_ref->bit_at(0) == 1) {
    auto anycast_slice = vm::CellSlice(*anycast_ref);
    anycast_slice.advance(1); // skip maybe bit
    block::gen::Anycast::Record anycast;
    if (!tlb::unpack_exact(anycast_slice, anycast)) {
      return td::Status::Error("Failed to unpack Anycast");
    }
    address_bits.copy_from(anycast.rewrite_pfx->cbits(), anycast.depth);
  }
  return td::Status::OK();
}

}  // namespace

td::Result<std::string> convert::to_raw_address(td::Ref<vm::CellSlice> cs) {
  auto tag = block::gen::MsgAddress().get_tag(*cs);
  switch (tag) {
    case block::gen::MsgAddress::cons1:
      switch (block::gen::MsgAddressInt().get_tag(*cs)) {
        case block::gen::MsgAddressInt::addr_var: {
          block::gen::MsgAddressInt::Record_addr_var addr;
          if (!tlb::csr_unpack(cs, addr)) {
            return td::Status::Error("Failed to unpack addr_var");
          }
          TRY_STATUS(apply_anycast(addr.anycast, addr.address.write().bits()));
          return "var$" + std::to_string(addr.workchain_id) + ":" +
                 std::to_string(addr.addr_len) + ":" +
                 addr.address->to_hex();
        }
        case block::gen::MsgAddressInt::addr_std: {
          block::gen::MsgAddressInt::Record_addr_std addr;
          if (!tlb::csr_unpack(cs, addr)) {
            return td::Status::Error("Failed to unpack MsgAddressInt");
          }
          TRY_STATUS(apply_anycast(addr.anycast, addr.address.bits()));
          return std::to_string(addr.workchain_id) + ":" + addr.address.to_hex();
        }
        default:
          return td::Status::Error("Failed to unpack MsgAddressInt");
      }
    case block::gen::MsgAddress::cons2:
      switch (block::gen::MsgAddressExt().get_tag(*cs)) {
        case block::gen::MsgAddressExt::addr_none:
          return "addr_none";
        case block::gen::MsgAddressExt::addr_extern: {
          block::gen::MsgAddressExt::Record_addr_extern addr;
          if (!tlb::csr_unpack(cs, addr)) {
            return td::Status::Error("Failed to unpack addr_extern");
          }
          return "ext$" + std::to_string(addr.len) + ":" +
                 bits_to_padded_hex(addr.external_address->cbits(), addr.len);
        }
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
          TRY_STATUS(apply_anycast(addr.anycast, addr.address.bits()));
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

std::optional<std::string> convert::to_raw_address(std::optional<block::StdAddress> address) {
  if (address.has_value()) {
    return to_raw_address(address.value());
  }
  return std::nullopt;
}


td::Result<std::optional<std::string>> convert::to_bytes(td::Ref<vm::Cell> cell) {
  if (cell.is_null()) {
    return std::nullopt;
  }
  TRY_RESULT(boc, vm::std_boc_serialize(cell, vm::BagOfCells::Mode::WithCRC32C));
  return td::base64_encode(boc.as_slice().str());
}

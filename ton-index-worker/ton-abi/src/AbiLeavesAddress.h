#pragma once

// Fail-closed load/store primitives for address, address?, ext_address, and
// any_address. They follow @ton/core tags and layouts but reject anycast and
// addr_var.

#include "common/bitstring.h"
#include "common/refint.h"
#include "td/utils/Status.h"

#include "vm/cellslice.h"
#include "vm/cells/CellBuilder.h"

namespace ton_abi {

enum class AbiAddressKind { None, Std, Extern };

// One decoded address. Std dumps as "wc:hex"; Extern as
// {"extern":{"bits":ext_bits,"value":<hex of ext_value>}}; None -> "none"
// (any_address) / null (address?). ext_value is a bit-slice (0 refs) because an
// external address value can be up to 511 bits -- wider than a RefInt256.
struct AbiAddress {
  AbiAddressKind kind = AbiAddressKind::None;

  // Std
  int workchain = 0;        // int8, signed
  td::Bits256 hash{};       // zero-initialized

  // Extern
  int ext_bits = 0;
  td::Ref<vm::CellSlice> ext_value;  // exactly ext_bits bits, 0 refs
};

// address: addr_std ONLY (tag 0b10); anycast set / any other tag -> error.
td::Result<AbiAddress> load_address(vm::CellSlice &cs);
td::Status store_address(vm::CellBuilder &cb, const AbiAddress &a);  // requires kind==Std

// address?: addr_none -> None; addr_std -> Std; anything else -> error.
td::Result<AbiAddress> load_maybe_address(vm::CellSlice &cs);
td::Status store_maybe_address(vm::CellBuilder &cb, const AbiAddress &a);  // None | Std

// ext_address: addr_extern ONLY (tag 0b01); any other tag -> error.
td::Result<AbiAddress> load_external_address(vm::CellSlice &cs);
td::Status store_external_address(vm::CellBuilder &cb, const AbiAddress &a);  // requires kind==Extern

// any_address: none / std / extern; addr_var (tag 0b11) -> error.
td::Result<AbiAddress> load_address_any(vm::CellSlice &cs);
td::Status store_address_any(vm::CellBuilder &cb, const AbiAddress &a);  // any kind

}  // namespace ton_abi

#include "AbiLeavesAddress.h"

namespace ton_abi {

namespace {

// This implementation rejects anycast. @ton/core instead reads
// rewrite_depth/rewrite_pfx and rewrites the hash prefix. No supported fixture
// or generated vector contains an anycast address.
constexpr bool kRejectAnycast = true;

// addr_std body after the 2-bit tag was consumed: anycast(1) wc(int8) hash(256).
td::Result<AbiAddress> load_std_after_tag(vm::CellSlice &cs) {
  unsigned long long anycast = 0;
  if (!cs.fetch_ulong_bool(1, anycast)) {
    return td::Status::Error("addr_std: truncated at anycast flag");
  }
  if (kRejectAnycast && anycast != 0) {
    return td::Status::Error("addr_std: anycast addresses are not supported");
  }
  long long wc = 0;
  if (!cs.fetch_long_bool(8, wc)) {
    return td::Status::Error("addr_std: truncated at workchain");
  }
  AbiAddress a;
  a.kind = AbiAddressKind::Std;
  a.workchain = static_cast<int>(wc);
  if (!cs.fetch_bits_to(a.hash)) {
    return td::Status::Error("addr_std: truncated at 256-bit hash");
  }
  return a;
}

// addr_extern body after the 2-bit tag was consumed: len(uint9) value(len bits).
td::Result<AbiAddress> load_extern_after_tag(vm::CellSlice &cs) {
  unsigned long long len = 0;
  if (!cs.fetch_ulong_bool(9, len)) {
    return td::Status::Error("addr_extern: truncated at length");
  }
  AbiAddress a;
  a.kind = AbiAddressKind::Extern;
  a.ext_bits = static_cast<int>(len);
  if (len > 0 && !cs.have(static_cast<unsigned>(len))) {
    return td::Status::Error(PSLICE() << "addr_extern: truncated at " << len << "-bit value");
  }
  a.ext_value = cs.fetch_subslice(static_cast<unsigned>(len), 0);
  if (a.ext_value.is_null()) {
    return td::Status::Error("addr_extern: failed to fetch value bits");
  }
  return a;
}

td::Result<int> fetch_tag(vm::CellSlice &cs) {
  unsigned long long tag = 0;
  if (!cs.fetch_ulong_bool(2, tag)) {
    return td::Status::Error("address: truncated at 2-bit tag");
  }
  return static_cast<int>(tag);
}


td::Status store_std_body(vm::CellBuilder &cb, const AbiAddress &a) {
  if (a.workchain < -128 || a.workchain > 127) {
    return td::Status::Error(PSLICE() << "addr_std: workchain " << a.workchain << " out of int8 range");
  }
  if (!cb.store_long_bool(2, 2) || !cb.store_long_bool(0, 1) ||
      !cb.store_long_bool(a.workchain, 8) || !cb.store_bits_bool(a.hash)) {
    return td::Status::Error("addr_std: cannot store (cell overflow?)");
  }
  return td::Status::OK();
}

td::Status store_extern_body(vm::CellBuilder &cb, const AbiAddress &a) {
  if (a.ext_bits < 0 || a.ext_bits > 511) {
    return td::Status::Error(PSLICE() << "addr_extern: bits " << a.ext_bits << " out of [0,511]");
  }
  if (a.ext_value.is_null() || a.ext_value->size() != static_cast<unsigned>(a.ext_bits) ||
      a.ext_value->size_refs() != 0) {
    return td::Status::Error("addr_extern: value slice must be exactly ext_bits bits and 0 refs");
  }
  if (!cb.store_long_bool(1, 2) || !cb.store_long_bool(a.ext_bits, 9) ||
      !cb.store_bits_bool(a.ext_value->data_bits(), static_cast<unsigned>(a.ext_bits))) {
    return td::Status::Error("addr_extern: cannot store (cell overflow?)");
  }
  return td::Status::OK();
}

td::Status store_none_body(vm::CellBuilder &cb) {
  if (!cb.store_long_bool(0, 2)) {
    return td::Status::Error("addr_none: cannot store");
  }
  return td::Status::OK();
}

}  // namespace


td::Result<AbiAddress> load_address(vm::CellSlice &cs) {
  TRY_RESULT(tag, fetch_tag(cs));
  if (tag != 2) {
    return td::Status::Error(PSLICE() << "address: expected addr_std (tag 0b10), got tag " << tag);
  }
  return load_std_after_tag(cs);
}

td::Status store_address(vm::CellBuilder &cb, const AbiAddress &a) {
  if (a.kind != AbiAddressKind::Std) {
    return td::Status::Error("address: expected a std address");
  }
  return store_std_body(cb, a);
}


td::Result<AbiAddress> load_maybe_address(vm::CellSlice &cs) {
  TRY_RESULT(tag, fetch_tag(cs));
  if (tag == 0) {
    return AbiAddress{};  // None
  }
  if (tag == 2) {
    return load_std_after_tag(cs);
  }
  return td::Status::Error(PSLICE() << "address?: expected addr_none or addr_std, got tag " << tag);
}

td::Status store_maybe_address(vm::CellBuilder &cb, const AbiAddress &a) {
  switch (a.kind) {
    case AbiAddressKind::None:
      return store_none_body(cb);
    case AbiAddressKind::Std:
      return store_std_body(cb, a);
    case AbiAddressKind::Extern:
      return td::Status::Error("address?: external address is not valid for this type");
  }
  return td::Status::Error("address?: unknown kind");
}


td::Result<AbiAddress> load_external_address(vm::CellSlice &cs) {
  TRY_RESULT(tag, fetch_tag(cs));
  if (tag != 1) {
    return td::Status::Error(PSLICE() << "ext_address: expected addr_extern (tag 0b01), got tag " << tag);
  }
  return load_extern_after_tag(cs);
}

td::Status store_external_address(vm::CellBuilder &cb, const AbiAddress &a) {
  if (a.kind != AbiAddressKind::Extern) {
    return td::Status::Error("ext_address: expected an external address");
  }
  return store_extern_body(cb, a);
}


td::Result<AbiAddress> load_address_any(vm::CellSlice &cs) {
  TRY_RESULT(tag, fetch_tag(cs));
  switch (tag) {
    case 0:
      return AbiAddress{};  // None
    case 2:
      return load_std_after_tag(cs);
    case 1:
      return load_extern_after_tag(cs);
    default:  // tag == 3, addr_var
      return td::Status::Error("any_address: addr_var (tag 0b11) is not supported");
  }
}

td::Status store_address_any(vm::CellBuilder &cb, const AbiAddress &a) {
  switch (a.kind) {
    case AbiAddressKind::None:
      return store_none_body(cb);
    case AbiAddressKind::Std:
      return store_std_body(cb, a);
    case AbiAddressKind::Extern:
      return store_extern_body(cb, a);
  }
  return td::Status::Error("any_address: unknown kind");
}

}  // namespace ton_abi

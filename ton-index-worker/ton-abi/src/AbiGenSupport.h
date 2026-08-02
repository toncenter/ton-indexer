#pragma once

// Runtime support included by generated C++ pairs: CellOf<T>, typed custom
// serializers, leaf operations, and AbiValue builders. Resolution and dispatch
// are already baked into generated code, so the loader and kernel stay out of
// the runtime dependency graph.

#include "AbiValue.h"
#include "AbiLeavesAddress.h"
#include "AbiLeavesContainer.h"
#include "AbiLeavesDict.h"
#include "AbiLeavesPrefix.h"
#include "AbiLeavesRef.h"
#include "AbiLeavesScalar.h"

#include "common/bitstring.h"
#include "common/refint.h"
#include "td/utils/Status.h"

#include "vm/cells/Cell.h"
#include "vm/cells/CellBuilder.h"
#include "vm/cells/CellSlice.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace ton_abi {
namespace gen {

// CellOf<T>: a value of type T that lives in a child cell (Tolk `Cell<T>`).
// Emitted field type for the `cellOf` ABI kind. Holds the inner
// value behind a shared_ptr indirection: cellOf is a cell-boundary, so a type
// may be recursive THROUGH a cellOf (e.g. `struct Node { next: Cell<Node> }`),
// which an inline by-value member could not represent. The indirection also
// means cellOf<Struct> only needs Struct forward-declared, not complete.
template <class T>
struct CellOf {
  std::shared_ptr<T> ref;
};

// Typed custom-serializer registry.
//
// One registry PER value type T (a function-local static, so no global-init
// ordering issues). Keys have the form "<contract_name>::<decl name>".
// pack / unpack / to_abi_value are independently optional. Missing registration
// or a missing operation at call time is
// a runtime td::Status error -- NEVER a compile/link error (reference
// semantics: registerCustomPackUnpack is a runtime map, not a specialization).
template <class T>
struct AbiCustomEntry {
  std::function<td::Status(const T &, vm::CellBuilder &)> pack;
  std::function<td::Result<T>(vm::CellSlice &)> unpack;
  std::function<td::Result<AbiValue>(const T &)> to_abi_value;
};

template <class T>
std::unordered_map<std::string, AbiCustomEntry<T>> &abi_custom_registry() {
  static std::unordered_map<std::string, AbiCustomEntry<T>> reg;
  return reg;
}

// Any of pack/unpack/to_abi may be an empty
// std::function. Re-registering the same key for the same T is an error.
template <class T>
td::Status register_abi_custom(const std::string &key,
                               std::function<td::Status(const T &, vm::CellBuilder &)> pack,
                               std::function<td::Result<T>(vm::CellSlice &)> unpack,
                               std::function<td::Result<AbiValue>(const T &)> to_abi_value) {
  auto &reg = abi_custom_registry<T>();
  if (reg.count(key)) {
    return td::Status::Error(PSLICE() << "custom serializer for '" << key << "' already registered");
  }
  reg.emplace(key, AbiCustomEntry<T>{std::move(pack), std::move(unpack), std::move(to_abi_value)});
  return td::Status::OK();
}

template <class T>
const AbiCustomEntry<T> *abi_custom_lookup(const std::string &key) {
  auto &reg = abi_custom_registry<T>();
  auto it = reg.find(key);
  return it == reg.end() ? nullptr : &it->second;
}

// to_abi_value leaf builders. The baked to_abi_value() bodies compose these;
// composite kinds (list/tuple/map/optional/union) are emitted inline as IIFEs.
// All return AbiValue by value (infallible for leaves); the struct-level
// to_abi_value() returns td::Result<AbiValue> so custom hooks can fail.
inline AbiValue abi_v_int(const td::RefInt256 &v) { return AbiValue::make_int(v); }
// Native-int overloads: intN/uintN fields with n<=64 are td::int64/td::uint64
// members; their AbiValue Int dump is byte-identical to the RefInt256 form
// (make_refint -> same decimal string), so vectors are unchanged.
inline AbiValue abi_v_int(td::int64 v) { return AbiValue::make_int(td::make_refint(v)); }
inline AbiValue abi_v_int(td::uint64 v) { return AbiValue::make_int(td::make_refint(v)); }
inline AbiValue abi_v_bool(bool v) { return AbiValue::make_bool(v); }
inline AbiValue abi_v_address(const AbiAddress &a) { return AbiValue::make_address(a); }
inline AbiValue abi_v_address_opt(const AbiAddress &a) {
  // addressOpt: a None address dumps as JSON null (NOT "none" -- that is
  // any_address only), matching the AbiValue AddressOpt dump convention.
  return a.kind == AbiAddressKind::None ? AbiValue::make_null() : AbiValue::make_address(a);
}
inline AbiValue abi_v_cell(const td::Ref<vm::Cell> &c) { return AbiValue::make_cell(c); }
inline AbiValue abi_v_bits(const td::Ref<vm::CellSlice> &s) { return AbiValue::make_bits(s); }
inline AbiValue abi_v_string(const std::string &s) { return AbiValue::make_string(s); }
inline AbiValue abi_v_null() { return AbiValue::make_null(); }
inline AbiValue abi_v_void() { return AbiValue::make_void(); }

// create()-default materializers for ConstExpr slice / address literals.
// Used only by generated create() bodies.
inline td::Ref<vm::CellSlice> bits_from_hex(const std::string &hex) {
  unsigned char buf[256];
  long bits = td::bitstring::parse_bitstring_hex_literal(buf, sizeof(buf), hex.data(), hex.data() + hex.size());
  vm::CellBuilder cb;
  if (bits > 0) {
    cb.store_bits(td::ConstBitPtr{buf}, static_cast<std::size_t>(bits));
  }
  return vm::load_cell_slice_ref(cb.finalize());
}

// Best-effort raw "wc:hex" address literal -> AbiAddress (Std). Friendly
// (base64) forms are not parsed here (no fixture default uses them); such a
// default yields a None address placeholder.
inline AbiAddress address_from_string(const std::string &s) {
  AbiAddress a;
  auto colon = s.find(':');
  if (colon == std::string::npos) {
    return a;  // None
  }
  unsigned char buf[64];
  std::string hex = s.substr(colon + 1);
  long bits = td::bitstring::parse_bitstring_hex_literal(buf, sizeof(buf), hex.data(), hex.data() + hex.size());
  if (bits != 256) {
    return a;  // None
  }
  a.kind = AbiAddressKind::Std;
  a.workchain = std::atoi(s.substr(0, colon).c_str());
  a.hash = td::Bits256(td::ConstBitPtr{buf});
  return a;
}

}  // namespace gen
}  // namespace ton_abi

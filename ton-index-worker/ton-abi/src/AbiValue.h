#pragma once

// Protocol-agnostic tagged values produced by generated parsers and adapted by
// downstream consumers. The compact JSON representation is the conformance
// vector format.

#include "AbiLeavesAddress.h"  // AbiAddress

#include "common/refint.h"

#include "vm/cells/Cell.h"
#include "vm/cells/CellSlice.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace ton_abi {

enum class AbiValueKind {
  Int, Bool, Address, Cell, CellOf, Bits, String, List, Struct, Union, Void, Null, Map,
};

struct AbiValue {
  AbiValueKind kind = AbiValueKind::Null;

  td::RefInt256 int_v;                            // Int
  bool bool_v = false;                             // Bool
  AbiAddress address_v;                            // Address
  td::Ref<vm::Cell> cell_v;                        // Cell
  std::unique_ptr<AbiValue> inner;                 // CellOf ref / Union wrapped value
  td::Ref<vm::CellSlice> bits_v;                   // Bits (bitsN / remaining)
  std::string string_v;                            // String
  std::vector<AbiValue> list_v;                    // List
  std::string struct_name;                         // Struct
  std::vector<std::pair<std::string, AbiValue>> struct_fields;  // Struct, decl order
  std::string union_label;                         // Union
  std::vector<std::pair<AbiValue, AbiValue>> map_entries;       // Map, wire order

  AbiValue() = default;
  AbiValue(AbiValue &&) = default;
  AbiValue &operator=(AbiValue &&) = default;
  AbiValue(const AbiValue &) = delete;
  AbiValue &operator=(const AbiValue &) = delete;

  static AbiValue make_int(td::RefInt256 v);
  static AbiValue make_bool(bool v);
  static AbiValue make_address(AbiAddress a);
  static AbiValue make_cell(td::Ref<vm::Cell> c);
  static AbiValue make_cell_of(AbiValue inner_value);
  static AbiValue make_bits(td::Ref<vm::CellSlice> s);
  static AbiValue make_string(std::string s);
  static AbiValue make_list(std::vector<AbiValue> items);
  static AbiValue make_struct(std::string name);
  static AbiValue make_union(std::string label, AbiValue inner_value);
  static AbiValue make_void();
  static AbiValue make_null();
  static AbiValue make_map(std::vector<std::pair<AbiValue, AbiValue>> entries);

  // Appends a field to a Struct-kind value, in call order (= decl order when
  // the walker iterates struct_fields_of in order). No-op / UB on other kinds.
  void add_field(std::string name, AbiValue v);

  // Compact canonical JSON. Field order is structural, and Map entries retain
  // their wire order, so dumping performs no sorting.
  std::string to_json() const;

 private:
  void dump_to(std::string &out) const;
};

}  // namespace ton_abi

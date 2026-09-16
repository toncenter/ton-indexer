// Converts Tolk-ABI generated structs into mch::Value instances so parsed
// message bodies plug into the existing message_parsers() registry alongside
// the kept hand parsers. Registration uses compile-time static rows and loads
// no ABI model at runtime.
#pragma once

#include "Value.h"
#include "MsgParse.h"  // MsgParserFn

#include "AbiValue.h"  // ton-abi (linked into mch-classify)

#include "td/utils/Status.h"
#include "vm/cells/Cell.h"
#include "vm/cellslice.h"

#include <string>
#include <utility>
#include <vector>

namespace mch {

// AbiValue -> mch::Value adapter. Tolk-faithful dump conventions, not the
// PSlice extern-string / none-as-null universe. Total: never errors. Every
// AbiValueKind maps onto an existing VType.
Value abi_value_to_mch(const ton_abi::AbiValue &v);

// Parse a message body as the generated ABI struct T, returning an mch::Value.
// T is a ton-abi-gen type (ton_abi::gen::<contract>::<Decl>) exposing:
//   static td::Result<T> from_slice(vm::CellSlice&);   // checks its own prefix
//   td::Result<ton_abi::AbiValue> to_abi_value() const;
//
// &abi_parse_body<T> has type td::Result<Value>(*)(const td::Ref<vm::Cell>&) ==
// MsgParserFn exactly. Dispatch is baked into from_slice, so the instantiation
// registers as an ordinary parser row. Top-level exotic cells return a clean
// error rather than aborting.
template <class T>
td::Result<Value> abi_parse_body(const td::Ref<vm::Cell> &body) {
  if (body.is_null()) {
    return td::Status::Error("abi bridge: null body cell");
  }
  bool special = false;
  vm::CellSlice cs = vm::load_cell_slice_special(body, special);
  if (special) {
    return td::Status::Error("abi bridge: exotic cell not supported");
  }
  TRY_RESULT(parsed, T::from_slice(cs));
  TRY_RESULT(av, parsed.to_abi_value());
  return abi_value_to_mch(av);
}

// The ABI-generated parser rows are merged into message_parsers().
// Rows are {"<DeclName>", &abi_parse_body<T>}: bare declaration names, not
// <contract>::<decl>: the .mch grammar's `parse CAP as Name` reads a bare IDENT
// because the grammar accepts a bare identifier at that position.
const std::vector<std::pair<std::string, MsgParserFn>> &abi_message_parsers();

}  // namespace mch

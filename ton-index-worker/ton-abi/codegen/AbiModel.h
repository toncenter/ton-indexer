#pragma once

// PODs mirroring the reference Tolk ABI schema in
// tolk-abi-to-typescript src/abi.ts and src/abi-types.ts.
//
// Field names/shapes/optionality are a 1:1 mirror of the TS source -- see the
// citation comment above each struct. Bigints (`bigint_as_string` in TS) are
// td::RefInt256. No behavior lives here (AbiLoader fills these in from JSON;
// AbiKernel resolves them; both are separate files) -- PODs only, aggregate
// init, no user-defined methods beyond what a recursive variant forces
// (unique_ptr indirection for ABIConstExpression's self-reference).

#include "common/refint.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace ton_abi {

// ---- Ty: abi-types.ts:12-45 ("ABI Type" union, ~30 kinds) ----

enum class TyKind {
  Int, IntN, UintN, VarIntN, VarUIntN, Coins, Bool, Cell, Builder, Slice,
  String, Remaining, Address, AddressOpt, AddressExt, AddressAny, BitsN,
  NullLiteral, Callable, Void, Unknown, Nullable, CellOf, ArrayOf,
  LispListOf, Tensor, ShapedTuple, MapKV, EnumRef, StructRef, AliasRef,
  GenericT, Union,
};

// Kinds carrying no extra data: int, coins, bool, cell, builder, slice,
// string, remaining, address, addressOpt, addressExt, addressAny,
// nullLiteral, callable, void, unknown.
struct TyNoPayload {};

// Shared shape for intN/uintN/varintN/varuintN/bitsN -- all are `{ n: number }`.
struct TyWidth {
  int n = 0;
};

// { kind: 'nullable'; inner_ty_idx: number } -- the JSON's optional stack_*
// keys are get-method-side data, out of this module's scope (AbiKernel.h note).
struct TyNullable {
  int inner_ty_idx = 0;
};

// { kind: 'cellOf' | 'arrayOf' | 'lispListOf'; inner_ty_idx: number }
struct TyInner {
  int inner_ty_idx = 0;
};

// { kind: 'tensor' | 'shapedTuple'; items_ty_idx: number[] }
struct TyItems {
  std::vector<int> items_ty_idx;
};

// { kind: 'mapKV'; key_ty_idx: number; value_ty_idx: number }
struct TyMapKV {
  int key_ty_idx = 0;
  int value_ty_idx = 0;
};

// { kind: 'EnumRef'; enum_name: string }
struct TyEnumRef {
  std::string enum_name;
};

// { kind: 'StructRef'; struct_name: string; type_args_ty_idx?: number[] }
struct TyStructRef {
  std::string struct_name;
  std::vector<int> type_args_ty_idx;  // absent in JSON == empty here
};

// { kind: 'AliasRef'; alias_name: string; type_args_ty_idx?: number[] }
struct TyAliasRef {
  std::string alias_name;
  std::vector<int> type_args_ty_idx;
};

// { kind: 'genericT'; name_t: string }
struct TyGenericT {
  std::string name_t;
};

// UnionVariant: abi-types.ts:62-69
struct UnionVariant {
  int variant_ty_idx = 0;
  std::uint32_t prefix_num = 0;
  int prefix_len = 0;
  bool is_prefix_implicit = false;
};

// { kind: 'union'; variants: UnionVariant[] } -- stack_* keys ignored, as above.
struct TyUnion {
  std::vector<UnionVariant> variants;
};

struct Ty {
  TyKind kind = TyKind::Unknown;
  std::variant<TyNoPayload, TyWidth, TyNullable, TyInner, TyItems, TyMapKV,
               TyEnumRef, TyStructRef, TyAliasRef, TyGenericT, TyUnion>
      data;
};

// ---- ABIConstExpression: abi-types.ts:71-90 ----
// Recursive (tensor/shapedTuple/object/castTo nest it) -- unique_ptr
// indirection so the variant alternatives don't need ABIConstExpression
// complete at the point they're declared.

struct ABIConstExpression;

struct ConstExprInt {
  td::RefInt256 v;
};
struct ConstExprBool {
  bool v = false;
};
struct ConstExprSlice {
  std::string hex;
};
struct ConstExprString {
  std::string str;
};
struct ConstExprAddress {
  std::string addr;
};
struct ConstExprTensor {
  std::vector<std::unique_ptr<ABIConstExpression>> items;
};
struct ConstExprShapedTuple {
  std::vector<std::unique_ptr<ABIConstExpression>> items;
};
struct ConstExprObject {
  std::string struct_name;
  std::vector<std::unique_ptr<ABIConstExpression>> fields;
};
struct ConstExprCastTo {
  std::unique_ptr<ABIConstExpression> inner;
  int cast_to_ty_idx = 0;
};
struct ConstExprNull {};

enum class ConstExprKind {
  Int, Bool, Slice, String, Address, Tensor, ShapedTuple, Object, CastTo, Null,
};

struct ABIConstExpression {
  ConstExprKind kind = ConstExprKind::Null;
  std::variant<ConstExprInt, ConstExprBool, ConstExprSlice, ConstExprString,
               ConstExprAddress, ConstExprTensor, ConstExprShapedTuple,
               ConstExprObject, ConstExprCastTo, ConstExprNull>
      data;
};

// ---- ABICustomSerializers: abi-types.ts:100-103 ----
struct ABICustomSerializers {
  bool pack_to_builder = false;
  bool unpack_from_slice = false;
};

// ---- ABIStruct: abi-types.ts:119-137 ----
struct ABIStructField {
  std::string name;
  int ty_idx = 0;
  std::optional<int> client_ty_idx;
  std::optional<ABIConstExpression> default_value;
  std::optional<std::string> description;
};

// NOTE: unlike UnionVariant's prefix (32-bit dispatch bound, loader-enforced
// -- see AbiLoader), a struct's own opcode prefix can legitimately exceed 32
// bits (verified: LotsOfWrappers' MsgSinglePrefix48 uses a real 48-bit
// prefix) -- hence uint64_t here, not uint32_t.
struct ABIStructPrefix {
  std::uint64_t prefix_num = 0;
  int prefix_len = 0;
};

struct ABIStruct {
  std::string name;
  int ty_idx = 0;
  std::vector<std::string> type_params;
  std::optional<ABIStructPrefix> prefix;
  std::vector<ABIStructField> fields;
  std::optional<ABICustomSerializers> custom_pack_unpack;
  std::optional<std::string> description;
};

// ---- ABIAlias: abi-types.ts:149-157 ----
struct ABIAlias {
  std::string name;
  int ty_idx = 0;
  int target_ty_idx = 0;
  std::vector<std::string> type_params;
  std::optional<ABICustomSerializers> custom_pack_unpack;
  std::optional<std::string> description;
};

// ---- ABIEnum: abi-types.ts:167-178 ----
struct ABIEnumMember {
  std::string name;
  td::RefInt256 value;
  std::optional<std::string> description;
};

struct ABIEnum {
  std::string name;
  int ty_idx = 0;
  int encoded_as_ty_idx = 0;
  std::vector<ABIEnumMember> members;
  std::optional<ABICustomSerializers> custom_pack_unpack;
  std::optional<std::string> description;
};

// A declaration is exactly one of ABIStruct/ABIAlias/ABIEnum, tagged by its
// own `kind` field in JSON ("struct"/"alias"/"enum") -- abi.ts:118.
enum class DeclKind { Struct, Alias, Enum };

struct ABIDeclaration {
  DeclKind kind;
  ABIStruct as_struct;
  ABIAlias as_alias;
  ABIEnum as_enum;
};

// ---- ABIStructInstantiation / ABIAliasInstantiation: abi-types.ts:185-202 ----
struct ABIStructInstantiation {
  int ty_idx = 0;
  std::string struct_name;
  std::vector<int> monomorphic_fields_ty_idx;
  std::optional<ABICustomSerializers> custom_pack_unpack;
};

struct ABIAliasInstantiation {
  int ty_idx = 0;
  std::string alias_name;
  int monomorphic_target_ty_idx = 0;
  std::optional<ABICustomSerializers> custom_pack_unpack;
};

// ---- ABIGetMethod: abi.ts:23-34 ----
struct ABIGetMethodParam {
  std::string name;
  int ty_idx = 0;
  std::optional<std::string> description;
  std::optional<ABIConstExpression> default_value;
};

struct ABIGetMethod {
  int tvm_method_id = 0;
  std::string name;
  std::vector<ABIGetMethodParam> parameters;
  int return_ty_idx = 0;
  std::optional<std::string> description;
};

// ---- Message/storage/error shapes: abi.ts:36-84 ----
struct ABIInternalMessage {
  int body_ty_idx = 0;
};
struct ABIExternalMessage {
  int body_ty_idx = 0;
};
struct ABIOutgoingMessage {
  int body_ty_idx = 0;
};

struct ABIStorage {
  std::optional<int> storage_ty_idx;
  std::optional<int> storage_at_deployment_ty_idx;
};

struct ABIThrownError {
  std::string kind;  // 'plain_int' | 'constant' | 'enum_member'
  std::optional<std::string> name;
  std::optional<std::string> description;
  int err_code = 0;
};

// ---- ContractABI: abi.ts:109-130 ----
struct ContractABI {
  std::string contract_name;
  std::optional<std::string> author;
  std::optional<std::string> version;
  std::optional<std::string> description;

  std::vector<Ty> unique_types;
  std::vector<ABIStructInstantiation> struct_instantiations;
  std::vector<ABIAliasInstantiation> alias_instantiations;
  std::vector<ABIDeclaration> declarations;

  ABIStorage storage;
  std::vector<ABIInternalMessage> incoming_messages;
  std::vector<ABIExternalMessage> incoming_external;
  std::vector<ABIOutgoingMessage> outgoing_messages;
  std::vector<ABIOutgoingMessage> emitted_events;
  std::vector<ABIGetMethod> get_methods;
  std::vector<ABIThrownError> thrown_errors;

  std::string compiler_name;
  std::string compiler_version;

  // NOT part of ContractABI in abi.ts -- sits alongside it in the raw JSON
  // every fixture file actually is (abi_schema_version is a sibling field
  // emitted by the compiler; code_boc64 is bolted on by convertTolkFileToABI
  // and is of no interest here). Kept on the model for loader convenience;
  // AbiLoader accepts exactly abi_schema_version "1.0".
  std::string abi_schema_version;
};

}  // namespace ton_abi

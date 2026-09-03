#include "AbiLoader.h"

#include "common/refint.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/logging.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace ton_abi {

namespace {

using Json = td::JsonValue;
using JType = td::JsonValue::Type;


const Json *jfield(const Json &e, td::Slice name) {
  if (e.type() != JType::Object) {
    return nullptr;
  }
  for (const auto &kv : e.get_object().field_values_) {
    if (kv.first == name) {
      return &kv.second;
    }
  }
  return nullptr;
}

bool has(const Json &e, td::Slice name) {
  return jfield(e, name) != nullptr;
}

std::string jstr(const Json &e, td::Slice name, const std::string &dflt = {}) {
  const Json *f = jfield(e, name);
  return (f != nullptr && f->type() == JType::String) ? f->get_string().str() : dflt;
}

bool jbool(const Json &e, td::Slice name, bool dflt = false) {
  const Json *f = jfield(e, name);
  return (f != nullptr && f->type() == JType::Boolean) ? f->get_boolean() : dflt;
}

td::Result<std::int64_t> num_i64(const Json &n) {
  if (n.type() != JType::Number) {
    return td::Status::Error("expected a JSON number");
  }
  const std::string s = n.get_number().str();
  try {
    return static_cast<std::int64_t>(std::stoll(s));
  } catch (const std::exception &) {
    return td::Status::Error("malformed JSON number '" + s + "'");
  }
}

// Required-number field: missing field / wrong type / malformed token all
// become a clean Status error (fail-closed) instead of an uncaught throw.
td::Result<std::int64_t> jint_req(const Json &e, td::Slice name) {
  const Json *f = jfield(e, name);
  if (f == nullptr) {
    return td::Status::Error(PSLICE() << "missing required field '" << name << "'");
  }
  TRY_RESULT_PREFIX(v, num_i64(*f), PSLICE() << "field '" << name << "': ");
  return v;
}

td::Result<std::string> jstr_req(const Json &e, td::Slice name) {
  const Json *f = jfield(e, name);
  if (f == nullptr || f->type() != JType::String) {
    return td::Status::Error(PSLICE() << "missing/invalid required string field '" << name << "'");
  }
  return f->get_string().str();
}

// Parses a JSON string field as a decimal-bigint; malformed-bigint invariant
// lives here.
td::Result<td::RefInt256> jbigint_req(const Json &e, td::Slice name) {
  TRY_RESULT(s, jstr_req(e, name));
  td::RefInt256 v = td::dec_string_to_int256(s);
  if (v.is_null()) {
    return td::Status::Error(PSLICE() << "malformed bigint in field '" << name << "': '" << s << "'");
  }
  return v;
}


td::Result<Ty> parse_ty(const Json &j, int idx) {
  if (j.type() != JType::Object) {
    return td::Status::Error(PSLICE() << "unique_types[" << idx << "] must be an object");
  }
  TRY_RESULT(kind, jstr_req(j, "kind"));
  Ty ty;

  auto width = [&](TyKind k, int lo, int hi) -> td::Result<Ty> {
    TRY_RESULT(n, jint_req(j, "n"));
    if (n < lo || n > hi) {
      return td::Status::Error(PSLICE() << "unique_types[" << idx << "] kind '" << kind << "': n=" << n
                                         << " out of allowed range [" << lo << "," << hi << "]");
    }
    ty.kind = k;
    ty.data = TyWidth{static_cast<int>(n)};
    return ty;
  };
  auto no_payload = [&](TyKind k) -> td::Result<Ty> {
    ty.kind = k;
    ty.data = TyNoPayload{};
    return ty;
  };
  auto inner_of = [&](TyKind k) -> td::Result<Ty> {
    TRY_RESULT(inner, jint_req(j, "inner_ty_idx"));
    ty.kind = k;
    ty.data = TyInner{static_cast<int>(inner)};
    return ty;
  };
  auto items_of = [&](TyKind k) -> td::Result<Ty> {
    const Json *arr = jfield(j, "items_ty_idx");
    if (arr == nullptr || arr->type() != JType::Array) {
      return td::Status::Error(PSLICE() << "unique_types[" << idx << "]: missing/invalid 'items_ty_idx'");
    }
    TyItems items;
    for (const auto &e : arr->get_array()) {
      TRY_RESULT(v, num_i64(e));
      items.items_ty_idx.push_back(static_cast<int>(v));
    }
    ty.kind = k;
    ty.data = items;
    return ty;
  };
  auto type_args_of = [&](const Json &e) -> td::Result<std::vector<int>> {
    std::vector<int> out;
    const Json *ta = jfield(e, "type_args_ty_idx");
    if (ta == nullptr) {
      return out;
    }
    if (ta->type() != JType::Array) {
      return td::Status::Error("'type_args_ty_idx' must be an array when present");
    }
    for (const auto &v : ta->get_array()) {
      TRY_RESULT(vi, num_i64(v));
      out.push_back(static_cast<int>(vi));
    }
    return out;
  };

  // Width bounds (loader invariant): 256-bit cell integers +/- sign bit is
  // the actual TON limit; bitsN/varintN/varuintN bounds are this loader's
  // own sanity fence -- revisit if a real fixture needs wider.
  if (kind == "int") return no_payload(TyKind::Int);
  if (kind == "intN") return width(TyKind::IntN, 1, 257);
  if (kind == "uintN") return width(TyKind::UintN, 1, 256);
  if (kind == "varintN") return width(TyKind::VarIntN, 1, 32);
  if (kind == "varuintN") return width(TyKind::VarUIntN, 1, 32);
  if (kind == "coins") return no_payload(TyKind::Coins);
  if (kind == "bool") return no_payload(TyKind::Bool);
  if (kind == "cell") return no_payload(TyKind::Cell);
  if (kind == "builder") return no_payload(TyKind::Builder);
  if (kind == "slice") return no_payload(TyKind::Slice);
  if (kind == "string") return no_payload(TyKind::String);
  if (kind == "remaining") return no_payload(TyKind::Remaining);
  if (kind == "address") return no_payload(TyKind::Address);
  if (kind == "addressOpt") return no_payload(TyKind::AddressOpt);
  if (kind == "addressExt") return no_payload(TyKind::AddressExt);
  if (kind == "addressAny") return no_payload(TyKind::AddressAny);
  if (kind == "bitsN") return width(TyKind::BitsN, 0, 1023);
  if (kind == "nullLiteral") return no_payload(TyKind::NullLiteral);
  if (kind == "callable") return no_payload(TyKind::Callable);
  if (kind == "void") return no_payload(TyKind::Void);
  if (kind == "unknown") return no_payload(TyKind::Unknown);

  if (kind == "nullable") {
    TRY_RESULT(inner, jint_req(j, "inner_ty_idx"));
    TyNullable n;
    n.inner_ty_idx = static_cast<int>(inner);
    ty.kind = TyKind::Nullable;
    ty.data = n;
    return ty;
  }
  if (kind == "cellOf") return inner_of(TyKind::CellOf);
  if (kind == "arrayOf") return inner_of(TyKind::ArrayOf);
  if (kind == "lispListOf") return inner_of(TyKind::LispListOf);
  if (kind == "tensor") return items_of(TyKind::Tensor);
  if (kind == "shapedTuple") return items_of(TyKind::ShapedTuple);

  if (kind == "mapKV") {
    TRY_RESULT(k, jint_req(j, "key_ty_idx"));
    TRY_RESULT(v, jint_req(j, "value_ty_idx"));
    ty.kind = TyKind::MapKV;
    ty.data = TyMapKV{static_cast<int>(k), static_cast<int>(v)};
    return ty;
  }
  if (kind == "EnumRef") {
    TRY_RESULT(name, jstr_req(j, "enum_name"));
    ty.kind = TyKind::EnumRef;
    ty.data = TyEnumRef{name};
    return ty;
  }
  if (kind == "StructRef") {
    TRY_RESULT(name, jstr_req(j, "struct_name"));
    TRY_RESULT(args, type_args_of(j));
    ty.kind = TyKind::StructRef;
    ty.data = TyStructRef{name, args};
    return ty;
  }
  if (kind == "AliasRef") {
    TRY_RESULT(name, jstr_req(j, "alias_name"));
    TRY_RESULT(args, type_args_of(j));
    ty.kind = TyKind::AliasRef;
    ty.data = TyAliasRef{name, args};
    return ty;
  }
  if (kind == "genericT") {
    TRY_RESULT(name, jstr_req(j, "name_t"));
    ty.kind = TyKind::GenericT;
    ty.data = TyGenericT{name};
    return ty;
  }
  if (kind == "union") {
    const Json *variants = jfield(j, "variants");
    if (variants == nullptr || variants->type() != JType::Array) {
      return td::Status::Error(PSLICE() << "unique_types[" << idx << "] union: missing 'variants'");
    }
    TyUnion u;
    int vi = 0;
    for (const auto &vj : variants->get_array()) {
      if (vj.type() != JType::Object) {
        return td::Status::Error(PSLICE() << "unique_types[" << idx << "].variants[" << vi << "] must be an object");
      }
      UnionVariant uv;
      TRY_RESULT_PREFIX(vt, jint_req(vj, "variant_ty_idx"),
                         PSLICE() << "unique_types[" << idx << "].variants[" << vi << "]: ");
      uv.variant_ty_idx = static_cast<int>(vt);
      TRY_RESULT_PREFIX(pn, jint_req(vj, "prefix_num"),
                         PSLICE() << "unique_types[" << idx << "].variants[" << vi << "]: ");
      uv.prefix_num = static_cast<std::uint32_t>(pn);
      TRY_RESULT_PREFIX(pl, jint_req(vj, "prefix_len"),
                         PSLICE() << "unique_types[" << idx << "].variants[" << vi << "]: ");
      uv.prefix_len = static_cast<int>(pl);
      // prefix_len must fit in 32 bits. prefix_num is a float64 in the JSON,
      // so a wider prefix would be un-representable; reject rather than
      // silently truncate.
      if (uv.prefix_len < 0 || uv.prefix_len > 32) {
        return td::Status::Error(PSLICE() << "unique_types[" << idx << "].variants[" << vi
                                           << "]: prefix_len=" << uv.prefix_len << " out of [0,32]");
      }
      uv.is_prefix_implicit = jbool(vj, "is_prefix_implicit", false);
      u.variants.push_back(uv);
      ++vi;
    }
    // Union prefix-freedom (loader invariant): no variant's prefix bit-string
    // may be a strict prefix of another's.
    TRY_STATUS(([&]() -> td::Status {
      for (std::size_t a = 0; a < u.variants.size(); ++a) {
        for (std::size_t b = a + 1; b < u.variants.size(); ++b) {
          const auto &va = u.variants[a];
          const auto &vb = u.variants[b];
          int common = std::min(va.prefix_len, vb.prefix_len);
          if (common == 0) continue;
          std::uint32_t mask = common == 32 ? 0xFFFFFFFFu : ((1u << common) - 1u);
          std::uint32_t pa = (va.prefix_num >> (va.prefix_len - common)) & mask;
          std::uint32_t pb = (vb.prefix_num >> (vb.prefix_len - common)) & mask;
          if (pa == pb) {
            return td::Status::Error(PSLICE() << "unique_types[" << idx << "] union: variants[" << a
                                               << "] and [" << b << "] violate prefix-freedom (one is a "
                                                  "strict prefix of the other)");
          }
        }
      }
      return td::Status::OK();
    })());
    ty.kind = TyKind::Union;
    ty.data = u;
    return ty;
  }

  return td::Status::Error(PSLICE() << "unique_types[" << idx << "]: unknown Ty kind '" << kind << "'");
}


td::Result<ABIConstExpression> parse_const_expr(const Json &j) {
  if (j.type() != JType::Object) {
    return td::Status::Error("const expression must be an object");
  }
  TRY_RESULT(kind, jstr_req(j, "kind"));
  ABIConstExpression e;
  if (kind == "int") {
    TRY_RESULT(v, jbigint_req(j, "v"));
    e.kind = ConstExprKind::Int;
    e.data = ConstExprInt{v};
    return e;
  }
  if (kind == "bool") {
    const Json *v = jfield(j, "v");
    if (v == nullptr || v->type() != JType::Boolean) {
      return td::Status::Error("const expression 'bool': missing/invalid 'v'");
    }
    e.kind = ConstExprKind::Bool;
    e.data = ConstExprBool{v->get_boolean()};
    return e;
  }
  if (kind == "slice") {
    TRY_RESULT(hex, jstr_req(j, "hex"));
    e.kind = ConstExprKind::Slice;
    e.data = ConstExprSlice{hex};
    return e;
  }
  if (kind == "string") {
    TRY_RESULT(str, jstr_req(j, "str"));
    e.kind = ConstExprKind::String;
    e.data = ConstExprString{str};
    return e;
  }
  if (kind == "address") {
    TRY_RESULT(addr, jstr_req(j, "addr"));
    e.kind = ConstExprKind::Address;
    e.data = ConstExprAddress{addr};
    return e;
  }
  if (kind == "tensor" || kind == "shapedTuple") {
    const Json *items = jfield(j, "items");
    if (items == nullptr || items->type() != JType::Array) {
      return td::Status::Error(PSLICE() << "const expression '" << kind << "': missing 'items'");
    }
    std::vector<std::unique_ptr<ABIConstExpression>> parsed;
    for (const auto &ij : items->get_array()) {
      TRY_RESULT(sub, parse_const_expr(ij));
      parsed.push_back(std::make_unique<ABIConstExpression>(std::move(sub)));
    }
    e.kind = kind == "tensor" ? ConstExprKind::Tensor : ConstExprKind::ShapedTuple;
    if (kind == "tensor") {
      e.data = ConstExprTensor{std::move(parsed)};
    } else {
      e.data = ConstExprShapedTuple{std::move(parsed)};
    }
    return e;
  }
  if (kind == "object") {
    TRY_RESULT(struct_name, jstr_req(j, "struct_name"));
    const Json *fields = jfield(j, "fields");
    if (fields == nullptr || fields->type() != JType::Array) {
      return td::Status::Error("const expression 'object': missing 'fields'");
    }
    std::vector<std::unique_ptr<ABIConstExpression>> parsed;
    for (const auto &fj : fields->get_array()) {
      TRY_RESULT(sub, parse_const_expr(fj));
      parsed.push_back(std::make_unique<ABIConstExpression>(std::move(sub)));
    }
    e.kind = ConstExprKind::Object;
    e.data = ConstExprObject{struct_name, std::move(parsed)};
    return e;
  }
  if (kind == "castTo") {
    const Json *inner = jfield(j, "inner");
    if (inner == nullptr) {
      return td::Status::Error("const expression 'castTo': missing 'inner'");
    }
    TRY_RESULT(sub, parse_const_expr(*inner));
    TRY_RESULT(cast_to, jint_req(j, "cast_to_ty_idx"));
    e.kind = ConstExprKind::CastTo;
    e.data = ConstExprCastTo{std::make_unique<ABIConstExpression>(std::move(sub)), static_cast<int>(cast_to)};
    return e;
  }
  if (kind == "null") {
    e.kind = ConstExprKind::Null;
    e.data = ConstExprNull{};
    return e;
  }
  return td::Status::Error(PSLICE() << "const expression: unknown kind '" << kind << "'");
}


td::Result<ABICustomSerializers> parse_custom_pack_unpack(const Json &j) {
  ABICustomSerializers c;
  c.pack_to_builder = jbool(j, "pack_to_builder", false);
  c.unpack_from_slice = jbool(j, "unpack_from_slice", false);
  return c;
}

td::Result<ABIStructField> parse_struct_field(const Json &j) {
  if (j.type() != JType::Object) {
    return td::Status::Error("struct field must be an object");
  }
  ABIStructField f;
  TRY_RESULT_ASSIGN(f.name, jstr_req(j, "name"));
  TRY_RESULT(ty_idx, jint_req(j, "ty_idx"));
  f.ty_idx = static_cast<int>(ty_idx);
  if (has(j, "client_ty_idx")) {
    TRY_RESULT(v, jint_req(j, "client_ty_idx"));
    f.client_ty_idx = static_cast<int>(v);
  }
  if (const Json *dv = jfield(j, "default_value"); dv != nullptr) {
    TRY_RESULT_PREFIX(v, parse_const_expr(*dv), PSLICE() << "field '" << f.name << "'.default_value: ");
    f.default_value = std::move(v);
  }
  if (has(j, "description")) {
    f.description = jstr(j, "description");
  }
  return f;
}

td::Result<ABIStruct> parse_struct(const Json &j) {
  ABIStruct s;
  TRY_RESULT_ASSIGN(s.name, jstr_req(j, "name"));
  TRY_RESULT(ty_idx, jint_req(j, "ty_idx"));
  s.ty_idx = static_cast<int>(ty_idx);
  if (const Json *tp = jfield(j, "type_params"); tp != nullptr && tp->type() == JType::Array) {
    for (const auto &e : tp->get_array()) {
      if (e.type() != JType::String) {
        return td::Status::Error(PSLICE() << "struct '" << s.name << "'.type_params: expected string entries");
      }
      s.type_params.push_back(e.get_string().str());
    }
  }
  if (const Json *pfx = jfield(j, "prefix"); pfx != nullptr) {
    ABIStructPrefix p;
    TRY_RESULT_PREFIX(pn, jint_req(*pfx, "prefix_num"), PSLICE() << "struct '" << s.name << "'.prefix: ");
    p.prefix_num = static_cast<std::uint64_t>(pn);
    TRY_RESULT_PREFIX(pl, jint_req(*pfx, "prefix_len"), PSLICE() << "struct '" << s.name << "'.prefix: ");
    p.prefix_len = static_cast<int>(pl);
    // Struct opcode prefixes are NOT bound to 32 bits like union-variant
    // dispatch prefixes are (verified: MsgSinglePrefix48 uses a real 48-bit
    // prefix) -- only bound by the TON cell bit-width limit (1023 bits).
    if (p.prefix_len < 0 || p.prefix_len > 1023) {
      return td::Status::Error(PSLICE() << "struct '" << s.name << "'.prefix.prefix_len=" << p.prefix_len
                                         << " out of [0,1023]");
    }
    s.prefix = p;
  }
  const Json *fields = jfield(j, "fields");
  if (fields == nullptr || fields->type() != JType::Array) {
    return td::Status::Error(PSLICE() << "struct '" << s.name << "': missing 'fields'");
  }
  for (const auto &fj : fields->get_array()) {
    TRY_RESULT_PREFIX(f, parse_struct_field(fj), PSLICE() << "struct '" << s.name << "': ");
    s.fields.push_back(std::move(f));
  }
  if (const Json *cpu = jfield(j, "custom_pack_unpack"); cpu != nullptr) {
    TRY_RESULT(c, parse_custom_pack_unpack(*cpu));
    s.custom_pack_unpack = c;
  }
  if (has(j, "description")) {
    s.description = jstr(j, "description");
  }
  return s;
}

td::Result<ABIAlias> parse_alias(const Json &j) {
  ABIAlias a;
  TRY_RESULT_ASSIGN(a.name, jstr_req(j, "name"));
  TRY_RESULT(ty_idx, jint_req(j, "ty_idx"));
  a.ty_idx = static_cast<int>(ty_idx);
  TRY_RESULT(target, jint_req(j, "target_ty_idx"));
  a.target_ty_idx = static_cast<int>(target);
  if (const Json *tp = jfield(j, "type_params"); tp != nullptr && tp->type() == JType::Array) {
    for (const auto &e : tp->get_array()) {
      if (e.type() != JType::String) {
        return td::Status::Error(PSLICE() << "alias '" << a.name << "'.type_params: expected string entries");
      }
      a.type_params.push_back(e.get_string().str());
    }
  }
  if (const Json *cpu = jfield(j, "custom_pack_unpack"); cpu != nullptr) {
    TRY_RESULT(c, parse_custom_pack_unpack(*cpu));
    a.custom_pack_unpack = c;
  }
  if (has(j, "description")) {
    a.description = jstr(j, "description");
  }
  return a;
}

td::Result<ABIEnum> parse_enum(const Json &j) {
  ABIEnum en;
  TRY_RESULT_ASSIGN(en.name, jstr_req(j, "name"));
  TRY_RESULT(ty_idx, jint_req(j, "ty_idx"));
  en.ty_idx = static_cast<int>(ty_idx);
  TRY_RESULT(enc, jint_req(j, "encoded_as_ty_idx"));
  en.encoded_as_ty_idx = static_cast<int>(enc);
  const Json *members = jfield(j, "members");
  if (members == nullptr || members->type() != JType::Array) {
    return td::Status::Error(PSLICE() << "enum '" << en.name << "': missing 'members'");
  }
  for (const auto &mj : members->get_array()) {
    ABIEnumMember m;
    TRY_RESULT_ASSIGN(m.name, jstr_req(mj, "name"));
    TRY_RESULT_PREFIX(v, jbigint_req(mj, "value"), PSLICE() << "enum '" << en.name << "' member '" << m.name << "': ");
    m.value = v;
    if (has(mj, "description")) {
      m.description = jstr(mj, "description");
    }
    en.members.push_back(std::move(m));
  }
  if (const Json *cpu = jfield(j, "custom_pack_unpack"); cpu != nullptr) {
    TRY_RESULT(c, parse_custom_pack_unpack(*cpu));
    en.custom_pack_unpack = c;
  }
  if (has(j, "description")) {
    en.description = jstr(j, "description");
  }
  return en;
}

td::Result<ABIDeclaration> parse_declaration(const Json &j, int idx) {
  if (j.type() != JType::Object) {
    return td::Status::Error(PSLICE() << "declarations[" << idx << "] must be an object");
  }
  TRY_RESULT(kind, jstr_req(j, "kind"));
  ABIDeclaration d;
  if (kind == "struct") {
    d.kind = DeclKind::Struct;
    TRY_RESULT_PREFIX(s, parse_struct(j), PSLICE() << "declarations[" << idx << "]: ");
    d.as_struct = std::move(s);
    return d;
  }
  if (kind == "alias") {
    d.kind = DeclKind::Alias;
    TRY_RESULT_PREFIX(a, parse_alias(j), PSLICE() << "declarations[" << idx << "]: ");
    d.as_alias = std::move(a);
    return d;
  }
  if (kind == "enum") {
    d.kind = DeclKind::Enum;
    TRY_RESULT_PREFIX(e, parse_enum(j), PSLICE() << "declarations[" << idx << "]: ");
    d.as_enum = std::move(e);
    return d;
  }
  return td::Status::Error(PSLICE() << "declarations[" << idx << "]: unknown decl kind '" << kind << "'");
}

const std::string &decl_name(const ABIDeclaration &d) {
  switch (d.kind) {
    case DeclKind::Struct:
      return d.as_struct.name;
    case DeclKind::Alias:
      return d.as_alias.name;
    case DeclKind::Enum:
      return d.as_enum.name;
  }
  return d.as_struct.name;  // unreachable
}

std::size_t decl_type_params_size(const ABIDeclaration &d) {
  switch (d.kind) {
    case DeclKind::Struct:
      return d.as_struct.type_params.size();
    case DeclKind::Alias:
      return d.as_alias.type_params.size();
    case DeclKind::Enum:
      return 0;
  }
  return 0;
}

td::Result<ABIStructInstantiation> parse_struct_instantiation(const Json &j) {
  ABIStructInstantiation si;
  TRY_RESULT(ty_idx, jint_req(j, "ty_idx"));
  si.ty_idx = static_cast<int>(ty_idx);
  TRY_RESULT_ASSIGN(si.struct_name, jstr_req(j, "struct_name"));
  const Json *mf = jfield(j, "monomorphic_fields_ty_idx");
  if (mf == nullptr || mf->type() != JType::Array) {
    return td::Status::Error(PSLICE() << "struct_instantiation '" << si.struct_name
                                       << "': missing 'monomorphic_fields_ty_idx'");
  }
  for (const auto &e : mf->get_array()) {
    TRY_RESULT(v, num_i64(e));
    si.monomorphic_fields_ty_idx.push_back(static_cast<int>(v));
  }
  if (const Json *cpu = jfield(j, "custom_pack_unpack"); cpu != nullptr) {
    TRY_RESULT(c, parse_custom_pack_unpack(*cpu));
    si.custom_pack_unpack = c;
  }
  return si;
}

td::Result<ABIAliasInstantiation> parse_alias_instantiation(const Json &j) {
  ABIAliasInstantiation ai;
  TRY_RESULT(ty_idx, jint_req(j, "ty_idx"));
  ai.ty_idx = static_cast<int>(ty_idx);
  TRY_RESULT_ASSIGN(ai.alias_name, jstr_req(j, "alias_name"));
  TRY_RESULT(mt, jint_req(j, "monomorphic_target_ty_idx"));
  ai.monomorphic_target_ty_idx = static_cast<int>(mt);
  if (const Json *cpu = jfield(j, "custom_pack_unpack"); cpu != nullptr) {
    TRY_RESULT(c, parse_custom_pack_unpack(*cpu));
    ai.custom_pack_unpack = c;
  }
  return ai;
}

td::Result<ABIGetMethod> parse_get_method(const Json &j) {
  ABIGetMethod m;
  TRY_RESULT(id, jint_req(j, "tvm_method_id"));
  m.tvm_method_id = static_cast<int>(id);
  TRY_RESULT_ASSIGN(m.name, jstr_req(j, "name"));
  const Json *params = jfield(j, "parameters");
  if (params == nullptr || params->type() != JType::Array) {
    return td::Status::Error(PSLICE() << "get_method '" << m.name << "': missing 'parameters'");
  }
  for (const auto &pj : params->get_array()) {
    ABIGetMethodParam p;
    TRY_RESULT_ASSIGN(p.name, jstr_req(pj, "name"));
    TRY_RESULT(ty_idx, jint_req(pj, "ty_idx"));
    p.ty_idx = static_cast<int>(ty_idx);
    if (has(pj, "description")) {
      p.description = jstr(pj, "description");
    }
    if (const Json *dv = jfield(pj, "default_value"); dv != nullptr) {
      TRY_RESULT(v, parse_const_expr(*dv));
      p.default_value = std::move(v);
    }
    m.parameters.push_back(std::move(p));
  }
  TRY_RESULT(ret, jint_req(j, "return_ty_idx"));
  m.return_ty_idx = static_cast<int>(ret);
  if (has(j, "description")) {
    m.description = jstr(j, "description");
  }
  return m;
}

td::Result<ABIStorage> parse_storage(const Json &j) {
  ABIStorage s;
  if (has(j, "storage_ty_idx")) {
    TRY_RESULT(v, jint_req(j, "storage_ty_idx"));
    s.storage_ty_idx = static_cast<int>(v);
  }
  if (has(j, "storage_at_deployment_ty_idx")) {
    TRY_RESULT(v, jint_req(j, "storage_at_deployment_ty_idx"));
    s.storage_at_deployment_ty_idx = static_cast<int>(v);
  }
  return s;
}

// ABIInternalMessage/ABIExternalMessage/ABIOutgoingMessage are distinct types
// that all happen to be `{ body_ty_idx: number }` -- a template avoids either
// duplicating this loop 4x or reinterpret_cast'ing between unrelated vector<T>
// element types (which would be UB).
template <typename MsgT>
td::Status parse_msg_array(const Json &root, td::Slice field_name, std::vector<MsgT> *out) {
  const Json *arr = jfield(root, field_name);
  if (arr == nullptr) {
    return td::Status::OK();
  }
  if (arr->type() != JType::Array) {
    return td::Status::Error(PSLICE() << "'" << field_name << "' must be an array");
  }
  for (const auto &e : arr->get_array()) {
    TRY_RESULT(body, jint_req(e, "body_ty_idx"));
    out->push_back(MsgT{static_cast<int>(body)});
  }
  return td::Status::OK();
}

td::Result<ABIThrownError> parse_thrown_error(const Json &j) {
  ABIThrownError e;
  TRY_RESULT_ASSIGN(e.kind, jstr_req(j, "kind"));
  if (e.kind != "plain_int" && e.kind != "constant" && e.kind != "enum_member") {
    return td::Status::Error(PSLICE() << "thrown_error: unknown kind '" << e.kind << "'");
  }
  if (has(j, "name")) {
    e.name = jstr(j, "name");
  }
  if (has(j, "description")) {
    e.description = jstr(j, "description");
  }
  TRY_RESULT(code, jint_req(j, "err_code"));
  e.err_code = static_cast<int>(code);
  return e;
}


// Every ty_idx a Ty can reference (single helper so OOB-index and future
// passes stay in one place).
std::vector<int> ty_referenced_indices(const Ty &t) {
  std::vector<int> out;
  switch (t.kind) {
    case TyKind::Nullable:
      out.push_back(std::get<TyNullable>(t.data).inner_ty_idx);
      break;
    case TyKind::CellOf:
    case TyKind::ArrayOf:
    case TyKind::LispListOf:
      out.push_back(std::get<TyInner>(t.data).inner_ty_idx);
      break;
    case TyKind::Tensor:
    case TyKind::ShapedTuple:
      for (int i : std::get<TyItems>(t.data).items_ty_idx) out.push_back(i);
      break;
    case TyKind::MapKV: {
      const auto &m = std::get<TyMapKV>(t.data);
      out.push_back(m.key_ty_idx);
      out.push_back(m.value_ty_idx);
      break;
    }
    case TyKind::StructRef:
      for (int i : std::get<TyStructRef>(t.data).type_args_ty_idx) out.push_back(i);
      break;
    case TyKind::AliasRef:
      for (int i : std::get<TyAliasRef>(t.data).type_args_ty_idx) out.push_back(i);
      break;
    case TyKind::Union:
      for (const auto &v : std::get<TyUnion>(t.data).variants) out.push_back(v.variant_ty_idx);
      break;
    default:
      break;
  }
  return out;
}

td::Status check_ty_idx(int idx, std::size_t n, const std::string &where) {
  if (idx < 0 || static_cast<std::size_t>(idx) >= n) {
    return td::Status::Error(PSLICE() << where << ": ty_idx " << idx << " out of bounds [0," << n << ")");
  }
  return td::Status::OK();
}

td::Status validate_oob_indices(const ContractABI &abi) {
  const std::size_t n = abi.unique_types.size();
  for (std::size_t i = 0; i < abi.unique_types.size(); ++i) {
    for (int ref : ty_referenced_indices(abi.unique_types[i])) {
      TRY_STATUS(check_ty_idx(ref, n, "unique_types[" + std::to_string(i) + "]"));
    }
  }
  for (const auto &d : abi.declarations) {
    if (d.kind == DeclKind::Struct) {
      for (const auto &f : d.as_struct.fields) {
        TRY_STATUS(check_ty_idx(f.ty_idx, n, "struct '" + d.as_struct.name + "'.field '" + f.name + "'"));
        if (f.client_ty_idx) {
          TRY_STATUS_PREFIX(
              check_ty_idx(*f.client_ty_idx, n, "struct '" + d.as_struct.name + "'.field '" + f.name + "'.client_ty_idx"),
              "");
        }
      }
    } else if (d.kind == DeclKind::Alias) {
      TRY_STATUS(check_ty_idx(d.as_alias.target_ty_idx, n, "alias '" + d.as_alias.name + "'"));
    } else {
      TRY_STATUS(check_ty_idx(d.as_enum.encoded_as_ty_idx, n, "enum '" + d.as_enum.name + "'"));
    }
  }
  for (const auto &si : abi.struct_instantiations) {
    for (int ref : si.monomorphic_fields_ty_idx) {
      TRY_STATUS(check_ty_idx(ref, n, "struct_instantiation '" + si.struct_name + "'"));
    }
  }
  for (const auto &ai : abi.alias_instantiations) {
    TRY_STATUS(check_ty_idx(ai.monomorphic_target_ty_idx, n, "alias_instantiation '" + ai.alias_name + "'"));
  }
  for (const auto &m : abi.incoming_messages) TRY_STATUS(check_ty_idx(m.body_ty_idx, n, "incoming_messages"));
  for (const auto &m : abi.incoming_external) TRY_STATUS(check_ty_idx(m.body_ty_idx, n, "incoming_external"));
  for (const auto &m : abi.outgoing_messages) TRY_STATUS(check_ty_idx(m.body_ty_idx, n, "outgoing_messages"));
  for (const auto &m : abi.emitted_events) TRY_STATUS(check_ty_idx(m.body_ty_idx, n, "emitted_events"));
  if (abi.storage.storage_ty_idx) {
    TRY_STATUS(check_ty_idx(*abi.storage.storage_ty_idx, n, "storage.storage_ty_idx"));
  }
  if (abi.storage.storage_at_deployment_ty_idx) {
    TRY_STATUS(check_ty_idx(*abi.storage.storage_at_deployment_ty_idx, n, "storage.storage_at_deployment_ty_idx"));
  }
  for (const auto &gm : abi.get_methods) {
    for (const auto &p : gm.parameters) {
      TRY_STATUS(check_ty_idx(p.ty_idx, n, "get_method '" + gm.name + "'.param '" + p.name + "'"));
    }
    TRY_STATUS(check_ty_idx(gm.return_ty_idx, n, "get_method '" + gm.name + "'.return_ty_idx"));
  }
  return td::Status::OK();
}

td::Status validate_duplicate_decl_names(const ContractABI &abi) {
  std::unordered_set<std::string> structs, aliases, enums;
  for (const auto &d : abi.declarations) {
    std::unordered_set<std::string> *bucket =
        d.kind == DeclKind::Struct ? &structs : d.kind == DeclKind::Alias ? &aliases : &enums;
    const std::string &name = decl_name(d);
    if (!bucket->insert(name).second) {
      const char *kindname = d.kind == DeclKind::Struct ? "struct" : d.kind == DeclKind::Alias ? "alias" : "enum";
      return td::Status::Error(PSLICE() << "duplicate " << kindname << " declaration name '" << name << "'");
    }
  }
  return td::Status::OK();
}

// struct_instantiations[i].struct_name / alias_instantiations[i].alias_name
// are MANGLED monomorph names ("ResetTo<int64>"), not the plain declared
// name ("ResetTo"). The generic declaration is looked up by the part before
// the first '<'.
std::string base_name(const std::string &mangled) {
  auto lt = mangled.find('<');
  return lt == std::string::npos ? mangled : mangled.substr(0, lt);
}

td::Status validate_instantiation_arity(const ContractABI &abi) {
  std::unordered_map<std::string, const ABIDeclaration *> structs, aliases;
  for (const auto &d : abi.declarations) {
    if (d.kind == DeclKind::Struct) structs[d.as_struct.name] = &d;
    if (d.kind == DeclKind::Alias) aliases[d.as_alias.name] = &d;
  }
  for (const auto &si : abi.struct_instantiations) {
    auto it = structs.find(base_name(si.struct_name));
    if (it == structs.end()) {
      return td::Status::Error(PSLICE() << "struct_instantiation references unknown struct '" << si.struct_name << "'");
    }
    const std::size_t expected = it->second->as_struct.fields.size();
    if (si.monomorphic_fields_ty_idx.size() != expected) {
      return td::Status::Error(PSLICE() << "struct_instantiation '" << si.struct_name << "': arity mismatch, "
                                         << si.monomorphic_fields_ty_idx.size() << " monomorphic fields vs "
                                         << expected << " declared fields");
    }
  }
  for (const auto &t : abi.unique_types) {
    if (t.kind == TyKind::StructRef) {
      const auto &sr = std::get<TyStructRef>(t.data);
      if (sr.type_args_ty_idx.empty()) continue;
      auto it = structs.find(sr.struct_name);
      if (it == structs.end()) {
        return td::Status::Error(PSLICE() << "StructRef references unknown struct '" << sr.struct_name << "'");
      }
      const std::size_t expected = it->second->as_struct.type_params.size();
      if (sr.type_args_ty_idx.size() != expected) {
        return td::Status::Error(PSLICE() << "StructRef '" << sr.struct_name << "': arity mismatch, "
                                           << sr.type_args_ty_idx.size() << " type args vs " << expected
                                           << " declared type params");
      }
    } else if (t.kind == TyKind::AliasRef) {
      const auto &ar = std::get<TyAliasRef>(t.data);
      if (ar.type_args_ty_idx.empty()) continue;
      auto it = aliases.find(ar.alias_name);
      if (it == aliases.end()) {
        return td::Status::Error(PSLICE() << "AliasRef references unknown alias '" << ar.alias_name << "'");
      }
      const std::size_t expected = it->second->as_alias.type_params.size();
      if (ar.type_args_ty_idx.size() != expected) {
        return td::Status::Error(PSLICE() << "AliasRef '" << ar.alias_name << "': arity mismatch, "
                                           << ar.type_args_ty_idx.size() << " type args vs " << expected
                                           << " declared type params");
      }
    }
  }
  return td::Status::OK();
}

// Alias target chains: alias -> target_ty_idx -> (if that Ty is itself an
// AliasRef) the next alias, etc. A cycle means no chain ever bottoms out at
// a non-alias Ty.
td::Status validate_alias_cycles(const ContractABI &abi) {
  std::unordered_map<std::string, const ABIAlias *> aliases;
  for (const auto &d : abi.declarations) {
    if (d.kind == DeclKind::Alias) aliases[d.as_alias.name] = &d.as_alias;
  }
  for (const auto &[start_name, start_alias] : aliases) {
    std::unordered_set<std::string> visited{start_name};
    const ABIAlias *cur = start_alias;
    for (;;) {
      if (cur->target_ty_idx < 0 || static_cast<std::size_t>(cur->target_ty_idx) >= abi.unique_types.size()) {
        break;  // OOB already reported by validate_oob_indices
      }
      const Ty &target = abi.unique_types[cur->target_ty_idx];
      if (target.kind != TyKind::AliasRef) {
        break;  // bottoms out at a concrete kind -- fine
      }
      const std::string &next_name = std::get<TyAliasRef>(target.data).alias_name;
      if (!visited.insert(next_name).second) {
        return td::Status::Error(PSLICE() << "alias cycle detected starting at '" << start_name << "' (revisits '"
                                           << next_name << "')");
      }
      auto it = aliases.find(next_name);
      if (it == aliases.end()) {
        break;  // unknown alias name already reported elsewhere
      }
      cur = it->second;
    }
  }
  return td::Status::OK();
}

td::Status validate(const ContractABI &abi) {
  if (abi.abi_schema_version != "1.0") {
    return td::Status::Error(PSLICE() << "unsupported abi_schema_version '" << abi.abi_schema_version
                                       << "' (expected exactly \"1.0\")");
  }
  TRY_STATUS(validate_oob_indices(abi));
  TRY_STATUS(validate_duplicate_decl_names(abi));
  TRY_STATUS(validate_instantiation_arity(abi));
  TRY_STATUS(validate_alias_cycles(abi));
  return td::Status::OK();
}

}  // namespace

td::Result<ContractABI> load_abi_from_json(const std::string &json_text) {
  std::string buf = json_text;
  TRY_RESULT_PREFIX(root, td::json_decode(td::MutableSlice(buf)), "abi JSON parse error: ");
  if (root.type() != JType::Object) {
    return td::Status::Error("abi JSON root must be an object");
  }

  ContractABI abi;
  TRY_RESULT_ASSIGN(abi.contract_name, jstr_req(root, "contract_name"));
  if (has(root, "author")) abi.author = jstr(root, "author");
  if (has(root, "version")) abi.version = jstr(root, "version");
  if (has(root, "description")) abi.description = jstr(root, "description");
  TRY_RESULT_ASSIGN(abi.abi_schema_version, jstr_req(root, "abi_schema_version"));

  const Json *uniq = jfield(root, "unique_types");
  if (uniq == nullptr || uniq->type() != JType::Array) {
    return td::Status::Error("missing 'unique_types'");
  }
  int ti = 0;
  for (const auto &tj : uniq->get_array()) {
    TRY_RESULT(t, parse_ty(tj, ti));
    abi.unique_types.push_back(std::move(t));
    ++ti;
  }

  if (const Json *si = jfield(root, "struct_instantiations"); si != nullptr) {
    if (si->type() != JType::Array) return td::Status::Error("'struct_instantiations' must be an array");
    for (const auto &e : si->get_array()) {
      TRY_RESULT(v, parse_struct_instantiation(e));
      abi.struct_instantiations.push_back(std::move(v));
    }
  }
  if (const Json *ai = jfield(root, "alias_instantiations"); ai != nullptr) {
    if (ai->type() != JType::Array) return td::Status::Error("'alias_instantiations' must be an array");
    for (const auto &e : ai->get_array()) {
      TRY_RESULT(v, parse_alias_instantiation(e));
      abi.alias_instantiations.push_back(std::move(v));
    }
  }

  const Json *decls = jfield(root, "declarations");
  if (decls == nullptr || decls->type() != JType::Array) {
    return td::Status::Error("missing 'declarations'");
  }
  int di = 0;
  for (const auto &dj : decls->get_array()) {
    TRY_RESULT(d, parse_declaration(dj, di));
    abi.declarations.push_back(std::move(d));
    ++di;
  }

  if (const Json *st = jfield(root, "storage"); st != nullptr) {
    TRY_RESULT(v, parse_storage(*st));
    abi.storage = v;
  }
  TRY_STATUS(parse_msg_array(root, "incoming_messages", &abi.incoming_messages));
  TRY_STATUS(parse_msg_array(root, "incoming_external", &abi.incoming_external));
  TRY_STATUS(parse_msg_array(root, "outgoing_messages", &abi.outgoing_messages));
  TRY_STATUS(parse_msg_array(root, "emitted_events", &abi.emitted_events));

  if (const Json *gms = jfield(root, "get_methods"); gms != nullptr) {
    if (gms->type() != JType::Array) return td::Status::Error("'get_methods' must be an array");
    for (const auto &e : gms->get_array()) {
      TRY_RESULT(m, parse_get_method(e));
      abi.get_methods.push_back(std::move(m));
    }
  }
  if (const Json *te = jfield(root, "thrown_errors"); te != nullptr) {
    if (te->type() != JType::Array) return td::Status::Error("'thrown_errors' must be an array");
    for (const auto &e : te->get_array()) {
      TRY_RESULT(t, parse_thrown_error(e));
      abi.thrown_errors.push_back(std::move(t));
    }
  }

  TRY_RESULT_ASSIGN(abi.compiler_name, jstr_req(root, "compiler_name"));
  TRY_RESULT_ASSIGN(abi.compiler_version, jstr_req(root, "compiler_version"));

  TRY_STATUS(validate(abi));
  return abi;
}

}  // namespace ton_abi

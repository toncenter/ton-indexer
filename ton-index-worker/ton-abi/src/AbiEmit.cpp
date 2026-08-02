#include "AbiEmit.h"

#include "td/utils/misc.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>
#include <vector>

namespace ton_abi {

namespace {

// HardFail aborts generation for non-standard dictionary keys. Per-direction
// stubs are represented by omitted members.
struct HardFail {
  std::string msg;
};

// Identifier rules
bool is_safe_ident(const std::string &s) {
  if (s.empty()) {
    return false;
  }
  char c0 = s[0];
  if (!(std::isalpha(static_cast<unsigned char>(c0)) || c0 == '_')) {
    return false;
  }
  for (char c : s) {
    if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) {
      return false;
    }
  }
  return true;
}

std::string sanitize_ident(const std::string &s) {
  if (is_safe_ident(s)) {
    return s;
  }
  std::string out;
  for (char c : s) {
    out += (std::isalnum(static_cast<unsigned char>(c)) || c == '_') ? c : '_';
  }
  if (out.empty() || std::isdigit(static_cast<unsigned char>(out[0]))) {
    out = "_" + out;
  }
  return out;
}

// Monomorph mangling: "ResetTo<int64>" -> "ResetTo_int64",
// "GenericPair<int32, int64>" -> "GenericPair_int32_int64". Replace
// punctuation with underscores, collapse runs, and trim the trailing one.
std::string mangle_monomorph(const std::string &s) {
  std::string t;
  for (char c : s) {
    t += (std::isalnum(static_cast<unsigned char>(c)) || c == '_') ? c : '_';
  }
  std::string out;
  for (char c : t) {
    if (c == '_' && (out.empty() || out.back() == '_')) {
      continue;
    }
    out += c;
  }
  while (!out.empty() && out.back() == '_') {
    out.pop_back();
  }
  if (out.empty() || std::isdigit(static_cast<unsigned char>(out[0]))) {
    out = "_" + out;
  }
  return out;
}

std::string snake_case(const std::string &s) {
  std::string out;
  for (std::size_t i = 0; i < s.size(); ++i) {
    char c = s[i];
    if (std::isupper(static_cast<unsigned char>(c))) {
      if (i > 0 && (std::islower(static_cast<unsigned char>(s[i - 1])) ||
                    std::isdigit(static_cast<unsigned char>(s[i - 1])))) {
        out += '_';
      }
      out += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    } else if (std::isalnum(static_cast<unsigned char>(c))) {
      out += c;
    } else {
      out += '_';
    }
  }
  return out.empty() ? "contract" : out;
}

std::string base_name(const std::string &mangled_or_plain) {
  auto lt = mangled_or_plain.find('<');
  return lt == std::string::npos ? mangled_or_plain : mangled_or_plain.substr(0, lt);
}

std::string cpp_str_lit(const std::string &s) {
  std::string out = "\"";
  for (unsigned char c : s) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (c < 0x20 || c >= 0x7f) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\x%02x", c);
          out += buf;
          out += "\"\"";  // terminate the \x escape so following hex digits don't merge
        } else {
          out += static_cast<char>(c);
        }
    }
  }
  out += "\"";
  return out;
}

std::string kind_render(TyKind k) {
  switch (k) {
    case TyKind::Int: return "int";
    case TyKind::Slice: return "slice";
    case TyKind::Builder: return "builder";
    case TyKind::Callable: return "continuation";
    case TyKind::Unknown: return "unknown";
    case TyKind::GenericT: return "genericT";
    default: return "?";
  }
}

// Emitter
class Emitter {
 public:
  Emitter(const ContractABI &abi, const AbiKernel &kernel, std::string out_name)
      : abi_(abi), kernel_(kernel), out_name_(std::move(out_name)) {}

  GeneratedFiles run() {
    snake_ = out_name_.empty() ? snake_case(abi_.contract_name) : snake_case(out_name_);
    ns_ = "ton_abi::gen::" + snake_;
    collect_targets();

    for (std::size_t i = 0; i < targets_.size(); ++i) {
      emit_target_bodies(i);
    }
    // Union bodies (load/store) -- created lazily during type/dep walking; the
    // vector may grow as nested unions are discovered, so re-check size.
    for (std::size_t i = 0; i < union_items_.size(); ++i) {
      emit_union_bodies(static_cast<int>(i));
    }

    build_order();

    GeneratedFiles out;
    out.contract_snake = snake_;
    out.header = assemble_header();
    out.source = assemble_source();
    return out;
  }

 private:
  // Target model
  enum class TKind { Struct, Alias, Enum };
  struct Target {
    TKind kind;
    std::string cpp_name;
    std::string orig_name;  // "$" label + custom key base
    int self_ty_idx = 0;
    const ABIStruct *s = nullptr;
    const ABIAlias *a = nullptr;
    const ABIEnum *e = nullptr;
    bool is_monomorph = false;
    bool abi_value_form = false;  // fully-custom struct emitted as `using N = AbiValue` (no forward-decl)

    // filled during body emission
    std::string h_decl;   // header text (type + method sigs / using)
    std::string cpp_body; // out-of-line bodies
    std::set<int> deps;   // item indices (targets/unions) this needs complete
  };

  struct UnionItem {
    int union_ty_idx = 0;
    std::string cpp_name;  // AbiUnion_<n>
    std::string variant_type_list;
    std::string h_decl;
    std::string cpp_body;
    std::set<int> deps;
  };

  const ContractABI &abi_;
  const AbiKernel &kernel_;
  std::string out_name_;
  std::string snake_;
  std::string ns_;

  std::vector<Target> targets_;
  std::unordered_map<std::string, int> concrete_struct_;
  std::unordered_map<std::string, int> concrete_alias_;
  std::unordered_map<std::string, int> concrete_enum_;
  std::unordered_map<int, int> mono_struct_ty_;  // instantiation ty_idx -> target
  std::unordered_map<int, int> mono_alias_ty_;
  std::set<std::string> used_names_;

  // union items keyed by union ty_idx; item "index" in the combined graph is
  // encoded as (kUnionBase + local index).
  std::map<int, int> union_of_ty_;  // union ty_idx -> local union item id
  std::vector<UnionItem> union_items_;
  static constexpr int kUnionBase = 1000000;

  std::vector<int> order_;  // combined emit order (target indices + union ids offset)

  const Ty &ty(int idx) const { return *kernel_.ty_by_idx(idx).move_as_ok(); }

  std::string uniq_name(std::string base) {
    if (used_names_.insert(base).second) {
      return base;
    }
    for (int k = 2;; ++k) {
      std::string cand = base + "_" + std::to_string(k);
      if (used_names_.insert(cand).second) {
        return cand;
      }
    }
  }

  // ---- collect the set of named C++ types to emit ------------------------
  void collect_targets() {
    for (const auto &d : abi_.declarations) {
      if (d.kind == DeclKind::Struct && d.as_struct.type_params.empty()) {
        Target t;
        t.kind = TKind::Struct;
        t.cpp_name = uniq_name(sanitize_ident(d.as_struct.name));
        t.orig_name = d.as_struct.name;
        t.self_ty_idx = d.as_struct.ty_idx;
        t.s = &d.as_struct;
        // Fully-custom struct -> type-erased AbiValue form (precomputed here so
        // callers emitted before this decl resolve the free-function form).
        t.abi_value_form = has_custom(d.as_struct.custom_pack_unpack, true) &&
                           has_custom(d.as_struct.custom_pack_unpack, false);
        concrete_struct_[d.as_struct.name] = static_cast<int>(targets_.size());
        targets_.push_back(std::move(t));
      } else if (d.kind == DeclKind::Enum) {
        Target t;
        t.kind = TKind::Enum;
        t.cpp_name = uniq_name(sanitize_ident(d.as_enum.name));
        t.orig_name = d.as_enum.name;
        t.self_ty_idx = d.as_enum.ty_idx;
        t.e = &d.as_enum;
        concrete_enum_[d.as_enum.name] = static_cast<int>(targets_.size());
        targets_.push_back(std::move(t));
      } else if (d.kind == DeclKind::Alias && d.as_alias.type_params.empty()) {
        Target t;
        t.kind = TKind::Alias;
        t.cpp_name = uniq_name(sanitize_ident(d.as_alias.name));
        t.orig_name = d.as_alias.name;
        t.self_ty_idx = d.as_alias.ty_idx;
        t.a = &d.as_alias;
        concrete_alias_[d.as_alias.name] = static_cast<int>(targets_.size());
        targets_.push_back(std::move(t));
      }
    }
    for (const auto &inst : abi_.struct_instantiations) {
      Target t;
      t.kind = TKind::Struct;
      t.cpp_name = uniq_name(mangle_monomorph(inst.struct_name));
      t.self_ty_idx = inst.ty_idx;
      t.is_monomorph = true;
      // "$" label = plain base struct name (that is what the value dump carries,
      // never the mangled monomorph name); generic decl carries prefix / custom.
      std::string base = base_name(inst.struct_name);
      auto rs = kernel_.get_struct(base);
      t.s = rs.is_ok() ? rs.move_as_ok() : nullptr;
      t.orig_name = base;
      mono_struct_ty_[inst.ty_idx] = static_cast<int>(targets_.size());
      targets_.push_back(std::move(t));
    }
    for (const auto &inst : abi_.alias_instantiations) {
      Target t;
      t.kind = TKind::Alias;
      t.cpp_name = uniq_name(mangle_monomorph(inst.alias_name));
      t.self_ty_idx = inst.ty_idx;
      t.is_monomorph = true;
      std::string base = base_name(inst.alias_name);
      auto ra = kernel_.get_alias(base);
      t.a = ra.is_ok() ? ra.move_as_ok() : nullptr;
      t.orig_name = base;
      mono_alias_ty_[inst.ty_idx] = static_cast<int>(targets_.size());
      targets_.push_back(std::move(t));
    }
  }

  int union_item_for(int union_ty_idx) {
    auto it = union_of_ty_.find(union_ty_idx);
    if (it != union_of_ty_.end()) {
      return it->second;
    }
    int id = static_cast<int>(union_items_.size());
    union_of_ty_[union_ty_idx] = id;
    UnionItem ui;
    ui.union_ty_idx = union_ty_idx;
    ui.cpp_name = "AbiUnion_" + std::to_string(id);
    union_items_.push_back(std::move(ui));
    // build variant type list now (may create more union items / deps)
    build_union_type(id);
    return id;
  }

  // encode combined graph index
  int gi_target(int t) const { return t; }
  int gi_union(int u) const { return kUnionBase + u; }
  bool is_union_gi(int gi) const { return gi >= kUnionBase; }

  // ---- name resolution for a referenced ty -------------------------------
  std::string name_for_ref(int ty_idx) {
    const Ty &t = ty(ty_idx);
    if (t.kind == TyKind::EnumRef) {
      const auto &n = std::get<TyEnumRef>(t.data).enum_name;
      auto it = concrete_enum_.find(n);
      if (it == concrete_enum_.end()) {
        throw HardFail{"enum not found: " + n};
      }
      return targets_[it->second].cpp_name;
    }
    if (t.kind == TyKind::StructRef) {
      const auto &sr = std::get<TyStructRef>(t.data);
      if (!sr.type_args_ty_idx.empty()) {
        auto it = mono_struct_ty_.find(ty_idx);
        if (it != mono_struct_ty_.end()) {
          return targets_[it->second].cpp_name;
        }
        // generic self-reference (inside a generic decl) -- not emitted; should
        // not be reached by concrete emission.
        throw HardFail{"unresolved generic struct ref: " + sr.struct_name};
      }
      auto it = concrete_struct_.find(sr.struct_name);
      if (it == concrete_struct_.end()) {
        throw HardFail{"struct not found: " + sr.struct_name};
      }
      return targets_[it->second].cpp_name;
    }
    if (t.kind == TyKind::AliasRef) {
      const auto &ar = std::get<TyAliasRef>(t.data);
      if (!ar.type_args_ty_idx.empty()) {
        auto it = mono_alias_ty_.find(ty_idx);
        if (it != mono_alias_ty_.end()) {
          return targets_[it->second].cpp_name;
        }
        throw HardFail{"unresolved generic alias ref: " + ar.alias_name};
      }
      auto it = concrete_alias_.find(ar.alias_name);
      if (it == concrete_alias_.end()) {
        throw HardFail{"alias not found: " + ar.alias_name};
      }
      return targets_[it->second].cpp_name;
    }
    throw HardFail{"name_for_ref on non-ref ty"};
  }

  // True if a StructRef resolves to a fully-custom (AbiValue-form) struct, which
  // exposes free N_from_slice/N_store/N_to_abi_value instead of member methods.
  bool is_abivalue_struct(int ty_idx) {
    const Ty &t = ty(ty_idx);
    if (t.kind != TyKind::StructRef) {
      return false;
    }
    return targets_[target_index_for_ref(ty_idx)].abi_value_form;
  }

  int target_index_for_ref(int ty_idx) {
    const Ty &t = ty(ty_idx);
    if (t.kind == TyKind::EnumRef) {
      return concrete_enum_.at(std::get<TyEnumRef>(t.data).enum_name);
    }
    if (t.kind == TyKind::StructRef) {
      const auto &sr = std::get<TyStructRef>(t.data);
      if (!sr.type_args_ty_idx.empty()) {
        return mono_struct_ty_.at(ty_idx);
      }
      return concrete_struct_.at(sr.struct_name);
    }
    const auto &ar = std::get<TyAliasRef>(t.data);
    if (!ar.type_args_ty_idx.empty()) {
      return mono_alias_ty_.at(ty_idx);
    }
    return concrete_alias_.at(ar.alias_name);
  }

  // ---- can this type be represented as a C++ member? ----------------------
  bool member_representable(int ty_idx) {
    DepthGuard __g(depth_, ty_idx);
    const Ty &t = ty(ty_idx);
    switch (t.kind) {
      case TyKind::Int:
      case TyKind::Slice:
      case TyKind::Builder:
      case TyKind::Callable:
      case TyKind::Unknown:
      case TyKind::GenericT:
        return false;
      case TyKind::Nullable:
        return member_representable(std::get<TyNullable>(t.data).inner_ty_idx);
      case TyKind::CellOf:
      case TyKind::ArrayOf:
      case TyKind::LispListOf:
        return member_representable(std::get<TyInner>(t.data).inner_ty_idx);
      case TyKind::Tensor:
      case TyKind::ShapedTuple: {
        for (int i : std::get<TyItems>(t.data).items_ty_idx) {
          if (!member_representable(i)) {
            return false;
          }
        }
        return true;
      }
      case TyKind::MapKV: {
        const auto &m = std::get<TyMapKV>(t.data);
        return member_representable(m.key_ty_idx) && member_representable(m.value_ty_idx);
      }
      case TyKind::Union: {
        for (const auto &v : std::get<TyUnion>(t.data).variants) {
          const Ty &vt = ty(v.variant_ty_idx);
          if (vt.kind == TyKind::NullLiteral || vt.kind == TyKind::Void) {
            continue;
          }
          if (!member_representable(v.variant_ty_idx)) {
            return false;
          }
        }
        return true;
      }
      default:
        return true;  // scalars, address, cell, bits, string, bool, named refs, null/void
    }
  }

  int depth_ = 0;
  int uid_ = 0;  // monotonic suffix for emitted local temps -> no shadowing

  // Raw try-binds emitted INSIDE composite IIFE/lambda bodies. We do NOT use
  // td's TRY_RESULT/TRY_STATUS there: nesting one td try-macro inside another
  // td try-macro's argument (e.g. Cell<Cell<T>>) is mishandled by MSVC's
  // parser even with /Zc:preprocessor. These expand inline with unique temp
  // names, so arbitrary nesting is fine. `var`/`rtmp` are caller-unique.
  static std::string bind_res(const std::string &var, const std::string &rtmp, const std::string &expr) {
    return "auto " + rtmp + " = (" + expr + ");\n    if (" + rtmp + ".is_error()) { return " + rtmp +
           ".move_as_error(); }\n    auto " + var + " = " + rtmp + ".move_as_ok();\n";
  }
  static std::string bind_stat(const std::string &stmp, const std::string &expr) {
    // `return <status>;` (not move_as_error) -- a Status error converts to both
    // td::Status and td::Result<T> return types, matching TRY_STATUS semantics.
    return "{ auto " + stmp + " = (" + expr + "); if (" + stmp + ".is_error()) { return " + stmp + "; } }";
  }

  struct DepthGuard {
    int &d;
    explicit DepthGuard(int &dd, int ty_idx) : d(dd) {
      if (++d > 500) {
        throw HardFail{"recursion depth exceeded at ty " + std::to_string(ty_idx)};
      }
    }
    ~DepthGuard() { --d; }
  };

  // intN/uintN with n<=64 map to a native C++ integer (td::int64 / td::uint64)
  // instead of td::RefInt256 -- the leaf fast paths (load_int64/store_int64/...)
  // are bit-for-bit identical on the wire, so this is a member-type change only.
  // n>64, varintN, varuintN and coins keep td::RefInt256. Returns "" for any
  // non-native-int kind.
  static std::string native_int_type(const Ty &t) {
    if (t.kind == TyKind::IntN && std::get<TyWidth>(t.data).n <= 64) return "td::int64";
    if (t.kind == TyKind::UintN && std::get<TyWidth>(t.data).n <= 64) return "td::uint64";
    return "";
  }

  // ---- C++ type string ----------------------------------------------------
  std::string cpp_type(int ty_idx) {
    DepthGuard __g(depth_, ty_idx);
    const Ty &t = ty(ty_idx);
    switch (t.kind) {
      case TyKind::IntN:
      case TyKind::UintN: {
        std::string nt = native_int_type(t);
        return nt.empty() ? "td::RefInt256" : nt;
      }
      case TyKind::VarIntN:
      case TyKind::VarUIntN:
      case TyKind::Coins:
        return "td::RefInt256";
      case TyKind::Bool:
        return "bool";
      case TyKind::BitsN:
      case TyKind::Remaining:
        return "td::Ref<vm::CellSlice>";
      case TyKind::String:
        return "std::string";
      case TyKind::Address:
      case TyKind::AddressOpt:
      case TyKind::AddressExt:
      case TyKind::AddressAny:
        return "ton_abi::AbiAddress";
      case TyKind::Cell:
        return "td::Ref<vm::Cell>";
      case TyKind::CellOf:
        return "ton_abi::gen::CellOf<" + cpp_type(std::get<TyInner>(t.data).inner_ty_idx) + ">";
      case TyKind::Nullable:
        return "std::optional<" + cpp_type(std::get<TyNullable>(t.data).inner_ty_idx) + ">";
      case TyKind::ArrayOf:
      case TyKind::LispListOf:
        return "std::vector<" + cpp_type(std::get<TyInner>(t.data).inner_ty_idx) + ">";
      case TyKind::Tensor:
      case TyKind::ShapedTuple: {
        std::string s = "std::tuple<";
        const auto &items = std::get<TyItems>(t.data).items_ty_idx;
        for (std::size_t i = 0; i < items.size(); ++i) {
          if (i) s += ", ";
          s += cpp_type(items[i]);
        }
        s += ">";
        return s;
      }
      case TyKind::MapKV: {
        const auto &m = std::get<TyMapKV>(t.data);
        return "std::vector<std::pair<" + cpp_key_type(m.key_ty_idx) + ", " + cpp_type(m.value_ty_idx) + ">>";
      }
      case TyKind::EnumRef:
        return "td::RefInt256";  // enums retain their bigint wire representation
      case TyKind::StructRef:
      case TyKind::AliasRef:
        return name_for_ref(ty_idx);
      case TyKind::Union:
        return union_items_[union_item_for(ty_idx)].cpp_name;
      case TyKind::NullLiteral:
      case TyKind::Void:
        return "std::monostate";
      default:
        throw HardFail{"cpp_type: non-representable ty kind"};
    }
  }

  std::string cpp_key_type(int key_ty_idx) {
    const Ty &t = ty(key_ty_idx);
    if (t.kind == TyKind::IntN || t.kind == TyKind::UintN) {
      std::string nt = native_int_type(t);
      return nt.empty() ? "td::RefInt256" : nt;
    }
    if (t.kind == TyKind::Address) {
      return "ton_abi::AbiAddress";
    }
    // Non-standard key: still emit a placeholder type; the body emitter will
    // HardFail before any output is produced.
    return "td::RefInt256";
  }

  // dict key wire width + non-standard check (shared with body emit)
  int dict_key_bits_or_fail(int key_ty_idx, const std::string &field_path) {
    const Ty &t = ty(key_ty_idx);
    if (t.kind == TyKind::IntN || t.kind == TyKind::UintN) {
      return std::get<TyWidth>(t.data).n;
    }
    if (t.kind == TyKind::Address) {
      return 267;
    }
    throw HardFail{"'" + field_path + "' is 'map<non-standard-key>': such a map key can not be handled (map-key)"};
  }

  // ---- union type construction -------------------------------------------
  void build_union_type(int union_local_id) {
    // NB: cpp_type / collect_type_deps below may create NESTED union items and
    // push_back into union_items_, reallocating it -- so we must NOT hold a
    // UnionItem& across those calls (use-after-realloc). Build into locals,
    // then re-index to store.
    int union_ty = union_items_[union_local_id].union_ty_idx;
    std::string name = union_items_[union_local_id].cpp_name;
    auto ru = kernel_.resolve_union(union_ty, std::nullopt).move_as_ok();
    std::string list;
    std::set<int> deps;
    for (std::size_t i = 0; i < ru.variants.size(); ++i) {
      if (i) list += ", ";
      const Ty &vt = ty(ru.variants[i].variant_ty_idx);
      if (vt.kind == TyKind::NullLiteral || vt.kind == TyKind::Void) {
        list += "std::monostate";
      } else {
        list += cpp_type(ru.variants[i].variant_ty_idx);
      }
      collect_type_deps(ru.variants[i].variant_ty_idx, deps);
    }
    UnionItem &ui = union_items_[union_local_id];
    ui.variant_type_list = list;
    ui.deps = std::move(deps);
    ui.h_decl = "using " + name + " = std::variant<" + list + ">;";
  }

  // ---- dependency collection (by-value completeness) ---------------------
  void collect_type_deps(int ty_idx, std::set<int> &out) {
    DepthGuard __g(depth_, ty_idx);
    const Ty &t = ty(ty_idx);
    switch (t.kind) {
      case TyKind::Nullable:
        collect_type_deps(std::get<TyNullable>(t.data).inner_ty_idx, out);
        break;
      case TyKind::CellOf: {
        // cellOf is a cell-boundary held via shared_ptr indirection: a struct
        // inner only needs a forward declaration (all structs are forward-
        // declared), so it is NOT an ordering dependency -- this breaks
        // recursive-struct cycles. Non-struct inners still need their using/type
        // ordered ahead.
        int inner = std::get<TyInner>(t.data).inner_ty_idx;
        if (ty(inner).kind != TyKind::StructRef) {
          collect_type_deps(inner, out);
        }
        break;
      }
      case TyKind::ArrayOf:
      case TyKind::LispListOf:
        collect_type_deps(std::get<TyInner>(t.data).inner_ty_idx, out);
        break;
      case TyKind::Tensor:
      case TyKind::ShapedTuple:
        for (int i : std::get<TyItems>(t.data).items_ty_idx) {
          collect_type_deps(i, out);
        }
        break;
      case TyKind::MapKV: {
        const auto &m = std::get<TyMapKV>(t.data);
        collect_type_deps(m.key_ty_idx, out);
        collect_type_deps(m.value_ty_idx, out);
        break;
      }
      case TyKind::Union:
        out.insert(gi_union(union_item_for(ty_idx)));
        break;
      case TyKind::StructRef:
      case TyKind::AliasRef:
      case TyKind::EnumRef:
        out.insert(gi_target(target_index_for_ref(ty_idx)));
        break;
      default:
        break;  // leaves, monostate
    }
  }

  // ---- field-path helpers -------------------------------------------------
  static std::string fpath(const std::string &base, const std::string &f) { return base + "." + f; }

  // ---- load expression (returns C++ expr of td::Result<CppType>) ---------
  // `sv` is the vm::CellSlice lvalue name in scope.
  std::string load_expr(int ty_idx, const std::string &sv, const std::string &path, std::optional<int> u_label) {
    DepthGuard __g(depth_, ty_idx);
    const Ty &t = ty(ty_idx);
    switch (t.kind) {
      case TyKind::IntN: {
        std::string w = std::to_string(std::get<TyWidth>(t.data).n);
        return (native_int_type(t).empty() ? "load_int(" : "load_int64(") + sv + ", " + w + ")";
      }
      case TyKind::UintN: {
        std::string w = std::to_string(std::get<TyWidth>(t.data).n);
        return (native_int_type(t).empty() ? "load_uint(" : "load_uint64(") + sv + ", " + w + ")";
      }
      case TyKind::VarIntN: return "load_varint(" + sv + ", " + std::to_string(std::get<TyWidth>(t.data).n) + ")";
      case TyKind::VarUIntN: return "load_varuint(" + sv + ", " + std::to_string(std::get<TyWidth>(t.data).n) + ")";
      case TyKind::Coins: return "load_coins(" + sv + ")";
      case TyKind::Bool: return "load_bool(" + sv + ")";
      case TyKind::Cell: return "load_cell(" + sv + ")";
      case TyKind::String: return "load_string(" + sv + ")";
      case TyKind::Remaining: return "load_remaining(" + sv + ")";
      case TyKind::BitsN: return "load_bits(" + sv + ", " + std::to_string(std::get<TyWidth>(t.data).n) + ")";
      case TyKind::Address: return "load_address(" + sv + ")";
      case TyKind::AddressOpt: return "load_maybe_address(" + sv + ")";
      case TyKind::AddressExt: return "load_external_address(" + sv + ")";
      case TyKind::AddressAny: return "load_address_any(" + sv + ")";
      case TyKind::NullLiteral:
      case TyKind::Void:
        return "td::Result<std::monostate>(std::monostate{})";
      case TyKind::EnumRef:
        return name_for_ref(ty_idx) + "_from_slice(" + sv + ")";
      case TyKind::StructRef:
        return is_abivalue_struct(ty_idx) ? name_for_ref(ty_idx) + "_from_slice(" + sv + ")"
                                          : name_for_ref(ty_idx) + "::from_slice(" + sv + ")";
      case TyKind::AliasRef:
        return name_for_ref(ty_idx) + "_from_slice(" + sv + ")";
      case TyKind::Union:
        return "load_union_" + std::to_string(union_item_for(ty_idx)) + "(" + sv + ")";
      case TyKind::Nullable: {
        int inner = std::get<TyNullable>(t.data).inner_ty_idx;
        std::string it = cpp_type(inner);
        int k = ++uid_;
        std::string nv = "__nv" + std::to_string(k);
        std::string s;
        s += "[&]() -> td::Result<std::optional<" + it + ">> {\n";
        s += "    " + bind_res("__pres" + std::to_string(k), "__rp" + std::to_string(k),
                                "load_maybe_prefix(" + sv + ")");
        s += "    if (!__pres" + std::to_string(k) + ") { return std::optional<" + it + ">(); }\n";
        s += "    " + bind_res(nv, "__r" + std::to_string(k), load_expr(inner, sv, path, std::nullopt));
        s += "    return std::optional<" + it + ">(std::move(" + nv + "));\n";
        s += "  }()";
        return s;
      }
      case TyKind::CellOf: {
        int inner = std::get<TyInner>(t.data).inner_ty_idx;
        std::string it = cpp_type(inner);
        int k = ++uid_;
        std::string cs = "__cs" + std::to_string(k), cv = "__cv" + std::to_string(k);
        std::string s;
        s += "[&]() -> td::Result<ton_abi::gen::CellOf<" + it + ">> {\n";
        s += "    " + bind_res("__rs" + std::to_string(k), "__rr" + std::to_string(k),
                                "load_ref_slice(" + sv + ")");
        s += "    vm::CellSlice " + cs + " = *__rs" + std::to_string(k) + ";\n";
        s += "    " + bind_res(cv, "__rc" + std::to_string(k), load_expr(inner, cs, path, std::nullopt));
        s += "    return ton_abi::gen::CellOf<" + it + ">{std::make_shared<" + it + ">(std::move(" + cv + "))};\n";
        s += "  }()";
        return s;
      }
      case TyKind::ArrayOf:
      case TyKind::LispListOf: {
        int inner = std::get<TyInner>(t.data).inner_ty_idx;
        std::string it = cpp_type(inner);
        std::string leaf = t.kind == TyKind::ArrayOf ? "load_array" : "load_lisp_list";
        int k = ++uid_;
        std::string out = "__out" + std::to_string(k), e = "__e" + std::to_string(k), ev = "__ev" + std::to_string(k);
        std::string s;
        s += "[&]() -> td::Result<std::vector<" + it + ">> {\n";
        s += "    std::vector<" + it + "> " + out + ";\n";
        s += "    " + bind_stat("__ls" + std::to_string(k),
                                leaf + "(" + sv + ", [&](vm::CellSlice& " + e + ") -> td::Status {\n" + "      " +
                                    bind_res(ev, "__re" + std::to_string(k), load_expr(inner, e, path, std::nullopt)) +
                                    "      " + out + ".push_back(std::move(" + ev + "));\n" +
                                    "      return td::Status::OK();\n    })");
        s += "\n    return " + out + ";\n";
        s += "  }()";
        return s;
      }
      case TyKind::Tensor:
      case TyKind::ShapedTuple: {
        const auto &items = std::get<TyItems>(t.data).items_ty_idx;
        std::string tt = cpp_type(ty_idx);
        int k = ++uid_;
        std::string s;
        s += "[&]() -> td::Result<" + tt + "> {\n";
        for (std::size_t i = 0; i < items.size(); ++i) {
          std::string ti = "__t" + std::to_string(k) + "_" + std::to_string(i);
          s += "    " + bind_res(ti, "__rt" + std::to_string(k) + "_" + std::to_string(i),
                                 load_expr(items[i], sv, path, std::nullopt));
        }
        s += "    return " + tt + "(";
        for (std::size_t i = 0; i < items.size(); ++i) {
          if (i) s += ", ";
          s += "std::move(__t" + std::to_string(k) + "_" + std::to_string(i) + ")";
        }
        s += ");\n";
        s += "  }()";
        return s;
      }
      case TyKind::MapKV: {
        const auto &m = std::get<TyMapKV>(t.data);
        int kb = dict_key_bits_or_fail(m.key_ty_idx, path);
        std::string kt = cpp_key_type(m.key_ty_idx);
        std::string vt = cpp_type(m.value_ty_idx);
        const Ty &kty = ty(m.key_ty_idx);
        int k = ++uid_;
        std::string kc = "__k" + std::to_string(k), vc = "__vv" + std::to_string(k), out = "__out" + std::to_string(k);
        std::string keyv = "__key" + std::to_string(k), valv = "__val" + std::to_string(k);
        std::string key_load;
        if (kty.kind == TyKind::IntN) {
          key_load = (native_int_type(kty).empty() ? "load_int(" : "load_int64(") + kc + ", " +
                     std::to_string(std::get<TyWidth>(kty.data).n) + ")";
        } else if (kty.kind == TyKind::UintN) {
          key_load = (native_int_type(kty).empty() ? "load_uint(" : "load_uint64(") + kc + ", " +
                     std::to_string(std::get<TyWidth>(kty.data).n) + ")";
        } else {
          key_load = "load_address(" + kc + ")";
        }
        std::string body;
        body += "      " + bind_res(keyv, "__rk" + std::to_string(k), key_load);
        body += "      " + bind_res(valv, "__rv" + std::to_string(k), load_expr(m.value_ty_idx, vc, path, std::nullopt));
        body += "      " + out + ".emplace_back(std::move(" + keyv + "), std::move(" + valv + "));\n";
        body += "      return td::Status::OK();\n    }";
        std::string s;
        s += "[&]() -> td::Result<std::vector<std::pair<" + kt + ", " + vt + ">>> {\n";
        s += "    std::vector<std::pair<" + kt + ", " + vt + ">> " + out + ";\n";
        s += "    " + bind_stat("__ds" + std::to_string(k),
                                "load_dict(" + sv + ", " + std::to_string(kb) + ", [&](vm::CellSlice& " + kc +
                                    ", vm::CellSlice& " + vc + ") -> td::Status {\n" + body + ")");
        s += "\n    return " + out + ";\n";
        s += "  }()";
        return s;
      }
      default:
        throw HardFail{"load_expr: non-serializable ty '" + kind_render(t.kind) + "' at " + path};
    }
  }

  // ---- store statement ----------------------------------------------------
  std::string store_stmt(int ty_idx, const std::string &expr, const std::string &bv, const std::string &path,
                         std::optional<int> u_label) {
    DepthGuard __g(depth_, ty_idx);
    const Ty &t = ty(ty_idx);
    auto W = [&]() { return std::to_string(std::get<TyWidth>(t.data).n); };
    switch (t.kind) {
      case TyKind::IntN:
        return "TRY_STATUS(" + std::string(native_int_type(t).empty() ? "store_int(" : "store_int64(") + bv + ", " +
               expr + ", " + W() + "));";
      case TyKind::UintN:
        return "TRY_STATUS(" + std::string(native_int_type(t).empty() ? "store_uint(" : "store_uint64(") + bv + ", " +
               expr + ", " + W() + "));";
      case TyKind::VarIntN: return "TRY_STATUS(store_varint(" + bv + ", " + expr + ", " + W() + "));";
      case TyKind::VarUIntN: return "TRY_STATUS(store_varuint(" + bv + ", " + expr + ", " + W() + "));";
      case TyKind::Coins: return "TRY_STATUS(store_coins(" + bv + ", " + expr + "));";
      case TyKind::Bool: return "TRY_STATUS(store_bool(" + bv + ", " + expr + "));";
      case TyKind::Cell: return "TRY_STATUS(store_cell(" + bv + ", " + expr + "));";
      case TyKind::String: return "TRY_STATUS(store_string(" + bv + ", " + expr + "));";
      case TyKind::Remaining: return "TRY_STATUS(store_remaining(" + bv + ", *" + expr + "));";
      case TyKind::BitsN: return "TRY_STATUS(store_bits(" + bv + ", *" + expr + ", " + W() + "));";
      case TyKind::Address: return "TRY_STATUS(store_address(" + bv + ", " + expr + "));";
      case TyKind::AddressOpt: return "TRY_STATUS(store_maybe_address(" + bv + ", " + expr + "));";
      case TyKind::AddressExt: return "TRY_STATUS(store_external_address(" + bv + ", " + expr + "));";
      case TyKind::AddressAny: return "TRY_STATUS(store_address_any(" + bv + ", " + expr + "));";
      case TyKind::NullLiteral:
      case TyKind::Void:
        return "";  // no wire
      case TyKind::EnumRef: return "TRY_STATUS(" + name_for_ref(ty_idx) + "_store(" + bv + ", " + expr + "));";
      case TyKind::StructRef:
        return is_abivalue_struct(ty_idx) ? "TRY_STATUS(" + name_for_ref(ty_idx) + "_store(" + bv + ", " + expr + "));"
                                          : "TRY_STATUS(" + expr + ".store(" + bv + "));";
      case TyKind::AliasRef: return "TRY_STATUS(" + name_for_ref(ty_idx) + "_store(" + bv + ", " + expr + "));";
      case TyKind::Union:
        return "TRY_STATUS(store_union_" + std::to_string(union_item_for(ty_idx)) + "(" + expr + ", " + bv + "));";
      case TyKind::Nullable: {
        int inner = std::get<TyNullable>(t.data).inner_ty_idx;
        std::string s;
        s += "if (" + expr + ") {\n";
        s += "    TRY_STATUS(store_maybe_prefix(" + bv + ", true));\n";
        s += "    " + store_stmt(inner, "(*" + expr + ")", bv, path, std::nullopt) + "\n";
        s += "  } else {\n";
        s += "    TRY_STATUS(store_maybe_prefix(" + bv + ", false));\n";
        s += "  }";
        return s;
      }
      case TyKind::CellOf: {
        int inner = std::get<TyInner>(t.data).inner_ty_idx;
        int k = ++uid_;
        std::string icb = "__icb" + std::to_string(k);
        std::string s;
        s += "{\n";
        s += "    vm::CellBuilder " + icb + ";\n";
        s += "    " + store_stmt(inner, "(*(" + expr + ".ref))", icb, path, std::nullopt) + "\n";
        s += "    " + bind_stat("__cs" + std::to_string(k), "store_cell(" + bv + ", " + icb + ".finalize())") + "\n";
        s += "  }";
        return s;
      }
      case TyKind::ArrayOf:
      case TyKind::LispListOf: {
        int inner = std::get<TyInner>(t.data).inner_ty_idx;
        std::string leaf = t.kind == TyKind::ArrayOf ? "store_array" : "store_lisp_list";
        int k = ++uid_;
        std::string e = "__e" + std::to_string(k), i = "__i" + std::to_string(k);
        std::string lam;
        lam += leaf + "(" + bv + ", " + expr + ".size(), [&](vm::CellBuilder& " + e + ", std::size_t " + i +
               ") -> td::Status {\n";
        lam += "    " + store_stmt(inner, "(" + expr + "[" + i + "])", e, path, std::nullopt) + "\n";
        lam += "    return td::Status::OK();\n  })";
        return bind_stat("__as" + std::to_string(k), lam);
      }
      case TyKind::Tensor:
      case TyKind::ShapedTuple: {
        const auto &items = std::get<TyItems>(t.data).items_ty_idx;
        std::string s;
        for (std::size_t i = 0; i < items.size(); ++i) {
          if (i) s += "\n  ";
          s += store_stmt(items[i], "std::get<" + std::to_string(i) + ">(" + expr + ")", bv, path, std::nullopt);
        }
        return s;
      }
      case TyKind::MapKV: {
        const auto &m = std::get<TyMapKV>(t.data);
        int kb = dict_key_bits_or_fail(m.key_ty_idx, path);
        const Ty &kty = ty(m.key_ty_idx);
        int k = ++uid_;
        std::string i = "__i" + std::to_string(k), ko = "__ko" + std::to_string(k), vo = "__vo" + std::to_string(k),
                    ent = "__ent" + std::to_string(k);
        std::string key_store;
        if (kty.kind == TyKind::IntN) {
          key_store = bind_stat("__ks" + std::to_string(k),
                                std::string(native_int_type(kty).empty() ? "store_int(" : "store_int64(") + ko + ", " +
                                    ent + ".first, " + std::to_string(std::get<TyWidth>(kty.data).n) + ")");
        } else if (kty.kind == TyKind::UintN) {
          key_store = bind_stat("__ks" + std::to_string(k),
                                std::string(native_int_type(kty).empty() ? "store_uint(" : "store_uint64(") + ko +
                                    ", " + ent + ".first, " + std::to_string(std::get<TyWidth>(kty.data).n) + ")");
        } else {
          key_store = bind_stat("__ks" + std::to_string(k), "store_address(" + ko + ", " + ent + ".first)");
        }
        std::string lam;
        lam += "store_dict(" + bv + ", " + std::to_string(kb) + ", " + expr + ".size(), [&](std::size_t " + i +
               ", vm::CellBuilder& " + ko + ", vm::CellBuilder& " + vo + ") -> td::Status {\n";
        lam += "    const auto& " + ent + " = " + expr + "[" + i + "];\n";
        lam += "    " + key_store + "\n";
        lam += "    " + store_stmt(m.value_ty_idx, ent + ".second", vo, path, std::nullopt) + "\n";
        lam += "    return td::Status::OK();\n  })";
        return bind_stat("__ms" + std::to_string(k), lam);
      }
      default:
        throw HardFail{"store_stmt: non-serializable ty '" + kind_render(t.kind) + "' at " + path};
    }
  }

  // A leaf kind whose to_abi_value converter is INFALLIBLE (a plain abi_v_*
  // call, never a td::Result). For these the struct to_abi_value() body can
  // add_field directly, without a TRY_RESULT unwrap.
  static bool is_infallible_leaf(const Ty &t) {
    switch (t.kind) {
      case TyKind::IntN:
      case TyKind::UintN:
      case TyKind::VarIntN:
      case TyKind::VarUIntN:
      case TyKind::Coins:
      case TyKind::Bool:
      case TyKind::Cell:
      case TyKind::BitsN:
      case TyKind::Remaining:
      case TyKind::String:
      case TyKind::Address:
      case TyKind::AddressExt:
      case TyKind::AddressAny:
      case TyKind::AddressOpt:
      case TyKind::NullLiteral:
      case TyKind::Void:
        return true;
      default:
        return false;
    }
  }

  // The bare (unwrapped) abi_v_* expression for an infallible leaf kind.
  // Precondition: is_infallible_leaf(ty(ty_idx)).
  std::string to_abi_leaf_expr(int ty_idx, const std::string &expr) {
    const Ty &t = ty(ty_idx);
    switch (t.kind) {
      case TyKind::IntN:
      case TyKind::UintN:
      case TyKind::VarIntN:
      case TyKind::VarUIntN:
      case TyKind::Coins:
        return "ton_abi::gen::abi_v_int(" + expr + ")";
      case TyKind::Bool: return "ton_abi::gen::abi_v_bool(" + expr + ")";
      case TyKind::Cell: return "ton_abi::gen::abi_v_cell(" + expr + ")";
      case TyKind::BitsN:
      case TyKind::Remaining:
        return "ton_abi::gen::abi_v_bits(" + expr + ")";
      case TyKind::String: return "ton_abi::gen::abi_v_string(" + expr + ")";
      case TyKind::Address:
      case TyKind::AddressExt:
      case TyKind::AddressAny:
        return "ton_abi::gen::abi_v_address(" + expr + ")";
      case TyKind::AddressOpt: return "ton_abi::gen::abi_v_address_opt(" + expr + ")";
      case TyKind::NullLiteral: return "ton_abi::gen::abi_v_null()";
      case TyKind::Void: return "ton_abi::gen::abi_v_void()";
      default:
        throw HardFail{"to_abi_leaf_expr: not an infallible leaf"};
    }
  }

  // ---- to_abi_value expression (returns C++ expr of td::Result<AbiValue>) -
  std::string to_abi_expr(int ty_idx, const std::string &expr, std::optional<int> u_label) {
    DepthGuard __g(depth_, ty_idx);
    const Ty &t = ty(ty_idx);
    auto wrap = [](const std::string &av) { return "td::Result<AbiValue>(" + av + ")"; };
    if (is_infallible_leaf(t)) {
      return wrap(to_abi_leaf_expr(ty_idx, expr));
    }
    switch (t.kind) {
      case TyKind::EnumRef: return name_for_ref(ty_idx) + "_to_abi_value(" + expr + ")";
      case TyKind::StructRef:
        return is_abivalue_struct(ty_idx) ? name_for_ref(ty_idx) + "_to_abi_value(" + expr + ")"
                                          : "(" + expr + ").to_abi_value()";
      case TyKind::AliasRef: return name_for_ref(ty_idx) + "_to_abi_value(" + expr + ")";
      case TyKind::Nullable: {
        int inner = std::get<TyNullable>(t.data).inner_ty_idx;
        std::string s;
        s += "[&]() -> td::Result<AbiValue> {\n";
        s += "    if (!(" + expr + ")) { return AbiValue::make_null(); }\n";
        s += "    return " + to_abi_expr(inner, "(*(" + expr + "))", std::nullopt) + ";\n";
        s += "  }()";
        return s;
      }
      case TyKind::CellOf: {
        int inner = std::get<TyInner>(t.data).inner_ty_idx;
        int k = ++uid_;
        std::string iv = "__iv" + std::to_string(k);
        std::string s;
        s += "[&]() -> td::Result<AbiValue> {\n";
        s += "    " + bind_res(iv, "__ri" + std::to_string(k),
                               to_abi_expr(inner, "(*(" + expr + ".ref))", std::nullopt));
        s += "    return AbiValue::make_cell_of(std::move(" + iv + "));\n";
        s += "  }()";
        return s;
      }
      case TyKind::ArrayOf:
      case TyKind::LispListOf: {
        int inner = std::get<TyInner>(t.data).inner_ty_idx;
        int k = ++uid_;
        std::string it = "__items" + std::to_string(k), x = "__x" + std::to_string(k), iv = "__iv" + std::to_string(k);
        std::string s;
        s += "[&]() -> td::Result<AbiValue> {\n";
        s += "    std::vector<AbiValue> " + it + ";\n";
        s += "    for (const auto& " + x + " : " + expr + ") {\n";
        s += "      " + bind_res(iv, "__ri" + std::to_string(k), to_abi_expr(inner, x, std::nullopt));
        s += "      " + it + ".push_back(std::move(" + iv + "));\n";
        s += "    }\n";
        s += "    return AbiValue::make_list(std::move(" + it + "));\n";
        s += "  }()";
        return s;
      }
      case TyKind::Tensor:
      case TyKind::ShapedTuple: {
        const auto &items = std::get<TyItems>(t.data).items_ty_idx;
        int k = ++uid_;
        std::string it = "__items" + std::to_string(k);
        std::string s;
        s += "[&]() -> td::Result<AbiValue> {\n";
        s += "    std::vector<AbiValue> " + it + ";\n";
        for (std::size_t i = 0; i < items.size(); ++i) {
          std::string iv = "__iv" + std::to_string(k) + "_" + std::to_string(i);
          s += "    { " +
               bind_res(iv, "__ri" + std::to_string(k) + "_" + std::to_string(i),
                        to_abi_expr(items[i], "std::get<" + std::to_string(i) + ">(" + expr + ")", std::nullopt)) +
               "      " + it + ".push_back(std::move(" + iv + ")); }\n";
        }
        s += "    return AbiValue::make_list(std::move(" + it + "));\n";
        s += "  }()";
        return s;
      }
      case TyKind::MapKV: {
        const auto &m = std::get<TyMapKV>(t.data);
        const Ty &kty = ty(m.key_ty_idx);
        int k = ++uid_;
        std::string en = "__entries" + std::to_string(k), ent = "__ent" + std::to_string(k),
                    vv = "__vv" + std::to_string(k);
        std::string key_to_abi = kty.kind == TyKind::Address ? "ton_abi::gen::abi_v_address(" + ent + ".first)"
                                                             : "ton_abi::gen::abi_v_int(" + ent + ".first)";
        std::string s;
        s += "[&]() -> td::Result<AbiValue> {\n";
        s += "    std::vector<std::pair<AbiValue, AbiValue>> " + en + ";\n";
        s += "    for (const auto& " + ent + " : " + expr + ") {\n";
        s += "      " + bind_res(vv, "__rv" + std::to_string(k), to_abi_expr(m.value_ty_idx, ent + ".second", std::nullopt));
        s += "      " + en + ".emplace_back(" + key_to_abi + ", std::move(" + vv + "));\n";
        s += "    }\n";
        s += "    return AbiValue::make_map(std::move(" + en + "));\n";
        s += "  }()";
        return s;
      }
      case TyKind::Union: {
        auto ru = kernel_.resolve_union(ty_idx, u_label).move_as_ok();
        int k = ++uid_;
        std::string s;
        s += "[&]() -> td::Result<AbiValue> {\n";
        s += "    switch ((" + expr + ").index()) {\n";
        for (std::size_t i = 0; i < ru.variants.size(); ++i) {
          const auto &v = ru.variants[i];
          std::string iv = "__iv" + std::to_string(k) + "_" + std::to_string(i);
          s += "      case " + std::to_string(i) + ": {\n";
          s += "        " +
               bind_res(iv, "__ri" + std::to_string(k) + "_" + std::to_string(i),
                        to_abi_expr(v.variant_ty_idx, "std::get<" + std::to_string(i) + ">(" + expr + ")",
                                    std::nullopt));
          if (v.has_value_field) {
            s += "        return AbiValue::make_union(" + cpp_str_lit(v.label_str) + ", std::move(" + iv + "));\n";
          } else {
            s += "        return std::move(" + iv + ");\n";
          }
          s += "      }\n";
        }
        s += "      default: return td::Status::Error(\"union: invalid variant index\");\n";
        s += "    }\n";
        s += "  }()";
        return s;
      }
      default:
        return wrap("ton_abi::gen::abi_v_null()");  // unreachable for representable tys
    }
  }

  // ---- const-expr materialization for create() defaults -------------------
  std::string const_expr(const ABIConstExpression &ce, int ty_idx) {
    DepthGuard __g(depth_, ty_idx);
    // A non-null default for a nullable field carries the INNER value's
    // ConstExpr shape (e.g. `t1: (int8,int8,int8)? = (1,2,3)` -> a tensor CE
    // against a nullable ty). Peel the wrapper so the Tensor/Object/etc. cases
    // below see the matching ty; a `null` default falls through to the Null case.
    const Ty &ft = ty(ty_idx);
    if (ft.kind == TyKind::Nullable && ce.kind != ConstExprKind::Null) {
      int inner = std::get<TyNullable>(ft.data).inner_ty_idx;
      return "std::optional<" + cpp_type(inner) + ">(" + const_expr(ce, inner) + ")";
    }
    switch (ce.kind) {
      case ConstExprKind::Int: {
        std::string dec = std::get<ConstExprInt>(ce.data).v->to_dec_string();
        std::string nt = native_int_type(ft);
        if (nt == "td::int64") return "static_cast<td::int64>(" + dec + "LL)";
        if (nt == "td::uint64") return "static_cast<td::uint64>(" + dec + "ULL)";
        return "td::dec_string_to_int256(std::string(\"" + dec + "\"))";
      }
      case ConstExprKind::Bool:
        return std::get<ConstExprBool>(ce.data).v ? "true" : "false";
      case ConstExprKind::Null: {
        const Ty &t = ty(ty_idx);
        if (t.kind == TyKind::Nullable) {
          return "std::optional<" + cpp_type(std::get<TyNullable>(t.data).inner_ty_idx) + ">()";
        }
        if (t.kind == TyKind::AddressOpt) {
          return "ton_abi::AbiAddress{}";
        }
        return "{}";
      }
      case ConstExprKind::String:
        return "std::string(" + cpp_str_lit(std::get<ConstExprString>(ce.data).str) + ")";
      case ConstExprKind::Slice:
        return "ton_abi::gen::bits_from_hex(\"" + std::get<ConstExprSlice>(ce.data).hex + "\")";
      case ConstExprKind::Address:
        return "ton_abi::gen::address_from_string(" + cpp_str_lit(std::get<ConstExprAddress>(ce.data).addr) + ")";
      case ConstExprKind::CastTo:
        return const_expr(*std::get<ConstExprCastTo>(ce.data).inner, ty_idx);
      case ConstExprKind::Tensor:
      case ConstExprKind::ShapedTuple: {
        const auto &items = ce.kind == ConstExprKind::Tensor
                                ? std::get<ConstExprTensor>(ce.data).items
                                : std::get<ConstExprShapedTuple>(ce.data).items;
        // The ty must actually be a tensor/shapedTuple for the item-type mapping
        // to be sound. arrayOf/lispListOf defaults also arrive as a tensor CE but
        // map every item to the container's inner ty; anything else can't be
        // materialized soundly, so use an empty initializer that still compiles.
        if (ft.kind == TyKind::Tensor || ft.kind == TyKind::ShapedTuple) {
          const auto &tys = std::get<TyItems>(ft.data).items_ty_idx;
          if (tys.size() == items.size()) {
            std::string s = cpp_type(ty_idx) + "(";
            for (std::size_t i = 0; i < items.size(); ++i) {
              if (i) s += ", ";
              s += const_expr(*items[i], tys[i]);
            }
            s += ")";
            return s;
          }
        } else if (ft.kind == TyKind::ArrayOf || ft.kind == TyKind::LispListOf) {
          int inner = std::get<TyInner>(ft.data).inner_ty_idx;
          std::string s = cpp_type(ty_idx) + "{";
          for (std::size_t i = 0; i < items.size(); ++i) {
            if (i) s += ", ";
            s += const_expr(*items[i], inner);
          }
          s += "}";
          return s;
        }
        return "{}";
      }
      case ConstExprKind::Object: {
        const auto &obj = std::get<ConstExprObject>(ce.data);
        auto rs = kernel_.get_struct(obj.struct_name);
        const ABIStruct *sd = rs.move_as_ok();
        std::string s = sanitize_ident(obj.struct_name) + "{";
        for (std::size_t i = 0; i < obj.fields.size(); ++i) {
          if (i) s += ", ";
          s += const_expr(*obj.fields[i], sd->fields[i].ty_idx);
        }
        s += "}";
        return s;
      }
    }
    return "{}";
  }

  // ---- emit bodies for one target ----------------------------------------
  void emit_target_bodies(int ti) {
    Target &t = targets_[ti];
    switch (t.kind) {
      case TKind::Struct: emit_struct(ti); break;
      case TKind::Alias: emit_alias(ti); break;
      case TKind::Enum: emit_enum(ti); break;
    }
  }

  bool has_custom(const std::optional<ABICustomSerializers> &c, bool pack) {
    if (!c) return false;
    return pack ? c->pack_to_builder : c->unpack_from_slice;
  }

  // Custom-registry delegation is identical for every
  // direction of every custom decl: look the entry up by "<contract>::<decl>"
  // and, when that direction was never registered, fail at RUNTIME -- never at
  // compile/link time. `value_type` is the registry's typed slot: the generated
  // struct, td::RefInt256, or AbiValue for the type-erased form.
  enum class CustomDir { Unpack, Pack, ToAbiValue };
  std::string emit_custom_delegate(const std::string &value_type, const std::string &orig_name, CustomDir dir) {
    const char *member = dir == CustomDir::Unpack ? "unpack" : dir == CustomDir::Pack ? "pack" : "to_abi_value";
    const char *fn_name =
        dir == CustomDir::Unpack ? "unpackFromSlice" : dir == CustomDir::Pack ? "packToBuilder" : "to_abi_value";
    std::string s;
    s += "  const auto* __e = ton_abi::gen::abi_custom_lookup<" + value_type + ">(" +
         cpp_str_lit(abi_.contract_name + "::" + orig_name) + ");\n";
    s += "  if (__e == nullptr || !__e->" + std::string(member) + ") { return td::Status::Error(" +
         cpp_str_lit("custom " + std::string(fn_name) + " was not registered for '" + orig_name + "'") + "); }\n";
    return s;
  }

  // A decl that is custom in EVERY direction it is used carries a value the
  // typed form can't reconstruct (e.g. CustomPoint is an empty ABI struct whose
  // custom (un)pack materialize x/y; TelegramString's target is `slice`). Emit
  // it type-erased: `using N = AbiValue` + free N_from_slice/N_store/
  // N_to_abi_value delegating to the typed registry<AbiValue>.
  void emit_custom_abivalue(Target &t) {
    t.abi_value_form = true;
    const std::string &N = t.cpp_name;
    std::string h;
    h += "using " + N + " = AbiValue;\n";
    h += "td::Result<" + N + "> " + N + "_from_slice(vm::CellSlice& cs);\n";
    h += "td::Status " + N + "_store(vm::CellBuilder& cb, const " + N + "& self);\n";
    h += "td::Result<AbiValue> " + N + "_to_abi_value(const " + N + "& self);";
    t.h_decl = h;

    std::string c;
    c += "td::Result<" + N + "> " + N + "_from_slice(vm::CellSlice& cs) {\n";
    c += emit_custom_delegate("AbiValue", t.orig_name, CustomDir::Unpack);
    c += "  return __e->unpack(cs);\n}\n\n";
    c += "td::Status " + N + "_store(vm::CellBuilder& cb, const " + N + "& self) {\n";
    c += emit_custom_delegate("AbiValue", t.orig_name, CustomDir::Pack);
    c += "  return __e->pack(self, cb);\n}\n\n";
    c += "td::Result<AbiValue> " + N + "_to_abi_value(const " + N + "& self) {\n";
    c += emit_custom_delegate("AbiValue", t.orig_name, CustomDir::ToAbiValue);
    c += "  return __e->to_abi_value(self);\n}";
    t.cpp_body = c;
  }

  void emit_struct(int ti) {
    Target &t = targets_[ti];
    const ABIStruct *sd = t.s;

    // Fully-custom struct -> type-erased AbiValue form (see emit_custom_abivalue).
    if (t.abi_value_form) {
      emit_custom_abivalue(t);
      return;
    }

    const auto &fields = *kernel_.struct_fields_of(t.self_ty_idx).move_as_ok();

    // representability: collect present (representable) fields.
    struct FieldInfo {
      const ResolvedField *rf;
      bool representable;
    };
    std::vector<FieldInfo> finfo;
    bool any_omitted = false;
    for (const auto &f : fields) {
      bool rep = member_representable(f.ty_idx);
      if (!rep) any_omitted = true;
      finfo.push_back({&f, rep});
      if (rep) {
        collect_type_deps(f.ty_idx, t.deps);
      }
    }

    bool custom_unpack = has_custom(sd ? sd->custom_pack_unpack : std::nullopt, false);
    bool custom_pack = has_custom(sd ? sd->custom_pack_unpack : std::nullopt, true);

    // Header
    std::string h;
    h += "struct " + t.cpp_name + " {\n";
    if (sd && sd->prefix) {
      h += "  static constexpr td::uint64 PREFIX = " + std::to_string(sd->prefix->prefix_num) + "ULL;\n";
      h += "  static constexpr int PREFIX_LEN = " + std::to_string(sd->prefix->prefix_len) + ";\n";
    }
    for (const auto &fi : finfo) {
      if (fi.representable) {
        h += "  " + cpp_type(fi.rf->ty_idx) + " " + member_name(fi.rf->name()) + ";\n";
      }
    }
    // create() args + factory (over present fields)
    h += "\n  struct CreateArgs {\n";
    for (const auto &fi : finfo) {
      if (!fi.representable) continue;
      bool has_def = fi.rf->orig->default_value.has_value() && default_supported(fi.rf->ty_idx);
      std::string ct = cpp_type(fi.rf->ty_idx);
      if (has_def) {
        h += "    std::optional<" + ct + "> " + member_name(fi.rf->name()) + ";\n";
      } else {
        h += "    " + ct + " " + member_name(fi.rf->name()) + ";\n";
      }
    }
    h += "  };\n";
    h += "  static " + t.cpp_name + " create(CreateArgs args);\n";
    h += "  static td::Result<" + t.cpp_name + "> from_slice(vm::CellSlice& cs);\n";
    h += "  td::Status store(vm::CellBuilder& cb) const;\n";
    h += "  td::Result<AbiValue> to_abi_value() const;\n";
    h += "};";
    t.h_decl = h;

    // Bodies
    std::string c;
    const std::string &N = t.cpp_name;

    // create()
    c += N + " " + N + "::create(CreateArgs args) {\n";
    c += "  " + N + " r;\n";
    for (const auto &fi : finfo) {
      if (!fi.representable) continue;
      std::string mn = member_name(fi.rf->name());
      bool has_def = fi.rf->orig->default_value.has_value() && default_supported(fi.rf->ty_idx);
      if (has_def) {
        c += "  r." + mn + " = args." + mn + " ? std::move(*args." + mn + ") : " +
             const_expr(*fi.rf->orig->default_value, fi.rf->ty_idx) + ";\n";
      } else {
        c += "  r." + mn + " = std::move(args." + mn + ");\n";
      }
    }
    c += "  return r;\n}\n\n";

    // from_slice
    c += "td::Result<" + N + "> " + N + "::from_slice(vm::CellSlice& cs) {\n";
    if (any_omitted) {
      c += "  return td::Status::Error(\"" + N + ": not serializable: " + omitted_reason(finfo) + "\");\n";
    } else if (custom_unpack) {
      c += emit_custom_delegate(N, t.orig_name, CustomDir::Unpack);
      c += "  return __e->unpack(cs);\n";
    } else {
      if (sd && sd->prefix) {
        c += "  TRY_STATUS_PREFIX(load_and_check_prefix(cs, PREFIX, PREFIX_LEN), " +
             cpp_str_lit(t.orig_name + ": ") + ");\n";
      }
      c += "  " + N + " r;\n";
      for (const auto &fi : finfo) {
        std::string mn = member_name(fi.rf->name());
        std::string le = load_expr(fi.rf->ty_idx, "cs", t.orig_name + "." + fi.rf->name(), fi.rf->u_label_ty_idx);
        c += "  TRY_RESULT_PREFIX_ASSIGN(r." + mn + ", (" + le + "), " +
             cpp_str_lit(t.orig_name + "." + fi.rf->name() + ": ") + ");\n";
      }
      c += "  return r;\n";
    }
    c += "}\n\n";

    // store
    c += "td::Status " + N + "::store(vm::CellBuilder& cb) const {\n";
    if (any_omitted) {
      c += "  return td::Status::Error(\"" + N + ": not serializable: " + omitted_reason(finfo) + "\");\n";
    } else if (custom_pack) {
      c += emit_custom_delegate(N, t.orig_name, CustomDir::Pack);
      c += "  return __e->pack(*this, cb);\n";
    } else {
      if (sd && sd->prefix) {
        c += "  TRY_STATUS(store_prefix(cb, PREFIX, PREFIX_LEN));\n";
      }
      for (const auto &fi : finfo) {
        std::string mn = member_name(fi.rf->name());
        c += "  " + store_stmt(fi.rf->ty_idx, "this->" + mn, "cb", t.orig_name + "." + fi.rf->name(),
                               fi.rf->u_label_ty_idx) +
             "\n";
      }
      c += "  return td::Status::OK();\n";
    }
    c += "}\n\n";

    // to_abi_value
    c += "td::Result<AbiValue> " + N + "::to_abi_value() const {\n";
    if (any_omitted) {
      c += "  return td::Status::Error(\"" + N + ": not representable as AbiValue\");\n";
    } else if (custom_unpack) {
      c += emit_custom_delegate(N, t.orig_name, CustomDir::ToAbiValue);
      c += "  return __e->to_abi_value(*this);\n";
    } else {
      c += "  AbiValue r = AbiValue::make_struct(" + cpp_str_lit(t.orig_name) + ");\n";
      for (const auto &fi : finfo) {
        std::string mn = member_name(fi.rf->name());
        if (is_infallible_leaf(ty(fi.rf->ty_idx))) {
          // Infallible converter: add_field directly, no TRY_RESULT ceremony.
          c += "  r.add_field(" + cpp_str_lit(fi.rf->name()) + ", " +
               to_abi_leaf_expr(fi.rf->ty_idx, "this->" + mn) + ");\n";
        } else {
          c += "  { TRY_RESULT(__f, (" + to_abi_expr(fi.rf->ty_idx, "this->" + mn, fi.rf->u_label_ty_idx) +
               ")); r.add_field(" + cpp_str_lit(fi.rf->name()) + ", std::move(__f)); }\n";
        }
      }
      c += "  return r;\n";
    }
    c += "}";
    t.cpp_body = c;
  }

  void emit_alias(int ti) {
    Target &t = targets_[ti];
    const ABIAlias *ad = t.a;
    auto target = kernel_.alias_target_of(t.self_ty_idx).move_as_ok();
    int tgt_ty = target.ty_idx;
    bool rep = member_representable(tgt_ty);

    bool custom_unpack = has_custom(ad ? ad->custom_pack_unpack : std::nullopt, false);
    bool custom_pack = has_custom(ad ? ad->custom_pack_unpack : std::nullopt, true);
    bool any_custom = custom_unpack || custom_pack;

    // C++ type for the alias. Representable target -> that type. Non-representable
    // target BUT custom-serialized in every used direction -> AbiValue (type-
    // erased; the custom value isn't the target's type -- e.g. TelegramString's
    // target is `slice` yet its custom unpack yields bits). Non-representable +
    // non-custom -> monostate placeholder (both directions stub).
    std::string ct;
    if (rep) {
      ct = cpp_type(tgt_ty);
      collect_type_deps(tgt_ty, t.deps);
    } else if (any_custom) {
      ct = "AbiValue";
    } else {
      ct = "std::monostate";
    }

    const std::string &N = t.cpp_name;
    std::string h;
    h += "using " + N + " = " + ct + ";\n";
    h += "td::Result<" + N + "> " + N + "_from_slice(vm::CellSlice& cs);\n";
    h += "td::Status " + N + "_store(vm::CellBuilder& cb, const " + N + "& self);\n";
    h += "td::Result<AbiValue> " + N + "_to_abi_value(const " + N + "& self);";
    t.h_decl = h;

    std::string c;
    // from_slice
    c += "td::Result<" + N + "> " + N + "_from_slice(vm::CellSlice& cs) {\n";
    if (custom_unpack) {
      c += emit_custom_delegate(N, t.orig_name, CustomDir::Unpack);
      c += "  return __e->unpack(cs);\n";
    } else if (!rep) {
      c += "  return td::Status::Error(\"" + N + ": not serializable (alias target unrepresentable)\");\n";
    } else {
      c += "  return " + load_expr(tgt_ty, "cs", t.orig_name, target.u_label_ty_idx) + ";\n";
    }
    c += "}\n\n";
    // store
    c += "td::Status " + N + "_store(vm::CellBuilder& cb, const " + N + "& self) {\n";
    if (custom_pack) {
      c += emit_custom_delegate(N, t.orig_name, CustomDir::Pack);
      c += "  return __e->pack(self, cb);\n";
    } else if (!rep) {
      c += "  return td::Status::Error(\"" + N + ": not serializable (alias target unrepresentable)\");\n";
    } else {
      c += "  " + store_stmt(tgt_ty, "self", "cb", t.orig_name, target.u_label_ty_idx) + "\n";
      c += "  return td::Status::OK();\n";
    }
    c += "}\n\n";
    // to_abi_value
    c += "td::Result<AbiValue> " + N + "_to_abi_value(const " + N + "& self) {\n";
    if (custom_unpack) {
      c += emit_custom_delegate(N, t.orig_name, CustomDir::ToAbiValue);
      c += "  return __e->to_abi_value(self);\n";
    } else if (!rep) {
      c += "  return td::Status::Error(\"" + N + ": not representable as AbiValue\");\n";
    } else {
      c += "  return " + to_abi_expr(tgt_ty, "self", target.u_label_ty_idx) + ";\n";
    }
    c += "}";
    t.cpp_body = c;
  }

  void emit_enum(int ti) {
    Target &t = targets_[ti];
    const ABIEnum *ed = t.e;
    int enc = ed->encoded_as_ty_idx;
    bool custom_unpack = has_custom(ed->custom_pack_unpack, false);
    bool custom_pack = has_custom(ed->custom_pack_unpack, true);

    // Enum values stay td::RefInt256 (bigint repr, matches the reference and
    // avoids rippling the free-function signatures). The encoded_as scalar is
    // therefore read/written via the RefInt256 leaves EVEN when its width <= 64
    // -- the native fast path would return td::int64/uint64 and mismatch the
    // RefInt256 helper signature. VarIntN/Coins encodings already yield RefInt256
    // through load_expr/store_stmt, so only IntN/UintN need the override.
    const Ty &et = ty(enc);
    bool enc_int = et.kind == TyKind::IntN || et.kind == TyKind::UintN;
    std::string enc_w = enc_int ? std::to_string(std::get<TyWidth>(et.data).n) : "";
    std::string enc_load = et.kind == TyKind::UintN ? "load_uint(cs, " + enc_w + ")"
                                                    : "load_int(cs, " + enc_w + ")";
    std::string enc_store = et.kind == TyKind::UintN ? "store_uint(cb, self, " + enc_w + ")"
                                                     : "store_int(cb, self, " + enc_w + ")";

    const std::string &N = t.cpp_name;
    std::string h;
    h += "// enum " + t.orig_name + " (bigint-valued, encoded_as leaf scalar)\n";
    h += "td::Result<td::RefInt256> " + N + "_from_slice(vm::CellSlice& cs);\n";
    h += "td::Status " + N + "_store(vm::CellBuilder& cb, const td::RefInt256& self);\n";
    h += "td::Result<AbiValue> " + N + "_to_abi_value(const td::RefInt256& self);";
    t.h_decl = h;

    std::string c;
    c += "td::Result<td::RefInt256> " + N + "_from_slice(vm::CellSlice& cs) {\n";
    if (custom_unpack) {
      c += emit_custom_delegate("td::RefInt256", t.orig_name, CustomDir::Unpack);
      c += "  return __e->unpack(cs);\n";
    } else {
      c += "  return " + (enc_int ? enc_load : load_expr(enc, "cs", t.orig_name, std::nullopt)) + ";\n";
    }
    c += "}\n\n";
    c += "td::Status " + N + "_store(vm::CellBuilder& cb, const td::RefInt256& self) {\n";
    if (custom_pack) {
      c += emit_custom_delegate("td::RefInt256", t.orig_name, CustomDir::Pack);
      c += "  return __e->pack(self, cb);\n";
    } else {
      c += "  " + (enc_int ? "TRY_STATUS(" + enc_store + ");" : store_stmt(enc, "self", "cb", t.orig_name, std::nullopt)) +
           "\n";
      c += "  return td::Status::OK();\n";
    }
    c += "}\n\n";
    c += "td::Result<AbiValue> " + N + "_to_abi_value(const td::RefInt256& self) {\n";
    if (custom_unpack) {
      c += emit_custom_delegate("td::RefInt256", t.orig_name, CustomDir::ToAbiValue);
      c += "  return __e->to_abi_value(self);\n";
    } else {
      c += "  return td::Result<AbiValue>(ton_abi::gen::abi_v_int(self));\n";
    }
    c += "}";
    t.cpp_body = c;
    collect_type_deps(enc, t.deps);  // scalar -> no deps, but keep uniform
  }

  // ---- union load/store bodies -------------------------------------------
  void emit_union_bodies(int local_id) {
    // Same use-after-realloc hazard as build_union_type: load_expr / store_stmt
    // may create nested union items and reallocate union_items_. Snapshot the
    // fields we need, build into locals, then re-index to store.
    int union_ty = union_items_[local_id].union_ty_idx;
    std::string N = union_items_[local_id].cpp_name;
    auto ru = kernel_.resolve_union(union_ty, std::nullopt).move_as_ok();

    std::string h_extra;
    h_extra += "\ntd::Result<" + N + "> load_union_" + std::to_string(local_id) + "(vm::CellSlice& cs);";
    h_extra += "\ntd::Status store_union_" + std::to_string(local_id) + "(const " + N + "& u, vm::CellBuilder& cb);";

    // find void variant index (trailing)
    int void_idx = -1;
    for (std::size_t i = 0; i < ru.variants.size(); ++i) {
      if (ty(ru.variants[i].variant_ty_idx).kind == TyKind::Void) {
        void_idx = static_cast<int>(i);
      }
    }

    std::string c;
    // load
    c += "td::Result<" + N + "> load_union_" + std::to_string(local_id) + "(vm::CellSlice& cs) {\n";
    c += "  " + N + " __u;\n";
    c += "  const bool __empty = cs.size() == 0 && cs.size_refs() == 0;\n";
    if (ru.has_void && ru.variants.size() == 2) {
      const auto &t0 = ru.variants[0];
      c += "  if (__empty) { __u.emplace<" + std::to_string(void_idx) + ">(std::monostate{}); return __u; }\n";
      if (t0.is_prefix_implicit && t0.prefix_len > 0) {
        c += "  if (!cs.advance(" + std::to_string(t0.prefix_len) +
             ")) { return td::Status::Error(\"union: truncated T|void prefix\"); }\n";
      }
      c += "  { TRY_RESULT(__v0, (" + load_expr(t0.variant_ty_idx, "cs", N, std::nullopt) +
           ")); __u.emplace<0>(std::move(__v0)); }\n";
      c += "  return __u;\n";
    } else {
      for (std::size_t idx : ru.dispatch_order) {
        const auto &v = ru.variants[idx];
        c += "  if (lookup_prefix(cs, " + std::to_string(v.prefix_num) + "ULL, " + std::to_string(v.prefix_len) +
             ")) {\n";
        if (v.is_prefix_implicit && v.prefix_len > 0) {
          c += "    if (!cs.advance(" + std::to_string(v.prefix_len) +
               ")) { return td::Status::Error(\"union: truncated implicit prefix\"); }\n";
        }
        c += "    TRY_RESULT(__v, (" + load_expr(v.variant_ty_idx, "cs", N, std::nullopt) + "));\n";
        c += "    __u.emplace<" + std::to_string(idx) + ">(std::move(__v));\n";
        c += "    return __u;\n";
        c += "  }\n";
      }
      if (ru.has_void) {
        c += "  if (__empty) { __u.emplace<" + std::to_string(void_idx) + ">(std::monostate{}); return __u; }\n";
      }
      c += "  return td::Status::Error(\"union: none of the variant prefixes match\");\n";
    }
    c += "}\n\n";

    // store
    c += "td::Status store_union_" + std::to_string(local_id) + "(const " + N + "& u, vm::CellBuilder& cb) {\n";
    c += "  switch (u.index()) {\n";
    for (std::size_t i = 0; i < ru.variants.size(); ++i) {
      const auto &v = ru.variants[i];
      const Ty &vt = ty(v.variant_ty_idx);
      c += "    case " + std::to_string(i) + ": {\n";
      if (vt.kind == TyKind::NullLiteral) {
        c += "      TRY_STATUS(store_prefix(cb, " + std::to_string(v.prefix_num) + "ULL, " +
             std::to_string(v.prefix_len) + "));\n";
      } else {
        if (v.is_prefix_implicit && v.prefix_len > 0) {
          c += "      TRY_STATUS(store_prefix(cb, " + std::to_string(v.prefix_num) + "ULL, " +
               std::to_string(v.prefix_len) + "));\n";
        }
        if (vt.kind != TyKind::Void) {
          c += "      " + store_stmt(v.variant_ty_idx, "std::get<" + std::to_string(i) + ">(u)", "cb", N,
                                     std::nullopt) +
               "\n";
        }
      }
      c += "      return td::Status::OK();\n";
      c += "    }\n";
    }
    c += "    default: return td::Status::Error(\"union: invalid variant index\");\n";
    c += "  }\n";
    c += "}";
    UnionItem &ui = union_items_[local_id];
    ui.h_decl += h_extra;
    ui.cpp_body = c;
  }

  // ---- member / default helpers ------------------------------------------
  std::string member_name(const std::string &field) {
    std::string s = sanitize_ident(field);
    static const std::set<std::string> reserved = {
        // emitted member/method names that would collide
        "PREFIX", "PREFIX_LEN", "create", "from_slice", "store", "to_abi_value", "CreateArgs",
        // C++ keywords that are invalid identifiers
        "alignas", "alignof", "and", "and_eq", "asm", "auto", "bitand", "bitor", "bool", "break", "case", "catch",
        "char", "char8_t", "char16_t", "char32_t", "class", "compl", "concept", "const", "consteval", "constexpr",
        "constinit", "const_cast", "continue", "co_await", "co_return", "co_yield", "decltype", "default", "delete",
        "do", "double", "dynamic_cast", "else", "enum", "explicit", "export", "extern", "false", "float", "for",
        "friend", "goto", "if", "inline", "int", "long", "mutable", "namespace", "new", "noexcept", "not", "not_eq",
        "nullptr", "operator", "or", "or_eq", "private", "protected", "public", "register", "reinterpret_cast",
        "requires", "return", "short", "signed", "sizeof", "static", "static_assert", "static_cast", "struct",
        "switch", "template", "this", "thread_local", "throw", "true", "try", "typedef", "typeid", "typename",
        "union", "unsigned", "using", "virtual", "void", "volatile", "wchar_t", "while", "xor", "xor_eq"};
    if (reserved.count(s)) {
      s += "_";
    }
    return s;
  }

  bool default_supported(int ty_idx) {
    const Ty &t = ty(ty_idx);
    if (t.kind == TyKind::ArrayOf || t.kind == TyKind::LispListOf) {
      return default_supported(std::get<TyInner>(t.data).inner_ty_idx);
    }
    if (t.kind == TyKind::Tensor || t.kind == TyKind::ShapedTuple) {
      for (int i : std::get<TyItems>(t.data).items_ty_idx) {
        if (!default_supported(i)) return false;
      }
      return true;
    }
    return t.kind != TyKind::Union && t.kind != TyKind::MapKV;
  }

  std::string omitted_reason(const auto &finfo) {
    for (const auto &fi : finfo) {
      if (!fi.representable) {
        return "field '" + fi.rf->name() + "' has type '" + kind_render(ty(fi.rf->ty_idx).kind) + "'";
      }
    }
    return "non-representable field";
  }

  // ---- topological emit order --------------------------------------------
  void build_order() {
    // combined nodes: targets 0..T-1, unions kUnionBase+0..
    int T = static_cast<int>(targets_.size());
    int U = static_cast<int>(union_items_.size());
    std::map<int, std::set<int>> deps;      // node -> deps (nodes)
    std::map<int, int> indeg;
    auto all_nodes = [&](auto fn) {
      for (int i = 0; i < T; ++i) fn(gi_target(i));
      for (int i = 0; i < U; ++i) fn(gi_union(i));
    };
    all_nodes([&](int n) {
      indeg[n] = 0;
      deps[n] = {};
    });
    for (int i = 0; i < T; ++i) {
      deps[gi_target(i)] = targets_[i].deps;
    }
    for (int i = 0; i < U; ++i) {
      deps[gi_union(i)] = union_items_[i].deps;
    }
    // reverse edges for Kahn: for n depends on d -> d must come first.
    std::map<int, std::vector<int>> users;
    for (auto &kv : deps) {
      for (int d : kv.second) {
        users[d].push_back(kv.first);
        indeg[kv.first]++;
      }
    }
    // deterministic: process ready nodes in ascending node id.
    std::set<int> ready;
    all_nodes([&](int n) {
      if (indeg[n] == 0) ready.insert(n);
    });
    while (!ready.empty()) {
      int n = *ready.begin();
      ready.erase(ready.begin());
      order_.push_back(n);
      for (int u : users[n]) {
        if (--indeg[u] == 0) {
          ready.insert(u);
        }
      }
    }
    if (static_cast<int>(order_.size()) != T + U) {
      throw HardFail{"cyclic type dependency (self-referential value type not representable)"};
    }
  }

  // ---- assembly -----------------------------------------------------------
  std::string assemble_header() {
    std::string fixture = snake_;
    std::string s;
    s += "// Generated by ton-abi-gen from " + fixture +
         ".abi.json -- DO NOT EDIT (regen: ton-abi-gen --check)\n";
    s += "#pragma once\n\n";
    // ONE include per generated header: AbiGenSupport.h is the support contract
    // for emitted code and already carries everything the decls name (refint,
    // Status, vm/cells, <optional>/<tuple>/<variant>/<vector>) transitively.
    s += "#include \"AbiGenSupport.h\"\n\n";
    s += "namespace " + ns_ + " {\n\n";
    s += "using ton_abi::AbiValue;\n";
    s += "using ton_abi::AbiAddress;\n\n";
    // Forward-declare every struct so cellOf<Struct> (shared_ptr indirection)
    // members can name a not-yet-defined struct (recursive-type support).
    for (const auto &t : targets_) {
      if (t.kind == TKind::Struct && !t.abi_value_form) {
        s += "struct " + t.cpp_name + ";\n";
      }
    }
    s += "\n";
    for (int n : order_) {
      if (is_union_gi(n)) {
        s += union_items_[n - kUnionBase].h_decl + "\n\n";
      } else {
        s += targets_[n].h_decl + "\n\n";
      }
    }
    s += "}  // namespace " + ns_ + "\n";
    return s;
  }

  std::string assemble_source() {
    std::string fixture = snake_;
    std::string s;
    s += "// Generated by ton-abi-gen from " + fixture +
         ".abi.json -- DO NOT EDIT (regen: ton-abi-gen --check)\n";
    // Own header only: it pulls AbiGenSupport.h, hence every ton_abi leaf the
    // baked bodies call.
    s += "#include \"" + fixture + "_gen.h\"\n\n";
    s += "namespace " + ns_ + " {\n\n";
    s += "using namespace ton_abi;\n\n";
    for (int n : order_) {
      if (is_union_gi(n)) {
        s += union_items_[n - kUnionBase].cpp_body + "\n\n";
      } else {
        s += targets_[n].cpp_body + "\n\n";
      }
    }
    s += "}  // namespace " + ns_ + "\n";
    return s;
  }
};

}  // namespace

td::Result<GeneratedFiles> emit_abi(const ContractABI &abi, const AbiKernel &kernel, const std::string &out_name) {
  try {
    Emitter em(abi, kernel, out_name);
    return em.run();
  } catch (const HardFail &hf) {
    return td::Status::Error(PSLICE() << "ton-abi-gen: " << hf.msg);
  }
}

}  // namespace ton_abi

#include "AbiKernel.h"

#include "td/utils/logging.h"

#include <algorithm>
#include <set>

namespace ton_abi {

namespace {

// StructRef.struct_name / AliasRef.alias_name are ALWAYS plain declared names
// (the mangled monomorph name "ResetTo<int64>" lives ONLY in
// struct_instantiations[].struct_name, matched by ty_idx, never by name).
// Lookups take the name verbatim; stripping <...> would mask a bug.

const std::string &struct_ref_name(const Ty &ty) { return std::get<TyStructRef>(ty.data).struct_name; }
const std::string &alias_ref_name(const Ty &ty) { return std::get<TyAliasRef>(ty.data).alias_name; }

}  // namespace


td::Result<const ABIStruct *> AbiKernel::get_struct(const std::string &name) const {
  auto it = structs_.find(name);
  if (it == structs_.end()) {
    return td::Status::Error(PSLICE() << "symbol not found: struct '" << name << "'");
  }
  return it->second;
}

td::Result<const ABIAlias *> AbiKernel::get_alias(const std::string &name) const {
  auto it = aliases_.find(name);
  if (it == aliases_.end()) {
    return td::Status::Error(PSLICE() << "symbol not found: alias '" << name << "'");
  }
  return it->second;
}

td::Result<const ABIEnum *> AbiKernel::get_enum(const std::string &name) const {
  auto it = enums_.find(name);
  if (it == enums_.end()) {
    return td::Status::Error(PSLICE() << "symbol not found: enum '" << name << "'");
  }
  return it->second;
}

td::Result<const Ty *> AbiKernel::ty_by_idx(int ty_idx) const {
  if (ty_idx < 0 || static_cast<std::size_t>(ty_idx) >= abi_->unique_types.size()) {
    return td::Status::Error(PSLICE() << "symbol not found: ty_idx " << ty_idx);
  }
  return &abi_->unique_types[static_cast<std::size_t>(ty_idx)];
}


td::Result<std::vector<ResolvedField>> AbiKernel::compute_struct_fields(int ty_idx) const {
  TRY_RESULT(ty, ty_by_idx(ty_idx));
  if (ty->kind != TyKind::StructRef) {
    return td::Status::Error(PSLICE() << "expected StructRef at ty_idx=" << ty_idx);
  }
  TRY_RESULT(decl, get_struct(struct_ref_name(*ty)));

  std::vector<ResolvedField> out;
  out.reserve(decl->fields.size());
  for (const auto &f : decl->fields) {
    out.push_back(ResolvedField{&f, f.ty_idx, std::nullopt});
  }

  // Generic instantiation: override each field's ty_idx from the monomorphic
  // representation, saving the original ty_idx as uLabelTyIdx (for generic
  // unions later).
  auto inst_it = struct_inst_.find(ty_idx);
  if (inst_it != struct_inst_.end()) {
    const ABIStructInstantiation *inst = inst_it->second;
    if (out.size() != inst->monomorphic_fields_ty_idx.size()) {
      return td::Status::Error(PSLICE() << "mismatch monomorphic fields size for '" << inst->struct_name << "'");
    }
    for (std::size_t i = 0; i < out.size(); ++i) {
      out[i].u_label_ty_idx = out[i].ty_idx;                     // original (potentially generic) ty
      out[i].ty_idx = inst->monomorphic_fields_ty_idx[i];        // monomorphic substitute
    }
  }

  // Cell side (isForStack == false, always here): @abi.clientType swaps the
  // field's cell type for its client type and drops uLabelTyIdx (clientType is
  // denied for generic fields -> no monomorphic representation exists).
  for (auto &rf : out) {
    if (rf.orig->client_ty_idx) {
      rf.ty_idx = *rf.orig->client_ty_idx;
      rf.u_label_ty_idx = std::nullopt;
    }
  }
  return out;
}

td::Result<const std::vector<ResolvedField> *> AbiKernel::struct_fields_of(int ty_idx) const {
  auto it = fields_cache_.find(ty_idx);
  if (it != fields_cache_.end()) {
    return &it->second;
  }
  // Not cached => not a StructRef (create() caches every StructRef). Re-run to
  // produce the proper error message.
  TRY_RESULT(fields, compute_struct_fields(ty_idx));
  return td::Status::Error(PSLICE() << "struct_fields_of: ty_idx=" << ty_idx << " not resolved");
}


td::Result<AliasTarget> AbiKernel::compute_alias_target(int ty_idx) const {
  TRY_RESULT(ty, ty_by_idx(ty_idx));
  if (ty->kind != TyKind::AliasRef) {
    return td::Status::Error(PSLICE() << "expected AliasRef at ty_idx=" << ty_idx);
  }
  TRY_RESULT(decl, get_alias(alias_ref_name(*ty)));

  AliasTarget target{decl->target_ty_idx, std::nullopt};
  auto inst_it = alias_inst_.find(ty_idx);
  if (inst_it != alias_inst_.end()) {
    target = AliasTarget{inst_it->second->monomorphic_target_ty_idx, decl->target_ty_idx};
  }
  return target;
}

td::Result<AliasTarget> AbiKernel::alias_target_of(int ty_idx) const {
  auto it = alias_cache_.find(ty_idx);
  if (it != alias_cache_.end()) {
    return it->second;
  }
  return compute_alias_target(ty_idx);  // yields the proper "not an AliasRef" error
}


std::string AbiKernel::render_type_args(const std::vector<int> &args) const {
  if (args.empty()) {
    return "";
  }
  std::string s = "<";
  for (std::size_t i = 0; i < args.size(); ++i) {
    if (i) s += ", ";
    s += render_ty(args[i]);
  }
  s += ">";
  return s;
}

std::string AbiKernel::render_ty(int ty_idx) const {
  const Ty &ty = ty_ref(ty_idx);
  switch (ty.kind) {
    case TyKind::Int:         return "int";
    case TyKind::IntN:        return "int" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::UintN:       return "uint" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::VarIntN:     return "varint" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::VarUIntN:    return "varuint" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::Coins:       return "coins";
    case TyKind::Bool:        return "bool";
    case TyKind::Cell:        return "cell";
    case TyKind::Builder:     return "builder";
    case TyKind::Slice:       return "slice";
    case TyKind::String:      return "string";
    case TyKind::Remaining:   return "RemainingBitsAndRefs";
    case TyKind::Address:     return "address";
    case TyKind::AddressOpt:  return "address?";
    case TyKind::AddressExt:  return "ext_address";
    case TyKind::AddressAny:  return "any_address";
    case TyKind::BitsN:       return "bits" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::NullLiteral: return "null";
    case TyKind::Callable:    return "continuation";
    case TyKind::Void:        return "void";
    case TyKind::Unknown:     return "unknown";
    case TyKind::Nullable:    return render_ty(std::get<TyNullable>(ty.data).inner_ty_idx) + "?";
    case TyKind::CellOf:      return "Cell<" + render_ty(std::get<TyInner>(ty.data).inner_ty_idx) + ">";
    case TyKind::ArrayOf:     return "array<" + render_ty(std::get<TyInner>(ty.data).inner_ty_idx) + ">";
    case TyKind::LispListOf:  return "lisp_list<" + render_ty(std::get<TyInner>(ty.data).inner_ty_idx) + ">";
    case TyKind::Tensor: {
      std::string s = "(";
      const auto &items = std::get<TyItems>(ty.data).items_ty_idx;
      for (std::size_t i = 0; i < items.size(); ++i) { if (i) s += ", "; s += render_ty(items[i]); }
      return s + ")";
    }
    case TyKind::ShapedTuple: {
      std::string s = "[";
      const auto &items = std::get<TyItems>(ty.data).items_ty_idx;
      for (std::size_t i = 0; i < items.size(); ++i) { if (i) s += ", "; s += render_ty(items[i]); }
      return s + "]";
    }
    case TyKind::MapKV: {
      const auto &m = std::get<TyMapKV>(ty.data);
      return "map<" + render_ty(m.key_ty_idx) + ", " + render_ty(m.value_ty_idx) + ">";
    }
    case TyKind::EnumRef:     return std::get<TyEnumRef>(ty.data).enum_name;
    case TyKind::StructRef: {
      const auto &sr = std::get<TyStructRef>(ty.data);
      return sr.struct_name + render_type_args(sr.type_args_ty_idx);
    }
    case TyKind::AliasRef: {
      const auto &ar = std::get<TyAliasRef>(ty.data);
      return ar.alias_name + render_type_args(ar.type_args_ty_idx);
    }
    case TyKind::GenericT:    return std::get<TyGenericT>(ty.data).name_t;
    case TyKind::Union: {
      const auto &u = std::get<TyUnion>(ty.data);
      std::string s;
      for (std::size_t i = 0; i < u.variants.size(); ++i) { if (i) s += " | "; s += render_ty(u.variants[i].variant_ty_idx); }
      return s;
    }
  }
  return "unknown";  // unreachable (all TyKind covered)
}

std::string AbiKernel::create_label(int ty_idx) const {
  const Ty &ty = ty_ref(ty_idx);
  switch (ty.kind) {
    case TyKind::Int:         return "int";
    case TyKind::IntN:        return "int" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::UintN:       return "uint" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::VarIntN:     return "varint" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::VarUIntN:    return "varuint" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::Coins:       return "coins";
    case TyKind::Bool:        return "bool";
    case TyKind::Cell:        return "cell";
    case TyKind::Builder:     return "builder";
    case TyKind::Slice:       return "slice";
    case TyKind::String:      return "string";
    case TyKind::Remaining:   return "RemainingBitsAndRefs";
    case TyKind::Address:     return "address";
    case TyKind::AddressOpt:  return "address?";
    case TyKind::AddressExt:  return "ext_address";
    case TyKind::AddressAny:  return "any_address";
    case TyKind::BitsN:       return "bits" + std::to_string(std::get<TyWidth>(ty.data).n);
    case TyKind::NullLiteral: return "null";
    case TyKind::Callable:    return "callable";  // NB: renderTy says "continuation", createLabel says "callable"
    case TyKind::Void:        return "void";
    case TyKind::Unknown:     return "unknown";
    case TyKind::Nullable:    return create_label(std::get<TyNullable>(ty.data).inner_ty_idx) + "?";
    case TyKind::CellOf:      return "Cell";
    case TyKind::ArrayOf:     return "array";
    case TyKind::LispListOf:  return "lisp_list";
    case TyKind::Tensor:      return "tensor";
    case TyKind::ShapedTuple: return "shaped";
    case TyKind::MapKV:       return "map";
    case TyKind::EnumRef:     return std::get<TyEnumRef>(ty.data).enum_name;
    case TyKind::StructRef:   return std::get<TyStructRef>(ty.data).struct_name;
    case TyKind::AliasRef: {
      auto target = compute_alias_target(ty_idx);
      if (target.is_error()) return "unknown";  // loader guarantees resolvable; be safe
      return create_label(target.ok().ty_idx);
    }
    case TyKind::GenericT:    return std::get<TyGenericT>(ty.data).name_t;
    case TyKind::Union: {
      const auto &u = std::get<TyUnion>(ty.data);
      std::string s;
      for (std::size_t i = 0; i < u.variants.size(); ++i) { if (i) s += "|"; s += create_label(u.variants[i].variant_ty_idx); }
      return s;
    }
  }
  return "unknown";  // unreachable
}

bool AbiKernel::is_struct_with_own_label(int ty_idx) const {
  const Ty &ty = ty_ref(ty_idx);
  if (ty.kind == TyKind::StructRef) return true;
  if (ty.kind == TyKind::AliasRef) {
    auto target = compute_alias_target(ty_idx);
    if (target.is_error()) return false;
    return is_struct_with_own_label(target.ok().ty_idx);
  }
  return false;
}


td::Result<std::vector<LabeledVariant>> AbiKernel::create_labels_for_union(
    const std::vector<UnionVariant> &variants, std::optional<int> u_label_ty_idx) const {
  // uLabelTyIdx: for a generic instantiation, e.g. original `Or<T1,T2>` used as
  // `Or<int32,int64>`, we want labels { $:'T1' }, { $:'T2' } (from the generic
  // union), not { $:'int32' }, { $:'int64' }. uLabelTyIdx points to that
  // original union; borrow variant types from there for labelling only.
  std::optional<std::vector<int>> generic_variants;
  if (u_label_ty_idx) {
    TRY_RESULT(label_ty, ty_by_idx(*u_label_ty_idx));
    if (label_ty->kind == TyKind::Union) {
      const auto &lu = std::get<TyUnion>(label_ty->data);
      if (lu.variants.size() == variants.size()) {
        std::vector<int> gv;
        gv.reserve(lu.variants.size());
        for (const auto &v : lu.variants) gv.push_back(v.variant_ty_idx);
        generic_variants = std::move(gv);
      }
    }
  }

  auto label_idx_of = [&](std::size_t i) -> int {
    return generic_variants ? (*generic_variants)[i] : variants[i].variant_ty_idx;
  };

  // A shortened label may collide (e.g. Wrapper<int32> | Wrapper<int64> both
  // shorten to 'Wrapper'); if so, fall back to full renderTy strings and force
  // a `value` field on every variant.
  std::set<std::string> unique;
  bool has_duplicates = false;
  for (std::size_t i = 0; i < variants.size(); ++i) {
    std::string label = create_label(label_idx_of(i));
    if (unique.count(label)) has_duplicates = true;
    unique.insert(label);
  }

  std::vector<LabeledVariant> out;
  out.reserve(variants.size());
  for (std::size_t i = 0; i < variants.size(); ++i) {
    const UnionVariant &v = variants[i];
    int label_ty_idx = label_idx_of(i);
    LabeledVariant lv;
    lv.variant_ty_idx = v.variant_ty_idx;  // ACTUAL (instantiated) variant to (de)serialize
    lv.prefix_num = v.prefix_num;
    lv.prefix_len = v.prefix_len;
    lv.is_prefix_implicit = v.is_prefix_implicit;

    TRY_RESULT(label_ty, ty_by_idx(label_ty_idx));
    if (label_ty->kind == TyKind::NullLiteral) {
      lv.label_str = "";
      lv.has_value_field = false;
    } else if (has_duplicates) {
      lv.label_str = render_ty(label_ty_idx);
      lv.has_value_field = true;
    } else {
      lv.label_str = create_label(label_ty_idx);
      lv.has_value_field = !is_struct_with_own_label(label_ty_idx);
    }
    out.push_back(std::move(lv));
  }
  return out;
}


void AbiKernel::build_union_dispatch(int union_ty_idx) {
  const auto &u = std::get<TyUnion>(ty_ref(union_ty_idx).data);
  // A trailing `void` variant (always last) does not participate in prefix
  // dispatch -- it's matched by an empty slice at runtime.
  bool has_void = !u.variants.empty() && ty_ref(u.variants.back().variant_ty_idx).kind == TyKind::Void;

  std::vector<std::size_t> order;
  std::size_t dispatch_count = u.variants.size() - (has_void ? 1u : 0u);
  order.reserve(dispatch_count);
  for (std::size_t i = 0; i < dispatch_count; ++i) order.push_back(i);

  // Sort by (prefix_len, prefix_num). This is behaviour-preserving vs the
  // reference's declaration-order first-match ONLY because the loader validated
  // union prefix-freedom (no variant prefix is a strict prefix of another): a
  // slice can then match at most one variant prefix, so dispatch is
  // order-independent (invariant iii). If that loader check were ever removed,
  // this sort would change results.
  std::stable_sort(order.begin(), order.end(), [&](std::size_t a, std::size_t b) {
    const auto &va = u.variants[a];
    const auto &vb = u.variants[b];
    if (va.prefix_len != vb.prefix_len) return va.prefix_len < vb.prefix_len;
    return va.prefix_num < vb.prefix_num;
  });

  union_dispatch_cache_[union_ty_idx] = std::move(order);
  union_has_void_cache_[union_ty_idx] = has_void;
}

td::Result<ResolvedUnion> AbiKernel::resolve_union(int union_ty_idx, std::optional<int> u_label_ty_idx) const {
  TRY_RESULT(ty, ty_by_idx(union_ty_idx));
  if (ty->kind != TyKind::Union) {
    return td::Status::Error(PSLICE() << "expected union at ty_idx=" << union_ty_idx);
  }
  const auto &u = std::get<TyUnion>(ty->data);
  TRY_RESULT(labeled, create_labels_for_union(u.variants, u_label_ty_idx));

  ResolvedUnion out;
  out.variants = std::move(labeled);
  // Dispatch order + has_void are use-site independent (prefixes don't vary
  // with uLabel) -> read straight from the eager cache. Labels above ARE
  // use-site dependent -> recomputed per (union ty_idx, uLabel) pair. That
  // split is invariant (i).
  out.dispatch_order = union_dispatch_cache_.at(union_ty_idx);
  out.has_void = union_has_void_cache_.at(union_ty_idx);
  return out;
}


td::Status AbiKernel::build_index() {
  for (const auto &d : abi_->declarations) {
    switch (d.kind) {
      case DeclKind::Struct: structs_[d.as_struct.name] = &d.as_struct; break;
      case DeclKind::Alias:  aliases_[d.as_alias.name] = &d.as_alias; break;
      case DeclKind::Enum:   enums_[d.as_enum.name] = &d.as_enum; break;
    }
  }
  for (const auto &si : abi_->struct_instantiations) struct_inst_[si.ty_idx] = &si;
  for (const auto &ai : abi_->alias_instantiations) alias_inst_[ai.ty_idx] = &ai;
  return td::Status::OK();
}

td::Status AbiKernel::build_resolution() {
  for (std::size_t i = 0; i < abi_->unique_types.size(); ++i) {
    const Ty &ty = abi_->unique_types[i];
    int ty_idx = static_cast<int>(i);
    switch (ty.kind) {
      case TyKind::StructRef: {
        TRY_RESULT(fields, compute_struct_fields(ty_idx));
        fields_cache_[ty_idx] = std::move(fields);
        break;
      }
      case TyKind::AliasRef: {
        TRY_RESULT(target, compute_alias_target(ty_idx));
        alias_cache_[ty_idx] = target;
        break;
      }
      case TyKind::Union:
        build_union_dispatch(ty_idx);
        break;
      default:
        break;
    }
  }
  return td::Status::OK();
}

td::Result<AbiKernel> AbiKernel::create(const ContractABI &abi) {
  // Re-verify every referenced ty index is in bounds so ty_ref() is sound for
  // the deep render/label recursion. The loader already does this; cheap to
  // re-assert and keeps AbiKernel usable on a hand-built ContractABI in tests.
  const std::size_t n = abi.unique_types.size();
  auto in_bounds = [&](int idx) { return idx >= 0 && static_cast<std::size_t>(idx) < n; };
  for (std::size_t i = 0; i < abi.unique_types.size(); ++i) {
    const Ty &t = abi.unique_types[i];
    auto bad = [&](int idx) {
      return td::Status::Error(PSLICE() << "unique_types[" << i << "]: ty_idx " << idx << " out of bounds");
    };
    switch (t.kind) {
      case TyKind::Nullable:
        if (!in_bounds(std::get<TyNullable>(t.data).inner_ty_idx)) return bad(std::get<TyNullable>(t.data).inner_ty_idx);
        break;
      case TyKind::CellOf:
      case TyKind::ArrayOf:
      case TyKind::LispListOf:
        if (!in_bounds(std::get<TyInner>(t.data).inner_ty_idx)) return bad(std::get<TyInner>(t.data).inner_ty_idx);
        break;
      case TyKind::Tensor:
      case TyKind::ShapedTuple:
        for (int idx : std::get<TyItems>(t.data).items_ty_idx) if (!in_bounds(idx)) return bad(idx);
        break;
      case TyKind::MapKV: {
        const auto &m = std::get<TyMapKV>(t.data);
        if (!in_bounds(m.key_ty_idx)) return bad(m.key_ty_idx);
        if (!in_bounds(m.value_ty_idx)) return bad(m.value_ty_idx);
        break;
      }
      case TyKind::StructRef:
        for (int idx : std::get<TyStructRef>(t.data).type_args_ty_idx) if (!in_bounds(idx)) return bad(idx);
        break;
      case TyKind::AliasRef:
        for (int idx : std::get<TyAliasRef>(t.data).type_args_ty_idx) if (!in_bounds(idx)) return bad(idx);
        break;
      case TyKind::Union:
        for (const auto &v : std::get<TyUnion>(t.data).variants) if (!in_bounds(v.variant_ty_idx)) return bad(v.variant_ty_idx);
        break;
      default:
        break;
    }
  }

  AbiKernel k(abi);
  TRY_STATUS(k.build_index());
  TRY_STATUS(k.build_resolution());
  return k;
}

}  // namespace ton_abi

#pragma once

// Cell-side ABI type resolution: symbol lookup, monomorph substitution,
// client-type replacement, alias targets, generic-union labels, and sorted
// prefix dispatch tables. Stack/get-method width calculation is intentionally
// outside this module.

#include "AbiModel.h"

#include "td/utils/Status.h"

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace ton_abi {

// A struct field after resolution. `orig` points into the ContractABI (kept
// alive by the non-owning ref the kernel holds) and carries name/default_value/
// description/original-client_ty_idx; `ty_idx` is the RESOLVED type (monomorph
// then cell-side client swap); `u_label_ty_idx` is the union-label back-ref.
struct ResolvedField {
  const ABIStructField *orig = nullptr;
  int ty_idx = 0;
  std::optional<int> u_label_ty_idx;

  const std::string &name() const { return orig->name; }
};

// Alias-target result.
struct AliasTarget {
  int ty_idx = 0;
  std::optional<int> u_label_ty_idx;
};

// One union variant plus its computed `$` label. `variant_ty_idx` and
// the prefix fields are the ACTUAL (instantiated) variant; `label_str` /
// `has_value_field` may derive from the GENERIC variant when uLabelTyIdx is supplied.
struct LabeledVariant {
  int variant_ty_idx = 0;
  std::uint32_t prefix_num = 0;
  int prefix_len = 0;
  bool is_prefix_implicit = false;
  std::string label_str;
  bool has_value_field = false;
};

// A resolved union at a specific use-site. `variants` are labeled and kept in
// DECLARATION order (index-stable for the emitter's std::variant and for label
// correctness). `dispatch_order` holds indices into `variants`, sorted ascending
// by (prefix_len, prefix_num), EXCLUDING a trailing `void` variant. `has_void`
// is the trailing-void special case -- when true, variants.back() is that void.
struct ResolvedUnion {
  std::vector<LabeledVariant> variants;
  std::vector<std::size_t> dispatch_order;
  bool has_void = false;
};

class AbiKernel {
 public:
  // Non-owning: `abi` MUST outlive the kernel. The loader already validated
  // OOB/arity/cycles/prefix-freedom; create() additionally re-verifies every ty
  // index is in bounds (cheap) so the internal render/label recursion may trust
  // ty_ref() without per-access bounds checks, and it eagerly builds the
  // use-site-independent caches (struct fields, alias targets, union dispatch
  // orders). Fail-closed: any resolution error aborts creation with a Status.
  static td::Result<AbiKernel> create(const ContractABI &abi);

  AbiKernel(AbiKernel &&) = default;
  AbiKernel &operator=(AbiKernel &&) = default;
  AbiKernel(const AbiKernel &) = delete;
  AbiKernel &operator=(const AbiKernel &) = delete;

  td::Result<const ABIStruct *> get_struct(const std::string &name) const;
  td::Result<const ABIAlias *> get_alias(const std::string &name) const;
  td::Result<const ABIEnum *> get_enum(const std::string &name) const;
  td::Result<const Ty *> ty_by_idx(int ty_idx) const;

  // Cell-side struct fields. Returns a pointer to the cached resolved-field
  // vector. Errors if ty_idx is not a StructRef.
  td::Result<const std::vector<ResolvedField> *> struct_fields_of(int ty_idx) const;

  // Alias target. Errors if ty_idx is not an AliasRef.
  td::Result<AliasTarget> alias_target_of(int ty_idx) const;

  // Union labels. Computed per call (labels are a USE-SITE property --
  // invariant (i)); `u_label_ty_idx` points to the original (generic) union
  // whose variant labels to borrow, when set.
  td::Result<std::vector<LabeledVariant>> create_labels_for_union(
      const std::vector<UnionVariant> &variants, std::optional<int> u_label_ty_idx) const;

  // Sorted + labeled union table for a (union ty_idx, uLabel) use-site.
  td::Result<ResolvedUnion> resolve_union(int union_ty_idx, std::optional<int> u_label_ty_idx) const;

 private:
  explicit AbiKernel(const ContractABI &abi) : abi_(&abi) {}

  // Trusted in-bounds (create() guarantees it). Used by the deep render/label
  // recursion where threading a Result through every switch arm would be noise.
  const Ty &ty_ref(int idx) const { return abi_->unique_types[static_cast<std::size_t>(idx)]; }

  std::string render_ty(int ty_idx) const;
  std::string render_type_args(const std::vector<int> &args) const;
  std::string create_label(int ty_idx) const;
  bool is_struct_with_own_label(int ty_idx) const;

  // Eager-pass builders (create() only).
  td::Status build_index();
  td::Status build_resolution();
  td::Result<std::vector<ResolvedField>> compute_struct_fields(int ty_idx) const;
  td::Result<AliasTarget> compute_alias_target(int ty_idx) const;
  void build_union_dispatch(int union_ty_idx);

  const ContractABI *abi_;

  std::unordered_map<std::string, const ABIStruct *> structs_;
  std::unordered_map<std::string, const ABIAlias *> aliases_;
  std::unordered_map<std::string, const ABIEnum *> enums_;
  std::unordered_map<int, const ABIStructInstantiation *> struct_inst_;
  std::unordered_map<int, const ABIAliasInstantiation *> alias_inst_;

  // Use-site-independent caches are populated in create() and read-only after
  // construction, so concurrent reads require no locking.
  std::unordered_map<int, std::vector<ResolvedField>> fields_cache_;
  std::unordered_map<int, AliasTarget> alias_cache_;
  std::unordered_map<int, std::vector<std::size_t>> union_dispatch_cache_;
  std::unordered_map<int, bool> union_has_void_cache_;
};

}  // namespace ton_abi

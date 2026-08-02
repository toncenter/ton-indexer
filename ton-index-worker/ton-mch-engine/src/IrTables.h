// Compiled matcher-table types shared by code generation and runtime. Node IDs are
// matcher-local; global_id keys the generated where table. Host-registry
// runnability is computed during setup rather than stored here. Opcodes are
// normalized to unsigned 32-bit values.
#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

namespace mch {

enum class NodeKind { Contract, BlockType, Pred, Any, Or, Recursive };
enum class RecStrategy { Frontier, Cyclic };
enum class AnchorKind { OpcodeSet, BType, Pred, Mixed };

struct CompiledNode {
  NodeKind kind{NodeKind::Any};
  int global_id{-1};  // artifact-global node index (key of the generated where table)
  std::uint32_t opcode{0};   // contract (unsigned 32)
  bool has_opcode{false};
  std::string btype;         // block_type
  std::string pred_name;     // pred kind (host predicate)
  std::string where_name;    // named `where` predicate (host predicate)
  bool has_where_expr{false};  // inline `where (expr)`: the generated fn keyed by global_id
  int slot{-1};
  bool optional{false};
  int child{-1};
  std::vector<int> children;
  int parent{-1};
  std::vector<int> branches;
  int step{-1};
  int exit{-1};
  bool exclusive{false};
  RecStrategy strategy{RecStrategy::Frontier};
};

struct CompiledMatcher {
  std::string name;
  int artifact_index{-1};  // index in the artifact `matchers` array (gen_builds key)
  bool has_build_program{false};
  std::vector<std::string> produces;

  AnchorKind anchor_kind{AnchorKind::OpcodeSet};
  std::unordered_set<std::uint32_t> anchor_opcodes;
  std::unordered_set<std::string> anchor_btypes;
  std::string anchor_pred;  // Pred-kind anchor's predicate name

  // Mixed anchor: XOR branches remain in source order, each an opcode
  // or a btype head with an optional `where` predicate. test_self takes union
  // membership and then the matching branch's predicate, so the branch order
  // (and the head/pred pairing) has to survive into the table.
  struct AnchorBranch {
    bool is_op{false};
    std::uint32_t opcode{0};
    std::string btype;
    std::string where;  // host predicate name, empty when the branch has none
  };
  std::vector<AnchorBranch> anchor_branches;

  std::vector<CompiledNode> nodes;      // local pool, remapped, root == 0
  std::vector<std::string> slot_names;  // slot id -> capture name (decl order)
  std::vector<std::string> cards;       // slot id -> "one"|"opt"|"many"

  // Recursion capture ownership (per recursive node id -> owned slot ids).
  std::map<int, std::set<int>> step_slots;
  std::map<int, std::set<int>> exit_slots;
  std::set<int> owned_slots;

  bool include_excess{true};
  bool include_bounces{true};
  int priority{100};

  // Host bindings referenced. `ref_preds` is the MATCH-phase dependency set
  // (anchor predicates + every reachable `pred` / named-`where` node) and gates
  // inclusion via match_skip_reason(); the rest are BUILD-phase bindings, gated
  // by build_skip_reason().
  std::set<std::string> ref_preds, ref_builders, ref_shapers;
  std::set<std::string> ref_msgtypes, ref_lookups, ref_fns;
};

}  // namespace mch

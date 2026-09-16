#include "Walker.h"

#include "HostRegistry.h"
#include "btypes_gen.h"

#include <algorithm>
#include <map>
#include <tuple>
#include <unordered_set>

namespace mch {

namespace {

constexpr long kExitIdx = -1;  // frame idx marker for the exit attempt

// Match recursion depth cap. depth == block-tree depth reached through the
// mutual recursion (match_node -> child/children/parent/or/recursive edges).
// 500 trips well before either runtime's native stack fails. No corpus
// trace is anywhere near this (max depth 2); this only guards pathological
// inputs, converting an undefined stack overflow into a clean per-trace fail.
constexpr int kMaxMatchDepth = 500;

struct Frame {
  int nid{-1};   // -1 == no frame
  long idx{0};   // iter_index (>=0) or kExitIdx
};

struct FrameEntry {
  bool is_step;
  int nid;
  long iter;
  bool consumed_any;
};

struct Mark {
  std::size_t nb, nc, nf;
};

std::vector<Block *> canonical(const std::vector<Block *> &v) {
  std::vector<Block *> out = v;
  std::stable_sort(out.begin(), out.end(),
                   [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
  return out;
}

// Per-attempt walk state. Owns the match recursion; the matcher is
// immutable.
class MatchState {
 public:
  MatchState(const CompiledMatcher &m, const WhereTable &gen, Block *anchor)
      : m_(m), gen_(gen), where_slots_(m.slot_names.size(), Value::null()) {
    if (!m_.nodes.empty()) {
      int entry_slot = m_.nodes[0].slot;
      if (entry_slot >= 0 && entry_slot < static_cast<int>(where_slots_.size())) {
        where_slots_[entry_slot] = Value::make_block(anchor);
      }
    }
  }

  bool match_node(int nid, Block *block, bool absorb = true) {
    struct DepthGuard {
      int &d;
      explicit DepthGuard(int &x) : d(x) { ++d; }
      ~DepthGuard() { --d; }
    } depth_guard(depth_);
    if (depth_ > kMaxMatchDepth) {
      throw MatchDepthExceeded();
    }
    const CompiledNode &node = m_.nodes[nid];
    if (node.kind == NodeKind::Or) {
      return match_or(node, block);
    }
    if (node.kind == NodeKind::Recursive) {
      return match_recursive(nid, node, block);
    }
    if (!test_head(nid, block)) {
      return false;
    }
    if (!node.peek) {
      consume(block);
    }
    if (node.slot >= 0) {
      bind(node.slot, block);
    }
    if (!match_child_edge(node, block)) {
      return false;
    }
    if (!match_children_set(node, block)) {
      return false;
    }
    if (!match_parent_edge(node, block)) {
      return false;
    }
    if (absorb && !node.peek) {
      absorb_aux(block);
    }
    return true;
  }

  MatchResult finish() {
    MatchResult r;
    r.consumed = consumed_;
    r.captures = assemble_captures();
    return r;
  }

 private:
  // Never raises/aborts: a fault or a non-bool result is simply False.
  bool eval_where(const CompiledNode &node, Block *block) const {
    auto it = gen_.find(node.global_id);
    if (it == gen_.end()) {
      return false;  // coverage is validated at setup (prepare_classify)
    }
    WhereEnv w{block, where_slots_.data(), where_slots_.size()};
    EvalResult r = it->second(w);
    return !r.faulted && r.value.t == VType::Bool && r.value.boolean;
  }

  bool test_head(int nid, Block *block) const {
    const CompiledNode &node = m_.nodes[nid];
    bool ok = false;
    switch (node.kind) {
      case NodeKind::Contract:
        ok = block->btype == mch::btype::kCallContract && block->opcode &&
             *block->opcode == node.opcode;
        break;
      case NodeKind::BlockType:
        ok = block->btype == node.btype;
        break;
      case NodeKind::Any:
        ok = true;
        break;
      case NodeKind::Or:
        if (node.exclusive) {
          int c = 0;
          for (int b : node.branches) {
            if (test_head(b, block)) c++;
          }
          ok = c == 1;
        } else {
          for (int b : node.branches) {
            if (test_head(b, block)) {
              ok = true;
              break;
            }
          }
        }
        break;
      case NodeKind::Recursive:
        ok = test_head(node.step, block);
        break;
      case NodeKind::Pred: {
        auto it = host_predicates().find(node.pred_name);
        ok = it != host_predicates().end() && it->second(block);
        break;
      }
    }
    if (!ok) {
      return false;
    }
    // Named `where` predicate, then inline where_exprs compose onto the head
    // test. Loader gates runnability on the registry, so a missing name here
    // means an unrunnable matcher.
    if (!node.where_name.empty()) {
      auto it = host_predicates().find(node.where_name);
      if (it == host_predicates().end() || !it->second(block)) {
        return false;
      }
    }
    if (node.has_where_expr && !eval_where(node, block)) {
      return false;
    }
    return true;
  }

  bool match_child_edge(const CompiledNode &node, Block *block) {
    if (node.child < 0) {
      return true;
    }
    bool matched_any = false;
    for (Block *cand : canonical(block->next_blocks)) {
      Mark mk = mark();
      if (match_node(node.child, cand)) {
        matched_any = true;
      } else {
        rollback(mk);
      }
    }
    return matched_any || m_.nodes[node.child].optional;
  }

  bool match_children_set(const CompiledNode &node, Block *block) {
    if (node.children.empty()) {
      return true;
    }
    std::vector<Block *> working = canonical(block->next_blocks);
    for (int item : node.children) {
      bool matched = false;
      for (std::size_t i = 0; i < working.size(); i++) {
        Mark mk = mark();
        if (match_node(item, working[i])) {
          working.erase(working.begin() + i);
          matched = true;
          break;
        }
        rollback(mk);
      }
      if (!matched && !m_.nodes[item].optional) {
        return false;
      }
    }
    return true;
  }

  bool match_parent_edge(const CompiledNode &node, Block *block) {
    if (node.parent < 0) {
      return true;
    }
    Block *prev = block->previous_block;
    if (prev != nullptr) {
      Mark mk = mark();
      if (match_node(node.parent, prev)) {
        return true;
      }
      rollback(mk);
    }
    return m_.nodes[node.parent].optional;
  }

  bool match_or(const CompiledNode &node, Block *block) {
    for (int bid : node.branches) {
      Mark mk = mark();
      if (match_node(bid, block)) {
        return true;
      }
      rollback(mk);
    }
    return false;
  }

  bool match_recursive(int nid, const CompiledNode &node, Block *entry) {
    // Entry gate: the recursive node's own where clauses run against the entry
    // block before the strategy dispatch.
    if (!node.where_name.empty()) {
      auto it = host_predicates().find(node.where_name);
      if (it == host_predicates().end() || !it->second(entry)) {
        return false;
      }
    }
    if (node.has_where_expr && !eval_where(node, entry)) {
      return false;
    }
    if (node.strategy == RecStrategy::Cyclic) {
      return match_node(node.step, entry);
    }
    if (!test_head(node.step, entry)) {
      return false;
    }
    Frame outer = frame_;
    std::vector<Block *> frontier{entry};
    long iter_index = 0;
    while (true) {
      bool stepped = false;
      for (Block *cand : frontier) {
        if (node.exit >= 0) {
          Mark mk = mark();
          frame_ = Frame{nid, kExitIdx};
          if (match_node(node.exit, cand)) {
            frames_.push_back(FrameEntry{false, nid, 0, false});
            frame_ = outer;
            return true;
          }
          rollback(mk);
        }
        Mark mk = mark();
        frame_ = Frame{nid, iter_index};
        if (match_node(node.step, cand)) {
          stepped = true;
          std::vector<Block *> step_consumed(consumed_.begin() + mk.nc, consumed_.end());
          frames_.push_back(FrameEntry{true, nid, iter_index, !step_consumed.empty()});
          std::vector<Block *> nf;
          for (Block *b : step_consumed) {
            for (Block *x : b->next_blocks) {
              nf.push_back(x);
            }
          }
          frontier = canonical(nf);
          break;
        }
        rollback(mk);
      }
      if (!stepped) {
        frame_ = outer;
        return iter_index > 0;
      }
      iter_index++;
    }
  }

  void absorb_aux(Block *block) {
    for (Block *nb : canonical(block->next_blocks)) {
      if (nb->btype == mch::btype::kCallContract && nb->opcode &&
          (*nb->opcode == kExcessOpcode || *nb->opcode == kBounceOpcode)) {
        consume(nb);
      }
    }
  }

  // Context primitives
  Mark mark() const { return Mark{bindings_.size(), consumed_.size(), frames_.size()}; }

  void rollback(const Mark &mk) {
    bindings_.resize(mk.nb);
    for (std::size_t i = mk.nc; i < consumed_.size(); i++) {
      visited_.erase(consumed_[i]);
    }
    consumed_.resize(mk.nc);
    frames_.resize(mk.nf);
  }

  void consume(Block *b) {
    if (visited_.insert(b).second) {
      consumed_.push_back(b);
    }
  }

  void bind(int slot, Block *b) { bindings_.emplace_back(slot, frame_, b); }

  std::vector<Capture> assemble_captures() const {
    std::map<int, Block *> first_any;
    std::map<std::tuple<int, int, long>, Block *> first_framed;
    for (const auto &t : bindings_) {
      int slot = std::get<0>(t);
      const Frame &f = std::get<1>(t);
      Block *b = std::get<2>(t);
      first_any.emplace(slot, b);
      first_framed.emplace(std::make_tuple(slot, f.nid, f.idx), b);
    }

    std::vector<Capture> out;
    for (int slot = 0; slot < static_cast<int>(m_.slot_names.size()); slot++) {
      Capture c;
      c.name = m_.slot_names[slot];
      if (m_.cards[slot] != "many") {
        c.is_list = false;
        auto it = first_any.find(slot);
        c.vals.push_back(it != first_any.end() ? it->second : nullptr);
        out.push_back(std::move(c));
        continue;
      }
      c.is_list = true;
      if (m_.owned_slots.count(slot) == 0) {
        for (const auto &t : bindings_) {
          if (std::get<0>(t) == slot) {
            c.vals.push_back(std::get<2>(t));
          }
        }
        out.push_back(std::move(c));
        continue;
      }
      for (const FrameEntry &e : frames_) {
        if (e.is_step) {
          auto ss = m_.step_slots.find(e.nid);
          if (e.consumed_any && ss != m_.step_slots.end() && ss->second.count(slot)) {
            auto it = first_framed.find(std::make_tuple(slot, e.nid, e.iter));
            c.vals.push_back(it != first_framed.end() ? it->second : nullptr);
          }
        } else {
          auto es = m_.exit_slots.find(e.nid);
          if (es != m_.exit_slots.end() && es->second.count(slot)) {
            auto it = first_framed.find(std::make_tuple(slot, e.nid, kExitIdx));
            c.vals.push_back(it != first_framed.end() ? it->second : nullptr);
          }
        }
      }
      out.push_back(std::move(c));
    }
    return out;
  }

  const CompiledMatcher &m_;
  const WhereTable &gen_;
  std::vector<Value> where_slots_;
  std::vector<std::tuple<int, Frame, Block *>> bindings_;
  std::vector<Block *> consumed_;
  std::unordered_set<Block *> visited_;
  std::vector<FrameEntry> frames_;
  Frame frame_;
  int depth_{0};  // current match recursion depth (kMaxMatchDepth cap)
};

}  // namespace

bool matcher_test_self(const CompiledMatcher &m, const Block *b) {
  switch (m.anchor_kind) {
    case AnchorKind::OpcodeSet:
      return b->btype == mch::btype::kCallContract && b->opcode &&
             m.anchor_opcodes.count(*b->opcode) > 0;
    case AnchorKind::BType:
      return m.anchor_btypes.count(b->btype) > 0;
    case AnchorKind::Pred: {
      // Full-scan predicate anchor, no prefilter.
      auto it = host_predicates().find(m.anchor_pred);
      return it != host_predicates().end() && it->second(b);
    }
    case AnchorKind::Mixed: {
      // Mixed anchors test union membership in source order, then
      // the matching branch's `where`, a head match whose predicate fails
      // falls through to a later branch sharing the head.
      for (const auto &br : m.anchor_branches) {
        bool ok = br.is_op ? (b->btype == mch::btype::kCallContract && b->opcode &&
                              *b->opcode == br.opcode)
                           : (b->btype == br.btype);
        if (!ok) {
          continue;
        }
        if (br.where.empty()) {
          return true;
        }
        auto it = host_predicates().find(br.where);
        if (it != host_predicates().end() && it->second(b)) {
          return true;
        }
      }
      return false;
    }
  }
  return false;
}

std::optional<MatchResult> matcher_match(const CompiledMatcher &m, Block *anchor,
                                         const WhereTable &gen) {
  if (!matcher_test_self(m, anchor)) {
    return std::nullopt;
  }
  MatchState st(m, gen, anchor);
  // Root skips nested aux absorption (root-level excess/bounce are a build-
  // result concern, appended after build, not part of the match consumed set).
  if (!st.match_node(0, anchor, /*absorb=*/false)) {
    return std::nullopt;
  }
  return st.finish();
}

}  // namespace mch

#include "fixtures/IrLoader.h"
#include "fixtures/IrJson.h"

#include "td/utils/JsonBuilder.h"

#include <algorithm>
#include <exception>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>

namespace mch {

namespace {

using Json = td::JsonValue;
using JType = td::JsonValue::Type;

// Parse a JSON Number token as int64. A malformed / overflowing token in a
// corrupt artifact becomes a clean Status error instead of an uncaught
// std::stoll throw (std::invalid_argument / std::out_of_range).
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

// Node ids in a fixed order (child, children..., parent, branches..., step,
// exit). Remap discovery order depends on it. Intentional.
std::vector<int> edge_refs(const Json &rec) {
  std::vector<int> out;
  if (has(rec, "child")) {
    out.push_back(static_cast<int>(jint(rec, "child")));
  }
  if (const Json *ch = jfield(rec, "children"); ch != nullptr && ch->type() == JType::Array) {
    for (const auto &c : ch->get_array()) {
      out.push_back(static_cast<int>(std::stoll(c.get_number().str())));
    }
  }
  if (has(rec, "parent")) {
    out.push_back(static_cast<int>(jint(rec, "parent")));
  }
  if (const Json *br = jfield(rec, "branches"); br != nullptr && br->type() == JType::Array) {
    for (const auto &b : br->get_array()) {
      out.push_back(static_cast<int>(std::stoll(b.get_number().str())));
    }
  }
  if (has(rec, "step")) {
    out.push_back(static_cast<int>(jint(rec, "step")));
  }
  if (has(rec, "exit")) {
    out.push_back(static_cast<int>(jint(rec, "exit")));
  }
  return out;
}

// Fail closed on an unrecognized kind: an unknown/typo'd kind must NOT
// silently degrade to the permissive `any` wildcard. That would make
// a corrupt artifact match more blocks than authored.
td::Result<NodeKind> parse_kind(const std::string &k) {
  if (k == "contract") return NodeKind::Contract;
  if (k == "block_type") return NodeKind::BlockType;
  if (k == "pred") return NodeKind::Pred;
  if (k == "or") return NodeKind::Or;
  if (k == "recursive") return NodeKind::Recursive;
  if (k == "any") return NodeKind::Any;
  return td::Status::Error("unknown node kind '" + k + "'");
}

void scan_program(const Json &e, CompiledMatcher &m) {
  if (e.type() == JType::Object) {
    const std::string k = jstr(e, "k");
    if (k == "call") {
      std::string fn = jstr(e, "fn");
      if (!fn.empty() && !is_builtin_name(fn)) {
        m.ref_fns.insert(fn);
      }
    } else if (k == "lookup") {
      std::string nm = jstr(e, "name");
      if (!nm.empty()) {
        m.ref_lookups.insert(nm);
      }
    }
    if (k == "parse" || jstr(e, "s") == "parse") {
      // Missing `nullable` defaults to strict parse.
      const bool nullable = k == "parse" && jbool(e, "nullable", false);
      (void)nullable;
      if (const Json *types = jfield(e, "types"); types != nullptr && types->type() == JType::Array) {
        for (const auto &t : types->get_array()) {
          if (t.type() == JType::String) {
            m.ref_msgtypes.insert(t.get_string().str());
          }
        }
      }
    }
    for (const auto &kv : e.get_object().field_values_) {
      scan_program(kv.second, m);
    }
  } else if (e.type() == JType::Array) {
    for (const auto &it : e.get_array()) {
      scan_program(it, m);
    }
  }
}

// Local reachability over every edge kind (child/parent/step/exit/children/
// branches), cycle-safe, discovery order. Intentional.
std::vector<int> reachable(const std::vector<CompiledNode> &nodes, int start) {
  std::vector<int> out{start};
  std::unordered_set<int> seen{start};
  std::size_t qi = 0;
  while (qi < out.size()) {
    const CompiledNode &n = nodes[out[qi++]];
    std::vector<int> refs;
    if (n.child >= 0) refs.push_back(n.child);
    if (n.parent >= 0) refs.push_back(n.parent);
    if (n.step >= 0) refs.push_back(n.step);
    if (n.exit >= 0) refs.push_back(n.exit);
    refs.insert(refs.end(), n.children.begin(), n.children.end());
    refs.insert(refs.end(), n.branches.begin(), n.branches.end());
    for (int r : refs) {
      if (r >= 0 && seen.insert(r).second) {
        out.push_back(r);
      }
    }
  }
  return out;
}

td::Status build_matcher(const Json &rec, const std::vector<const Json *> &pool,
                         CompiledMatcher &m) {
  m.name = jstr(rec, "name");
  if (const Json *pr = jfield(rec, "produces"); pr != nullptr && pr->type() == JType::Array) {
    for (const auto &p : pr->get_array()) {
      m.produces.push_back(p.get_string().str());
    }
  }

  const Json *anchor = jfield(rec, "anchor");
  const std::string ak = anchor != nullptr ? jstr(*anchor, "kind") : "";
  if (ak == "opcode_set") {
    m.anchor_kind = AnchorKind::OpcodeSet;
    const Json *values = jfield(*anchor, "values");
    if (values == nullptr || values->type() != JType::Array) {
      return td::Status::Error("matcher " + m.name + ": opcode_set anchor missing 'values' array");
    }
    for (const auto &v : values->get_array()) {
      TRY_RESULT(op, num_i64(v));
      m.anchor_opcodes.insert(static_cast<std::uint32_t>(op & 0xFFFFFFFF));
    }
  } else if (ak == "btype") {
    m.anchor_kind = AnchorKind::BType;
    const Json *values = jfield(*anchor, "values");
    if (values == nullptr || values->type() != JType::Array) {
      return td::Status::Error("matcher " + m.name + ": btype anchor missing 'values' array");
    }
    for (const auto &v : values->get_array()) {
      m.anchor_btypes.insert(v.get_string().str());
    }
  } else if (ak == "pred") {
    m.anchor_kind = AnchorKind::Pred;
    m.anchor_pred = jstr(*anchor, "pred");
    m.ref_preds.insert(m.anchor_pred);
  } else if (ak == "mixed") {
    m.anchor_kind = AnchorKind::Mixed;
    if (const Json *br = jfield(*anchor, "branches"); br != nullptr && br->type() == JType::Array) {
      for (const auto &b : br->get_array()) {
        CompiledMatcher::AnchorBranch ab;
        if (const Json *op = jfield(b, "op"); op != nullptr) {
          TRY_RESULT(v, num_i64(*op));
          ab.is_op = true;
          ab.opcode = static_cast<std::uint32_t>(v & 0xFFFFFFFF);
          m.anchor_opcodes.insert(ab.opcode);
        } else {
          ab.btype = jstr(b, "btype");
          m.anchor_btypes.insert(ab.btype);
        }
        if (has(b, "where")) {
          ab.where = jstr(b, "where");
          m.ref_preds.insert(ab.where);
        }
        m.anchor_branches.push_back(std::move(ab));
      }
    }
  } else {
    return td::Status::Error("matcher " + m.name + ": bad anchor kind '" + ak + "'");
  }

  // Slots + cards (captures table declaration order).
  std::unordered_map<std::string, int> slot_of;
  if (const Json *caps = jfield(rec, "captures"); caps != nullptr && caps->type() == JType::Array) {
    for (const auto &c : caps->get_array()) {
      std::string cn = jstr(c, "name");
      slot_of[cn] = static_cast<int>(m.slot_names.size());
      m.slot_names.push_back(cn);
      m.cards.push_back(jstr(c, "card"));
    }
  }

  // Reachable subgraph remapped to local ids (root = 0), discovery order over
  // edge_refs. A missing or out-of-range root is a corrupt artifact, not a
  // default to node 0.
  if (!has(rec, "root")) {
    return td::Status::Error("matcher " + m.name + ": missing 'root'");
  }
  int root = static_cast<int>(jint(rec, "root"));
  if (root < 0 || root >= static_cast<int>(pool.size())) {
    return td::Status::Error("matcher " + m.name + ": root index out of range");
  }
  std::vector<int> order{root};
  std::unordered_map<int, int> remap{{root, 0}};
  std::size_t qi = 0;
  while (qi < order.size()) {
    const Json &nrec = *pool[order[qi++]];
    for (int ref : edge_refs(nrec)) {
      if (ref < 0 || ref >= static_cast<int>(pool.size())) {
        return td::Status::Error("matcher " + m.name + ": node ref out of range");
      }
      if (remap.find(ref) == remap.end()) {
        remap[ref] = static_cast<int>(order.size());
        order.push_back(ref);
      }
    }
  }

  auto rm = [&](int old) { return old < 0 ? -1 : remap[old]; };
  for (int old : order) {
    const Json &nrec = *pool[old];
    CompiledNode cn;
    cn.global_id = old;
    TRY_RESULT(kind, parse_kind(jstr(nrec, "kind")));
    cn.kind = kind;
    if (has(nrec, "opcode")) {
      cn.opcode = static_cast<std::uint32_t>(jint(nrec, "opcode") & 0xFFFFFFFF);
      cn.has_opcode = true;
    }
    cn.btype = jstr(nrec, "btype");
    if (cn.kind == NodeKind::Pred) {
      cn.pred_name = jstr(nrec, "pred");
    }
    cn.where_name = jstr(nrec, "where");
    // Only the presence of an inline where is compiled; the tree is a generated
    // function keyed by cn.global_id (GenWheres.h).
    cn.has_where_expr = jfield(nrec, "where_expr") != nullptr;
    std::string cap = jstr(nrec, "capture");
    cn.slot = (!cap.empty() && slot_of.count(cap)) ? slot_of[cap] : -1;
    cn.optional = jbool(nrec, "optional");
    if (const Json *peek = jfield(nrec, "peek");
        peek != nullptr && peek->type() != JType::Boolean) {
      return td::Status::Error("matcher " + m.name + ": node 'peek' must be a bool");
    }
    cn.peek = jbool(nrec, "peek");
    cn.child = has(nrec, "child") ? rm(static_cast<int>(jint(nrec, "child"))) : -1;
    if (const Json *ch = jfield(nrec, "children"); ch != nullptr && ch->type() == JType::Array) {
      for (const auto &c : ch->get_array()) {
        TRY_RESULT(cid, num_i64(c));
        cn.children.push_back(rm(static_cast<int>(cid)));
      }
    }
    cn.parent = has(nrec, "parent") ? rm(static_cast<int>(jint(nrec, "parent"))) : -1;
    if (const Json *br = jfield(nrec, "branches"); br != nullptr && br->type() == JType::Array) {
      for (const auto &b : br->get_array()) {
        TRY_RESULT(bid, num_i64(b));
        cn.branches.push_back(rm(static_cast<int>(bid)));
      }
    }
    cn.step = has(nrec, "step") ? rm(static_cast<int>(jint(nrec, "step"))) : -1;
    cn.exit = has(nrec, "exit") ? rm(static_cast<int>(jint(nrec, "exit"))) : -1;
    cn.exclusive = jbool(nrec, "exclusive");
    cn.strategy = jstr(nrec, "strategy") == "cyclic" ? RecStrategy::Cyclic : RecStrategy::Frontier;
    m.nodes.push_back(std::move(cn));
  }

  // Recursion ownership + load-time gates (nested recursion, cyclic captures).
  std::set<int> owned;
  for (int nid = 0; nid < static_cast<int>(m.nodes.size()); nid++) {
    const CompiledNode &node = m.nodes[nid];
    if (node.kind != NodeKind::Recursive) {
      continue;
    }
    std::vector<int> reach = reachable(m.nodes, node.step);
    for (int r : reach) {
      if (m.nodes[r].kind == NodeKind::Recursive) {
        return td::Status::Error("matcher " + m.name + ": nested recursion is not supported");
      }
    }
    if (node.strategy == RecStrategy::Cyclic) {
      for (int r : reach) {
        if (m.nodes[r].slot >= 0) {
          return td::Status::Error("matcher " + m.name +
                                   ": captures inside a cyclic recursion body are not supported");
        }
      }
      m.step_slots[nid] = {};
      m.exit_slots[nid] = {};
      continue;
    }
    std::set<int> ss;
    for (int r : reach) {
      if (m.nodes[r].slot >= 0) ss.insert(m.nodes[r].slot);
    }
    m.step_slots[nid] = ss;
    std::set<int> es;
    if (node.exit >= 0) {
      for (int r : reachable(m.nodes, node.exit)) {
        if (m.nodes[r].slot >= 0) es.insert(m.nodes[r].slot);
      }
    }
    m.exit_slots[nid] = es;
    owned.insert(ss.begin(), ss.end());
    owned.insert(es.begin(), es.end());
  }
  m.owned_slots = owned;

  m.include_excess = jbool(rec, "include_excess", true);
  m.include_bounces = jbool(rec, "include_bounces", true);
  // A present-but-non-numeric priority is corruption, not a silent default-100
  // Absence still defaults to 100.
  if (const Json *pf = jfield(rec, "priority"); pf != nullptr && pf->type() != JType::Number) {
    return td::Status::Error("matcher " + m.name + ": 'priority' must be a number");
  }
  m.priority = static_cast<int>(jint(rec, "priority", 100));
  if (has(rec, "builder")) {
    m.ref_builders.insert(jstr(rec, "builder"));
  }
  if (has(rec, "shape")) {
    m.ref_shapers.insert(jstr(rec, "shape"));
  }
  if (const Json *bp = jfield(rec, "build_program"); bp != nullptr) {
    m.has_build_program = true;
    scan_program(*bp, m);
  }

  // Match-phase host dependencies: named predicates plus every `pred` /
  // named-`where` node. Inline where_exprs are not host dependencies
  // (generated where table). Host presence is match_skip_reason()'s call.
  for (const CompiledNode &n : m.nodes) {
    if (n.kind == NodeKind::Pred && !n.pred_name.empty()) {
      m.ref_preds.insert(n.pred_name);
    }
    if (!n.where_name.empty()) {
      m.ref_preds.insert(n.where_name);
    }
  }
  return td::Status::OK();
}

td::Status validate_btype_references(const std::vector<CompiledMatcher> &matchers) {
  std::unordered_set<std::string> known;
  for (std::string_view btype : kLeafBtypes) {
    known.emplace(btype);
  }
  for (const CompiledMatcher &matcher : matchers) {
    known.insert(matcher.produces.begin(), matcher.produces.end());
  }

  for (const CompiledMatcher &matcher : matchers) {
    auto validate = [&](const std::string &btype) -> td::Status {
      if (known.count(btype) == 0) {
        return td::Status::Error("matcher " + matcher.name + ": unknown btype reference '" +
                                 btype + "'");
      }
      return td::Status::OK();
    };
    if (matcher.anchor_kind == AnchorKind::BType) {
      for (const std::string &btype : matcher.anchor_btypes) {
        TRY_STATUS(validate(btype));
      }
    } else if (matcher.anchor_kind == AnchorKind::Mixed) {
      for (const CompiledMatcher::AnchorBranch &branch : matcher.anchor_branches) {
        if (!branch.is_op) {
          TRY_STATUS(validate(branch.btype));
        }
      }
    }
    for (const CompiledNode &node : matcher.nodes) {
      if (node.kind == NodeKind::BlockType) {
        TRY_STATUS(validate(node.btype));
      }
    }
  }
  return td::Status::OK();
}

td::Status validate_peek_invariants(const std::vector<CompiledMatcher> &matchers) {
  for (const CompiledMatcher &matcher : matchers) {
    if (!matcher.nodes.empty() && matcher.nodes[0].peek) {
      return td::Status::Error("matcher " + matcher.name +
                               ": root node cannot be peek (the entry is always consumed)");
    }
    for (int nid = 0; nid < static_cast<int>(matcher.nodes.size()); nid++) {
      if (!matcher.nodes[nid].peek) {
        continue;
      }
      for (int reached : reachable(matcher.nodes, nid)) {
        if (!matcher.nodes[reached].peek) {
          return td::Status::Error(
              "matcher " + matcher.name +
              ": a peek subtree contains a consuming node; every reachable node must also be peek");
        }
      }
    }
  }
  return td::Status::OK();
}

}  // namespace

td::Result<LoadedIr> load_ir(const std::string &path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return td::Status::Error("cannot open IR artifact: " + path);
  }
  std::stringstream ss;
  ss << in.rdbuf();
  std::string buf = ss.str();

  // Hash BEFORE json_decode mutates the buffer.
  std::string source_sha = sha256_hex(td::Slice(buf));

  auto r_root = td::json_decode(td::MutableSlice(buf));
  if (r_root.is_error()) {
    return td::Status::Error("IR json parse error: " + r_root.error().message().str());
  }
  Json root = r_root.move_as_ok();
  if (root.type() != JType::Object) {
    return td::Status::Error("IR artifact root must be an object");
  }
  if (jstr(root, "mch_ir_version") != "1.1") {
    return td::Status::Error("unsupported mch_ir_version (expected 1.1)");
  }
  const Json *nodes = jfield(root, "nodes");
  const Json *matchers = jfield(root, "matchers");
  if (nodes == nullptr || nodes->type() != JType::Array ||
      matchers == nullptr || matchers->type() != JType::Array) {
    return td::Status::Error("IR artifact needs 'nodes' and 'matchers' arrays");
  }

  std::vector<const Json *> pool;
  for (const auto &n : nodes->get_array()) {
    pool.push_back(&n);
  }

  LoadedIr out;
  out.source_sha256 = source_sha;

  std::vector<CompiledMatcher> compiled;
  for (const auto &rec : matchers->get_array()) {
    CompiledMatcher m;
    m.artifact_index = static_cast<int>(compiled.size());
    TRY_STATUS(build_matcher(rec, pool, m));
    compiled.push_back(std::move(m));
  }
  TRY_STATUS(validate_peek_invariants(compiled));
  TRY_STATUS(validate_btype_references(compiled));

  // registration_order (default: identity), then stable-sort by priority.
  std::vector<int> reg_order;
  if (const Json *ro = jfield(root, "registration_order"); ro != nullptr && ro->type() == JType::Array) {
    // Must be a permutation of [0, matchers): a stray/out-of-range/duplicate
    // index would index compiled[] out of bounds (or drop a matcher) during
    // the priority sort below. Fail closed instead.
    std::vector<bool> seen(compiled.size(), false);
    for (const auto &mi : ro->get_array()) {
      TRY_RESULT(idx64, num_i64(mi));
      int idx = static_cast<int>(idx64);
      if (idx < 0 || idx >= static_cast<int>(compiled.size())) {
        return td::Status::Error("registration_order index out of range");
      }
      if (seen[idx]) {
        return td::Status::Error("registration_order has a duplicate index");
      }
      seen[idx] = true;
      reg_order.push_back(idx);
    }
    if (reg_order.size() != compiled.size()) {
      return td::Status::Error("registration_order must list every matcher exactly once");
    }
  } else {
    for (int i = 0; i < static_cast<int>(compiled.size()); i++) {
      reg_order.push_back(i);
    }
  }
  std::stable_sort(reg_order.begin(), reg_order.end(),
                   [&](int a, int b) { return compiled[a].priority < compiled[b].priority; });

  for (int mi : reg_order) {
    out.matchers.push_back(std::move(compiled[mi]));
  }

  return out;
}

}  // namespace mch

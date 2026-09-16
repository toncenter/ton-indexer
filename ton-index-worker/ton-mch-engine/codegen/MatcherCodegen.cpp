#include "MatcherCodegen.h"

#include "ExprCodegen.h"  // cstr

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace mch_codegen {

namespace {

using mch::AnchorKind;
using mch::CompiledMatcher;
using mch::CompiledNode;
using mch::NodeKind;
using mch::RecStrategy;

std::string hex32(std::uint32_t v) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "0x%08xu", v);
  return buf;
}

const char *kind_name(NodeKind k) {
  switch (k) {
    case NodeKind::Contract: return "NodeKind::Contract";
    case NodeKind::BlockType: return "NodeKind::BlockType";
    case NodeKind::Pred: return "NodeKind::Pred";
    case NodeKind::Any: return "NodeKind::Any";
    case NodeKind::Or: return "NodeKind::Or";
    case NodeKind::Recursive: return "NodeKind::Recursive";
  }
  return "NodeKind::Any";
}

const char *anchor_name(AnchorKind k) {
  switch (k) {
    case AnchorKind::OpcodeSet: return "AnchorKind::OpcodeSet";
    case AnchorKind::BType: return "AnchorKind::BType";
    case AnchorKind::Pred: return "AnchorKind::Pred";
    case AnchorKind::Mixed: return "AnchorKind::Mixed";
  }
  return "AnchorKind::OpcodeSet";
}

// Braced initializer over an ordered range, rendered by `render`. Unordered
// containers are SORTED by the callers below so the output is byte-stable.
template <typename It, typename F>
std::string braced(It first, It last, F render) {
  std::vector<std::string> parts;
  for (It it = first; it != last; ++it) {
    parts.push_back(render(*it));
  }
  return "{" + join(parts, ", ") + "}";
}

std::string str_list(const std::vector<std::string> &v) {
  return braced(v.begin(), v.end(), [](const std::string &s) { return cstr(s); });
}

std::string str_set(const std::set<std::string> &v) {  // std::set: already sorted
  return braced(v.begin(), v.end(), [](const std::string &s) { return cstr(s); });
}

std::string str_set(const std::unordered_set<std::string> &v) {
  std::vector<std::string> sorted(v.begin(), v.end());
  std::sort(sorted.begin(), sorted.end());
  return str_list(sorted);
}

std::string int_list(const std::vector<int> &v) {
  return braced(v.begin(), v.end(), [](int i) { return std::to_string(i); });
}

std::string int_set(const std::set<int> &v) {
  return braced(v.begin(), v.end(), [](int i) { return std::to_string(i); });
}

std::string opcode_set(const std::unordered_set<std::uint32_t> &v) {
  std::vector<std::uint32_t> sorted(v.begin(), v.end());
  std::sort(sorted.begin(), sorted.end());
  return braced(sorted.begin(), sorted.end(), hex32);
}

std::string slot_map(const std::map<int, std::set<int>> &m) {
  return braced(m.begin(), m.end(), [](const std::pair<const int, std::set<int>> &kv) {
    return "{" + std::to_string(kv.first) + ", " + int_set(kv.second) + "}";
  });
}

std::string btype_constant_name(const std::string &btype) {
  std::string out = "k";
  bool capitalize = true;
  for (unsigned char c : btype) {
    if (c == '_') {
      capitalize = true;
      continue;
    }
    out.push_back(capitalize ? static_cast<char>(std::toupper(c)) : static_cast<char>(c));
    capitalize = false;
  }
  return out;
}

// One builder per matcher. Only fields that differ from the struct default
// are assigned; the defaults are the contract (IrTables.h).
std::string emit_matcher(const CompiledMatcher &m, std::size_t idx) {
  std::vector<std::string> out;
  auto line = [&out](const std::string &s) { out.push_back("  " + s); };
  auto set_if = [&line](bool cond, const std::string &lhs, const std::string &rhs) {
    if (cond) line("m." + lhs + " = " + rhs + ";");
  };

  out.push_back("CompiledMatcher mk_" + std::to_string(idx) + "() {  // " + m.name);
  line("CompiledMatcher m;");
  line("m.name = " + cstr(m.name) + ";");
  line("m.artifact_index = " + std::to_string(m.artifact_index) + ";");
  set_if(m.has_build_program, "has_build_program", "true");
  set_if(!m.produces.empty(), "produces", str_list(m.produces));
  line("m.anchor_kind = " + std::string(anchor_name(m.anchor_kind)) + ";");
  set_if(!m.anchor_opcodes.empty(), "anchor_opcodes", opcode_set(m.anchor_opcodes));
  set_if(!m.anchor_btypes.empty(), "anchor_btypes", str_set(m.anchor_btypes));
  set_if(!m.anchor_pred.empty(), "anchor_pred", cstr(m.anchor_pred));
  for (std::size_t i = 0; i < m.anchor_branches.size(); i++) {
    const auto &br = m.anchor_branches[i];
    const std::string b = "m.anchor_branches[" + std::to_string(i) + "]";
    if (i == 0) {
      line("m.anchor_branches.resize(" + std::to_string(m.anchor_branches.size()) + ");");
    }
    if (br.is_op) {
      line(b + ".is_op = true;");
      line(b + ".opcode = " + std::to_string(br.opcode) + "u;");
    } else {
      line(b + ".btype = " + cstr(br.btype) + ";");
    }
    if (!br.where.empty()) {
      line(b + ".where = " + cstr(br.where) + ";");
    }
  }
  set_if(!m.slot_names.empty(), "slot_names", str_list(m.slot_names));
  set_if(!m.cards.empty(), "cards", str_list(m.cards));

  line("m.nodes.resize(" + std::to_string(m.nodes.size()) + ");");
  for (std::size_t i = 0; i < m.nodes.size(); i++) {
    const CompiledNode &n = m.nodes[i];
    std::vector<std::string> nl;
    auto nset = [&nl](bool cond, const std::string &lhs, const std::string &rhs) {
      if (cond) nl.push_back("    n." + lhs + " = " + rhs + ";");
    };
    nl.push_back("    CompiledNode &n = m.nodes[" + std::to_string(i) + "];");
    nl.push_back("    n.kind = " + std::string(kind_name(n.kind)) + ";");
    nset(true, "global_id", std::to_string(n.global_id));
    nset(n.has_opcode, "opcode", hex32(n.opcode));
    nset(n.has_opcode, "has_opcode", "true");
    nset(!n.btype.empty(), "btype", cstr(n.btype));
    nset(!n.pred_name.empty(), "pred_name", cstr(n.pred_name));
    nset(!n.where_name.empty(), "where_name", cstr(n.where_name));
    nset(n.has_where_expr, "has_where_expr", "true");
    nset(n.slot >= 0, "slot", std::to_string(n.slot));
    nset(n.optional, "optional", "true");
    nset(n.peek, "peek", "true");
    nset(n.child >= 0, "child", std::to_string(n.child));
    nset(!n.children.empty(), "children", int_list(n.children));
    nset(n.parent >= 0, "parent", std::to_string(n.parent));
    nset(!n.branches.empty(), "branches", int_list(n.branches));
    nset(n.step >= 0, "step", std::to_string(n.step));
    nset(n.exit >= 0, "exit", std::to_string(n.exit));
    nset(n.exclusive, "exclusive", "true");
    nset(n.strategy == RecStrategy::Cyclic, "strategy", "RecStrategy::Cyclic");
    line("{");
    out.insert(out.end(), nl.begin(), nl.end());
    line("}");
  }

  set_if(!m.step_slots.empty(), "step_slots", slot_map(m.step_slots));
  set_if(!m.exit_slots.empty(), "exit_slots", slot_map(m.exit_slots));
  set_if(!m.owned_slots.empty(), "owned_slots", int_set(m.owned_slots));
  set_if(!m.include_excess, "include_excess", "false");
  set_if(!m.include_bounces, "include_bounces", "false");
  set_if(!m.ref_preds.empty(), "ref_preds", str_set(m.ref_preds));
  set_if(!m.ref_builders.empty(), "ref_builders", str_set(m.ref_builders));
  set_if(!m.ref_shapers.empty(), "ref_shapers", str_set(m.ref_shapers));
  set_if(!m.ref_msgtypes.empty(), "ref_msgtypes", str_set(m.ref_msgtypes));
  set_if(!m.ref_lookups.empty(), "ref_lookups", str_set(m.ref_lookups));
  set_if(!m.ref_fns.empty(), "ref_fns", str_set(m.ref_fns));
  line("return m;");
  out.push_back("}");
  out.push_back("");
  return join(out, "\n");
}

}  // namespace

std::string generate_matchers_file(const mch::LoadedIr &ir, const std::string &header,
                                   const std::string &suffix) {
  std::vector<std::string> out = {
      header, "#include \"GenMatchers.h\"", "", "namespace mch {", "namespace {", ""};
  for (std::size_t i = 0; i < ir.matchers.size(); i++) {
    out.push_back(emit_matcher(ir.matchers[i], i));
  }
  const std::string table_fn = "gen_matchers_" + suffix;
  out.push_back("}  // namespace");
  out.push_back("");
  out.push_back("const std::vector<CompiledMatcher> &" + table_fn + "() {");
  out.push_back("  static const std::vector<CompiledMatcher> table = [] {");
  out.push_back("    std::vector<CompiledMatcher> t;");
  out.push_back("    t.reserve(" + std::to_string(ir.matchers.size()) + ");");
  for (std::size_t i = 0; i < ir.matchers.size(); i++) {
    out.push_back("    t.push_back(mk_" + std::to_string(i) + "());");
  }
  out.push_back("    return t;");
  out.push_back("  }();");
  out.push_back("  return table;");
  out.push_back("}");
  out.push_back("");
  out.push_back("const char *" + table_fn + "_source_sha() { return " + cstr(ir.source_sha256) +
                "; }");
  out.push_back("");
  out.push_back("}  // namespace mch");
  out.push_back("");
  return join(out, "\n");
}

std::string generate_btypes_header(const mch::LoadedIr &ir) {
  std::set<std::string> btypes;
  for (std::string_view btype : mch::kLeafBtypes) {
    btypes.emplace(btype);
  }
  for (const mch::CompiledMatcher &matcher : ir.matchers) {
    btypes.insert(matcher.produces.begin(), matcher.produces.end());
  }

  std::vector<std::string> out{
      "// Generated by mch-codegen --btypes. One constant per btype the engine can see.",
      "#pragma once",
      "namespace mch::btype {",
  };
  for (const std::string &btype : btypes) {
    out.push_back("inline constexpr char " + btype_constant_name(btype) + "[] = " + cstr(btype) +
                  ";");
  }
  out.push_back("}  // namespace mch::btype");
  out.push_back("");
  return join(out, "\n");
}

}  // namespace mch_codegen

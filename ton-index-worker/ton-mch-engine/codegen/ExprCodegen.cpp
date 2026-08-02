#include "ExprCodegen.h"

#include <cctype>
#include <cstdio>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

namespace mch_codegen {

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

std::string jstr(const Json &e, const char *name) {
  const Json *f = jfield(e, td::Slice(name));
  return f != nullptr && f->type() == JType::String ? f->get_string().str() : std::string{};
}

// The fixed builtin set (rt_call_builtin's arity table). Anything else in a
// `call` node is a host fn (registries.fns) in build programs.
bool is_builtin(const std::string &name) {
  static const std::set<std::string> builtins = {
      "account", "amount", "asset", "ton_asset", "addr_none", "b64",
      "asset_of", "tail_unwrap", "bytes_of", "first", "last", "len",
      "sum", "zip", "map", "concat", "contains"};
  return builtins.count(name) != 0;
}

std::string join(const std::vector<std::string> &parts, const std::string &sep) {
  std::string out;
  for (std::size_t i = 0; i < parts.size(); i++) {
    if (i) out += sep;
    out += parts[i];
  }
  return out;
}

// Ports cpp_expr_emit._Emitter: emits one function body, returning the Value
// temp name for each subexpression. The temp counter is shared across nested
// blocks so names never collide (byte-parity with the Python numbering).
enum class EmitMode { Vectors, Where, Build };

class Emitter {
 public:
  // Modes change only the environment-dependent leaves + the fault return:
  //  - Vectors: name -> rt_name(env,..), lookup -> rt_lookup(lk,..);
  //  - Where: dotfield -> rt_dotfield(w,..); name/lookup fault (Python sync
  //    where evaluator has no env);
  //  - Build: name -> scope map (slot / let-local / well-known), attr ->
  //    rt_access_build (bodies-aware), lookup -> rt_lookup_build, and every
  //    fault returns BuildOutcome::rejected.
  explicit Emitter(EmitMode mode = EmitMode::Vectors) : mode_(mode) {
  }

  std::vector<std::string> lines;
  // Build mode: name -> C++ expression yielding its Value. Seeded with the
  // well-knowns + captures; `let` statements overwrite entries.
  std::map<std::string, std::string> scope;

  int fresh_public() { return fresh(); }
  void line_public(const std::string &s) { line(s); }
  std::string fault_return(const std::string &res) const { return fault_line(res); }
  std::pair<std::string, std::vector<std::string>> emit_block(const Json &node) {
    return block(node);
  }

  std::string emit(const Json &e) {
    const std::string k = jstr(e, "k");
    if (k == "int") {
      int i = fresh();
      line("static const td::RefInt256 lit" + num(i) +
           " = td::dec_string_to_int256(std::string(\"" + jfield(e, "v")->get_number().str() + "\"));");
      line("Value v" + num(i) + " = Value::make_int(lit" + num(i) + ");");
      return "v" + num(i);
    }
    if (k == "str") {
      int i = fresh();
      line("Value v" + num(i) + " = Value::make_str(" + cstr(jfield(e, "v")->get_string().str()) + ");");
      return "v" + num(i);
    }
    if (k == "bool") {
      int i = fresh();
      line("Value v" + num(i) + " = Value::make_bool(" +
           (jfield(e, "v")->get_boolean() ? "true" : "false") + std::string(");"));
      return "v" + num(i);
    }
    if (k == "null") {
      int i = fresh();
      line("Value v" + num(i) + " = Value::null();");
      return "v" + num(i);
    }
    if (k == "name") {
      const std::string id = jstr(e, "id");
      // Comprehension element binders shadow every mode's name resolution: a
      // reference to the bound variable reads the lambda parameter directly.
      if (auto lit = locals_.find(id); lit != locals_.end()) {
        int i = fresh();
        line("const Value &v" + num(i) + " = " + lit->second + ";");
        return "v" + num(i);
      }
      if (mode_ == EmitMode::Where) {
        // No environment exists at test_self time (expr_eval._sync_eval).
        return bind("rt_fault(\"name '" + id + "' is not bound in a `where` clause\")");
      }
      if (mode_ == EmitMode::Build) {
        auto it = scope.find(id);
        if (it == scope.end()) {
          return bind("rt_fault(\"name '" + id + "' is not bound\")");
        }
        int i = fresh();
        // Bind by const reference. A name read (env.slots[k],
        // consumed / a let alias / a lifetime-extended anchor temporary) is only
        // ever read or copied downstream, never moved-from, so this elides a
        // ~120-byte Value copy per reference on the build hot path.
        line("const Value &v" + num(i) + " = " + it->second + ";");
        return "v" + num(i);
      }
      return bind("rt_name(env, " + cstr(id) + ")");
    }
    if (k == "dotfield") {
      if (mode_ == EmitMode::Where) {
        return bind("rt_dotfield(w, " + cstr(jstr(e, "name")) + ")");
      }
      return bind("rt_fault(\"leading-dot field is only valid inside an inline `where (expr)`\")");
    }
    if (k == "attr") {
      std::string of = emit(*jfield(e, "of"));
      if (mode_ == EmitMode::Build) {
        return bind("rt_access_build(env, " + of + ", " + cstr(jstr(e, "name")) + ")");
      }
      return bind("rt_access(" + of + ", " + cstr(jstr(e, "name")) + ")");
    }
    if (k == "call") {
      const std::string fn = jstr(e, "fn");
      std::string argv = emit_args(*jfield(e, "args"));
      // Build mode: a non-builtin call is a host `fn` (registries.fns), emit
      // rt_call_hostfn (env-aware, NULL ARGS PASS THROUGH, unlike rt_call_builtin
      // which is null-strict). Vectors/Where modes only ever see builtins.
      if (mode_ == EmitMode::Build && !is_builtin(fn)) {
        return bind("rt_call_hostfn(env, " + cstr(fn) + ", std::vector<Value>{" + argv + "})");
      }
      // The builtin name and arity are known at emit time, so call the typed
      // core directly without name-keyed dispatch or a per-call heap vector.
      // is_builtin() gates this; the argument temporaries are
      // Value locals passed by const-ref (rt_builtin_* take const Value&).
      return bind("rt_builtin_" + fn + "(" + argv + ")");
    }
    if (k == "lookup") {
      if (mode_ == EmitMode::Where) {
        // Load-time rejected in a where_expr; defensive fault (sync test_self).
        return bind("rt_fault(\"lookup is not evaluable in a `where` clause\")");
      }
      std::string argv = emit_args(*jfield(e, "args"));
      if (mode_ == EmitMode::Build) {
        return bind("rt_lookup_build(env, " + cstr(jstr(e, "name")) + ", std::vector<Value>{" + argv + "})");
      }
      return bind("rt_lookup(lk, " + cstr(jstr(e, "name")) + ", std::vector<Value>{" + argv + "})");
    }
    if (k == "unary") {
      std::string x = emit(*jfield(e, "x"));
      std::string fn = jstr(e, "op") == "not" ? "rt_not" : "rt_neg";
      return bind(fn + "(" + x + ")");
    }
    if (k == "bin") {
      return emit_bin(e);
    }
    if (k == "ternary") {
      return emit_ternary(e);
    }
    if (k == "list") {
      std::vector<std::string> items;
      for (const auto &it : jfield(e, "items")->get_array()) {
        items.push_back(emit(it));
      }
      int i = fresh();
      line("Value v" + num(i) + " = Value::make_list(std::vector<Value>{" + join(items, ", ") + "});");
      return "v" + num(i);
    }
    if (k == "record") {
      std::vector<std::string> pairs;
      for (const auto &f : jfield(e, "fields")->get_array()) {
        std::string fv = emit(*jfield(f, "expr"));
        pairs.push_back("{" + cstr(jstr(f, "name")) + ", " + fv + "}");
      }
      int i = fresh();
      line("Value v" + num(i) + " = Value::make_dict(Value::Fields{" + join(pairs, ", ") + "});");
      return "v" + num(i);
    }
    if (k == "mapc" || k == "quant") {
      return emit_comprehension(e, k == "quant");
    }
    if (k == "parse") {
      // Parse expressions require a build environment and message parsers.
      // Other modes return a defensive fault, as for lookup and comprehension.
      if (mode_ != EmitMode::Build) {
        return bind("rt_fault(\"parse expression is only evaluable in a build program\")");
      }
      std::string x = emit(*jfield(e, "x"));
      std::vector<std::string> types;
      for (const auto &t : jfield(e, "types")->get_array()) {
        types.push_back(cstr(t.get_string().str()));
      }
      return bind("rt_parse_expr(env, " + x + ", std::vector<std::string>{" +
                  join(types, ", ") + "})");
    }
    throw std::runtime_error("unsupported expression kind '" + k + "'");
  }

 private:
  EmitMode mode_ = EmitMode::Vectors;
  int n_ = 0;
  // Comprehension element binders map names to C++ expressions yielding the
  // element Value (the lambda parameter). Checked first in the `name` branch,
  // in every mode. Nested comprehensions are rejected, so one flat map suffices.
  std::map<std::string, std::string> locals_;
  // True while emitting a comprehension body. The body is an EvalResult-
  // returning lambda, so a fault must `return r;` (EvalResult) even in Build
  // mode, not `return BuildOutcome::rejected(...)` (which is the outer fn type).
  bool in_lambda_ = false;

  int fresh() { return n_++; }
  static std::string num(int i) { return std::to_string(i); }
  void line(const std::string &s) { lines.push_back(s); }

  // Early return on a faulted EvalResult temp: expression fns propagate the
  // EvalResult; a build fn's fault is a clean rejection (program.py EvalError).
  // Inside a comprehension body lambda (EvalResult-returning), a Build-mode
  // fault still returns the EvalResult. rt_mapc and rt_quant propagate it.
  std::string fault_line(const std::string &res) const {
    if (mode_ == EmitMode::Build && !in_lambda_) {
      return "if (" + res + ".faulted) return BuildOutcome::rejected(std::move(" + res +
             ".message));";
    }
    return "if (" + res + ".faulted) return " + res + ";";
  }

  // `map|any|all(xs as VAR => body)` emits the list expression, then a
  // lambda binding VAR per element, dispatched through the shared rt_mapc /
  // rt_quant cores shared with the interpreter.
  std::string emit_comprehension(const Json &e, bool is_quant) {
    if (mode_ == EmitMode::Where) {
      // Load/compile-rejected in a where_expr; defensive fault (sync test_self).
      return bind("rt_fault(\"comprehensions are not evaluable in a `where` clause\")");
    }
    std::string xs = emit(*jfield(e, "xs"));
    const std::string var = jstr(e, "var");
    int i = fresh();
    const std::string param = "el" + num(i);
    // Bind VAR to the lambda param for the body only (shadow-safe save/restore).
    const bool had = locals_.count(var) != 0;
    const std::string prev = had ? locals_[var] : std::string();
    locals_[var] = param;
    const bool saved_in_lambda = in_lambda_;
    in_lambda_ = true;
    auto [bval, blines] = block(*jfield(e, "body"));
    in_lambda_ = saved_in_lambda;
    if (had) {
      locals_[var] = prev;
    } else {
      locals_.erase(var);
    }
    line("auto body" + num(i) + " = [&](const Value &" + param + ") -> EvalResult {");
    line("  (void)" + param + ";");
    emit_block_into(blines);
    line("  return rt_ok(" + bval + ");");
    line("};");
    if (is_quant) {
      const std::string is_all = jstr(e, "op") == "all" ? "true" : "false";
      return bind("rt_quant(" + is_all + ", " + xs + ", body" + num(i) + ")");
    }
    return bind("rt_mapc(" + xs + ", body" + num(i) + ")");
  }

  std::string bind(const std::string &rexpr) {
    int i = fresh();
    line("EvalResult r" + num(i) + " = " + rexpr + ";");
    line(fault_line("r" + num(i)));
    line("Value v" + num(i) + " = std::move(r" + num(i) + ".value);");
    return "v" + num(i);
  }

  std::string emit_args(const Json &args) {
    std::vector<std::string> vs;
    for (const auto &a : args.get_array()) {
      vs.push_back(emit(a));
    }
    return join(vs, ", ");
  }

  // Emit `node` into a fresh line buffer (for a nested if/else body), returning
  // (value_temp, lines). The temp counter stays shared.
  std::pair<std::string, std::vector<std::string>> block(const Json &node) {
    std::vector<std::string> saved;
    saved.swap(lines);
    std::string val = emit(node);
    std::vector<std::string> block_lines;
    block_lines.swap(lines);
    lines = std::move(saved);
    return {val, block_lines};
  }

  void emit_block_into(const std::vector<std::string> &block_lines) {
    for (const auto &ln : block_lines) {
      line("  " + ln);
    }
  }

  std::string emit_bin(const Json &e) {
    const std::string op = jstr(e, "op");
    if (op == "and" || op == "or") {
      return emit_logical(e, op);
    }
    if (op == "??") {
      return emit_coalesce(e);
    }
    std::string left = emit(*jfield(e, "l"));
    std::string right = emit(*jfield(e, "r"));
    return bind(strict_fn(op) + "(" + left + ", " + right + ")");
  }

  static std::string strict_fn(const std::string &op) {
    if (op == "==") return "rt_eq";
    if (op == "!=") return "rt_ne";
    if (op == "<") return "rt_lt";
    if (op == "<=") return "rt_le";
    if (op == ">") return "rt_gt";
    if (op == ">=") return "rt_ge";
    if (op == "+") return "rt_add";
    if (op == "-") return "rt_sub";
    if (op == "*") return "rt_mul";
    throw std::runtime_error("unknown binary op '" + op + "'");
  }

  std::string emit_logical(const Json &e, const std::string &op) {
    std::string left = emit(*jfield(e, "l"));
    int i = fresh();
    line("EvalResult rb" + num(i) + " = rt_require_bool(" + left + ");");
    line(fault_line("rb" + num(i)));
    line("Value v" + num(i) + ";");
    auto [right, rblock] = block(*jfield(e, "r"));
    std::string short_val = op == "and" ? "false" : "true";
    std::string cond = op == "and" ? "!rb" + num(i) + ".value.boolean" : "rb" + num(i) + ".value.boolean";
    line("if (" + cond + ") { v" + num(i) + " = Value::make_bool(" + short_val + "); } else {");
    emit_block_into(rblock);
    line("  EvalResult rr" + num(i) + " = rt_require_bool(" + right + ");");
    line("  " + fault_line("rr" + num(i)));
    line("  v" + num(i) + " = Value::make_bool(rr" + num(i) + ".value.boolean);");
    line("}");
    return "v" + num(i);
  }

  std::string emit_coalesce(const Json &e) {
    std::string left = emit(*jfield(e, "l"));
    int i = fresh();
    line("Value v" + num(i) + ";");
    auto [right, rblock] = block(*jfield(e, "r"));
    line("if (!rt_is_null(" + left + ")) { v" + num(i) + " = " + left + "; } else {");
    emit_block_into(rblock);
    line("  v" + num(i) + " = " + right + ";");
    line("}");
    return "v" + num(i);
  }

  std::string emit_ternary(const Json &e) {
    std::string cond = emit(*jfield(e, "cond"));
    int i = fresh();
    line("EvalResult rc" + num(i) + " = rt_require_bool(" + cond + ");");
    line(fault_line("rc" + num(i)));
    line("Value v" + num(i) + ";");
    auto [tval, tblock] = block(*jfield(e, "then"));
    auto [eval_, eblock] = block(*jfield(e, "else"));
    line("if (rc" + num(i) + ".value.boolean) {");
    emit_block_into(tblock);
    line("  v" + num(i) + " = " + tval + ";");
    line("} else {");
    emit_block_into(eblock);
    line("  v" + num(i) + " = " + eval_ + ";");
    line("}");
    return "v" + num(i);
  }
};

// Build-statement generator
// Ports program.py BuildProgram.run statement-for-statement into straight-line
// C++ over BuildRuntime/ExprRuntime. One BuildGen per matcher record.
class BuildGen {
 public:
  explicit BuildGen(const Json &matcher) : em_(EmitMode::Build) {
    // Well-known env names first; captures shadow them (engine.py setdefault).
    em_.scope["consumed"] = "env.consumed";
    em_.scope["anchor"] = "Value::make_block(env.anchor)";
    if (const Json *caps = jfield(matcher, "captures"); caps != nullptr && caps->type() == JType::Array) {
      int slot = 0;
      for (const auto &c : caps->get_array()) {
        em_.scope[jstr(c, "name")] = "env.slots[" + std::to_string(slot) + "]";
        slot++;
      }
    }
    program_ = jfield(matcher, "build_program");
  }

  std::string generate(const std::string &fn_name) {
    if (program_ != nullptr && program_->type() == JType::Array) {
      for (const auto &stmt : program_->get_array()) {
        emit_stmt(stmt);
      }
    }
    std::vector<std::string> body;
    for (const auto &ln : em_.lines) {
      body.push_back("  " + ln);
    }
    return "BuildOutcome " + fn_name + "(BuildEnv &env) {\n"
           "  (void)env;\n"
           "  BuildOutcome o = BuildOutcome::accepted();\n" +
           join(body, "\n") + (body.empty() ? "" : "\n") +
           "  return o;\n"
           "}\n";
  }

 private:
  Emitter em_;
  const Json *program_{nullptr};

  template <typename Fn>
  std::vector<std::string> buffered(Fn &&fn) {
    std::vector<std::string> saved;
    saved.swap(em_.lines);
    fn();
    std::vector<std::string> captured;
    captured.swap(em_.lines);
    em_.lines = std::move(saved);
    return captured;
  }

  void indent_into(const std::vector<std::string> &block_lines) {
    for (const auto &ln : block_lines) {
      em_.line_public("  " + ln);
    }
  }

  // Evaluate a bool condition (program.py eval_bool: non-bool -> EvalError ->
  // rejection); returns the EvalResult temp holding the checked bool.
  std::string emit_bool(const Json &expr) {
    std::string val = em_.emit(expr);
    int i = em_.fresh_public();
    em_.line_public("EvalResult sb" + std::to_string(i) + " = rt_require_bool(" + val + ");");
    em_.line_public(em_.fault_return("sb" + std::to_string(i)));
    return "sb" + std::to_string(i);
  }

  // `out` / switch-branch fields -> a Value::Fields temp (program.py
  // _eval_out_fields: an `optional` field whose value is null is omitted).
  std::string emit_fields(const Json &fields) {
    int i = em_.fresh_public();
    std::string fs = "fs" + std::to_string(i);
    em_.line_public("Value::Fields " + fs + ";");
    for (const auto &f : fields.get_array()) {
      std::string val = em_.emit(*jfield(f, "expr"));
      const Json *opt = jfield(f, "optional");
      bool optional = opt != nullptr && opt->type() == JType::Boolean && opt->get_boolean();
      if (optional) {
        em_.line_public("if (!" + val + ".is_null()) " + fs + ".emplace_back(" +
                        cstr(jstr(f, "name")) + ", " + val + ");");
      } else {
        em_.line_public(fs + ".emplace_back(" + cstr(jstr(f, "name")) + ", " + val + ");");
      }
    }
    return fs;
  }

  // `produces switch`: first branch whose `when` is true (else arm has no
  // `when`); LAZY, later branch conditions must not evaluate once one
  // matched; no branch -> fault -> rejection (program.py _eval_switch).
  void emit_switch(const td::JsonArray &branches, std::size_t idx) {
    if (idx == branches.size()) {
      em_.line_public(
          "return BuildOutcome::rejected(\"produces switch: no branch matched and no `else` "
          "arm\");");
      return;
    }
    const Json &b = branches[idx];
    if (jfield(b, "when") == nullptr) {  // else arm: unconditional
      std::string fs = emit_fields(*jfield(b, "fields"));
      em_.line_public("o.btype = " + cstr(jstr(b, "btype")) + ";");
      em_.line_public("o.data = Value::make_dict(std::move(" + fs + "));");
      return;
    }
    std::string cond = emit_bool(*jfield(b, "when"));
    auto then_lines = buffered([&] {
      std::string fs = emit_fields(*jfield(b, "fields"));
      em_.line_public("o.btype = " + cstr(jstr(b, "btype")) + ";");
      em_.line_public("o.data = Value::make_dict(std::move(" + fs + "));");
    });
    auto else_lines = buffered([&] { emit_switch(branches, idx + 1); });
    em_.line_public("if (" + cond + ".value.boolean) {");
    indent_into(then_lines);
    em_.line_public("} else {");
    indent_into(else_lines);
    em_.line_public("}");
  }

  void emit_stmt(const Json &stmt) {
    const std::string s = jstr(stmt, "s");
    if (s == "parse") {
      const std::string target = jstr(stmt, "target");
      auto it = em_.scope.find(target);
      if (it == em_.scope.end()) {
        // program.py _do_parse: env.get(target) is None -> no-op.
        em_.line_public("// parse target '" + target + "' is unbound: no-op");
        return;
      }
      std::vector<std::string> types;
      for (const auto &t : jfield(stmt, "types")->get_array()) {
        types.push_back(cstr(t.get_string().str()));
      }
      int i = em_.fresh_public();
      em_.line_public("Value pt" + std::to_string(i) + " = " + it->second + ";");
      em_.line_public("EvalResult sp" + std::to_string(i) + " = rt_parse(env, pt" +
                      std::to_string(i) + ", std::vector<std::string>{" + join(types, ", ") +
                      "});");
      em_.line_public(em_.fault_return("sp" + std::to_string(i)));
      return;
    }
    if (s == "let") {
      std::string val = em_.emit(*jfield(stmt, "expr"));
      em_.scope[jstr(stmt, "name")] = val;  // later refs copy the local
      return;
    }
    if (s == "reject") {
      std::string cond = emit_bool(*jfield(stmt, "when"));
      em_.line_public("if (" + cond +
                      ".value.boolean) return BuildOutcome::rejected(\"reject when\");");
      return;
    }
    if (s == "failed" || s == "broken") {
      // program.py: `failed = failed or await eval_bool(...)`, the condition
      // is NOT evaluated once the flag is already true (short-circuit).
      const std::string flag = s == "failed" ? "o.failed" : "o.broken";
      auto body = buffered([&] {
        std::string cond = emit_bool(*jfield(stmt, "when"));
        em_.line_public(flag + " = " + cond + ".value.boolean;");
      });
      em_.line_public("if (!" + flag + ") {");
      indent_into(body);
      em_.line_public("}");
      return;
    }
    if (s == "out") {
      std::string fs = emit_fields(*jfield(stmt, "fields"));
      em_.line_public("o.data = Value::make_dict(std::move(" + fs + "));");
      return;
    }
    if (s == "switch") {
      emit_switch(jfield(stmt, "branches")->get_array(), 0);
      return;
    }
    throw std::runtime_error("unsupported build statement '" + s + "'");
  }
};

std::string sanitize_ident(const std::string &s) {
  std::string out;
  for (char c : s) {
    out += (std::isalnum(static_cast<unsigned char>(c)) != 0) ? c : '_';
  }
  return out;
}

}  // namespace

std::string cstr(const std::string &s) {
  std::string out = "\"";
  for (char ch : s) {
    auto uc = static_cast<unsigned char>(ch);
    if (ch == '\\') {
      out += "\\\\";
    } else if (ch == '"') {
      out += "\\\"";
    } else if (ch == '\n') {
      out += "\\n";
    } else if (ch == '\t') {
      out += "\\t";
    } else if (ch == '\r') {
      out += "\\r";
    } else if (uc < 0x20 || uc == 0x7f) {
      // Every other control byte (incl. NUL, a real past bug) as a 3-digit
      // octal escape. Octal is used instead of \x because \x greedily consumes
      // ALL following hex digits: `\x0` before a literal 'a' would parse as
      // \x0a. A 3-digit octal escape reads at most 3 digits, so an adjacent
      // digit can never extend it.
      char buf[5];
      std::snprintf(buf, sizeof(buf), "\\%03o", uc);
      out += buf;
    } else {
      out += ch;
    }
  }
  out += "\"";
  return out;
}

std::string generate_function(const std::string &fn_name, const Json &expr) {
  Emitter em;
  std::string result = em.emit(expr);
  std::vector<std::string> body;
  for (const auto &ln : em.lines) {
    body.push_back("  " + ln);
  }
  return "EvalResult " + fn_name + "(const mch::Env &env, const mch::Lookups &lk) {\n"
         "  (void)env; (void)lk;\n" +
         join(body, "\n") + "\n"
         "  return rt_ok(" + result + ");\n"
         "}\n";
}

std::string generate_where_function(const std::string &fn_name, const Json &expr) {
  Emitter em(EmitMode::Where);
  std::string result = em.emit(expr);
  std::vector<std::string> body;
  for (const auto &ln : em.lines) {
    body.push_back("  " + ln);
  }
  return "EvalResult " + fn_name + "(const mch::WhereEnv &w) {\n"
         "  (void)w;\n" +
         join(body, "\n") + "\n"
         "  return rt_ok(" + result + ");\n"
         "}\n";
}

std::string generate_vectors_file(const Json &root, const std::string &header) {
  const Json *vectors = jfield(root, "vectors");
  if (vectors == nullptr || vectors->type() != JType::Array) {
    throw std::runtime_error("vectors file has no `vectors` array");
  }
  const auto &arr = vectors->get_array();

  std::vector<std::string> out = {
      header, "#include \"GenVectors.h\"", "", "namespace mch {", "namespace {", ""};
  for (std::size_t i = 0; i < arr.size(); i++) {
    out.push_back(generate_function("vec_" + std::to_string(i), *jfield(arr[i], "expr")));
  }
  out.push_back("}  // namespace");
  out.push_back("");
  out.push_back("const std::vector<GenVec> &gen_vectors() {");
  out.push_back("  static const std::vector<GenVec> table = {");
  for (std::size_t i = 0; i < arr.size(); i++) {
    out.push_back("      {" + cstr(jstr(arr[i], "name")) + ", &vec_" + std::to_string(i) + "},");
  }
  out.push_back("  };");
  out.push_back("  return table;");
  out.push_back("}");
  out.push_back("");
  out.push_back("}  // namespace mch");
  out.push_back("");
  return join(out, "\n");
}

std::string generate_wheres_file(const Json &root, const std::string &header,
                                 const std::string &source_sha, const std::string &suffix) {
  const Json *nodes = jfield(root, td::Slice("nodes"));
  if (nodes == nullptr || nodes->type() != JType::Array) {
    throw std::runtime_error("IR artifact has no `nodes` array");
  }
  const auto &arr = nodes->get_array();

  std::vector<std::size_t> where_ids;
  for (std::size_t i = 0; i < arr.size(); i++) {
    if (jfield(arr[i], td::Slice("where_expr")) != nullptr) {
      where_ids.push_back(i);
    }
  }

  std::vector<std::string> out = {
      header, "#include \"GenWheres.h\"", "", "namespace mch {", "namespace {", ""};
  for (std::size_t id : where_ids) {
    out.push_back(generate_where_function("where_" + std::to_string(id),
                                          *jfield(arr[id], td::Slice("where_expr"))));
  }
  const std::string table_fn = "gen_wheres_" + sanitize_ident(suffix);
  out.push_back("}  // namespace");
  out.push_back("");
  out.push_back("const std::vector<GenWhere> &" + table_fn + "() {");
  out.push_back("  static const std::vector<GenWhere> table = {");
  for (std::size_t id : where_ids) {
    out.push_back("      {" + std::to_string(id) + ", &where_" + std::to_string(id) + "},");
  }
  out.push_back("  };");
  out.push_back("  return table;");
  out.push_back("}");
  out.push_back("");
  out.push_back("const char *" + table_fn + "_source_sha() { return " + cstr(source_sha) + "; }");
  out.push_back("");
  out.push_back("}  // namespace mch");
  out.push_back("");
  return join(out, "\n");
}

std::string generate_builds_file(const Json &root, const std::string &header,
                                 const std::string &source_sha, const std::string &suffix) {
  // Two accepted roots: an IR artifact ({matchers:[...]}, fn per matcher with
  // a build_program, id = matcher index) or a build-vectors document
  // ({build_vectors:[...]}, fn per case, id = case index).
  struct Entry {
    std::size_t id;
    std::string name;
    const Json *matcher;
  };
  std::vector<Entry> entries;
  // TODO: remove (harness): the `build_vectors` root is the TEST-ONLY document
  // (ir/build_vectors.json -> mch-fixtures via fixtures/GenBuildsVectors.h). The
  // production root is the `matchers` branch below. The dual-root shape is what
  // forces --suffix to exist at all.
  if (const Json *cases = jfield(root, "build_vectors");
      cases != nullptr && cases->type() == JType::Array) {
    const auto &arr = cases->get_array();
    for (std::size_t i = 0; i < arr.size(); i++) {
      const Json *m = jfield(arr[i], "matcher");
      if (m == nullptr) {
        throw std::runtime_error("build vector " + std::to_string(i) + " has no `matcher`");
      }
      entries.push_back({i, jstr(arr[i], "name"), m});
    }
  } else if (const Json *matchers = jfield(root, "matchers");
             matchers != nullptr && matchers->type() == JType::Array) {
    const auto &arr = matchers->get_array();
    for (std::size_t i = 0; i < arr.size(); i++) {
      if (jfield(arr[i], "build_program") != nullptr) {
        entries.push_back({i, jstr(arr[i], "name"), &arr[i]});
      }
    }
  } else {
    throw std::runtime_error("input has neither `build_vectors` nor `matchers`");
  }

  std::vector<std::string> out = {
      header, "#include \"GenBuilds.h\"", "", "namespace mch {", "namespace {", ""};
  for (const Entry &e : entries) {
    BuildGen gen(*e.matcher);
    out.push_back(gen.generate("build_" + std::to_string(e.id) + "_" + sanitize_ident(e.name)));
  }
  const std::string table_fn = "gen_builds_" + sanitize_ident(suffix);
  out.push_back("}  // namespace");
  out.push_back("");
  out.push_back("const std::vector<GenBuild> &" + table_fn + "() {");
  out.push_back("  static const std::vector<GenBuild> table = {");
  for (const Entry &e : entries) {
    out.push_back("      {" + std::to_string(e.id) + ", " + cstr(e.name) + ", &build_" +
                  std::to_string(e.id) + "_" + sanitize_ident(e.name) + "},");
  }
  out.push_back("  };");
  out.push_back("  return table;");
  out.push_back("}");
  out.push_back("");
  out.push_back("const char *" + table_fn + "_source_sha() { return " + cstr(source_sha) + "; }");
  out.push_back("");
  out.push_back("}  // namespace mch");
  out.push_back("");
  return join(out, "\n");
}

}  // namespace mch_codegen

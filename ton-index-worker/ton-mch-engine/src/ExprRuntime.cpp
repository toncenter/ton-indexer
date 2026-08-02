#include "ExprRuntime.h"

#include "BlockTree.h"

#include "td/utils/base64.h"
#include "td/utils/misc.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

#include <algorithm>
#include <cctype>

namespace mch {

namespace {

// A float-backed Amount (make_amount_float): a render-only representation from
// fixture data (getgems price). The language has no floats, so any USE of one
// as an expression value (comparison, arithmetic, `.value` unwrap) faults.
// Only rendering and structural comparison are allowed.
bool amount_float_backed(const Value &v) {
  return v.t == VType::Amount && v.amount_float;
}

// Python Amount(None): a present Amount object with a null value inside.
// A float-backed Amount also carries a null `num`, but it is NOT Amount(None)
// (it has a float payload). Exclude it so it faults at use instead of
// silently comparing equal to Amount(None).
bool amount_none(const Value &v) {
  return v.t == VType::Amount && v.num.is_null() && !v.amount_float;
}

std::string to_lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

// Integer carrier for Int and Amount; nullptr otherwise.
const td::RefInt256 *num_of(const Value &v) {
  return (v.t == VType::Int || v.t == VType::Amount) ? &v.num : nullptr;
}

bool asset_eq(const Value &l, const Value &r) {
  if (l.is_ton != r.is_ton || l.has_jetton != r.has_jetton) {
    return false;
  }
  return !l.has_jetton || l.str == r.str;
}

// Asset == string: case-insensitive compare of the jetton master's raw string.
// Python Asset.__eq__ evaluates `self.jetton_address.as_str().lower()` FIRST,
// so an asset WITHOUT a jetton master (TON) raises AttributeError -> EvalError
// -> fault; its `is_ton and other == "ton"` clause is unreachable. eq_result
// faults that pair before reaching here.
bool asset_eq_str(const Value &asset, const std::string &s) {
  return asset.has_jetton && to_lower(asset.str) == to_lower(s);
}

bool account_eq(const Value &l, const Value &r) {
  if (l.addr_none || r.addr_none) {
    return l.addr_none && r.addr_none;
  }
  return l.str == r.str;
}

bool account_eq_str(const Value &acc, const std::string &s) {
  if (acc.addr_none) {
    return false;
  }
  auto norm = normalize_raw_address(s);
  return norm.has_value() && acc.str == *norm;
}

// Symmetric container equality for the language `==` (rt_eq); defined below.
// The runtime owns its own container `==` instead of borrowing the
// harness comparator `structural_equal` (an expected-vs-actual vector helper).
// The two are behaviorally equivalent for the value model. This is a
// separation of concerns so `structural_equal` stays harness-only.
bool container_eq(const Value &l, const Value &r);

// Structural `==` on two non-null values (mirrors _eq_result's raw compare).
bool raw_eq(const Value &l, const Value &r) {
  if (amount_none(l) || amount_none(r)) {
    // Amount(None) == Amount(None) (None == None); != anything else.
    return amount_none(l) && amount_none(r);
  }
  if (l.t == VType::Block || r.t == VType::Block) {
    // Python Block.__eq__ is object identity.
    return l.t == VType::Block && r.t == VType::Block && l.block == r.block;
  }
  const td::RefInt256 *ln = num_of(l);
  const td::RefInt256 *rn = num_of(r);
  if (ln != nullptr && rn != nullptr) {
    return cmp(*ln, *rn) == 0;
  }
  if (l.t == VType::Str && r.t == VType::Str) {
    return l.str == r.str;
  }
  if (l.t == VType::Bool && r.t == VType::Bool) {
    return l.boolean == r.boolean;
  }
  if (l.t == VType::Bytes && r.t == VType::Bytes) {
    return l.str == r.str;
  }
  if (l.t == VType::Asset && r.t == VType::Asset) {
    return asset_eq(l, r);
  }
  if (l.t == VType::Asset && r.t == VType::Str) {
    return asset_eq_str(l, r.str);
  }
  if (l.t == VType::Str && r.t == VType::Asset) {
    return asset_eq_str(r, l.str);
  }
  if (l.t == VType::Account && r.t == VType::Account) {
    return account_eq(l, r);
  }
  if (l.t == VType::Account && r.t == VType::Str) {
    return account_eq_str(l, r.str);
  }
  if (l.t == VType::Str && r.t == VType::Account) {
    return account_eq_str(r, l.str);
  }
  return container_eq(l, r);  // lists/dicts/cells and mismatched types
}

// Symmetric `==` on containers, recursing through raw_eq (so nested scalars use
// the language's typed equality). List: same length, element-wise. Dict/Obj:
// same map kind + exact key-set + per-key equal. Cell: root-hash equal. Any
// other/mismatched pairing is unequal. Mirrors Python list/dict `==`.
bool container_eq(const Value &l, const Value &r) {
  if (l.t == VType::List && r.t == VType::List) {
    const auto &xs = *l.items;
    const auto &ys = *r.items;
    if (xs.size() != ys.size()) return false;
    for (std::size_t i = 0; i < xs.size(); i++) {
      if (!raw_eq(xs[i], ys[i])) return false;
    }
    return true;
  }
  if ((l.t == VType::Dict || l.t == VType::Obj) && l.t == r.t) {
    if (!l.fields || !r.fields || l.fields->size() != r.fields->size()) return false;
    for (const auto &kv : *l.fields) {
      const Value *rv = r.field(kv.first);
      if (rv == nullptr || !raw_eq(kv.second, *rv)) return false;
    }
    return true;
  }
  if (l.t == VType::Cell && r.t == VType::Cell) {
    if (l.cell.is_null() || r.cell.is_null()) return l.cell.is_null() && r.cell.is_null();
    return l.cell->get_hash() == r.cell->get_hash();
  }
  return false;  // mismatched types / non-container
}

EvalResult eq_result(const Value &l, const Value &r, bool want_eq) {
  bool eq;
  if (rt_is_null(l) || rt_is_null(r)) {
    eq = rt_is_null(l) && rt_is_null(r);
  } else if (amount_float_backed(l) || amount_float_backed(r)) {
    // The language has no floats. Comparing a float-backed Amount faults and
    // never aliases Amount(None).
    return rt_fault("'==' on a float-backed Amount (the language has no floats)");
  } else {
    // Asset-without-jetton-master vs string: Python Asset.__eq__ raises
    // (None.as_str()) before its is_ton clause -> EvalError -> fault.
    if ((l.t == VType::Asset && !l.has_jetton && r.t == VType::Str) ||
        (r.t == VType::Asset && !r.has_jetton && l.t == VType::Str)) {
      return rt_fault("'==' failed: asset has no jetton master string");
    }
    eq = raw_eq(l, r);
  }
  return rt_ok(Value::make_bool(want_eq ? eq : !eq));
}

// Ordered comparison: null on either side -> false; Int/Int and Str/Str
// orderable, everything else faults. `sense` picks the operator from cmp().
EvalResult ord_result(const Value &l, const Value &r, const char *op) {
  if (rt_is_null(l) || rt_is_null(r)) {
    return rt_ok(Value::make_bool(false));
  }
  int c;
  // Int/Int and Str/Str only, the host (Python) has no Amount ordering
  // (Amount defines __eq__ only), so ordering an Amount is a fault there
  // Non-orderable types fault.
  if (l.t == VType::Int && r.t == VType::Int) {
    c = cmp(l.num, r.num);
  } else if (l.t == VType::Str && r.t == VType::Str) {
    c = l.str.compare(r.str);
  } else {
    return rt_fault("cannot order " + l.describe() + " and " + r.describe());
  }
  bool res = op[0] == '<' ? (op[1] == '=' ? c <= 0 : c < 0) : (op[1] == '=' ? c >= 0 : c > 0);
  return rt_ok(Value::make_bool(res));
}

// Integer arithmetic coercion: Amount/Int -> its value, else fault.
// Amount(None) faults here (the Python reference would raise an uncaught
// TypeError out of matching, a crash; we choose the containable outcome).
bool coerce_int(const Value &v, td::RefInt256 &out, std::string &err) {
  if (amount_float_backed(v)) {
    err = "arithmetic operand is a float-backed Amount (the language has no floats)";
    return false;
  }
  if (amount_none(v)) {
    err = "arithmetic operand is Amount(None)";
    return false;
  }
  if (v.t == VType::Amount || v.t == VType::Int) {
    out = v.num;
    return true;
  }
  err = "arithmetic operand must be an integer or Amount, got " + v.describe();
  return false;
}

EvalResult arith(const Value &l, const Value &r, char op) {
  if (rt_is_null(l) || rt_is_null(r)) {
    return rt_ok(Value::null());
  }
  td::RefInt256 a, b;
  std::string err;
  if (!coerce_int(l, a, err) || !coerce_int(r, b, err)) {
    return rt_fault(err);
  }
  if (op == '+') return rt_ok(Value::make_int(a + b));
  if (op == '-') return rt_ok(Value::make_int(a - b));
  return rt_ok(Value::make_int(a * b));
}

}  // namespace

EvalResult rt_fault(const std::string &msg) { return EvalResult{true, Value::null(), msg}; }

EvalResult rt_name(const Env &env, const std::string &id) {
  auto it = env.find(id);
  if (it == env.end()) {
    return rt_fault("name '" + id + "' is not bound");
  }
  return rt_ok(it->second);
}

namespace {

// The `.msg` message envelope of a Block, as an Obj Value. Mirrors Python
// Block.get_message() (event_nodes[0].message): the field set is the subset of
// the host Message row the C++ trace loader decodes, a field the loader does
// not carry (e.g. fwd_fee) faults here where Python would return it; keep
// where_exprs to the envelope set below. `exit_code` resolves through the
// nested `transaction` object (rt_access Obj branch), like Python's
// message.transaction.compute_exit_code.
Value msg_envelope(const Message *m) {
  auto opt_str = [](const std::optional<std::string> &s) {
    return s ? Value::make_str(*s) : Value::null();
  };
  auto opt_int = [](const std::optional<std::int64_t> &n) {
    return n ? Value::make_int64(*n) : Value::null();
  };
  Value::Fields tx_fields;
  if (m->tx != nullptr) {
    tx_fields = {
        {"hash", Value::make_str(m->tx->hash)},
        {"lt", Value::make_int64(m->tx->lt)},
        {"now", Value::make_int64(m->tx->now)},
        {"account", Value::make_str(m->tx->account)},
        {"aborted", Value::make_bool(m->tx->aborted)},
        {"orig_status", Value::make_str(m->tx->orig_status)},
        {"end_status", Value::make_str(m->tx->end_status)},
        {"compute_exit_code", opt_int(m->tx->compute_exit_code)},
    };
  }
  Value::Fields fs = {
      {"msg_hash", Value::make_str(m->msg_hash)},
      {"tx_hash", Value::make_str(m->tx_hash)},
      {"tx_lt", Value::make_int64(m->tx_lt)},
      {"direction", Value::make_str(m->direction)},
      {"source", opt_str(m->source)},
      {"destination", opt_str(m->destination)},
      {"value", opt_int(m->value)},
      {"opcode", opt_int(m->opcode)},  // SIGNED, as stored (envelope != block data)
      {"created_lt", opt_int(m->created_lt)},
      {"created_at", opt_int(m->created_at)},
      {"bounce", m->bounce ? Value::make_bool(*m->bounce) : Value::null()},
      {"bounced", Value::make_bool(m->bounced)},
      {"transaction", m->tx != nullptr ? Value::make_obj(std::move(tx_fields)) : Value::null()},
  };
  return Value::make_obj(std::move(fs));
}

// Python expr_eval._access, Block branch: only msg/body/data/failed/broken/
// btype are defined; `body` always faults in a where context (no prior parse).
EvalResult block_access(const Block *b, const std::string &field) {
  if (field == "msg") {
    if (b->event_nodes.empty()) {
      return rt_fault("block has no message envelope");
    }
    const Message *m = b->event_nodes.front()->msg;
    if (m == nullptr) {
      return rt_ok(Value::null());  // Python: message is None -> null propagates
    }
    return rt_ok(msg_envelope(m));
  }
  if (field == "body") {
    return rt_fault("`.body` accessed on a capture that was not `parse`d");
  }
  if (field == "data") {
    return rt_ok(b->data);
  }
  if (field == "failed") {
    return rt_ok(Value::make_bool(b->failed));
  }
  if (field == "broken") {
    return rt_ok(Value::make_bool(b->broken));
  }
  if (field == "btype") {
    return rt_ok(Value::make_str(b->btype));
  }
  return rt_fault("unknown block accessor '" + field +
                  "' (expected msg/body/data/failed/broken/btype)");
}

}  // namespace

EvalResult rt_access(const Value &obj, const std::string &field) {
  if (rt_is_null(obj)) {
    return rt_ok(Value::null());  // null propagation
  }
  if (obj.t == VType::Block) {
    return block_access(obj.block, field);
  }
  if (obj.t == VType::Obj) {
    if (field == "exit_code") {
      const Value *tx = obj.field("transaction");
      if (tx == nullptr) {
        return rt_fault("`exit_code` is only valid on a message envelope carrying a transaction");
      }
      if (tx->t == VType::Obj || tx->t == VType::Dict) {
        const Value *ec = tx->field("compute_exit_code");
        return rt_ok(ec != nullptr ? *ec : Value::null());
      }
      return rt_ok(Value::null());
    }
    const Value *fv = obj.field(field);
    if (fv == nullptr) {
      return rt_fault("object has no field '" + field + "'");
    }
    return rt_ok(*fv);
  }
  if (obj.t == VType::Dict) {
    const Value *fv = obj.field(field);
    if (fv == nullptr) {
      return rt_fault("data has no field '" + field + "'");
    }
    return rt_ok(*fv);
  }
  if (obj.t == VType::List) {
    return rt_fault("field '" + field + "' on a list capture requires matcher context");
  }
  if (obj.t == VType::Amount) {
    // Amount.value unwraps the wrapper to its raw integer. The
    // inverse of amount(x). Amount(None) carries a null num -> yields null.
    if (field == "value") {
      if (obj.amount_float) {
        // A float-backed Amount has no integer value to unwrap; the language
        // has no floats, so `.value` faults rather than yielding
        // null (which would alias Amount(None).value).
        return rt_fault("`.value` on a float-backed Amount (the language has no floats)");
      }
      return rt_ok(obj.num.is_null() ? Value::null() : Value::make_int(obj.num));
    }
    return rt_fault("unknown Amount accessor '" + field + "' (expected value)");
  }
  if (field == "exit_code") {
    return rt_fault("`exit_code` is only valid on a message envelope");
  }
  return rt_fault(obj.describe() + " has no attribute '" + field + "'");
}

EvalResult rt_dotfield(const WhereEnv &w, const std::string &name) {
  if (w.block == nullptr) {
    return rt_fault("dotfield without a candidate block");
  }
  const Value &data = w.block->data;
  if (data.is_null()) {
    return rt_ok(Value::null());  // Python: data is None -> the dotfield is null
  }
  if (data.t == VType::Dict) {
    const Value *fv = data.field(name);
    if (fv == nullptr) {
      return rt_fault("block data has no field '" + name + "'");
    }
    return rt_ok(*fv);
  }
  return rt_access(data, name);
}

EvalResult rt_lookup(const Lookups &lk, const std::string &name, const std::vector<Value> &args) {
  for (const auto &a : args) {
    if (rt_is_null(a)) {
      return rt_ok(Value::null());  // null-strict
    }
  }
  auto it = lk.find(name);
  if (it == lk.end()) {
    return rt_fault("lookup kind '" + name + "' is not registered");
  }
  return rt_ok(it->second);
}

EvalResult rt_neg(const Value &x) {
  if (rt_is_null(x)) {
    return rt_ok(Value::null());
  }
  if (x.t == VType::Int) {
    return rt_ok(Value::make_int(-x.num));
  }
  return rt_fault("unary '-' on " + x.describe());
}

EvalResult rt_require_bool(const Value &x) {
  if (x.t != VType::Bool) {
    return rt_fault("condition must be a bool, got " + x.describe());
  }
  return rt_ok(x);
}

EvalResult rt_not(const Value &x) {
  EvalResult b = rt_require_bool(x);
  if (b.faulted) {
    return b;
  }
  return rt_ok(Value::make_bool(!b.value.boolean));
}

EvalResult rt_eq(const Value &l, const Value &r) { return eq_result(l, r, true); }
EvalResult rt_ne(const Value &l, const Value &r) { return eq_result(l, r, false); }
EvalResult rt_lt(const Value &l, const Value &r) { return ord_result(l, r, "<"); }
EvalResult rt_le(const Value &l, const Value &r) { return ord_result(l, r, "<="); }
EvalResult rt_gt(const Value &l, const Value &r) { return ord_result(l, r, ">"); }
EvalResult rt_ge(const Value &l, const Value &r) { return ord_result(l, r, ">="); }
EvalResult rt_add(const Value &l, const Value &r) { return arith(l, r, '+'); }
EvalResult rt_sub(const Value &l, const Value &r) { return arith(l, r, '-'); }
EvalResult rt_mul(const Value &l, const Value &r) { return arith(l, r, '*'); }

// Per-builtin cores (null-strict for 1+ arg builtins). BOTH the interpreter
// dispatch (rt_call_builtin, vector args) and generated code (static direct
// calls go through these typed cores. Generated code
// emits rt_builtin_<name>(args...) directly, skipping the name-keyed dispatch
// and the per-call heap argument vector.
static bool bi_require_list(const Value &v, const std::vector<Value> *&out) {
  if (v.t != VType::List) return false;
  out = v.items.get();
  return true;
}

static bool bi_open_cell(const Value &v, vm::CellSlice &out) {
  if (v.t != VType::Cell || v.cell.is_null()) return false;
  try {
    out = vm::load_cell_slice(v.cell);
  } catch (...) {
    return false;
  }
  return true;
}

EvalResult rt_builtin_account(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  if (x.t == VType::Account) return rt_ok(x);
  if (x.t == VType::Str) {
    if (x.str == "addr_none") return rt_ok(Value::make_account_none());
    auto norm = normalize_raw_address(x.str);
    if (!norm) return rt_fault("account: invalid address " + x.str);
    return rt_ok(Value::make_account_raw(*norm));
  }
  return rt_fault("account: cannot build from " + x.describe());
}

EvalResult rt_builtin_amount(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  if (x.t == VType::Amount || x.t == VType::Int) return rt_ok(Value::make_amount(x.num));
  return rt_fault("amount: cannot build from " + x.describe());
}

EvalResult rt_builtin_asset(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  if (x.t == VType::Str) {
    auto norm = normalize_raw_address(x.str);
    if (!norm) return rt_fault("asset: invalid jetton master " + x.str);
    return rt_ok(Value::make_asset_jetton(*norm));
  }
  if (x.t == VType::Account && !x.addr_none) return rt_ok(Value::make_asset_jetton(x.str));
  return rt_fault("asset: cannot build from " + x.describe());
}

EvalResult rt_builtin_ton_asset() { return rt_ok(Value::make_asset_ton()); }

EvalResult rt_builtin_addr_none() { return rt_ok(Value::make_account_none()); }

EvalResult rt_builtin_b64(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  if (x.t == VType::Bytes) return rt_ok(Value::make_str(td::base64_encode(x.str)));
  if (x.t == VType::Cell) {
    // Use native BOC bytes. The base64 string may differ across writers on
    // multi-ref trees, but every consumer decodes it back to the same cell
    // (root hash equal), and the dumps render such fields by cellhash.
    auto r = td_boc_serialize(x.cell);
    if (r.is_error()) return rt_fault("b64: cell serialization failed");
    return rt_ok(Value::make_str(td::base64_encode(r.move_as_ok())));
  }
  return rt_fault("b64: expects bytes or a cell, got " + x.describe());
}

EvalResult rt_builtin_asset_of(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  if (x.t != VType::Obj) return rt_fault("asset_of: expects an ABI object, got " + x.describe());
  const Value *tag = x.field("$");
  if (tag == nullptr || tag->t != VType::Str) {
    return rt_fault("asset_of: ABI object '$' tag must be a string");
  }

  // Frozen F1 declaration arm names.
  static const std::string ton_tag = "AssetTon";
  static const std::string jetton_tag = "AssetJetton";
  if (tag->str == ton_tag) return rt_ok(Value::make_asset_ton());
  if (tag->str != jetton_tag) return rt_fault("asset_of: unknown ABI arm '" + tag->str + "'");

  const Value *workchain = x.field("workchain");
  const Value *hash = x.field("hash");
  if (workchain == nullptr || workchain->t != VType::Int || workchain->num.is_null()) {
    return rt_fault("asset_of: jetton workchain must be an integer");
  }
  vm::CellSlice hash_slice;
  if (hash == nullptr || !bi_open_cell(*hash, hash_slice)) {
    return rt_fault("asset_of: jetton hash must be a cell");
  }
  if (hash_slice.size() != 256 || hash_slice.size_refs() != 0) {
    return rt_fault("asset_of: jetton hash cell must have exactly 256 bits and no refs");
  }
  std::string hash_raw(32, '\0');
  if (!hash_slice.fetch_bytes(reinterpret_cast<unsigned char *>(hash_raw.data()), 32)) {
    return rt_fault("asset_of: jetton hash fetch failed");
  }
  std::string raw = workchain->num->to_dec_string() + ":" + td::hex_encode(td::Slice(hash_raw));
  auto canonical = normalize_raw_address(raw);
  if (!canonical) return rt_fault("asset_of: invalid jetton address " + raw);
  return rt_ok(Value::make_asset_jetton(*canonical));
}

EvalResult rt_builtin_tail_unwrap(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  if (x.t != VType::Obj) return rt_fault("tail_unwrap: expects an ABI object, got " + x.describe());
  const Value *tag = x.field("$");
  if (tag == nullptr || tag->t != VType::Str) {
    return rt_fault("tail_unwrap: ABI object '$' tag must be a string");
  }

  enum class TailPolicy { Ref, Bits, Refs };
  // Frozen F1 declaration arm names and empty-inline policies.
  static const std::map<std::string, TailPolicy> tail_tags = {
      {"JettonForwardPayloadRef", TailPolicy::Ref},
      {"JettonForwardPayloadInline", TailPolicy::Bits},
      {"PTonForwardPayloadRef", TailPolicy::Ref},
      {"PTonForwardPayloadInline", TailPolicy::Refs},
  };
  auto policy = tail_tags.find(tag->str);
  if (policy == tail_tags.end()) return rt_fault("tail_unwrap: unknown ABI arm '" + tag->str + "'");

  const Value *value = x.field("value");
  if (value == nullptr || value->t != VType::Cell || value->cell.is_null()) {
    return rt_fault("tail_unwrap: ABI arm value must be a cell");
  }
  if (policy->second == TailPolicy::Ref) return rt_ok(*value);

  vm::CellSlice value_slice;
  if (!bi_open_cell(*value, value_slice)) return rt_fault("tail_unwrap: cannot open value cell");
  if (policy->second == TailPolicy::Bits && value_slice.size() == 0) {
    return rt_ok(Value::null());
  }
  if (policy->second == TailPolicy::Refs && value_slice.size_refs() == 0) {
    return rt_ok(Value::null());
  }
  return rt_ok(*value);
}

EvalResult rt_builtin_bytes_of(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  if (x.t == VType::Bytes) return rt_ok(x);
  vm::CellSlice cell_slice;
  if (!bi_open_cell(x, cell_slice)) {
    return rt_fault("bytes_of: expects bytes or a cell, got " + x.describe());
  }
  if (cell_slice.size() % 8 != 0 || cell_slice.size_refs() != 0) {
    return rt_fault("bytes_of: cell must be byte-aligned and have no refs");
  }
  std::string raw(cell_slice.size() / 8, '\0');
  if (!raw.empty() &&
      !cell_slice.fetch_bytes(reinterpret_cast<unsigned char *>(raw.data()),
                              static_cast<int>(raw.size()))) {
    return rt_fault("bytes_of: cell byte fetch failed");
  }
  return rt_ok(Value::make_bytes(std::move(raw)));
}

EvalResult rt_builtin_first(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  const std::vector<Value> *xs = nullptr;
  if (!bi_require_list(x, xs)) return rt_fault("first expects a list, got " + x.describe());
  return rt_ok(xs->empty() ? Value::null() : xs->front());
}

EvalResult rt_builtin_last(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  const std::vector<Value> *xs = nullptr;
  if (!bi_require_list(x, xs)) return rt_fault("last expects a list, got " + x.describe());
  return rt_ok(xs->empty() ? Value::null() : xs->back());
}

EvalResult rt_builtin_len(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  const std::vector<Value> *xs = nullptr;
  if (!bi_require_list(x, xs)) return rt_fault("len expects a list, got " + x.describe());
  return rt_ok(Value::make_int64(static_cast<std::int64_t>(xs->size())));
}

EvalResult rt_builtin_sum(const Value &x) {
  if (rt_is_null(x)) return rt_ok(Value::null());
  const std::vector<Value> *xs = nullptr;
  if (!bi_require_list(x, xs)) return rt_fault("sum expects a list, got " + x.describe());
  td::RefInt256 acc = td::make_refint(0);
  for (const auto &el : *xs) {
    td::RefInt256 v;
    std::string err;
    if (!coerce_int(el, v, err)) {
      return rt_fault("sum: element is not an integer or Amount (" + el.describe() + ")");
    }
    acc += v;
  }
  return rt_ok(Value::make_int(std::move(acc)));
}

EvalResult rt_builtin_zip(const Value &a, const Value &b) {
  if (rt_is_null(a) || rt_is_null(b)) return rt_ok(Value::null());
  const std::vector<Value> *xs = nullptr;
  const std::vector<Value> *ys = nullptr;
  if (!bi_require_list(a, xs) || !bi_require_list(b, ys)) return rt_fault("zip expects two lists");
  std::vector<Value> out;
  std::size_t n = std::min(xs->size(), ys->size());
  out.reserve(n);
  for (std::size_t i = 0; i < n; i++) out.push_back(Value::make_list({(*xs)[i], (*ys)[i]}));
  return rt_ok(Value::make_list(std::move(out)));
}

EvalResult rt_builtin_concat(const Value &a, const Value &b) {
  if (rt_is_null(a) || rt_is_null(b)) return rt_ok(Value::null());
  const std::vector<Value> *xs = nullptr;
  const std::vector<Value> *ys = nullptr;
  if (!bi_require_list(a, xs) || !bi_require_list(b, ys)) {
    return rt_fault("concat expects two lists");
  }
  std::vector<Value> out;
  out.reserve(xs->size() + ys->size());
  out.insert(out.end(), xs->begin(), xs->end());
  out.insert(out.end(), ys->begin(), ys->end());
  return rt_ok(Value::make_list(std::move(out)));
}

// Case-sensitive substring test. Both arguments must be Str; a non-string faults
// rather than answering false. An empty needle is contained in everything (npos-free
// std::string::find semantics, identical to Python's `in`).
EvalResult rt_builtin_contains(const Value &a, const Value &b) {
  if (rt_is_null(a) || rt_is_null(b)) return rt_ok(Value::null());
  if (a.t != VType::Str) {
    return rt_fault("contains expects a string haystack, got " + a.describe());
  }
  if (b.t != VType::Str) {
    return rt_fault("contains expects a string needle, got " + b.describe());
  }
  return rt_ok(Value::make_bool(a.str.find(b.str) != std::string::npos));
}

EvalResult rt_builtin_map(const Value &a, const Value &b) {
  if (rt_is_null(a) || rt_is_null(b)) return rt_ok(Value::null());
  const std::vector<Value> *xs = nullptr;
  if (!bi_require_list(a, xs)) return rt_fault("map expects a list, got " + a.describe());
  if (b.t != VType::Str) return rt_fault("map field must be a string, got " + b.describe());
  const std::string &f = b.str;
  std::vector<Value> out;
  out.reserve(xs->size());
  for (const auto &x : *xs) {
    if (rt_is_null(x)) {
      out.push_back(Value::null());
      continue;
    }
    if (x.t == VType::Dict || x.t == VType::Obj) {
      const Value *fv = x.field(f);
      if (fv == nullptr) return rt_fault("map: element has no field '" + f + "'");
      out.push_back(*fv);
    } else {
      return rt_fault("map: " + x.describe() + " has no field '" + f + "'");
    }
  }
  return rt_ok(Value::make_list(std::move(out)));
}

EvalResult rt_mapc(const Value &xs, const ElementFn &body) {
  if (rt_is_null(xs)) return rt_ok(Value::null());
  if (xs.t != VType::List) return rt_fault("map comprehension expects a list, got " + xs.describe());
  std::vector<Value> out;
  out.reserve(xs.items->size());
  for (const auto &el : *xs.items) {
    EvalResult r = body(el);
    if (r.faulted) return r;
    out.push_back(std::move(r.value));
  }
  return rt_ok(Value::make_list(std::move(out)));
}

EvalResult rt_quant(bool is_all, const Value &xs, const ElementFn &body) {
  if (rt_is_null(xs)) return rt_ok(Value::null());
  if (xs.t != VType::List) {
    return rt_fault(std::string(is_all ? "all" : "any") + " comprehension expects a list, got " +
                    xs.describe());
  }
  bool result = is_all;  // all of empty = true, any of empty = false
  for (const auto &el : *xs.items) {
    EvalResult r = body(el);
    if (r.faulted) return r;
    EvalResult b = rt_require_bool(r.value);
    if (b.faulted) return b;
    if (!is_all && b.value.boolean) {
      result = true;
      break;
    }
    if (is_all && !b.value.boolean) {
      result = false;
      break;
    }
  }
  return rt_ok(Value::make_bool(result));
}

EvalResult rt_call_builtin(const std::string &name, const std::vector<Value> &args) {
  static const std::map<std::string, int> arity = {
      {"account", 1}, {"amount", 1},  {"asset", 1}, {"ton_asset", 0},
      {"addr_none", 0}, {"b64", 1},   {"first", 1}, {"last", 1},
      {"asset_of", 1}, {"tail_unwrap", 1}, {"bytes_of", 1},
      {"len", 1},     {"sum", 1},     {"zip", 2},   {"map", 2},
      {"concat", 2}, {"contains", 2},
  };
  auto ar = arity.find(name);
  if (ar == arity.end()) {
    return rt_fault("unknown builtin or host fn '" + name + "'");
  }
  if (static_cast<int>(args.size()) != ar->second) {
    return rt_fault("builtin " + name + " arity mismatch");
  }
  // Dispatch to the shared per-builtin cores (null-strict handled inside each).
  if (name == "account") return rt_builtin_account(args[0]);
  if (name == "amount") return rt_builtin_amount(args[0]);
  if (name == "asset") return rt_builtin_asset(args[0]);
  if (name == "ton_asset") return rt_builtin_ton_asset();
  if (name == "addr_none") return rt_builtin_addr_none();
  if (name == "b64") return rt_builtin_b64(args[0]);
  if (name == "asset_of") return rt_builtin_asset_of(args[0]);
  if (name == "tail_unwrap") return rt_builtin_tail_unwrap(args[0]);
  if (name == "bytes_of") return rt_builtin_bytes_of(args[0]);
  if (name == "first") return rt_builtin_first(args[0]);
  if (name == "last") return rt_builtin_last(args[0]);
  if (name == "len") return rt_builtin_len(args[0]);
  if (name == "sum") return rt_builtin_sum(args[0]);
  if (name == "zip") return rt_builtin_zip(args[0], args[1]);
  if (name == "map") return rt_builtin_map(args[0], args[1]);
  if (name == "concat") return rt_builtin_concat(args[0], args[1]);
  if (name == "contains") return rt_builtin_contains(args[0], args[1]);
  return rt_fault("unreachable builtin " + name);
}

}  // namespace mch

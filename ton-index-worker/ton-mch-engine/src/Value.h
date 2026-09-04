// Runtime value model shared by expression evaluation, builds, host calls, and
// action serialization.
#pragma once

#include "common/refint.h"
#include "vm/cells/Cell.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace mch {

struct Block;  // BlockTree.h; a Value can reference a matched block (VType::Block)

enum class VType {
  Null,
  Bool,
  Int,      // td::RefInt256
  Str,      // utf-8 text
  Bytes,    // raw bytes (b64 input)
  Amount,   // td::RefInt256 nanoton-style amount; null num == Amount-none
  Account,  // AccountId (raw addr or addr_none)
  Asset,    // TON | jetton-master
  Cell,     // td::Ref<vm::Cell>
  List,
  Dict,     // pure key lookup
  Obj,      // attribute access + .exit_code
  Block,    // reference to a matched Block (msg/data/failed/broken/btype accessors)
};

struct Value {
  using Fields = std::vector<std::pair<std::string, Value>>;

  VType t{VType::Null};
  bool boolean{false};
  td::RefInt256 num;          // Int, Amount
  std::string str;            // Str (text), Bytes (raw), Account/Asset canonical addr
  bool addr_none{false};      // Account: addr_none marker
  bool is_ton{false};         // Asset: TON asset
  bool has_jetton{false};     // Asset: jetton master present (str holds it)
  td::Ref<vm::Cell> cell;     // Cell
  std::shared_ptr<std::vector<Value>> items;  // List
  std::shared_ptr<Fields> fields;             // Dict / Obj
  const Block *block{nullptr};                // Block (non-owning; arena outlives values)

  static Value null() { return Value{}; }
  static Value make_bool(bool b);
  static Value make_int(td::RefInt256 v);
  static Value make_int64(std::int64_t v);
  static Value make_str(std::string s);
  static Value make_bytes(std::string raw);
  static Value make_amount(td::RefInt256 v);
  static Value make_amount_none();  // present Amount with null num; == only itself
  static Value make_block(const Block *b);
  static Value make_account_raw(std::string raw);  // stores canonical or malformed-as-given
  static Value make_account_none();
  static Value make_asset_ton();
  static Value make_asset_jetton(std::string raw_master);
  static Value make_cell(td::Ref<vm::Cell> c);
  static Value make_list(std::vector<Value> xs);
  static Value make_dict(Fields fs);
  static Value make_obj(Fields fs);

  bool is_null() const { return t == VType::Null; }

  // Find a field in a Dict/Obj; nullptr when absent.
  const Value *field(const std::string &name) const;

  // Diagnostic rendering (for vector FAIL lines).
  std::string describe() const;
};

// Serialize cell trees as bare BOCs for rendering or CRC32C BOCs for parser fields.
td::Result<std::string> td_boc_serialize(const td::Ref<vm::Cell> &root);
td::Result<std::string> td_boc_serialize_crc(const td::Ref<vm::Cell> &root);

// Normalize a raw TON address string "wc:hex64" to canonical "wc:HEX64".
// Returns nullopt for anything that is not a well-formed raw address (the
// vectors never use user-friendly base64). "addr_none" is NOT handled here.
std::optional<std::string> normalize_raw_address(const std::string &s);

// Canonical "wc:HEX64" when well-formed; otherwise the original string
// unchanged. This is the shared normalize-or-passthrough policy.
std::string canonicalize_or_passthrough(const std::string &raw);

}  // namespace mch

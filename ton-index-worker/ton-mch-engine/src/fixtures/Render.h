// Test-only dump renderers, parser diagnostics, and vector comparator. Product
// code does not depend on these rendering or structural-comparison helpers.
#pragma once

#include "IrTables.h"
#include "Value.h"

#include <set>
#include <string>

namespace mch {

struct Block;  // BlockTree.h

// `<btype>[#<opcode-hex8>]@<min_lt>` canonical block key.
std::string block_key(const Block *b);

// Canonical outcome-value rendering (see BuildSmoke.h contract).
std::string render_value(const Value &v);

// Canonical message-parse rendering (byte-for-byte twin of py_msg_dump.py).
// Unlike render_value(), objects have no `obj` prefix and integers/amounts
// share their bare decimal spelling. Message-vector exact-value fixtures use
// this same renderer so parse-dump and vector comparison cannot drift.
std::string render_parse_value(const Value &v);
std::string render_parse_fields_sorted(const Value::Fields &fields);

// A produced or parsed field whose value is a
// Cell-derived BOC container (a b64 string from the b64() builtin, or raw BOC
// Bytes from MsgParse) is rendered in the dumps as the cell's ROOT HASH
// ("cellhash:HEX-UPPER") instead of its exact bytes, so serialization-order /
// CRC provenance is invisible to the comparison.
// `is_boc_field` gates the render by field name (comment / bitcoin_txid / pubkey
// are raw non-BOC bytes and stay as-is). `boc_field_cellhash` returns the
// cellhash string, or "" if the value is not a decodable BOC (caller falls back
// to the normal render).
bool is_boc_field(const std::string &key);
// The BOC-field name set backing is_boc_field (surfaced by --surface for the twins).
const std::set<std::string> &boc_field_names();
std::string boc_field_cellhash(const Value &v);

// Structural equality used by the conformance-vector comparator (mirrors the
// Python runner's _equal: type-checked + deep). NOT the `==` operator.
bool structural_equal(const Value &a, const Value &b);

// One-shot startup diagnostic for every message type any matcher's
// build_program references via `parse` that is ABSENT from message_parsers().
// Such a gap silently forces a `types:` build skip; surfacing it on stderr
// makes a missing-parser artifact loud. Stderr only; it never touches the
// stdout dump the A/B gate compares. Production surfaces the same gap through
// prepare_classify's skip table, without writing to stderr from a library.
void warn_missing_artifact_parsers(const std::vector<CompiledMatcher> &matchers);

}  // namespace mch

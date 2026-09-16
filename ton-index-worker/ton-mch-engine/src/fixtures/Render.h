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

// Canonical outcome-value rendering (see BuildSmoke.h contract). Action rows
// may opt into omitting null-valued map keys; the default preserves dump bytes.
std::string render_value(const Value &v, bool omit_null_fields = false);

// YAML spelling of a string: plain when no YAML reader could mistake it for
// syntax or another type, double-quoted (escaped) otherwise.
std::string yaml_str(const std::string &s);

// `key: <value>` in YAML block style at `indent` spaces, appended to `out`.
// Dict keys sorted, nulls explicit, lists one item per line, BOC fields as
// their cell hash. The actions golden is written through this.
void render_yaml_field(const std::string &key, const Value &v, int indent, std::string &out);

// Canonical message-parse rendering. Unlike render_value(), objects have no
// `obj` prefix and integers/amounts share their bare decimal spelling. Parse-dump
// and vector comparison share this renderer so they cannot drift.
std::string render_parse_value(const Value &v);

// `is_boc_field` gates BOC-as-cellhash render by field name (comment /
// bitcoin_txid / pubkey are raw non-BOC bytes and stay as-is).
bool is_boc_field(const std::string &key);
// The BOC-field name set backing is_boc_field (surfaced by --surface).
const std::set<std::string> &boc_field_names();

// Structural equality for the conformance-vector comparator: type-checked
// and deep. Not the `==` operator. Intentional.
bool structural_equal(const Value &a, const Value &b);

// One-shot stderr diagnostic for parse types a matcher references that are
// absent from message_parsers() (those silently force a `types:` skip).
// Never writes to the stdout dump. Production uses prepare_classify's skip table.
void warn_missing_artifact_parsers(const std::vector<CompiledMatcher> &matchers);

}  // namespace mch

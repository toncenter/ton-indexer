// Message-parsing registry for hand-written adapters and generated ABI rows.
#pragma once

#include "Value.h"

#include "td/utils/Status.h"
#include "vm/cells/Cell.h"
#include "vm/cellslice.h"

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mch {

using MsgParserFn = td::Result<Value> (*)(const td::Ref<vm::Cell> &body);

// Message-type registry: IR `parse.types[]` name -> parser. Names are the
// Python class names (Registries.message_types keys), plus the ABI-bridge rows
// under bare declaration names. First-source-wins on a key collision, except that a
// key claimed by more than one source maps to a nullptr parser (ambiguous, see
// validate_registries): the name stays registered, calling it is what fails.
// Callers that only test membership are unaffected; anyone dereferencing a
// mapped value directly instead of via parse_message_body must null-check.
const std::map<std::string, MsgParserFn> &message_parsers();

// Startup consistency check: succeeds unless a key is
// registered by more than one source (kept-hand / ABI bridge), in
// which case an error listing the ambiguous names. Called from startup paths;
// NOT a gate on message_parsers() access (the Meyer's-static map cannot itself
// surface an error, and the no-abort rule forbids failing inside it).
td::Status validate_registries();

// The parser-row sources to merge, in first-wins order (non-owning; the rows
// are Meyer's statics that outlive any scan).
using ParserSources = std::vector<const std::vector<std::pair<std::string, MsgParserFn>> *>;

// Keys appearing in more than one source, first-wins scan order. Exposed for the
// bridge self-test to exercise the duplicate policy without a
// production collision.
// TODO: remove (harness): only the PUBLIC EXPORT is harness-driven, the sole
// external caller is AbiBridgeTest.cpp:203 (synthetic colliding sources). The
// real production use is internal (MsgParse.cpp:86, from built_registry()), so
// when the self-test retires this declaration and `ParserSources` above drop
// out of the header and the function moves into MsgParse.cpp's anon namespace.
std::vector<std::string> duplicate_parser_keys(const ParserSources &sources);

// Parse `body` as the named registered type; error when the name is unknown
// or the parse fails (caller implements soft-parse alternatives on top).
td::Result<Value> parse_message_body(const std::string &type_name, const td::Ref<vm::Cell> &body);

// pytoniq Slice.load_coins() (len nibble 0 -> 0), exported for host predicates
// that parse variable-length headers (HostRegistry, e.g. EVAA).
td::Result<td::RefInt256> pyslice_load_coins(vm::CellSlice &cs);

// blocks/swaps.py _parse_dedust_steps: walk the SwapStep chain from a slice
// positioned at the first step, returning the pool addresses in walk order.
// Exported for the dedust_swap host fn (HostRegistry).
td::Result<std::vector<std::string>> parse_dedust_steps(vm::CellSlice cs);

// TonTransferBlock.comment derivation (basic_blocks.py + common.py
// TonTransferMessage): nullopt == comment None; encrypted comments come back
// base64-encoded; plain comments are utf-8 decoded with backslashreplace and
// U+0000 stripped, exactly the string Python compares. Parse trouble that
// Python's inner try swallows yields nullopt.
std::optional<std::string> ton_transfer_comment(const td::Ref<vm::Cell> &body);

// Decode raw comment bytes with Python's bytes.decode("utf-8",
// errors="backslashreplace").replace("\u0000", ""). This is the codec the
// serializer applies to jetton_transfer/nft `comment` payload bytes.
std::string decode_comment_bytes(const std::string &raw);

// The parse-dump harness is declared in ParseDump.h.

}  // namespace mch

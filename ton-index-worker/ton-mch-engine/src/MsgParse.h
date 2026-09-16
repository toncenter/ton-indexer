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
// registered type keys plus ABI-bridge rows under bare declaration names.
// First-source-wins on a key collision, except that a key claimed by more
// than one source maps to a nullptr parser (ambiguous, see
// validate_registries): the name stays registered, calling it is what fails.
// Anyone dereferencing a mapped value directly instead of via
// parse_message_body must null-check.
const std::map<std::string, MsgParserFn> &message_parsers();

// Startup consistency check: succeeds unless a key is
// registered by both handwritten parsers and the ABI bridge, in
// which case an error listing the ambiguous names. Called from startup paths;
// NOT a gate on message_parsers() access (the Meyer's-static map cannot itself
// surface an error, and the no-abort rule forbids failing inside it).
td::Status validate_registries();

// Parse `body` as the named registered type; error when the name is unknown
// or the parse fails (caller implements soft-parse alternatives on top).
td::Result<Value> parse_message_body(const std::string &type_name, const td::Ref<vm::Cell> &body);

// Ton-transfer comment: nullopt == none; encrypted comments come back
// base64-encoded; plain comments are utf-8 decoded with backslashreplace
// and U+0000 stripped. Parse trouble yields nullopt.
std::optional<std::string> ton_transfer_comment(const td::Ref<vm::Cell> &body);

// Decode raw comment bytes with Python's bytes.decode("utf-8",
// errors="backslashreplace").replace("\u0000", ""). This is the codec the
// serializer applies to jetton_transfer/nft `comment` payload bytes.
std::string decode_comment_bytes(const std::string &raw);

// The parse-dump harness is declared in ParseDump.h.

}  // namespace mch

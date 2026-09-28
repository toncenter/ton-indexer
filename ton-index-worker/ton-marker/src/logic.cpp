#include "logic.h"
#include "schemes.h"
#include "special.h"
#include "vm/boc.h"
#include "crypto/tl/tlblib.hpp"
#include "td/utils/misc.h"
#include "td/utils/base64.h"
#include "vm/excno.hpp"
#include <cstddef>
#include <stdexcept>
#include <string_view>

namespace ton_marker {

namespace {

constexpr int kMaxRecursiveDepth = 10;
constexpr std::size_t kMaxRecursiveDecodesPerBody = 256;
constexpr std::size_t kMaxRecursiveDecodesPerBatch = 4096;
constexpr std::size_t kMaxDecodeInputBytesPerBody = 16 * 1024 * 1024;
constexpr std::size_t kMaxDecodeInputBytesPerBatch = 64 * 1024 * 1024;
constexpr std::size_t kMaxDecodedBocBytes = 1024 * 1024;
constexpr std::size_t kMaxBocCells = 4096;
constexpr std::size_t kMaxExpandedBocBits = 2 * 1024 * 1024;
constexpr int kMaxParserRecursiveCalls = 4096;
constexpr std::size_t kMaxDecodedBodyBytes = 4 * 1024 * 1024;
constexpr std::size_t kMaxDecodedBatchBytes = 16 * 1024 * 1024;
constexpr std::string_view kBatchOutputLimitError = "unknown: decoded batch output size limit exceeded";

class DecodeLimitError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

struct RecursiveDecodeBudget {
    std::size_t body_decodes{0};
    std::size_t body_input_bytes{0};
    std::size_t& batch_decodes;
    std::size_t& batch_input_bytes;

    void consume(std::size_t input_bytes) {
        if (body_decodes >= kMaxRecursiveDecodesPerBody || batch_decodes >= kMaxRecursiveDecodesPerBatch) {
            throw DecodeLimitError("recursive decode operation limit exceeded");
        }
        if (input_bytes > kMaxDecodeInputBytesPerBody - body_input_bytes ||
            input_bytes > kMaxDecodeInputBytesPerBatch - batch_input_bytes) {
            throw DecodeLimitError("recursive decode input size limit exceeded");
        }
        ++body_decodes;
        ++batch_decodes;
        body_input_bytes += input_bytes;
        batch_input_bytes += input_bytes;
    }
};

void check_decoded_body_size(std::size_t size) {
    if (size > kMaxDecodedBodyBytes) {
        throw DecodeLimitError("decoded output size limit exceeded");
    }
}

void check_boc_header(const td::Slice& boc) {
    vm::BagOfCells::Info info;
    const auto serialized_size = info.parse_serialized_header(boc);
    if (serialized_size > 0 && static_cast<std::size_t>(info.cell_count) > kMaxBocCells) {
        throw DecodeLimitError("boc cell count limit exceeded");
    }
}

void check_boc_expansion(const vm::Ref<vm::Cell>& root) {
    std::vector<vm::Ref<vm::Cell>> cells{root};
    std::size_t expanded_cells = 0;
    std::size_t expanded_bits = 0;

    while (!cells.empty()) {
        auto cell = std::move(cells.back());
        cells.pop_back();
        if (++expanded_cells > kMaxBocCells) {
            throw DecodeLimitError("boc cell expansion limit exceeded");
        }

        auto cs = vm::load_cell_slice(cell);
        const auto cell_bits = static_cast<std::size_t>(cs.size());
        if (cell_bits > kMaxExpandedBocBits - expanded_bits) {
            throw DecodeLimitError("boc bit expansion limit exceeded");
        }
        expanded_bits += cell_bits;

        for (unsigned i = 0; i < cs.size_refs(); ++i) {
            cells.push_back(cs.prefetch_ref(i));
        }
    }
}

std::string get_opcode_name(unsigned opcode) {
    const schemes::InternalMsgBody0 parser0;
    const schemes::InternalMsgBody1 parser1;
    const schemes::InternalMsgBody2 parser2;
    const schemes::InternalMsgBody3 parser3;
    const schemes::InternalMsgBody4 parser4;
    const schemes::InternalMsgBody5 parser5;
    const schemes::InternalMsgBody6 parser6;
    const schemes::InternalMsgBody7 parser7;
    const schemes::InternalMsgBody8 parser8;
    const schemes::InternalMsgBody9 parser9;
    const schemes::InternalMsgBody10 parser10;
    const schemes::InternalMsgBody11 parser11;
    const schemes::ExternalMsgBody parser12;
    const schemes::ForwardPayload parser13;

    const auto check_parser = [opcode](const auto& parser) -> std::optional<std::string> {
        for (size_t i = 0; i < sizeof(parser.cons_tag) / sizeof(parser.cons_tag[0]); ++i) {
            if (parser.cons_tag[i] == opcode) {
                return parser.cons_name[i];
            }
        }
        return std::nullopt;
    };

    if (auto name = check_parser(parser0)) return *name;
    if (auto name = check_parser(parser1)) return *name;
    if (auto name = check_parser(parser2)) return *name;
    if (auto name = check_parser(parser3)) return *name;
    if (auto name = check_parser(parser4)) return *name;
    if (auto name = check_parser(parser5)) return *name;
    if (auto name = check_parser(parser6)) return *name;
    if (auto name = check_parser(parser7)) return *name;
    if (auto name = check_parser(parser8)) return *name;
    if (auto name = check_parser(parser9)) return *name;
    if (auto name = check_parser(parser10)) return *name;
    if (auto name = check_parser(parser11)) return *name;
    if (auto name = check_parser(parser12)) return *name;
    if (auto name = check_parser(parser13)) return *name;

    return "unknown";
}

// would be good to load forward and custom_payload also.
// they can't be described in TLB, so they have type Cell
std::string replace_boc_cells_recursive(const std::string& json_str, RecursiveDecodeBudget& budget, int depth = 0) {
    check_decoded_body_size(json_str.size());
    if (depth > kMaxRecursiveDepth) {
        return json_str;
    }
    // find all BOC strings and replace them
    const std::string start_marker = "\"te6c";
    const char end_marker = '"';
    std::string result = json_str;
    std::vector<std::pair<size_t, size_t>> matches; // (position, length)
    
    // find all matches first
    size_t current_pos = 0;
    while ((current_pos = result.find(start_marker, current_pos)) != std::string::npos) {
        size_t value_start_pos = current_pos + 1; // skip opening quote
        size_t end_pos = result.find(end_marker, value_start_pos);
        if (end_pos != std::string::npos) {
            size_t match_length = end_pos - current_pos + 1; // include both quotes
            matches.emplace_back(current_pos, match_length);
            current_pos = end_pos + 1;
        } else {
            break;
        }
    }
    
    // process matches in reverse order to avoid index shifting
    for (auto it = matches.rbegin(); it != matches.rend(); ++it) {
        size_t pos = it->first;
        size_t length = it->second;

        std::string boc_with_quotes = result.substr(pos, length);
        std::string boc_value = boc_with_quotes.substr(1, boc_with_quotes.length() - 2); // remove quotes

        budget.consume(boc_value.size());
        std::string decoded = decode_boc(boc_value);
        check_decoded_body_size(decoded.size());

        if (!decoded.empty() && decoded.find("unknown") != 0) {
            std::string recursive_decoded = replace_boc_cells_recursive(decoded, budget, depth + 1);
            if (recursive_decoded.size() > length &&
                recursive_decoded.size() - length > kMaxDecodedBodyBytes - result.size()) {
                throw DecodeLimitError("decoded output size limit exceeded");
            }
            result.replace(pos, length, recursive_decoded);
        }
    }
    return result;
}

std::string decode_boc_recursive(const std::string& boc_base64, std::size_t& batch_decodes,
                                 std::size_t& batch_input_bytes) {
    try {
        RecursiveDecodeBudget budget{0, 0, batch_decodes, batch_input_bytes};
        budget.consume(boc_base64.size());
        std::string initial_result = decode_boc(boc_base64);
        if (initial_result.empty() || initial_result.find("unknown") == 0) {
            return initial_result;
        }
        check_decoded_body_size(initial_result.size());
        return replace_boc_cells_recursive(initial_result, budget);
    } catch (const std::exception& e) {
        return "unknown: recursive decode error - " + std::string(e.what());
    } catch (...) {
        return "unknown: recursive decode unhandled error";
    }
}
} // namespace

std::string decode_boc(const std::string& boc_input) {
    try {
        if (boc_input.size() > kMaxEncodedBocBytes) {
            return "unknown: encoded boc size limit exceeded";
        }

        td::Result<std::string> decoded_result;

        // check if input is base64 (starts with te6)
        if (boc_input.substr(0, 3) == "te6") {
            decoded_result = td::base64_decode(boc_input);
            if (decoded_result.is_error()) {
                return "unknown: failed to decode base64: " + decoded_result.error().message().str();
            }
        } else {
            // try as hex
            decoded_result = td::hex_decode(td::Slice(boc_input));
            if (decoded_result.is_error()) {
                return "unknown: failed to decode hex: " + decoded_result.error().message().str();
            }
        }

        auto decoded_boc = decoded_result.move_as_ok();
        if (decoded_boc.size() > kMaxDecodedBocBytes) {
            return "unknown: decoded boc size limit exceeded";
        }
        check_boc_header(decoded_boc);

        // deserialize
        auto cell_result = vm::std_boc_deserialize(decoded_boc);
        if (cell_result.is_error()) {
            return "unknown: failed to deserialize boc: " + cell_result.error().message().str();
        }

        auto cell = cell_result.move_as_ok();
        check_boc_expansion(cell);
        auto cs = vm::load_cell_slice(cell);
        if (cs.size() == 0 && cs.size_refs() == 0) {
            return "{\"@type\": \"empty_cell\"}";
        }
        if (cs.size() < 32) {
            return "unknown: boc is too small, size " + std::to_string(cs.size());
        }

        unsigned opcode = cs.prefetch_ulong(32);

        // tlbc doesn't allow more than 64 constructors,
        // so we split InternalMsgBody into 6 types,
        // and try each...
        const schemes::InternalMsgBody0 parser0;
        const schemes::InternalMsgBody1 parser1;
        const schemes::InternalMsgBody2 parser2;
        const schemes::InternalMsgBody3 parser3;
        const schemes::InternalMsgBody4 parser4;
        const schemes::InternalMsgBody5 parser5;
        const schemes::InternalMsgBody6 parser6;
        const schemes::InternalMsgBody7 parser7;
        const schemes::InternalMsgBody8 parser8;
        const schemes::InternalMsgBody9 parser9;
        const schemes::InternalMsgBody10 parser10;
        const schemes::InternalMsgBody11 parser11;
        const schemes::ExternalMsgBody parser12;
        const schemes::ForwardPayload parser13;

        std::string json_output;
        // tlb::PrettyPrinter pp(std::cout, 2);
        tlb::JsonPrinter pp(&json_output);
        pp.set_limit(kMaxParserRecursiveCalls);

        // try special parsers first (w5, highload v3, wallets without opcode)
        if (try_parse_special(get_opcode_name(opcode), cs, pp, json_output, kMaxParserRecursiveCalls)) {
            check_decoded_body_size(json_output.size());
            return json_output;
        }

        // find matching parser by opcode and try to parse
        bool parsed = false;
        const auto check_and_parse = [&](const auto& parser) -> bool {
            for (size_t i = 0; i < sizeof(parser.cons_tag) / sizeof(parser.cons_tag[0]); ++i) {
                if (parser.cons_tag[i] == opcode) {
                    auto cs_copy = cs;
                    if (parser.print_skip(pp, cs_copy)) return true;
                    // else std::cout << "    ton-marker: failed to parse " << json_output << "\n";
                    // restore output on failure
                    json_output = "";
                    pp = tlb::JsonPrinter(&json_output); // JsonPrinter has state vars like is_first_field, reset them
                    pp.set_limit(kMaxParserRecursiveCalls);
                }
            }
            return false;
        };
        
        if (check_and_parse(parser0)) parsed = true;
        else if (check_and_parse(parser1)) parsed = true;
        else if (check_and_parse(parser2)) parsed = true;
        else if (check_and_parse(parser3)) parsed = true;
        else if (check_and_parse(parser4)) parsed = true;
        else if (check_and_parse(parser5)) parsed = true;
        else if (check_and_parse(parser6)) parsed = true;
        else if (check_and_parse(parser7)) parsed = true;
        else if (check_and_parse(parser8)) parsed = true;
        else if (check_and_parse(parser9)) parsed = true;
        else if (check_and_parse(parser10)) parsed = true;
        else if (check_and_parse(parser11)) parsed = true;
        else if (check_and_parse(parser12)) parsed = true;
        else if (check_and_parse(parser13)) parsed = true;

        if (!parsed) {
            // std::cout << "    ton-marker: no parser succeeded" << "\n";
            return "unknown: no parser succeeded";
        }
        check_decoded_body_size(json_output.size());
        return json_output;

    } catch (const DecodeLimitError& e) {
        return "unknown: decode limit - " + std::string(e.what());
    } catch (const vm::VmError& e) {
        return "unknown: vm error - " + std::string(e.get_msg());
    } catch (const td::Status& s) {
        return "unknown: status error - " + s.to_string();
    } catch (const std::exception& e) {
        return "unknown: std error - " + std::string(e.what());
    } catch (...) {
        return "unknown: unhandled error";
    }
}

std::string decode_boc_recursive(const std::string& boc_base64) {
    std::size_t batch_decodes = 0;
    std::size_t batch_input_bytes = 0;
    return decode_boc_recursive(boc_base64, batch_decodes, batch_input_bytes);
}

std::string decode_opcode(unsigned int opcode) {
    try {
        std::string name = get_opcode_name(opcode);
        return name;
    } catch (...) {
        return "unknown: unhandled error";
    }
}

BatchResponse process_batch(const BatchRequest& request) {
    if (request.boc_requests.size() > kMaxBocBatchRequests ||
        request.opcode_requests.size() > kMaxOpcodeBatchRequests) {
        throw std::invalid_argument("batch request count limit exceeded");
    }

    std::size_t encoded_boc_bytes = 0;
    for (const auto& req : request.boc_requests) {
        if (req.boc_base64.size() > kMaxEncodedBocBytes ||
            req.boc_base64.size() > kMaxBatchEncodedBocBytes - encoded_boc_bytes) {
            throw std::invalid_argument("batch encoded boc size limit exceeded");
        }
        encoded_boc_bytes += req.boc_base64.size();
    }

    BatchResponse response;
    std::size_t batch_decodes = 0;
    std::size_t batch_input_bytes = 0;
    std::size_t batch_output_bytes = 0;
    bool batch_output_limit_reached = false;

    // process boc requests
    const auto boc_count = request.boc_requests.size();
    response.boc_responses.reserve(boc_count);
    for (std::size_t i = 0; i < boc_count; ++i) {
        const auto& req = request.boc_requests[i];
        DecodeBocResponse resp;
        if (batch_output_limit_reached) {
            resp.json_output = kBatchOutputLimitError;
        } else {
            resp.json_output = decode_boc_recursive(req.boc_base64, batch_decodes, batch_input_bytes);
            const auto responses_left = boc_count - i - 1;
            const auto reserved_error_bytes = responses_left * kBatchOutputLimitError.size();
            if (resp.json_output.size() > kMaxDecodedBatchBytes - batch_output_bytes - reserved_error_bytes) {
                resp.json_output = kBatchOutputLimitError;
                batch_output_limit_reached = true;
            }
        }
        batch_output_bytes += resp.json_output.size();
        response.boc_responses.push_back(std::move(resp));
    }

    // process opcode requests
    const auto opcode_count = request.opcode_requests.size();
    response.opcode_responses.reserve(opcode_count);
    for (std::size_t i = 0; i < opcode_count; ++i) {
        const auto& req = request.opcode_requests[i];
        DecodeOpcodeResponse resp;
        resp.name = decode_opcode(req.opcode);
        response.opcode_responses.push_back(std::move(resp));
    }

    return response;
}

} // namespace ton_marker

#include "logic.h"
#include "schemes.h"
#include "interfaces.h"
#include "special.h"
#include "vm/boc.h"
#include "crypto/tl/tlblib.hpp"
#include "td/utils/base64.h"
#include "vm/excno.hpp"
#include <set>
#include <regex>

namespace ton_marker {

namespace {

std::string get_opcode_name(unsigned opcode) {
    const schemes::InternalMsgBody0 parser0;
    const schemes::InternalMsgBody1 parser1;
    const schemes::InternalMsgBody2 parser2;
    const schemes::InternalMsgBody3 parser3;
    const schemes::InternalMsgBody4 parser4;
    const schemes::InternalMsgBody5 parser5;
    const schemes::InternalMsgBody6 parser6;
    const schemes::InternalMsgBody7 parser7;
    const schemes::ExternalMsgBody parser8;
    const schemes::ForwardPayload parser9;

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

    return "unknown";
}

// would be good to load forward and custom_payload also.
// they can't be described in TLB, so they have type Cell
std::string replace_boc_cells_recursive(const std::string& json_str, int depth = 0) {
    if (depth > 10) {
        return json_str;
    }
    // find all BOC strings and replace them
    std::regex boc_b64_pattern("\"(te6c[^\"]+)\"");
    std::string result = json_str;
    std::vector<std::pair<size_t, size_t>> matches; // (position, length)
    std::sregex_iterator iter(result.begin(), result.end(), boc_b64_pattern);
    std::sregex_iterator end;
    for (; iter != end; ++iter) {
        matches.emplace_back(iter->position(), iter->length());
    }
    // process matches in reverse order to avoid index shifting
    for (auto it = matches.rbegin(); it != matches.rend(); ++it) {
        size_t pos = it->first;
        size_t length = it->second;

        std::string boc_with_quotes = result.substr(pos, length);
        std::string boc_value = boc_with_quotes.substr(1, boc_with_quotes.length() - 2); // remove quotes

        std::string decoded = decode_boc(boc_value);

        if (!decoded.empty() && decoded.find("unknown") != 0) {
            std::string recursive_decoded = replace_boc_cells_recursive(decoded, depth + 1);
            result.replace(pos, length, recursive_decoded);
        }
    }
    return result;
}
} // namespace

std::string decode_boc(const std::string& boc_base64) {
    try {
        // decode base64
        auto boc_data = td::base64_decode(boc_base64);
        if (boc_data.is_error()) {
            return "unknown: failed to decode base64";
        }

        // deserialize
        auto cell_result = vm::std_boc_deserialize(boc_data.move_as_ok());
        if (cell_result.is_error()) {
            return "unknown: failed to deserialize boc";
        }

        auto cell = cell_result.move_as_ok();
        auto cs = vm::load_cell_slice(cell);
        if (cs.size() == 0 && cs.size_refs() == 0) {
            return "{\"empty_cell\": \"\"}";
        }
        if (cs.size() < 32) {
            return "unknown: boc is too small, size=" + std::to_string(cs.size());
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
        const schemes::ExternalMsgBody parser8;
        const schemes::ForwardPayload parser9;

        std::string json_output;
        tlb::JsonPrinter pp(&json_output);

        // helper to try parsing with a specific parser
        const auto try_parse = [&cs, &pp](const schemes::TLB_Complex& parser) -> bool {
            auto cs_copy = cs; // make a copy since parsing modifies the slice
            return parser.print_skip(pp, cs_copy);
        };

        // find matching parser by opcode and try to parse
        bool parsed = false;
        const auto check_and_parse = [&](const auto& parser) -> bool {
            json_output = "";
            pp = tlb::JsonPrinter(&json_output);
            for (size_t i = 0; i < sizeof(parser.cons_tag) / sizeof(parser.cons_tag[0]); ++i) {
                if (parser.cons_tag[i] == opcode) {
                    if (try_parse(parser)) return true;
                    else {
                        std::cout << "ton-marker: some parser matched but failed for OPCODE=" << parser.cons_name[i] 
                        << ", JSON_OUTPUT=" << json_output << std::endl;
                        json_output = "";
                        pp = tlb::JsonPrinter(&json_output);
                        // continue searching, may be another parser for the same opcode
                    }
                }
            }
            json_output = "unknown";
            return false;
        };

        if (try_parse_special(opcode, cs, pp)) {
            return json_output;
        }
        
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

        if (!parsed) {
            return "unknown: no parser succeeded";
        }
        return json_output;

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
    try {
        std::string initial_result = decode_boc(boc_base64);
        if (initial_result.empty() || initial_result.find("unknown") == 0) {
            return initial_result;
        }
        return replace_boc_cells_recursive(initial_result);
    } catch (const std::exception& e) {
        return "unknown: recursive decode error - " + std::string(e.what());
    } catch (...) {
        return "unknown: recursive decode unhandled error";
    }
}

std::string decode_opcode(unsigned int opcode) {
    try {
        std::string name = get_opcode_name(opcode);
        return name;
    } catch (...) {
        return "unknown: unhandled error";
    }
}

std::string detect_interface(const std::vector<unsigned int>& method_ids) {
    try {
        // convert to set for easier lookup
        std::set<unsigned> method_set(method_ids.begin(), method_ids.end());
        
        // find matching interfaces
        std::vector<std::string> matching;
        for (const auto& interface : g_interfaces) {
            if (std::includes(method_set.begin(), method_set.end(),
                            interface.methods.begin(), interface.methods.end())) {
                matching.push_back(interface.name);
            }
        }

        // join with commas
        std::string result;
        for (size_t i = 0; i < matching.size(); ++i) {
            if (i > 0) result += ",";
            result += matching[i];
        }
        return result;
    } catch (const std::exception& e) {
        return "unknown: std error - " + std::string(e.what());
    } catch (...) {
        return "unknown: unhandled error";
    }
}

BatchResponse process_batch(const BatchRequest& request) {
    BatchResponse response;

    // process boc requests
    response.boc_responses.reserve(request.boc_requests.size());
    for (const auto& req : request.boc_requests) {
        DecodeBocResponse resp;
        resp.json_output = decode_boc_recursive(req.boc_base64);
        response.boc_responses.push_back(std::move(resp));
    }

    // process opcode requests
    response.opcode_responses.reserve(request.opcode_requests.size());
    for (const auto& req : request.opcode_requests) {
        DecodeOpcodeResponse resp;
        resp.name = decode_opcode(req.opcode);
        response.opcode_responses.push_back(std::move(resp));
    }

    // process interface requests
    response.interface_responses.reserve(request.interface_requests.size());
    for (const auto& req : request.interface_requests) {
        DetectInterfaceResponse resp;
        resp.interfaces = detect_interface(req.method_ids);
        response.interface_responses.push_back(std::move(resp));
    }

    return response;
}

} // namespace ton_marker

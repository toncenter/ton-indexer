#include "logic.h"
#include "schemes.h"
#include "interfaces.h"
#include "vm/boc.h"
#include "crypto/tl/tlblib.hpp"
#include "td/utils/base64.h"
#include <set>

namespace ton_marker {

namespace {
// helper function to get opcode name
std::string get_opcode_name(unsigned opcode) {
    // try all message body types
    const schemes::InternalMsgBody0 parser0;
    const schemes::InternalMsgBody1 parser1;
    const schemes::InternalMsgBody2 parser2;
    const schemes::InternalMsgBody3 parser3;
    const schemes::InternalMsgBody4 parser4;
    const schemes::InternalMsgBody5 parser5;

    // check each parser's tags
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

    return "unknown";
}
} // namespace

std::optional<std::string> decode_boc(const std::string& boc_base64) {
    try {
        // decode base64
        auto boc_data = td::base64_decode(boc_base64);
        if (boc_data.is_error()) {
            return std::nullopt;
        }
        
        // deserialize
        auto cell_result = vm::std_boc_deserialize(boc_data.move_as_ok());
        if (cell_result.is_error()) {
            return std::nullopt;
        }

        auto cell = cell_result.move_as_ok();
        auto cs = vm::load_cell_slice(cell);
        if (cs.size() < 32) {
            return std::nullopt;
        }

        // get opcode first
        unsigned opcode = cs.prefetch_ulong(32);

        // try all message body types
        const schemes::InternalMsgBody0 parser0;
        const schemes::InternalMsgBody1 parser1;
        const schemes::InternalMsgBody2 parser2;
        const schemes::InternalMsgBody3 parser3;
        const schemes::InternalMsgBody4 parser4;
        const schemes::InternalMsgBody5 parser5;

        // try to parse with each parser
        std::string json_output;
        tlb::JsonPrinter pp(&json_output);

        // helper to try parsing with a specific parser
        const auto try_parse = [&cs, &pp](const auto& parser) -> bool {
            auto cs_copy = cs; // make a copy since parsing modifies the slice
            return parser.print_skip(pp, cs_copy);
        };

        // find matching parser by opcode and try to parse
        bool parsed = false;
        const auto check_and_parse = [&](const auto& parser) -> bool {
            for (size_t i = 0; i < sizeof(parser.cons_tag) / sizeof(parser.cons_tag[0]); ++i) {
                if (parser.cons_tag[i] == opcode) {
                    return try_parse(parser);
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

        if (!parsed) {
            return "unknown";
        }
        return json_output;

    } catch (...) {
        return std::nullopt;
    }
}

std::optional<std::string> decode_opcode(unsigned int opcode) {
    try {
        std::string name = get_opcode_name(opcode);
        return name == "unknown" ? std::nullopt : std::optional<std::string>(name);
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<std::string> detect_interface(const std::vector<unsigned int>& method_ids) {
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
        return result.empty() ? std::nullopt : std::optional<std::string>(result);
    } catch (...) {
        return std::nullopt;
    }
}

BatchResponse process_batch(const BatchRequest& request) {
    BatchResponse response;

    // process boc requests
    response.boc_responses.reserve(request.boc_requests.size());
    for (const auto& req : request.boc_requests) {
        DecodeBocResponse resp;
        resp.json_output = decode_boc(req.boc_base64);
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

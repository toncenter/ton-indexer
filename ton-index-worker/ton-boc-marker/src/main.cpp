#include "schemes.h"
#include <iostream>
#include <string>
#include <vector>
#include <cstdio>
#include "vm/boc.h"
#include "td/utils/misc.h"
#include "crypto/tl/tlblib.hpp"
#include <optional>

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

std::string process_decode_boc(const std::string& hex_boc) {
    std::string boc_data;

    // decode hex
    auto hex_result = td::hex_decode(td::Slice(hex_boc));
    if (hex_result.is_error()) {
        return "Error: Cannot decode input as Hex - " + hex_result.error().message().str();
    }

    boc_data = hex_result.move_as_ok();

    // deserialize
    auto cell_result = vm::std_boc_deserialize(td::Slice(boc_data));
    if (cell_result.is_error()) {
        return "Error: Cannot deserialize BOC - " + cell_result.error().message().str();
    }

    auto cell = cell_result.move_as_ok();

    try {
        auto cs = vm::load_cell_slice(cell);
        if (cs.size() < 32) {
            return "Error: Cell is too short";
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

        tlb::PrettyPrinter pp1(std::cout, 2, 1);  // indent=2, mode=1 for better formatting

        // helper to try parsing with a specific parser
        const auto try_parse = [&cs, &pp1](const auto& parser) -> bool {
            auto cs_copy = cs; // make a copy since parsing modifies the slice
            return parser.print_skip(pp1, cs_copy);
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

    } catch (const std::exception& e) {
        return "Error parsing message body: " + std::string(e.what());
    }
}

std::string process_decode_opcode(const std::string& hex_opcode) {
    if (hex_opcode.size() < 3 || hex_opcode.substr(0, 2) != "0x") {
        return "Error: Invalid opcode format, expected 0x...";
    }

    std::string hex_value = hex_opcode.substr(2);

    // pad with leading zeros if necessary
    while (hex_value.size() < 8) {
        hex_value = "0" + hex_value;
    }

    if (hex_value.size() != 8) {
        return "Error: Opcode must be exactly 8 hex characters (32 bits)";
    }

    try {
        unsigned opcode = std::stoul(hex_value, nullptr, 16);
        return get_opcode_name(opcode);
    } catch (const std::exception& e) {
        return "Error: Cannot decode opcode as Hex - " + std::string(e.what());
    }
}

enum RequestType {
    DECODE_BOC,
    DECODE_OPCODE
};

struct Request {
    RequestType type;
    std::string value;
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " [-o <hex_opcode> | -b <hex_boc>]..." << std::endl;
        std::cerr << "Examples:" << std::endl;
        std::cerr << "  " << argv[0] << " -o 0xf8a7ea5" << std::endl;
        std::cerr << "  " << argv[0] << " -b b5ee9c724101030100b200015f642b7d070000000000000000800015dcfb67ea144d8dad9d9d9d017a297e0b8e9cdb6988e68723b5b39f70a791c409b70101a7178d45190000000000000000204d9800015dcfb67ea144d8dad9d9d9d017a297e0b8e9cdb6988e68723b5b39f70a791d00002bb9f6cfd4289b1b5b3b3b3a02f452fc171d39b6d311cd0e476b673ee14f2388136b02004b12345678800015dcfb67ea144d8" << std::endl;
        std::cerr << "  " << argv[0] << " -o 0xf8a7ea5 -b b5ee9c72... -o 0x595f07bc" << std::endl;
        return 1;
    }

    std::vector<Request> requests;

    // parse arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-o" && i + 1 < argc) {
            requests.push_back({DECODE_OPCODE, argv[i + 1]});
            ++i; // skip next argument as it's the value
        } else if (arg == "-b" && i + 1 < argc) {
            requests.push_back({DECODE_BOC, argv[i + 1]});
            ++i; // skip next argument as it's the value
        } else {
            std::cerr << "Error: Invalid argument '" << arg << "'" << std::endl;
            return 1;
        }
    }

    if (requests.empty()) {
        std::cerr << "Error: No valid requests found" << std::endl;
        return 1;
    }

    // process requests in order
    bool has_errors = false;
    for (const auto& request : requests) {
        std::string result;
        try {
            if (request.type == DECODE_BOC) {
                result = process_decode_boc(request.value);
            } else if (request.type == DECODE_OPCODE) {
                result = process_decode_opcode(request.value);
            }
        } catch (const std::exception& e) {
            result = "Error: Unexpected exception - " + std::string(e.what());
            has_errors = true;
        } catch (...) {
            result = "Error: Unknown exception occurred";
            has_errors = true;
        }

        std::cout << result << std::endl;
    }

    return has_errors ? 1 : 0;
}
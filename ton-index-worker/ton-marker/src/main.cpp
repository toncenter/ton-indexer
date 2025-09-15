#include "logic.h"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include "td/utils/misc.h"
#include "td/utils/base64.h"

// cli request types
enum RequestType {
    DECODE_BOC,
    DECODE_OPCODE,
    DETECT_INTERFACE
};

struct Request {
    RequestType type;
    std::string value;
};

// process cli boc request
std::string process_cli_boc(const std::string& input_boc) {
    // check if input is base64 (starts with te6cckEB)
    if (input_boc.substr(0, 3) == "te6") {
        // already in base64, use directly
        auto result = ton_marker::decode_boc_recursive(input_boc);
        return result;
    }

    // try as hex
    auto hex_result = td::hex_decode(td::Slice(input_boc));
    if (hex_result.is_error()) {
        return "Error: Cannot decode input as Hex - " + hex_result.error().message().str();
    }

    // convert hex to base64
    auto boc_data = hex_result.move_as_ok();
    auto base64 = td::base64_encode(boc_data);
    
    auto result = ton_marker::decode_boc_recursive(base64);
    return result;
}

// process cli opcode request
std::string process_cli_opcode(const std::string& hex_opcode) {
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
        auto result = ton_marker::decode_opcode(opcode);
        return result;
    } catch (const std::exception& e) {
        return "Error: Cannot decode opcode as Hex - " + std::string(e.what());
    }
}

// process cli interface request
std::string process_cli_interface(const std::string& methods_list) {
    std::vector<unsigned int> method_ids;
    std::stringstream ss(methods_list);
    std::string item;
    
    while (std::getline(ss, item, ',')) {
        try {
            method_ids.push_back(std::stoul(item));
        } catch (const std::exception& e) {
            return "Error: Invalid method id - " + std::string(e.what());
        }
    }
    
    auto result = ton_marker::detect_interface(method_ids);
    return result;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " [-o <hex_opcode> | -b <hex_or_base64_boc> | -m <method_ids>]..." << std::endl;
        std::cerr << "Examples:" << std::endl;
        std::cerr << "  " << argv[0] << " -o 0xf8a7ea5" << std::endl;
        std::cerr << "  " << argv[0] << " -b b5ee9c724101030100b200015f642b7d070000000000000000800015dcfb67ea144d8dad9d9d9d017a297e0b8e9cdb6988e68723b5b39f70a791c409b70101a7178d45190000000000000000204d9800015dcfb67ea144d8dad9d9d9d017a297e0b8e9cdb6988e68723b5b39f70a791d00002bb9f6cfd4289b1b5b3b3b3a02f452fc171d39b6d311cd0e476b673ee14f2388136b02004b12345678800015dcfb67ea144d8" << std::endl;
        std::cerr << "  " << argv[0] << " -o 0xf8a7ea5 -b b5ee9c72... -o 0x595f07bc" << std::endl;
        std::cerr << "  " << argv[0] << " -m 0,1,2,81689,83575,84246,111161,112473,113789,130309" << std::endl;
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
        } else if (arg == "-m" && i + 1 < argc) {
            requests.push_back({DETECT_INTERFACE, argv[i + 1]});
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
                result = process_cli_boc(request.value);
            } else if (request.type == DECODE_OPCODE) {
                result = process_cli_opcode(request.value);
            } else if (request.type == DETECT_INTERFACE) {
                result = process_cli_interface(request.value);
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
#include "schemes.h"
#include <iostream>
#include <string>
#include "vm/boc.h"
#include "td/utils/misc.h"
#include "crypto/tl/tlblib.hpp"

int print_message_body(const schemes::InternalMessageBody& parser, td::Ref<vm::Cell> cell) {
    auto debug_cs = vm::load_cell_slice(cell);
    if (debug_cs.size() < 32) {
        std::cerr << "Cell is too short" << std::endl;
        return 1;
    }
    auto cs = vm::load_cell_slice(cell);
    std::string json_output;
    tlb::JsonPrinter pp(&json_output);
    if (!parser.print_skip(pp, cs)) {
        std::cerr << "Failed to parse message body" << std::endl;
        return 1;
    } else {
        std::cout << json_output << std::endl;
        return 0;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <hex_boc>" << std::endl;
        std::cerr << "Example: " << argv[0] << " b5ee9c724101030100b200015f642b7d070000000000000000800015dcfb67ea144d8dad9d9d9d017a297e0b8e9cdb6988e68723b5b39f70a791c409b70101a7178d45190000000000000000204d9800015dcfb67ea144d8dad9d9d9d017a297e0b8e9cdb6988e68723b5b39f70a791d00002bb9f6cfd4289b1b5b3b3b3a02f452fc171d39b6d311cd0e476b673ee14f2388136b02004b12345678800015dcfb67ea144d8" << std::endl;
        return 1;
    }

    std::string input = argv[1];
    std::string boc_data;

    // decode
    auto hex_result = td::hex_decode(td::Slice(input));
    if (hex_result.is_error()) {
        std::cerr << "Error: Cannot decode input as Hex" << std::endl;
        std::cerr << "Hex error: " << hex_result.error().message().str() << std::endl;
        return 1;
    }

    boc_data = hex_result.move_as_ok();

    // deserialize
    auto cell_result = vm::std_boc_deserialize(td::Slice(boc_data));
    if (cell_result.is_error()) {
        std::cerr << "Error: Cannot deserialize BOC: " << cell_result.error().message().str() << std::endl;
        return 1;
    }
    
    auto cell = cell_result.move_as_ok();
    schemes::InternalMessageBody parser;
    try {
        return print_message_body(parser, cell);
    } catch (const std::exception& e) {
        std::cerr << "Error parsing message body: " << e.what() << std::endl;
        return 1;
    }
}
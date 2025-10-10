#pragma once

#include <string>
#include <vector>

namespace ton_marker {

// core types for library
struct DecodeBocRequest {
    std::string boc_base64;
};

struct DecodeBocResponse {
    std::string json_output;
};

struct DecodeOpcodeRequest {
    unsigned int opcode;
};

struct DecodeOpcodeResponse {
    std::string name;
};

// batch request container
struct BatchRequest {
    std::vector<DecodeBocRequest> boc_requests;
    std::vector<DecodeOpcodeRequest> opcode_requests;
};

// batch response container
struct BatchResponse {
    std::vector<DecodeBocResponse> boc_responses;
    std::vector<DecodeOpcodeResponse> opcode_responses;
};

// core functions
std::string decode_boc(const std::string& boc_input);
std::string decode_boc_recursive(const std::string& boc_input);
std::string decode_opcode(unsigned int opcode);

// batch processing function
BatchResponse process_batch(const BatchRequest& request);

} // namespace ton_marker

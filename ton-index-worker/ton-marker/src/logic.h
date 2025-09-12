#pragma once

#include <string>
#include <vector>
#include <optional>

namespace ton_marker {

// core types for library
struct DecodeBocRequest {
    std::string boc_base64;
};

struct DecodeBocResponse {
    std::optional<std::string> json_output;
};

struct DecodeOpcodeRequest {
    unsigned int opcode;
};

struct DecodeOpcodeResponse {
    std::optional<std::string> name;
};

struct DetectInterfaceRequest {
    std::vector<unsigned int> method_ids;
};

struct DetectInterfaceResponse {
    std::optional<std::string> interfaces;
};

// batch request container
struct BatchRequest {
    std::vector<DecodeBocRequest> boc_requests;
    std::vector<DecodeOpcodeRequest> opcode_requests;
    std::vector<DetectInterfaceRequest> interface_requests;
};

// batch response container
struct BatchResponse {
    std::vector<DecodeBocResponse> boc_responses;
    std::vector<DecodeOpcodeResponse> opcode_responses;
    std::vector<DetectInterfaceResponse> interface_responses;
};

// core functions
std::optional<std::string> decode_boc(const std::string& boc_base64);
std::optional<std::string> decode_opcode(unsigned int opcode);
std::optional<std::string> detect_interface(const std::vector<unsigned int>& method_ids);

// batch processing function
BatchResponse process_batch(const BatchRequest& request);

} // namespace ton_marker

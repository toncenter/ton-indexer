#include "wrapper.h"
#include "logic.h"
#include <cstring>
#include <vector>
#include <optional>
#include <string>

namespace {
// helper to convert optional string to C-string (returns nullptr for nullopt)
char* to_c_str(const std::optional<std::string>& opt) {
    if (!opt) return nullptr;
    // allocate memory that will be freed by the caller
    char* result = new char[opt->length() + 1];
    std::strcpy(result, opt->c_str());
    return result;
}

// overload for regular strings (always returns valid pointer)
char* to_c_str(const std::string& str) {
    char* result = new char[str.length() + 1];
    std::strcpy(result, str.c_str());
    return result;
}
} // namespace

extern "C" {

const char* ton_marker_decode_opcode(unsigned int opcode) {
    return to_c_str(ton_marker::decode_opcode(opcode));
}

const char* ton_marker_decode_boc(const char* boc_base64) {
    if (!boc_base64) return nullptr;
    return to_c_str(ton_marker::decode_boc(std::string(boc_base64)));
}

TonMarkerBatchResponse* ton_marker_process_batch(const TonMarkerBatchRequest* request) {
    // prepare C++ request
    ton_marker::BatchRequest cpp_request;
    
    // convert boc requests
    cpp_request.boc_requests.reserve(request->boc_count);
    for (int i = 0; i < request->boc_count; ++i) {
        if (!request->boc_base64_list[i]) continue; // skip null pointers
        ton_marker::DecodeBocRequest req;
        req.boc_base64 = std::string(request->boc_base64_list[i]); // explicit conversion from C-string
        cpp_request.boc_requests.push_back(std::move(req));
    }
    
    // convert opcode requests
    cpp_request.opcode_requests.reserve(request->opcode_count);
    for (int i = 0; i < request->opcode_count; ++i) {
        cpp_request.opcode_requests.push_back({request->opcodes[i]});
    }
    
    // process batch
    auto cpp_response = ton_marker::process_batch(cpp_request);
    
    // prepare C response
    auto* response = new TonMarkerBatchResponse();
    
    // convert boc responses
    response->boc_count = cpp_response.boc_responses.size();
    response->boc_results = new char*[response->boc_count];
    for (int i = 0; i < response->boc_count; ++i) {
        const auto& json_output = cpp_response.boc_responses[i].json_output;
        response->boc_results[i] = to_c_str(json_output);
    }
    
    // convert opcode responses
    response->opcode_count = cpp_response.opcode_responses.size();
    response->opcode_results = new char*[response->opcode_count];
    for (int i = 0; i < response->opcode_count; ++i) {
        const auto& name = cpp_response.opcode_responses[i].name;
        response->opcode_results[i] = to_c_str(name);
    }
    
    return response;
}

void ton_marker_free_batch_response(TonMarkerBatchResponse* response) {
    if (!response) return;
    
    // free all strings
    for (int i = 0; i < response->boc_count; ++i) {
        delete[] response->boc_results[i];
    }
    for (int i = 0; i < response->opcode_count; ++i) {
        delete[] response->opcode_results[i];
    }
    // free arrays
    delete[] response->boc_results;
    delete[] response->opcode_results;
    
    // free response struct
    delete response;
}

void ton_marker_free_string(const char* str) {
    if (str) {
        delete[] str;
    }
}

} // extern "C"

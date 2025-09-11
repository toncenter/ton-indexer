#include "wrapper.h"
#include "logic.h"
#include <cstring>
#include <vector>

namespace {
// helper to convert string to C-string (returns nullptr for nullopt)
char* to_c_str(const std::optional<std::string>& opt) {
    if (!opt) return nullptr;
    // allocate memory that will be freed by the caller
    char* result = new char[opt->length() + 1];
    std::strcpy(result, opt->c_str());
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

const char* ton_marker_detect_interface(const unsigned int* method_ids, int count) {
    std::vector<unsigned int> ids(method_ids, method_ids + count);
    return to_c_str(ton_marker::detect_interface(ids));
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
    
    // convert interface requests
    cpp_request.interface_requests.reserve(request->interface_count);
    for (int i = 0; i < request->interface_count; ++i) {
        ton_marker::DetectInterfaceRequest req;
        req.method_ids.assign(
            request->method_ids[i],
            request->method_ids[i] + request->method_counts[i]
        );
        cpp_request.interface_requests.push_back(std::move(req));
    }
    
    // process batch
    auto cpp_response = ton_marker::process_batch(cpp_request);
    
    // prepare C response
    auto* response = new TonMarkerBatchResponse();
    
    // convert boc responses
    response->boc_count = cpp_response.boc_responses.size();
    response->boc_results = new char*[response->boc_count];
    for (int i = 0; i < response->boc_count; ++i) {
        response->boc_results[i] = to_c_str(cpp_response.boc_responses[i].json_output);
    }
    
    // convert opcode responses
    response->opcode_count = cpp_response.opcode_responses.size();
    response->opcode_results = new char*[response->opcode_count];
    for (int i = 0; i < response->opcode_count; ++i) {
        response->opcode_results[i] = to_c_str(cpp_response.opcode_responses[i].name);
    }
    
    // convert interface responses
    response->interface_count = cpp_response.interface_responses.size();
    response->interface_results = new char*[response->interface_count];
    for (int i = 0; i < response->interface_count; ++i) {
        response->interface_results[i] = to_c_str(cpp_response.interface_responses[i].interfaces);
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
    for (int i = 0; i < response->interface_count; ++i) {
        delete[] response->interface_results[i];
    }
    
    // free arrays
    delete[] response->boc_results;
    delete[] response->opcode_results;
    delete[] response->interface_results;
    
    // free response struct
    delete response;
}

} // extern "C"

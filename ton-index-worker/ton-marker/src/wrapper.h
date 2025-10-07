#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// decode opcode
const char* ton_marker_decode_opcode(unsigned int opcode);

// decode boc
const char* ton_marker_decode_boc(const char* boc_base64);

// batch processing
struct TonMarkerBatchRequest {
    // decode boc requests
    const char** boc_base64_list;
    int boc_count;
    
    // decode opcode requests
    const unsigned int* opcodes;
    int opcode_count;
    
};

struct TonMarkerBatchResponse {
    // decode boc responses
    char** boc_results;
    int boc_count;
    
    // decode opcode responses
    char** opcode_results;
    int opcode_count;
};

// batch processing function
struct TonMarkerBatchResponse* ton_marker_process_batch(const struct TonMarkerBatchRequest* request);

// free batch response
void ton_marker_free_batch_response(struct TonMarkerBatchResponse* response);

// free single string result (from decode_opcode, decode_boc, detect_interface)
void ton_marker_free_string(const char* str);

int test_funct();

#ifdef __cplusplus
}
#endif

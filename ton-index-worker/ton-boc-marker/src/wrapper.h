#pragma once

#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

// decode opcode
const char* ton_marker_decode_opcode(unsigned int opcode);

// decode boc
const char* ton_marker_decode_boc(const unsigned char* data, size_t length);

// detect interface
const char* ton_marker_detect_interface(const unsigned int* method_ids, int count);

// batch processing
struct TonMarkerBatchRequest {
    // decode boc requests
    const unsigned char** boc_data;
    const size_t* boc_lengths;
    int boc_count;
    
    // decode opcode requests
    const unsigned int* opcodes;
    int opcode_count;
    
    // detect interface requests
    const unsigned int** method_ids;
    const int* method_counts;
    int interface_count;
};

struct TonMarkerBatchResponse {
    // decode boc responses
    const char** boc_results;
    int boc_count;
    
    // decode opcode responses
    const char** opcode_results;
    int opcode_count;
    
    // detect interface responses
    const char** interface_results;
    int interface_count;
};

// batch processing function
TonMarkerBatchResponse* ton_marker_process_batch(const TonMarkerBatchRequest* request);

// free batch response
void ton_marker_free_batch_response(TonMarkerBatchResponse* response);

#ifdef __cplusplus
}
#endif

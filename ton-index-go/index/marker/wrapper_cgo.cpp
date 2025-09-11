#include "wrapper_cgo.h"

extern "C" {
    const char* ton_marker_decode_opcode(unsigned int opcode) {
        return ::ton_marker_decode_opcode(opcode);
    }

    const char* ton_marker_decode_boc(const char* boc_base64) {
        return ::ton_marker_decode_boc(boc_base64);
    }

    const char* ton_marker_detect_interface(const unsigned int* method_ids, int count) {
        return ::ton_marker_detect_interface(method_ids, count);
    }

    struct TonMarkerBatchResponse* ton_marker_process_batch(const struct TonMarkerBatchRequest* request) {
        return ::ton_marker_process_batch(request);
    }

    void ton_marker_free_batch_response(struct TonMarkerBatchResponse* response) {
        ::ton_marker_free_batch_response(response);
    }
}
#include "wrapper.h"
#include "logic.h"
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace {
std::optional<std::size_t> boc_input_length(const char* input) {
    if (!input) return std::nullopt;
    const auto length = strnlen(input, ton_marker::kMaxEncodedBocBytes + 1);
    if (length > ton_marker::kMaxEncodedBocBytes) return std::nullopt;
    return length;
}

std::unique_ptr<char[]> to_c_str(const std::string& str) {
    auto result = std::make_unique<char[]>(str.length() + 1);
    std::memcpy(result.get(), str.c_str(), str.length() + 1);
    return result;
}

void destroy_batch_response(TonMarkerBatchResponse* response) {
    if (!response) return;
    for (int i = 0; i < response->boc_count; ++i) {
        delete[] response->boc_results[i];
    }
    for (int i = 0; i < response->opcode_count; ++i) {
        delete[] response->opcode_results[i];
    }
    delete[] response->boc_results;
    delete[] response->opcode_results;
    delete response;
}

struct CStringArrayDeleter {
    std::size_t size;

    void operator()(char** values) const {
        if (!values) return;
        for (std::size_t i = 0; i < size; ++i) {
            delete[] values[i];
        }
        delete[] values;
    }
};

using CStringArray = std::unique_ptr<char*[], CStringArrayDeleter>;

CStringArray make_c_string_array(std::vector<std::unique_ptr<char[]>>& values) {
    if (values.empty()) return CStringArray(nullptr, CStringArrayDeleter{0});
    CStringArray result(new char*[values.size()]{}, CStringArrayDeleter{values.size()});
    for (std::size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i].release();
    }
    return result;
}
} // namespace

extern "C" {

const char* ton_marker_decode_opcode(unsigned int opcode) {
    try {
        return to_c_str(ton_marker::decode_opcode(opcode)).release();
    } catch (...) {
        return nullptr;
    }
}

const char* ton_marker_decode_boc(const char* boc_base64) {
    try {
        const auto length = boc_input_length(boc_base64);
        if (!length) return nullptr;
        return to_c_str(ton_marker::decode_boc(std::string(boc_base64, *length))).release();
    } catch (...) {
        return nullptr;
    }
}

TonMarkerBatchResponse* ton_marker_process_batch(const TonMarkerBatchRequest* request) {
    try {
        if (!request || request->boc_count < 0 || request->opcode_count < 0 ||
            static_cast<std::size_t>(request->boc_count) > ton_marker::kMaxBocBatchRequests ||
            static_cast<std::size_t>(request->opcode_count) > ton_marker::kMaxOpcodeBatchRequests ||
            (request->boc_count > 0 && !request->boc_base64_list) ||
            (request->opcode_count > 0 && !request->opcodes)) {
            return nullptr;
        }

        std::vector<std::size_t> boc_lengths;
        boc_lengths.reserve(request->boc_count);
        std::size_t encoded_boc_bytes = 0;
        for (int i = 0; i < request->boc_count; ++i) {
            const auto length = boc_input_length(request->boc_base64_list[i]);
            if (!length || *length > ton_marker::kMaxBatchEncodedBocBytes - encoded_boc_bytes) return nullptr;
            encoded_boc_bytes += *length;
            boc_lengths.push_back(*length);
        }

        ton_marker::BatchRequest cpp_request;
        cpp_request.boc_requests.reserve(request->boc_count);
        for (int i = 0; i < request->boc_count; ++i) {
            cpp_request.boc_requests.push_back(
                {std::string(request->boc_base64_list[i], boc_lengths[static_cast<std::size_t>(i)])});
        }

        cpp_request.opcode_requests.reserve(request->opcode_count);
        for (int i = 0; i < request->opcode_count; ++i) {
            cpp_request.opcode_requests.push_back({request->opcodes[i]});
        }

        auto cpp_response = ton_marker::process_batch(cpp_request);
        std::vector<std::unique_ptr<char[]>> boc_results;
        boc_results.reserve(cpp_response.boc_responses.size());
        for (const auto& item : cpp_response.boc_responses) {
            boc_results.push_back(to_c_str(item.json_output));
        }
        std::vector<std::unique_ptr<char[]>> opcode_results;
        opcode_results.reserve(cpp_response.opcode_responses.size());
        for (const auto& item : cpp_response.opcode_responses) {
            opcode_results.push_back(to_c_str(item.name));
        }

        auto boc_result_array = make_c_string_array(boc_results);
        auto opcode_result_array = make_c_string_array(opcode_results);
        auto response = std::make_unique<TonMarkerBatchResponse>();
        response->boc_results = boc_result_array.release();
        response->boc_count = static_cast<int>(boc_results.size());
        response->opcode_results = opcode_result_array.release();
        response->opcode_count = static_cast<int>(opcode_results.size());
        return response.release();
    } catch (...) {
        return nullptr;
    }
}

void ton_marker_free_batch_response(TonMarkerBatchResponse* response) {
    destroy_batch_response(response);
}

void ton_marker_free_string(const char* str) {
    if (str) {
        delete[] str;
    }
}

} // extern "C"

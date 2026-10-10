#include "logic.h"
#include "wrapper.h"
#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cells.h"
#include "vm/cellslice.h"

#include <iostream>
#include <stdexcept>
#include <string>

namespace {

const std::string kJettonTransferBoc =
    "te6cckEBAgEAXwABrA+KfqXd1cM7477pClMu6EG4CAGpIg3SmqRBb3f118SmKj9v88uiL"
    "n/wr10L8o/P1edBoQALRoNZaHCTREZ/qCrIa3pc1jOHL2t49dK6CvI3sot9R8IDAQAITSOFQ1gqiHU=";

// Internal request to tg-wallet in https://tonscan.org/tx/a086ecfd8a211c807f3ddd3ec63f619b7c5083f0dbaeb5c9633addf80a60382e
const std::string kTgWalletSendOneBoc =
    "te6cckEBAwEAjQABotiOvE4ZYtZkxtd9Se9sbmMXAdUFMt/PJKEKg9WYv40Kiy8QH7eILzwvyND08HsVxJmtmHMsObJYLkjIjnVaaw5jiW50f/9/"
    "EWrJEDEAAAABAwEBaEIAEH2cJUG60f6QrAzzZPsu4ydsGVofsuan7JkNRKRUDLcgL68IAAAAAAAAAAAAAAAAAAECAAB/RhCT";

std::string tg_wallet_bulk_boc(unsigned declared_count) {
    auto bytes = td::base64_decode(kTgWalletSendOneBoc).move_as_ok();
    auto cell = vm::std_boc_deserialize(bytes).move_as_ok();
    auto message = vm::load_cell_slice(cell).fetch_ref();

    vm::CellBuilder last;
    last.store_zeroes(1);
    for (unsigned i = 0; i < 4; ++i) last.store_long(3, 8).store_ref(message);
    vm::CellBuilder first;
    first.store_ones(1).store_ref(last.finalize());
    first.store_long(3, 8).store_ref(message);

    vm::CellBuilder request;
    request.store_zeroes(512).store_long(0x73896e75, 32);
    request.store_long(2147450641, 32).store_long(1791561777, 32).store_long(1, 32);
    request.store_long(declared_count, 8).store_ones(1).store_ref(first.finalize());
    auto boc = vm::std_boc_serialize(request.finalize()).move_as_ok();
    return td::base64_encode(boc);
}

const std::string kRecursiveDictionaryBoc =
    "te6cckEBGgEAogABGQMCzXkAAAAAAAAAAMABAgPPWAICAgEgAwMCASAEBAIBIAUFAgEgBgYCASAHBwIBIAgIAgEgCQkC"
    "ASAKCgIBIAsLAgEgDAwBGQDAs15AAAAAAAAAADANAgPPWA4OAgEgDw8CASAQEAIBIBERAgEgEhICASATEwIBIBQUAgEg"
    "FRUCASAWFgIBIBcXAgEgGBgBGQDAs15AAAAAAAAAADAZAACeI/C2";

const std::string kRecursiveSharedCellBoc =
    "te6cckECCQEAAtYAAqwPin6l3dXDO+O+6QpTLuhBuAgBqSIN0pqkQW939dfEpio/b/PLoi5/8K9dC/KPz9XnQaEAC0aDWW"
    "hwk0RGf6gqyGt6XNYzhy9rePXSugryN7KLfUfiAwEBAqwPin6l3dXDO+O+6QpTLuhBuAgBqSIN0pqkQW939dfEpio/b/PL"
    "oi5/8K9dC/KPz9XnQaEAC0aDWWhwk0RGf6gqyGt6XNYzhy9rePXSugryN7KLfUfiAwICAqwPin6l3dXDO+O+6QpTLuhBu"
    "AgBqSIN0pqkQW939dfEpio/b/PLoi5/8K9dC/KPz9XnQaEAC0aDWWhwk0RGf6gqyGt6XNYzhy9rePXSugryN7KLfUfiAw"
    "MDAqwPin6l3dXDO+O+6QpTLuhBuAgBqSIN0pqkQW939dfEpio/b/PLoi5/8K9dC/KPz9XnQaEAC0aDWWhwk0RGf6gqyGt"
    "6XNYzhy9rePXSugryN7KLfUfiAwQEAqwPin6l3dXDO+O+6QpTLuhBuAgBqSIN0pqkQW939dfEpio/b/PLoi5/8K9dC/KP"
    "z9XnQaEAC0aDWWhwk0RGf6gqyGt6XNYzhy9rePXSugryN7KLfUfiAwUFAqwPin6l3dXDO+O+6QpTLuhBuAgBqSIN0pqkQ"
    "W939dfEpio/b/PLoi5/8K9dC/KPz9XnQaEAC0aDWWhwk0RGf6gqyGt6XNYzhy9rePXSugryN7KLfUfiAwYGAqwPin6l3"
    "dXDO+O+6QpTLuhBuAgBqSIN0pqkQW939dfEpio/b/PLoi5/8K9dC/KPz9XnQaEAC0aDWWhwk0RGf6gqyGt6XNYzhy9re"
    "PXSugryN7KLfUfiAwcHAqwPin6l3dXDO+O+6QpTLuhBuAgBqSIN0pqkQW939dfEpio/b/PLoi5/8K9dC/KPz9XnQaEAC0"
    "aDWWhwk0RGf6gqyGt6XNYzhy9rePXSugryN7KLfUfiAwgIAAhNI4VDgcJxgg==";

}  // namespace

bool test_regular_message_body() {
    const auto result = ton_marker::decode_boc_recursive(kJettonTransferBoc);
    if (!result.starts_with("{\"@type\":\"jetton_transfer\"")) {
        std::cerr << "regular message decode failed: " << result << '\n';
        return false;
    }
    return true;
}

bool test_tg_wallet_signed_request() {
    const auto result = ton_marker::decode_boc_recursive(kTgWalletSendOneBoc);
    if (result.find("\"@type\":\"tg_wallet_signed\"") == std::string::npos ||
        result.find("\"@type\":\"tg_wallet_send_one_internal\"") == std::string::npos ||
        result.find("\"message\":{\"@type\":\"message\"") == std::string::npos ||
        result.find("\"send_mode\":\"3\"") == std::string::npos) {
        std::cerr << "tg-wallet request decode failed: " << result << '\n';
        return false;
    }
    return ton_marker::decode_opcode(0x63896e74) == "tg_wallet_send_one_internal" &&
           ton_marker::decode_opcode(0xeba19948) == "tg_wallet_key_changed";
}

bool test_tg_wallet_bulk_request() {
    const auto result = ton_marker::decode_boc_recursive(tg_wallet_bulk_boc(5));
    const std::string item = "\"@type\":\"tg_wallet_message_to_send\"";
    std::size_t count = 0;
    for (std::size_t pos = 0; (pos = result.find(item, pos)) != std::string::npos; pos += item.size()) ++count;
    if (result.find("\"@type\":\"tg_wallet_send_bulk_external\"") == std::string::npos ||
        result.find("\"messages_count\":\"5\",\"messages\":[") == std::string::npos || count != 5 ||
        result.find("\"message\":{\"@type\":\"message\"") == std::string::npos) {
        std::cerr << "tg-wallet bulk decode failed: " << result << '\n';
        return false;
    }

    // An inconsistent length still has the original opaque TL-B representation.
    const auto malformed = ton_marker::decode_boc_recursive(tg_wallet_bulk_boc(6));
    if (malformed.find("\"first_chunk\"") == std::string::npos ||
        malformed.find("\"messages\":[") != std::string::npos) {
        std::cerr << "malformed tg-wallet bulk fallback failed: " << malformed << '\n';
        return false;
    }
    return true;
}

bool test_recursive_dictionary_amplification() {
    const auto result = ton_marker::decode_boc_recursive(kRecursiveDictionaryBoc);
    const std::string expected = "unknown: decode limit - boc cell expansion limit exceeded";
    if (result != expected) {
        std::cerr << "recursive dictionary returned: " << result << '\n';
        return false;
    }
    return true;
}

bool test_recursive_decode_budget() {
    const auto result = ton_marker::decode_boc_recursive(kRecursiveSharedCellBoc);
    const std::string expected = "unknown: recursive decode error - recursive decode operation limit exceeded";
    if (result != expected) {
        std::cerr << "recursive shared-cell body returned: " << result << '\n';
        return false;
    }
    return true;
}

bool test_encoded_boc_size_limit() {
    const std::string oversized_boc(ton_marker::kMaxEncodedBocBytes + 1, 'A');
    const auto result = ton_marker::decode_boc(oversized_boc);
    if (result != "unknown: encoded boc size limit exceeded") {
        std::cerr << "oversized BOC returned: " << result << '\n';
        return false;
    }
    return true;
}

bool test_batch_cardinality_limit() {
    ton_marker::BatchRequest request;
    request.opcode_requests.resize(ton_marker::kMaxOpcodeBatchRequests + 1);
    try {
        (void)ton_marker::process_batch(request);
    } catch (const std::invalid_argument&) {
        return true;
    }
    std::cerr << "oversized batch was not rejected\n";
    return false;
}

bool test_payload_fanout_batch() {
    ton_marker::BatchRequest request;
    request.boc_requests.resize(1002, {""});
    const auto response = ton_marker::process_batch(request);
    if (response.boc_responses.size() != request.boc_requests.size()) {
        std::cerr << "payload fan-out batch returned " << response.boc_responses.size() << " responses\n";
        return false;
    }
    return true;
}

bool test_batch_encoded_boc_size_limit() {
    ton_marker::BatchRequest request;
    const std::string boc(1024 * 1024, 'A');
    request.boc_requests.resize(17, {boc});
    try {
        (void)ton_marker::process_batch(request);
    } catch (const std::invalid_argument&) {
        return true;
    }
    std::cerr << "oversized batch input was not rejected\n";
    return false;
}

bool test_c_api_limits() {
    TonMarkerBatchRequest count_request{nullptr, static_cast<int>(ton_marker::kMaxBocBatchRequests + 1), nullptr, 0};
    if (ton_marker_process_batch(&count_request) != nullptr) {
        std::cerr << "C API accepted an oversized batch\n";
        return false;
    }

    const std::string boc(1024 * 1024, 'A');
    std::vector<const char*> bocs(17, boc.c_str());
    TonMarkerBatchRequest bytes_request{bocs.data(), static_cast<int>(bocs.size()), nullptr, 0};
    if (ton_marker_process_batch(&bytes_request) != nullptr) {
        std::cerr << "C API accepted oversized aggregate BOC input\n";
        return false;
    }

    const char* empty_boc = "";
    std::vector<const char*> fanout_bocs(1002, empty_boc);
    TonMarkerBatchRequest fanout_request{fanout_bocs.data(), static_cast<int>(fanout_bocs.size()), nullptr, 0};
    auto* fanout_response = ton_marker_process_batch(&fanout_request);
    if (!fanout_response || fanout_response->boc_count != static_cast<int>(fanout_bocs.size())) {
        std::cerr << "C API payload fan-out batch failed\n";
        ton_marker_free_batch_response(fanout_response);
        return false;
    }
    ton_marker_free_batch_response(fanout_response);

    ton_marker_free_batch_response(nullptr);
    ton_marker_free_string(nullptr);
    return true;
}

int main() {
    if (!test_regular_message_body() || !test_tg_wallet_signed_request() || !test_tg_wallet_bulk_request() ||
        !test_recursive_dictionary_amplification() ||
        !test_recursive_decode_budget() || !test_encoded_boc_size_limit() || !test_batch_cardinality_limit()) {
        return 1;
    }
    if (!test_payload_fanout_batch() || !test_batch_encoded_boc_size_limit() || !test_c_api_limits()) {
        return 1;
    }
    return 0;
}

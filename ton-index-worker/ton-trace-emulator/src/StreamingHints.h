#pragma once

#include <msgpack.hpp>

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

enum class StreamingTransactionKind : std::uint8_t {
    Pending = 0,
    Confirmed = 1,
    Finalized = 2,
};

struct StreamingTransactionHint {
    std::string trace_key;
    std::uint64_t update_seq{0};
    std::uint8_t kind{0};
    std::uint8_t finality{0};
    std::vector<std::string> accounts;

    MSGPACK_DEFINE_MAP(trace_key, update_seq, kind, finality, accounts);
};

struct StreamingAccountStateHint {
    std::string account;
    std::uint64_t lt{0};
    std::uint8_t finality{0};

    MSGPACK_DEFINE_MAP(account, lt, finality);
};

template <class Hint>
std::string pack_streaming_hint(const Hint& hint) {
    std::stringstream buffer;
    msgpack::pack(buffer, hint);
    return buffer.str();
}

#pragma once

#include <cstdint>
#include <msgpack.hpp>
#include <sstream>
#include <string>
#include <vector>

enum class StreamingUpdateFinality : std::uint8_t {
  Pending = 0,
  Confirmed = 1,
  Finalized = 2,
};

struct StreamingTransactionHint {
  std::string trace_key;
  std::uint64_t update_seq{0};
  std::uint8_t update_finality{0};
  std::uint8_t trace_finality{0};
  std::vector<std::string> accounts;

  MSGPACK_DEFINE_MAP(trace_key, update_seq, update_finality, trace_finality, accounts);
};

struct StreamingAccountStateHint {
  std::string account;
  std::uint64_t lt{0};
  std::uint8_t finality{0};

  MSGPACK_DEFINE_MAP(account, lt, finality);
};

struct StreamingActionRoute {
  std::string type;
  std::vector<std::string> accounts;

  MSGPACK_DEFINE_MAP(type, accounts);
};

struct StreamingActionsHint {
  std::string trace_key;
  std::uint64_t update_seq{0};
  std::uint8_t update_finality{0};
  std::uint8_t trace_finality{0};
  bool actions_updated{false};
  std::vector<StreamingActionRoute> action_types_and_accounts;

  MSGPACK_DEFINE_MAP(trace_key, update_seq, update_finality, trace_finality, actions_updated,
                     action_types_and_accounts);
};

template <class Hint>
std::string pack_streaming_hint(const Hint& hint) {
  std::stringstream buffer;
  msgpack::pack(buffer, hint);
  return buffer.str();
}

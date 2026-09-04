#include <memory>
#include <string>
#include <utility>

#include "td/utils/tests.h"

#include "TraceAssembler.h"
#include "TraceUpdate.h"

namespace {

td::Bits256 hash(char digit) {
  td::Bits256 result;
  ASSERT_EQ(256, result.from_hex(std::string(64, digit)));
  return result;
}

Trace fragment(char trace_digit, char root_digit) {
  Trace trace;
  trace.ext_in_msg_hash_norm = hash(trace_digit);
  trace.root_tx_hash = hash(trace_digit);
  trace.root = std::make_unique<TraceNode>();
  trace.root->node_id = hash(root_digit);
  return trace;
}

block::StdAddress address(char digit) {
  return block::StdAddress{0, hash(digit)};
}

Trace interface_fragment(const block::StdAddress& account, bool emulated, bool marker) {
  Trace trace;
  JettonWalletDetectorR::Result detected;
  detected.address = account;
  detected.mintless_is_claimed = marker;
  trace.interfaces[account].emplace_back(std::move(detected));

  block::Account state;
  state.last_trans_lt_ = emulated ? 200 : 100;
  if (emulated) {
    trace.emulated_accounts.emplace(account, std::move(state));
  } else {
    trace.committed_accounts.emplace(account, std::move(state));
  }
  return trace;
}

bool selected_interface_marker(const TraceUpdate& update, const block::StdAddress& account) {
  std::size_t occurrences = 0;
  bool marker = false;
  for (const auto& fragment : update.fragments) {
    auto interfaces = fragment.interfaces.find(account);
    if (interfaces == fragment.interfaces.end()) {
      continue;
    }
    occurrences++;
    ASSERT_EQ(1u, interfaces->second.size());
    const auto* wallet = std::get_if<JettonWalletDetectorR::Result>(&interfaces->second.front());
    ASSERT_TRUE(wallet != nullptr && wallet->mintless_is_claimed.has_value());
    marker = *wallet->mintless_is_claimed;
  }
  ASSERT_EQ(1u, occurrences);
  return marker;
}

}  // namespace

TEST(TraceUpdate, owns_one_measurement_for_all_fragments) {
  auto measurement = std::make_shared<Measurement>();
  TraceUpdate update;
  update.fragments.push_back(fragment('a', '2'));
  update.fragments.push_back(fragment('a', '4'));
  update.measurement = measurement;

  ASSERT_EQ(2u, update.size());
  ASSERT_EQ(hash('2'), update.fragments[0].root->node_id);
  ASSERT_EQ(hash('4'), update.fragments[1].root->node_id);
  ASSERT_TRUE(update.measurement == measurement);
}

TEST(TraceUpdate, wraps_a_single_listener_update) {
  Trace trace;
  trace.ext_in_msg_hash_norm = hash('a');
  auto measurement = std::make_shared<Measurement>();

  auto update = make_trace_update(std::move(trace), measurement);

  ASSERT_EQ(1u, update.size());
  ASSERT_EQ(hash('a'), update.fragments.front().ext_in_msg_hash_norm);
  ASSERT_TRUE(update.measurement == measurement);
}

TEST(TraceUpdate, assembler_normalizes_interfaces_before_applying_fragments) {
  const auto account = address('a');
  for (const bool emulated_first : {false, true}) {
    TraceUpdate update;
    if (emulated_first) {
      update.fragments.push_back(interface_fragment(account, true, true));
      update.fragments.push_back(interface_fragment(account, false, false));
    } else {
      update.fragments.push_back(interface_fragment(account, false, false));
      update.fragments.push_back(interface_fragment(account, true, true));
    }

    ActiveTrace current;
    auto result = TraceAssembler().apply_update(current, update, "trace");

    ASSERT_TRUE(result.is_ok());
    ASSERT_TRUE(selected_interface_marker(update, account));
  }
}

TEST(TraceUpdate, empty_emulated_interface_removes_committed_interface) {
  const auto account = address('a');
  TraceUpdate update;
  update.fragments.push_back(interface_fragment(account, false, false));

  Trace emulated;
  emulated.interfaces[account] = {};
  block::Account final_state;
  final_state.last_trans_lt_ = 200;
  emulated.emulated_accounts.emplace(account, std::move(final_state));
  update.fragments.push_back(std::move(emulated));

  normalize_trace_update_interfaces(update);

  std::size_t occurrences = 0;
  for (const auto& fragment : update.fragments) {
    auto interfaces = fragment.interfaces.find(account);
    if (interfaces != fragment.interfaces.end()) {
      occurrences++;
      ASSERT_TRUE(interfaces->second.empty());
    }
  }
  ASSERT_EQ(1u, occurrences);
}

// Emulator-header-free classifier types. EmuFinality mirrors FinalityState;
// only EmuClassifierBridge.cpp translates between the two type sets.
#pragma once

#include "ActionBuild.h"             // Action (the rows the classifier hands back)
#include "EmuActionPayload.h"        // EmuActionPayload (the write-back bytes)
#include "EmuCelldbLookup.h"         // Tier2Stats, AllShardStates
#include "EnginePrep.h"             // MchEnginePrep
#include "ParsedBlockLookupSource.h" // InterfaceMap, LookupStats

#include "crypto/block/block.h"  // block::StdAddress
#include "td/utils/port/Clocks.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace mch {

// Constant global_version for the off-pipeline message parse. It selects
// extra_flags vs reference ihr_fee in int_msg_info; neither field is read by
// SchemaTraceLoader, so the choice cannot affect classification.
// TODO: wire live config global_version
inline constexpr int kEmuGlobalVersion = 12;

// Monotonic microseconds used for classifier timing measurements.
inline std::int64_t emu_now_us() {
  return static_cast<std::int64_t>(td::Clocks::monotonic() * 1e6);
}

enum class EmuFinality : std::uint8_t { emulated = 0, confirmed = 1, finalized = 2 };

struct EmuTxRef {
  block::StdAddress address;
  // Standalone BOC: classifier input never depends on a source BlockData.
  std::shared_ptr<const std::string> tx_boc;
  std::uint32_t mc_seqno{0};
  EmuFinality finality{EmuFinality::emulated};
};

// Fully owned, read-only full trace.
struct EmuTraceView {
  std::string trace_id;  // base64(ext_in_msg_hash_norm) == the Redis trace key
  bool tx_limit_exceeded{false};
  // Pre-order nodes; to_tree reconstructs edges by message hash.
  std::vector<EmuTxRef> nodes;
  std::shared_ptr<const ParsedBlockLookupSource::InterfaceMap> interfaces =
      std::make_shared<const ParsedBlockLookupSource::InterfaceMap>();  // already V2-adapted
  // Shared shard-state and config handles for tier-2 lookups. Listener traces
  // leave them empty and receive no tier-2 results.
  AllShardStates shard_states;
  std::shared_ptr<block::ConfigInfo> config;
  std::uint64_t update_seq{0};
};

// Minimum node finality shared by the wire payload and insert guard.
inline EmuFinality view_finality(const EmuTraceView &view) {
  if (view.nodes.empty()) {
    // No emission to describe. `emulated` is the least advanced value, so an
    // empty view loses every guard comparison rather than blocking a real one.
    return EmuFinality::emulated;
  }
  EmuFinality f = EmuFinality::finalized;
  for (const EmuTxRef &node : view.nodes) {
    f = std::min(f, node.finality);
  }
  return f;
}

enum class EmuClassifyOutcome : std::uint8_t {
  classified,
  classify_failed,
  convert_failed
};

struct EmuClassifyResult {
  std::string trace_id;
  EmuClassifyOutcome outcome{EmuClassifyOutcome::classify_failed};
  // Serialized before the owned view leaves the classifier actor.
  EmuActionPayload payload;
  bool used_fallback{false};       // rows came from ClassifyResult::fallback_rows
  std::size_t unported_btypes{0};  // spine blocks build_action() declined
  bool failure{false};
  std::string failure_reason;
  FailureCategory failure_category{FailureCategory::none};
  ParsedBlockLookupSource::LookupStats lookup_stats;
  Tier2Stats tier2_stats;
  // Serialization and classification have separate latency measurements.
  std::int64_t queue_us{0}, classify_us{0}, serialize_us{0};
};

struct EmuClassifierConfig {
  std::shared_ptr<const MchEnginePrep> prep;  // nullptr = feature off
  int workers{1};
  int global_version{kEmuGlobalVersion};
  // Cell-db tier-2 lookups. Enabled by default; disabling them uses tier 1 only.
  bool tier2{true};
};

}  // namespace mch

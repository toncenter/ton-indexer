// Emulator-header-free classifier types. EmuFinality mirrors FinalityState;
// only EmuClassifierBridge.cpp translates between the two type sets.
#pragma once

#include "ActionBuild.h"             // Action (the rows the classifier hands back)
#include "EmuActionPayload.h"        // EmuActionPayload (the write-back bytes)
#include "EmuCelldbLookup.h"         // Tier2Budget, Tier2Stats, AllShardStates
#include "EnginePrep.h"             // MchEnginePrep
#include "ParsedBlockLookupSource.h" // InterfaceMap, LookupStats

#include "crypto/block/block.h"  // block::StdAddress
#include "vm/cells/Cell.h"

#include "td/utils/port/Clocks.h"

#include <algorithm>
#include <atomic>
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

// Admission and timeout limits for classification work.
inline constexpr std::size_t kMaxInFlightClassify = 1024;
inline constexpr std::int64_t kClassifyDeadlineUs = 200'000;
inline constexpr std::int64_t kStallFailOpenUs = 5'000'000;

// Monotonic microseconds: the deadline is stamped on an emulator thread and
// read on the classifier's, so it must not move with the wall clock.
inline std::int64_t emu_now_us() {
  return static_cast<std::int64_t>(td::Clocks::monotonic() * 1e6);
}

enum class EmuFinality : std::uint8_t { emulated = 0, confirmed = 1, finalized = 2 };

// Keeps the lazy block store alive while interior transaction cells are in use.
// Listener-emulated traces do not reference a block BOC and use a null anchor.
using EmuCellAnchor = std::shared_ptr<const std::vector<td::Ref<vm::Cell>>>;

struct EmuTxRef {
  block::StdAddress address;
  // Transaction cell kept loadable by EmuTraceView::anchor. Null means absent.
  td::Ref<vm::Cell> tx_root;
  std::uint32_t mc_seqno{0};
  EmuFinality finality{EmuFinality::emulated};
};

// Read-only trace emission plus its cell-lifetime handle. Construction loads no cells.
struct EmuTraceView {
  std::string trace_id;  // base64(ext_in_msg_hash_norm) == the Redis trace key
  bool tx_limit_exceeded{false};
  // Pre-order nodes; to_tree reconstructs edges by message hash.
  std::vector<EmuTxRef> nodes;
  ParsedBlockLookupSource::InterfaceMap interfaces;  // already V2-adapted
  EmuCellAnchor anchor;
  // Shared shard-state and config handles for tier-2 lookups. Listener traces
  // leave them empty and receive no tier-2 results.
  AllShardStates shard_states;
  std::shared_ptr<block::ConfigInfo> config;
  std::int64_t sent_us{0}, deadline_us{0};  // emu_now_us basis
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
  convert_failed,   // the classifier ran
  shed_admission,
  shed_deadline,
  bypassed_disabled  // classifier did not run
};

struct EmuClassifyResult {
  std::string trace_id;
  EmuClassifyOutcome outcome{EmuClassifyOutcome::bypassed_disabled};
  // Serialized while the view's anchor is alive; no anchor-backed value leaves
  // the classifier actor.
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

// Shared gate state: written by emitter-thread admission and by the classifier
// on its own.
struct EmuGate {
  std::atomic<std::size_t> in_flight{0};
  // Seeded at construction so the first stalled request can trip fail-open.
  std::atomic<std::int64_t> last_response_us{emu_now_us()};
  std::atomic<bool> disabled{false};  // sticky fail-open
};

struct EmuClassifierConfig {
  std::shared_ptr<const MchEnginePrep> prep;  // nullptr = feature off
  int global_version{kEmuGlobalVersion};
  // Required whenever `prep` is set; scheduler integration and actor dereference it.
  std::shared_ptr<EmuGate> gate;
  // Cell-db tier-2 lookups. Enabled by default; disabling them uses tier 1 only.
  bool tier2{true};
  Tier2Budget tier2_budget;
};

}  // namespace mch

// Classifies trace emissions and returns serialized actions plus telemetry for
// scheduler integration. The actor performs no Redis or database writes.
#pragma once

#include "EmuTypes.h"

#include "td/actor/actor.h"

#include <array>
#include <cstddef>
#include <deque>
#include <map>
#include <string>
#include <unordered_set>

namespace mch {

// Cumulative counters; the periodic log line prints these plus the delta since
// the previous line.
struct EmuClassifierStats {
  std::size_t emissions{0}, reemissions{0};
  // Gate-side outcomes: the classifier did not run.
  std::size_t shed_admission{0}, shed_deadline{0}, bypassed_disabled{0};
  std::size_t classified{0}, failed{0}, convert_failed{0}, tx_limit_exceeded{0}, actions{0};
  // Actions produced by the basic-action fallback after classification failure.
  std::size_t fallback_actions{0};
  std::size_t unported_btypes{0}, arena_refs_scrubbed{0};
  std::size_t queue_us_max{0}, classify_us_max{0}, classify_us_total{0}, latency_samples{0};
  std::size_t serialize_us_max{0}, serialize_us_total{0}, actions_blob_bytes{0};
  // Serializer sightings of a Value shape an Action field should not carry.
  std::size_t ser_float_values{0}, ser_cell_values{0}, ser_unrenderable{0};
  std::array<std::size_t, 3> by_finality{};  // root-node EmuFinality of the emission
  std::map<FailureCategory, std::size_t> by_category;
  ParsedBlockLookupSource::LookupStats lookups;  // tier1 / tier2 / miss
  Tier2Stats tier2;  // celldb reads / memo hits / budget refusals
};

class EmuClassifierActor : public td::actor::Actor {
 public:
  explicit EmuClassifierActor(EmuClassifierConfig cfg) : cfg_(std::move(cfg)) {
    CHECK(cfg_.gate);  // Enabled EmuClassifierConfig requires a gate.
  }

  // Request entry from scheduler integration. Always answers exactly once.
  void classify(EmuTraceView view, td::Promise<EmuClassifyResult> promise);
  // The gate turned an emission away before it became a request (admission cap
  // or the sticky fail-open flag).
  void note_shed(EmuClassifyOutcome why);

  void start_up() override;
  void alarm() override;

 private:
  // Bounded trace-id LRU distinguishes first emissions from reemissions.
  bool remember_seen(const std::string &trace_id);

  EmuClassifierConfig cfg_;
  EmuClassifierStats stats_;
  EmuClassifierStats prev_;  // snapshot at the previous log line, for the delta
  std::unordered_set<std::string> seen_traces_;
  std::deque<std::string> seen_order_;
};

}  // namespace mch

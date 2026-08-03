#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "emu/EmuTypes.h"
#include "td/utils/Status.h"

#include "TraceEmulator.h"
#include "TraceState.h"

using TraceMetadata = std::map<std::string, std::string>;

struct RedisTraceNode;

struct ActionState {
  std::optional<std::string> blob;
  std::optional<std::string> classify_state;
  std::optional<std::uint8_t> blob_finality;
  std::vector<TraceStateIndexRef> aai_refs;
  std::vector<mch::EmuActionRoute> routes;
  // False means the blob belongs to an older trace version and must not be streamed.
  bool blob_is_current{false};
};

// Canonical, fully owned state of one active trace. TraceStateNode keeps both
// the Redis representation and an independent transaction BOC for classifier
// inputs. No field here depends on the lifetime of a source BlockData.
struct ActiveTrace {
  TraceState nodes;
  ActionState actions;
  TraceMetadata metadata;
  mch::ParsedBlockLookupSource::InterfaceMap classifier_interfaces;
  std::uint64_t update_seq{0};
  std::optional<std::string> root_account;
  FinalityState finality{FinalityState::Emulated};
  bool tx_limit_exceeded{false};
};

struct AcceptedNode {
  std::string key;
  FinalityState finality{FinalityState::Emulated};
};

struct TraceTransition {
  bool needs_redis_write{false};
  ActiveTrace next_trace;
  TraceStateDelta node_delta;
  TraceMetadata metadata_patch;
  std::vector<AcceptedNode> accepted_nodes;
  std::size_t cached_nodes_count{0};
  std::size_t reused_serializations{0};
  std::string raw_external_message_hash;
};

class TraceAssembler {
 public:
  td::Result<TraceTransition> apply(const ActiveTrace& current, const Trace& patch, const std::string& trace_key) const;

  td::Result<mch::EmuTraceView> build_full_trace(const ActiveTrace& trace, const std::string& trace_key,
                                                 const Trace& lookup_context) const;
};

std::optional<std::string> trace_metadata_value(const ActiveTrace& trace, const std::string& field);

std::string trace_node_fingerprint(const RedisTraceNode& node);

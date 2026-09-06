#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "td/actor/ActorStats.h"
#include "td/actor/actor.h"
#include "td/utils/Status.h"

// Zero disables recording. Positive values are intervals in seconds.
td::Result<double> parse_actor_stats_interval(td::Slice value);

struct ActorStatsSnapshot {
  std::string report;
  std::uint64_t captured_at_us = 0;
  double interval_seconds = 0;
};

// Filesystem operations are deliberately separate from the actor. The store
// is used only by the file writer thread (and directly by filesystem tests).
class ActorStatsSnapshotStore {
 public:
  static constexpr std::size_t kMaxSnapshots = 500;
  explicit ActorStatsSnapshotStore(const std::string& working_dir);
  td::Status initialize();
  td::Status save(const ActorStatsSnapshot& snapshot);
  const std::string& directory() const;

 private:
  std::string directory_;
  std::uint64_t last_id_ = 0;
  td::Result<std::vector<std::string>> snapshots() const;
  td::Status prune(const std::vector<std::string>& files) const;
};

// One writer thread, at most one outstanding snapshot including disk I/O.
// Destroy after stopping the scheduler; destruction drains that snapshot.
class ActorStatsFileWriter {
 public:
  static td::Result<std::shared_ptr<ActorStatsFileWriter>> create(const std::string& working_dir);
  ~ActorStatsFileWriter();
  bool busy() const;
  bool submit(ActorStatsSnapshot snapshot);

 private:
  struct Impl;
  explicit ActorStatsFileWriter(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

class ActorStatsRecorder final : public td::actor::Actor {
 public:
  ActorStatsRecorder(double interval_seconds, std::shared_ptr<ActorStatsFileWriter> writer);

 private:
  double interval_seconds_;
  std::shared_ptr<ActorStatsFileWriter> writer_;
  td::actor::ActorOwn<td::actor::ActorStats> stats_;
  bool report_pending_ = false;

  void start_up() override;
  void alarm() override;
  void report_ready(td::Result<std::string> result, std::uint64_t captured_at_us);
};

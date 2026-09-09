#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "td/actor/ActorStats.h"
#include "td/actor/actor.h"
#include "td/utils/Status.h"

// Zero disables actor stats (application stats keep their 60-second interval).
// Values of at least one second select the shared recording interval.
td::Result<double> parse_actor_stats_interval(td::Slice value);

struct StatsSnapshot {
  std::string report;
  std::optional<std::string> actor_report;
  std::uint64_t captured_at_seconds = 0;
  double interval_seconds = 0;
  double window_seconds = 0;
};

// Filesystem operations are deliberately separate from the actor. The store
// is used only by the file writer thread (and directly by filesystem tests).
class StatsSnapshotStore {
 public:
  static constexpr std::size_t kMaxSnapshots = 500;
  explicit StatsSnapshotStore(const std::string& working_dir);
  td::Status initialize();
  td::Status save(const StatsSnapshot& snapshot);
  const std::string& directory() const;

 private:
  std::string directory_;
  std::uint64_t last_id_ = 0;
  using SnapshotFiles = std::map<std::uint64_t, std::vector<std::string>>;
  td::Result<SnapshotFiles> snapshots() const;
  td::Status prune(const SnapshotFiles& files) const;
};

// One writer thread, at most one outstanding snapshot including disk I/O.
// Destroy after stopping the scheduler; destruction drains that snapshot.
class StatsFileWriter {
 public:
  static td::Result<std::shared_ptr<StatsFileWriter>> create(const std::string& working_dir);
  ~StatsFileWriter();
  bool busy() const;
  bool submit(StatsSnapshot snapshot);

 private:
  struct Impl;
  explicit StatsFileWriter(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

class StatsRecorder final : public td::actor::Actor {
 public:
  // Owns collection/reset and must be the writer's only producer.
  StatsRecorder(double interval_seconds, bool actor_stats_enabled, std::shared_ptr<StatsFileWriter> writer,
                std::function<std::string()> collect_statistics);

 private:
  double interval_seconds_;
  bool actor_stats_enabled_;
  std::shared_ptr<StatsFileWriter> writer_;
  std::function<std::string()> collect_statistics_;
  double last_collection_at_ = 0;
  td::actor::ActorOwn<td::actor::ActorStats> stats_;
  bool report_pending_ = false;

  void start_up() override;
  void alarm() override;
  void report_ready(td::Result<std::string> result);
  void save_snapshot(std::optional<std::string> actor_report);
};

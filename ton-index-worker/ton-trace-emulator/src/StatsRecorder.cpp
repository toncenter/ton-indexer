#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>

#include "td/utils/ScopeGuard.h"
#include "td/utils/port/FileFd.h"
#include "td/utils/port/thread.h"

#include "StatsRecorder.h"

namespace {
namespace fs = std::filesystem;
constexpr std::string_view kStatsPrefix = "stats-";
constexpr std::string_view kActorStatsPrefix = "actor-stats-";
constexpr std::string_view kSuffix = ".txt";

std::optional<std::uint64_t> snapshot_id(std::string_view name) {
  auto prefix = name.starts_with(kActorStatsPrefix) ? kActorStatsPrefix : kStatsPrefix;
  if (!name.starts_with(prefix) || !name.ends_with(kSuffix) || name.size() <= prefix.size() + kSuffix.size())
    return {};
  auto digits = name.substr(prefix.size(), name.size() - prefix.size() - kSuffix.size());
  if (!std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; }))
    return {};
  std::uint64_t id;
  auto result = std::from_chars(digits.data(), digits.data() + digits.size(), id);
  if (result.ec != std::errc{} || std::to_string(id) != digits)
    return {};
  return id;
}

std::string snapshot_name(std::string_view prefix, std::uint64_t id) {
  return std::string(prefix) + std::to_string(id) + std::string(kSuffix);
}

td::Status filesystem_error(const std::string& operation, const std::error_code& error) {
  return td::Status::Error(operation + ": " + error.message());
}

std::uint64_t unix_time_seconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string snapshot_header(const StatsSnapshot& snapshot) {
  auto seconds = static_cast<std::time_t>(snapshot.captured_at_seconds);
  std::tm utc{};
#if TD_PORT_WINDOWS
  gmtime_s(&utc, &seconds);
#else
  gmtime_r(&seconds, &utc);
#endif
  std::ostringstream header;
  header << "# captured_at_utc: " << std::put_time(&utc, "%Y-%m-%dT%H:%M:%SZ") << "\n"
         << "# interval_seconds: " << snapshot.interval_seconds << "\n";
  return header.str();
}

td::Status write_temporary_file(const fs::path& path, const std::string& contents) {
  TRY_RESULT(file, td::FileFd::open(path.string(), td::FileFd::Write | td::FileFd::CreateNew, 0600));
  auto cleanup = td::ScopeExit() + [&] {
    file.close();
    std::error_code ignored;
    fs::remove(path, ignored);
  };
  TRY_STATUS(file.write_all(contents));
  TRY_STATUS(file.sync());
  file.close();
  cleanup.dismiss();
  return td::Status::OK();
}
}  // namespace

td::Result<double> parse_actor_stats_interval(td::Slice value) {
  try {
    auto text = value.str();
    std::size_t consumed = 0;
    auto interval = std::stod(text, &consumed);
    if (consumed == text.size() && std::isfinite(interval) && (interval == 0 || interval >= 1))
      return interval;
  } catch (...) {
  }
  return td::Status::Error(
      "--actor-stats-interval must be 0 (actor stats disabled) or a finite interval of at least 1 second");
}

StatsSnapshotStore::StatsSnapshotStore(const std::string& working_dir)
    : directory_((fs::path(working_dir) / "stats").string()) {
}

const std::string& StatsSnapshotStore::directory() const {
  return directory_;
}

td::Result<StatsSnapshotStore::SnapshotFiles> StatsSnapshotStore::snapshots() const {
  std::error_code error;
  // Do not follow a stats directory replaced by a symlink.
  auto status = fs::symlink_status(directory_, error);
  if (error)
    return filesystem_error("Cannot inspect stats directory", error);
  if (!fs::is_directory(status))
    return td::Status::Error("Stats path must be a directory, not a symlink or file");
  SnapshotFiles files;
  fs::directory_iterator it(directory_, error), end;
  if (error)
    return filesystem_error("Cannot list stats directory", error);
  for (; it != end; it.increment(error)) {
    if (error)
      return filesystem_error("Cannot list stats directory", error);
    auto name = it->path().filename().string();
    auto id = snapshot_id(name);
    if (!id)
      continue;
    auto entry_status = it->symlink_status(error);
    if (error)
      return filesystem_error("Cannot inspect stats snapshot", error);
    if (fs::is_regular_file(entry_status))
      files[*id].push_back(std::move(name));
  }
  if (error)
    return filesystem_error("Cannot list stats directory", error);
  return files;
}

td::Status StatsSnapshotStore::prune(const SnapshotFiles& files) const {
  auto it = files.begin();
  for (std::size_t i = kMaxSnapshots; i < files.size(); ++i, ++it) {
    for (const auto& name : it->second) {
      auto path = fs::path(directory_) / name;
      std::error_code error;
      auto status = fs::symlink_status(path, error);
      if (error == std::errc::no_such_file_or_directory)
        continue;
      if (error)
        return filesystem_error("Cannot inspect old stats snapshot", error);
      if (!fs::is_regular_file(status))
        continue;
      fs::remove(path, error);
      if (error)
        return filesystem_error("Cannot remove old stats snapshot", error);
    }
  }
  return td::Status::OK();
}

td::Status StatsSnapshotStore::initialize() {
  std::error_code error;
  const bool created = fs::create_directories(directory_, error);
  if (error)
    return filesystem_error("Cannot create stats directory", error);
  if (created) {
    fs::permissions(directory_, fs::perms::owner_all, fs::perm_options::replace, error);
    if (error)
      return filesystem_error("Cannot set stats directory permissions", error);
  }
  TRY_RESULT(files, snapshots());
  if (!files.empty())
    last_id_ = files.rbegin()->first;
  return prune(files);
}

td::Status StatsSnapshotStore::save(const StatsSnapshot& snapshot) {
  TRY_RESULT(files, snapshots());
  // If an earlier rotation failed, do not keep adding files without a bound.
  TRY_STATUS(prune(files));
  while (files.size() > kMaxSnapshots)
    files.erase(files.begin());
  if (!files.empty())
    last_id_ = std::max(last_id_, files.rbegin()->first);
  if (last_id_ == std::numeric_limits<std::uint64_t>::max())
    return td::Status::Error("Stats snapshot ID overflow");
  auto id = std::max(snapshot.captured_at_seconds, last_id_ + 1);
  auto name = snapshot_name(kStatsPrefix, id);
  auto actor_name = snapshot_name(kActorStatsPrefix, id);
  auto header = snapshot_header(snapshot);
  std::ostringstream stats_contents;
  stats_contents << header << "# window_seconds: " << snapshot.window_seconds << "\n\n" << snapshot.report;
  std::vector<std::pair<std::string, std::string>> reports{{name, stats_contents.str()}};
  if (snapshot.actor_report) {
    reports.emplace_back(actor_name, header + "# columns: 10 seconds, 10 minutes, since actor-stats startup\n\n" +
                                         *snapshot.actor_report);
  }

  std::vector<fs::path> temporary_files;
  std::vector<fs::path> published_files;
  auto cleanup = td::ScopeExit() + [&] {
    std::error_code ignored;
    for (const auto& path : temporary_files)
      fs::remove(path, ignored);
    // Roll back a partly published pair on a recoverable error.
    for (const auto& path : published_files)
      fs::remove(path, ignored);
  };
  std::error_code error;
  for (const auto& [report_name, contents] : reports) {
    auto path = fs::path(directory_) / report_name;
    // Protect unrelated entries (including dangling symlinks) from overwrite.
    if (fs::exists(fs::symlink_status(path, error)))
      return td::Status::Error("Stats snapshot path already exists");
    if (error && error != std::errc::no_such_file_or_directory)
      return filesystem_error("Cannot inspect snapshot path", error);
    auto temporary = path.string() + ".tmp";
    TRY_STATUS(write_temporary_file(temporary, contents));
    temporary_files.push_back(std::move(temporary));
  }
  for (std::size_t i = 0; i < reports.size(); ++i) {
    auto path = fs::path(directory_) / reports[i].first;
    fs::rename(temporary_files[i], path, error);
    if (error)
      return filesystem_error("Cannot publish stats snapshot", error);
    published_files.push_back(std::move(path));
  }
  cleanup.dismiss();
  last_id_ = id;
  files[id].push_back(std::move(name));
  if (snapshot.actor_report)
    files[id].push_back(std::move(actor_name));
  return prune(files);
}

struct StatsFileWriter::Impl {
  explicit Impl(StatsSnapshotStore store) : store(std::move(store)), thread([this] { run(); }) {
    thread.set_name("stats-io");
  }
  ~Impl() {
    {
      std::lock_guard lock(mutex);
      stopping = true;
    }
    cv.notify_one();
    thread.join();
  }
  void run() {
    while (true) {
      StatsSnapshot snapshot;
      {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return stopping || pending.has_value(); });
        if (!pending)
          return;
        snapshot = std::move(*pending);
        pending.reset();
        writing = true;
      }
      try {
        auto status = store.save(snapshot);
        if (status.is_error())
          LOG(ERROR) << "Failed to save stats: " << status;
      } catch (const std::exception& error) {
        LOG(ERROR) << "Failed to save stats: " << error.what();
      }
      {
        std::lock_guard lock(mutex);
        writing = false;
      }
    }
  }
  StatsSnapshotStore store;
  mutable std::mutex mutex;
  std::condition_variable cv;
  std::optional<StatsSnapshot> pending;
  bool writing = false;
  bool stopping = false;
  td::thread thread;
};

StatsFileWriter::StatsFileWriter(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {
}

StatsFileWriter::~StatsFileWriter() = default;

td::Result<std::shared_ptr<StatsFileWriter>> StatsFileWriter::create(const std::string& working_dir) {
  StatsSnapshotStore store(working_dir);
  TRY_STATUS(store.initialize());
  return std::shared_ptr<StatsFileWriter>(new StatsFileWriter(std::make_unique<Impl>(std::move(store))));
}

bool StatsFileWriter::busy() const {
  std::lock_guard lock(impl_->mutex);
  return impl_->writing || impl_->pending.has_value() || impl_->stopping;
}

bool StatsFileWriter::submit(StatsSnapshot snapshot) {
  {
    std::lock_guard lock(impl_->mutex);
    if (impl_->writing || impl_->pending || impl_->stopping)
      return false;
    impl_->pending = std::move(snapshot);
  }
  impl_->cv.notify_one();
  return true;
}

StatsRecorder::StatsRecorder(double interval_seconds, bool actor_stats_enabled, std::shared_ptr<StatsFileWriter> writer,
                             std::function<std::string()> collect_statistics)
    : interval_seconds_(interval_seconds)
    , actor_stats_enabled_(actor_stats_enabled)
    , writer_(std::move(writer))
    , collect_statistics_(std::move(collect_statistics)) {
}

void StatsRecorder::start_up() {
  CHECK(interval_seconds_ >= 1 && writer_ && collect_statistics_);
  if (actor_stats_enabled_)
    stats_ = td::actor::create_actor<td::actor::ActorStats>("ActorStats");
  last_collection_at_ = td::Time::now();
  alarm_timestamp() = td::Timestamp::in(interval_seconds_);
}

void StatsRecorder::alarm() {
  alarm_timestamp() = td::Timestamp::in(interval_seconds_);
  // No catch-up bursts and no growing queue if collection or disk is slow.
  if (report_pending_ || writer_->busy())
    return;
  if (!actor_stats_enabled_) {
    save_snapshot(std::nullopt);
    return;
  }
  report_pending_ = true;
  auto promise = td::PromiseCreator::lambda([self = actor_id(this)](td::Result<std::string> result) mutable {
    td::actor::send_closure(self, &StatsRecorder::report_ready, std::move(result));
  });
  td::actor::send_closure(stats_, &td::actor::ActorStats::prepare_stats, std::move(promise));
}

void StatsRecorder::report_ready(td::Result<std::string> result) {
  report_pending_ = false;
  if (result.is_error()) {
    LOG(ERROR) << "Failed to collect actor stats: " << result.move_as_error();
    return;
  }
  save_snapshot(result.move_as_ok());
}

void StatsRecorder::save_snapshot(std::optional<std::string> actor_report) {
  if (writer_->busy())
    return;
  auto captured_at_seconds = unix_time_seconds();
  auto now = td::Time::now();
  auto report = collect_statistics_();
  // This recorder is the writer's only producer, so no other submit can race
  // the busy check and discard counters that have already been reset.
  CHECK(writer_->submit(StatsSnapshot{std::move(report), std::move(actor_report), captured_at_seconds,
                                      interval_seconds_, now - last_collection_at_}));
  last_collection_at_ = now;
}

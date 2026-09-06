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

#include "ActorStatsRecorder.h"

namespace {
namespace fs = std::filesystem;
constexpr std::string_view kPrefix = "actor-stats-";
constexpr std::string_view kSuffix = ".txt";
constexpr std::size_t kIdWidth = 20;

std::optional<std::uint64_t> snapshot_id(std::string_view name) {
  if (name.size() != kPrefix.size() + kIdWidth + kSuffix.size() || !name.starts_with(kPrefix) ||
      !name.ends_with(kSuffix))
    return {};
  auto digits = name.substr(kPrefix.size(), kIdWidth);
  if (!std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; }))
    return {};
  std::uint64_t id;
  auto result = std::from_chars(digits.data(), digits.data() + digits.size(), id);
  if (result.ec != std::errc{})
    return {};
  return id;
}

std::string snapshot_name(std::uint64_t id) {
  std::ostringstream name;
  name << kPrefix << std::setfill('0') << std::setw(kIdWidth) << id << kSuffix;
  return name.str();
}

td::Status filesystem_error(const std::string& operation, const std::error_code& error) {
  return td::Status::Error(operation + ": " + error.message());
}

std::uint64_t unix_time_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string snapshot_contents(const ActorStatsSnapshot& snapshot) {
  auto seconds = static_cast<std::time_t>(snapshot.captured_at_us / 1000000);
  std::tm utc{};
#if TD_PORT_WINDOWS
  gmtime_s(&utc, &seconds);
#else
  gmtime_r(&seconds, &utc);
#endif
  std::ostringstream header;
  header << "# captured_at_utc: " << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S") << '.' << std::setfill('0')
         << std::setw(6) << snapshot.captured_at_us % 1000000 << "Z\n"
         << "# interval_seconds: " << snapshot.interval_seconds << "\n"
         << "# columns: 10 seconds, 10 minutes, since actor-stats startup\n\n";
  return header.str() + snapshot.report;
}
}  // namespace

td::Result<double> parse_actor_stats_interval(td::Slice value) {
  try {
    auto text = value.str();
    std::size_t consumed = 0;
    auto interval = std::stod(text, &consumed);
    if (consumed == text.size() && std::isfinite(interval) && interval >= 0)
      return interval;
  } catch (...) {
  }
  return td::Status::Error("--actor-stats-interval must be a finite nonnegative number of seconds (0 disables it)");
}

ActorStatsSnapshotStore::ActorStatsSnapshotStore(const std::string& working_dir)
    : directory_((fs::path(working_dir) / "actor-stats").string()) {
}

const std::string& ActorStatsSnapshotStore::directory() const {
  return directory_;
}

td::Result<std::vector<std::string>> ActorStatsSnapshotStore::snapshots() const {
  std::error_code error;
  // Do not follow an actor-stats directory replaced by a symlink.
  auto status = fs::symlink_status(directory_, error);
  if (error)
    return filesystem_error("Cannot inspect actor stats directory", error);
  if (!fs::is_directory(status))
    return td::Status::Error("Actor stats path must be a directory, not a symlink or file");
  std::vector<std::string> files;
  fs::directory_iterator it(directory_, error), end;
  if (error)
    return filesystem_error("Cannot list actor stats directory", error);
  for (; it != end; it.increment(error)) {
    if (error)
      return filesystem_error("Cannot list actor stats directory", error);
    auto name = it->path().filename().string();
    if (!snapshot_id(name))
      continue;
    auto entry_status = it->symlink_status(error);
    if (error)
      return filesystem_error("Cannot inspect actor stats snapshot", error);
    if (fs::is_regular_file(entry_status))
      files.push_back(std::move(name));
  }
  if (error)
    return filesystem_error("Cannot list actor stats directory", error);
  std::sort(files.begin(), files.end());
  return files;
}

td::Status ActorStatsSnapshotStore::prune(const std::vector<std::string>& files) const {
  for (std::size_t i = kMaxSnapshots; i < files.size(); ++i) {
    auto path = fs::path(directory_) / files[i - kMaxSnapshots];
    std::error_code error;
    auto status = fs::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory)
      continue;
    if (error)
      return filesystem_error("Cannot inspect old actor stats snapshot", error);
    if (!fs::is_regular_file(status))
      continue;
    fs::remove(path, error);
    if (error)
      return filesystem_error("Cannot remove old actor stats snapshot", error);
  }
  return td::Status::OK();
}

td::Status ActorStatsSnapshotStore::initialize() {
  std::error_code error;
  const bool created = fs::create_directories(directory_, error);
  if (error)
    return filesystem_error("Cannot create actor stats directory", error);
  if (created) {
    fs::permissions(directory_, fs::perms::owner_all, fs::perm_options::replace, error);
    if (error)
      return filesystem_error("Cannot set actor stats directory permissions", error);
  }
  TRY_RESULT(files, snapshots());
  if (!files.empty())
    last_id_ = *snapshot_id(files.back());
  return prune(files);
}

td::Status ActorStatsSnapshotStore::save(const ActorStatsSnapshot& snapshot) {
  TRY_RESULT(files, snapshots());
  // If an earlier rotation failed, do not keep adding files without a bound.
  TRY_STATUS(prune(files));
  if (files.size() > kMaxSnapshots)
    files.erase(files.begin(), files.end() - kMaxSnapshots);
  if (!files.empty())
    last_id_ = std::max(last_id_, *snapshot_id(files.back()));
  if (last_id_ == std::numeric_limits<std::uint64_t>::max())
    return td::Status::Error("Actor stats snapshot ID overflow");
  auto id = std::max(snapshot.captured_at_us, last_id_ + 1);
  auto name = snapshot_name(id);
  auto path = fs::path(directory_) / name;
  std::error_code error;
  // Protect unrelated entries (including dangling symlinks) from overwrite.
  if (fs::exists(fs::symlink_status(path, error)))
    return td::Status::Error("Actor stats snapshot path already exists");
  if (error && error != std::errc::no_such_file_or_directory)
    return filesystem_error("Cannot inspect snapshot path", error);

  auto temporary = path.string() + ".tmp";
  TRY_RESULT(file, td::FileFd::open(temporary, td::FileFd::Write | td::FileFd::CreateNew, 0600));
  SCOPE_EXIT {
    file.close();
    std::error_code ignored;
    fs::remove(temporary, ignored);
  };
  TRY_STATUS(file.write_all(snapshot_contents(snapshot)));
  TRY_STATUS(file.sync());
  file.close();
  fs::rename(temporary, path, error);
  if (error)
    return filesystem_error("Cannot publish actor stats snapshot", error);
  last_id_ = id;
  files.push_back(std::move(name));
  return prune(files);
}

struct ActorStatsFileWriter::Impl {
  explicit Impl(ActorStatsSnapshotStore store) : store(std::move(store)), thread([this] { run(); }) {
    thread.set_name("actor-stats-io");
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
      ActorStatsSnapshot snapshot;
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
          LOG(ERROR) << "Failed to save actor stats: " << status;
      } catch (const std::exception& error) {
        LOG(ERROR) << "Failed to save actor stats: " << error.what();
      }
      {
        std::lock_guard lock(mutex);
        writing = false;
      }
    }
  }
  ActorStatsSnapshotStore store;
  mutable std::mutex mutex;
  std::condition_variable cv;
  std::optional<ActorStatsSnapshot> pending;
  bool writing = false;
  bool stopping = false;
  td::thread thread;
};

ActorStatsFileWriter::ActorStatsFileWriter(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {
}

ActorStatsFileWriter::~ActorStatsFileWriter() = default;

td::Result<std::shared_ptr<ActorStatsFileWriter>> ActorStatsFileWriter::create(const std::string& working_dir) {
  ActorStatsSnapshotStore store(working_dir);
  TRY_STATUS(store.initialize());
  return std::shared_ptr<ActorStatsFileWriter>(new ActorStatsFileWriter(std::make_unique<Impl>(std::move(store))));
}

bool ActorStatsFileWriter::busy() const {
  std::lock_guard lock(impl_->mutex);
  return impl_->writing || impl_->pending.has_value() || impl_->stopping;
}

bool ActorStatsFileWriter::submit(ActorStatsSnapshot snapshot) {
  {
    std::lock_guard lock(impl_->mutex);
    if (impl_->writing || impl_->pending || impl_->stopping)
      return false;
    impl_->pending = std::move(snapshot);
  }
  impl_->cv.notify_one();
  return true;
}

ActorStatsRecorder::ActorStatsRecorder(double interval_seconds, std::shared_ptr<ActorStatsFileWriter> writer)
    : interval_seconds_(interval_seconds), writer_(std::move(writer)) {
}

void ActorStatsRecorder::start_up() {
  CHECK(interval_seconds_ > 0 && writer_);
  stats_ = td::actor::create_actor<td::actor::ActorStats>("ActorStats");
  alarm_timestamp() = td::Timestamp::in(interval_seconds_);
}

void ActorStatsRecorder::alarm() {
  alarm_timestamp() = td::Timestamp::in(interval_seconds_);
  // No catch-up bursts and no growing queue if collection or disk is slow.
  if (report_pending_ || writer_->busy())
    return;
  report_pending_ = true;
  auto promise = td::PromiseCreator::lambda([self = actor_id(this)](td::Result<std::string> result) mutable {
    td::actor::send_closure(self, &ActorStatsRecorder::report_ready, std::move(result), unix_time_us());
  });
  td::actor::send_closure(stats_, &td::actor::ActorStats::prepare_stats, std::move(promise));
}

void ActorStatsRecorder::report_ready(td::Result<std::string> result, std::uint64_t captured_at_us) {
  report_pending_ = false;
  if (result.is_error()) {
    LOG(ERROR) << "Failed to collect actor stats: " << result.move_as_error();
    return;
  }
  writer_->submit(ActorStatsSnapshot{result.move_as_ok(), captured_at_us, interval_seconds_});
}

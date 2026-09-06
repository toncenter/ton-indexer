#include <chrono>
#include <filesystem>
#include <iomanip>
#include <sstream>

#include "td/utils/filesystem.h"
#include "td/utils/port/path.h"
#include "td/utils/tests.h"

#include "ActorStatsRecorder.h"

namespace {
namespace fs = std::filesystem;

class TempDirectory {
 public:
  TempDirectory() : path(td::mkdtemp("/tmp", "ton-actor-stats-test").move_as_ok()) {
  }
  ~TempDirectory() {
    // Only this test's freshly allocated directory is removed, never a supplied path.
    std::error_code ignored;
    fs::remove_all(path, ignored);
  }
  std::string path;
};

std::string name(std::uint64_t id) {
  std::ostringstream result;
  result << "actor-stats-" << std::setfill('0') << std::setw(20) << id << ".txt";
  return result.str();
}

void write_fixture(const fs::path& path, const std::string& text = "old snapshot") {
  td::write_file(path.string(), text, {.need_sync = false, .need_lock = false}).ensure();
}

std::vector<fs::path> snapshots(const fs::path& directory) {
  std::vector<fs::path> result;
  for (const auto& entry : fs::directory_iterator(directory)) {
    // The writer may rename a temporary file while this observer lists it.
    if (entry.path().extension() != ".txt" || !entry.path().filename().string().starts_with("actor-stats-")) {
      continue;
    }
    std::error_code error;
    if (fs::is_regular_file(entry.symlink_status(error)) && !error) {
      result.push_back(entry.path());
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

class ActorStatsTestWorker : public td::actor::Actor {
  void start_up() override {
    alarm_timestamp() = td::Timestamp::in(0.01);
  }
  void alarm() override {
    alarm_timestamp() = td::Timestamp::in(0.01);
  }
};
}  // namespace

TEST(ActorStatsRecorder, interval_validation) {
  ASSERT_EQ(0.0, parse_actor_stats_interval("0").move_as_ok());
  ASSERT_EQ(10.0, parse_actor_stats_interval("10").move_as_ok());
  ASSERT_EQ(0.25, parse_actor_stats_interval("0.25").move_as_ok());
  for (const auto* value : {"", "-1", "nan", "inf", "1e999", "10seconds", "1 2"}) {
    ASSERT_TRUE(parse_actor_stats_interval(td::Slice(value)).is_error());
  }
}

TEST(ActorStatsRecorder, saves_complete_private_snapshot_with_metadata) {
  TempDirectory temporary;
  ActorStatsSnapshotStore store(temporary.path);
  store.initialize().ensure();
  store.save({"report body\n", 1000000, 10}).ensure();
  auto files = snapshots(store.directory());
  ASSERT_EQ(1u, files.size());
  ASSERT_EQ(name(1000000), files.front().filename().string());
  auto text = td::read_file_str(files.front().string()).move_as_ok();
  ASSERT_TRUE(text.find("# captured_at_utc: 1970-01-01T00:00:01.000000Z") != std::string::npos);
  ASSERT_TRUE(text.find("# interval_seconds: 10") != std::string::npos);
  ASSERT_TRUE(text.ends_with("report body\n"));
  ASSERT_TRUE(!fs::exists(files.front().string() + ".tmp"));
#if TD_PORT_POSIX
  auto permissions = fs::status(files.front()).permissions();
  ASSERT_TRUE((permissions & (fs::perms::group_all | fs::perms::others_all)) == fs::perms::none);
#endif
}

TEST(ActorStatsRecorder, retains_latest_500_across_restart_and_clock_rollback) {
  TempDirectory temporary;
  auto directory = fs::path(temporary.path) / "actor-stats";
  fs::create_directory(directory);
  for (std::uint64_t id = 1; id <= 501; ++id)
    write_fixture(directory / name(id));

  ActorStatsSnapshotStore first(temporary.path);
  first.initialize().ensure();
  ASSERT_EQ(500u, snapshots(directory).size());
  ASSERT_TRUE(!fs::exists(directory / name(1)));
  first.save({"first new report", 502, 10}).ensure();
  ASSERT_EQ(500u, snapshots(directory).size());
  ASSERT_TRUE(!fs::exists(directory / name(2)));

  ActorStatsSnapshotStore restarted(temporary.path);
  restarted.initialize().ensure();
  restarted.save({"after restart and backwards clock", 1, 10}).ensure();
  auto files = snapshots(directory);
  ASSERT_EQ(500u, files.size());
  ASSERT_EQ(name(4), files.front().filename().string());
  ASSERT_EQ(name(503), files.back().filename().string());
  ASSERT_TRUE(
      td::read_file_str((directory / name(503)).string()).move_as_ok().ends_with("after restart and backwards clock"));
}

TEST(ActorStatsRecorder, rotation_preserves_unrelated_files_directories_and_symlinks) {
  TempDirectory temporary;
  ActorStatsSnapshotStore store(temporary.path);
  store.initialize().ensure();
  auto directory = fs::path(store.directory());
  for (std::uint64_t id = 1; id <= 500; ++id)
    write_fixture(directory / name(id));
  write_fixture(directory / "notes.txt", "keep notes");
  write_fixture(directory / "actor-stats-manual.txt", "keep manual");
  write_fixture(directory / "actor-stats-00000000000000000000.txt.tmp", "keep unrelated temp");
  fs::create_directory(directory / name(9999));
  write_fixture(directory / name(9999) / "inside", "keep nested");
#if TD_PORT_POSIX
  write_fixture(fs::path(temporary.path) / "outside", "keep target");
  fs::create_symlink(fs::path(temporary.path) / "outside", directory / name(0));
#endif
  store.save({"new", 501, 1}).ensure();
  ASSERT_TRUE(!fs::exists(directory / name(1)));
  ASSERT_EQ("keep notes", td::read_file_str((directory / "notes.txt").string()).move_as_ok());
  ASSERT_EQ("keep manual", td::read_file_str((directory / "actor-stats-manual.txt").string()).move_as_ok());
  ASSERT_TRUE(fs::exists(directory / "actor-stats-00000000000000000000.txt.tmp"));
  ASSERT_EQ("keep nested", td::read_file_str((directory / name(9999) / "inside").string()).move_as_ok());
#if TD_PORT_POSIX
  ASSERT_TRUE(fs::is_symlink(fs::symlink_status(directory / name(0))));
  ASSERT_EQ("keep target", td::read_file_str((fs::path(temporary.path) / "outside").string()).move_as_ok());
#endif
}

#if TD_PORT_POSIX
TEST(ActorStatsRecorder, refuses_symlink_directory_and_does_not_overwrite_temporary_symlink) {
  TempDirectory temporary;
  auto outside = fs::path(temporary.path) / "outside";
  fs::create_directory(outside);
  auto working = fs::path(temporary.path) / "work";
  fs::create_directory(working);
  fs::create_directory_symlink(outside, working / "actor-stats");
  ActorStatsSnapshotStore symlink_store(working.string());
  ASSERT_TRUE(symlink_store.initialize().is_error());
  ASSERT_TRUE(fs::is_empty(outside));

  ActorStatsSnapshotStore store(temporary.path);
  store.initialize().ensure();
  store.save({"previous", 1, 10}).ensure();
  write_fixture(outside / "target", "must survive");
  fs::create_symlink(outside / "target", fs::path(store.directory()) / (name(2) + ".tmp"));
  ASSERT_TRUE(store.save({"new", 2, 10}).is_error());
  ASSERT_EQ("must survive", td::read_file_str((outside / "target").string()).move_as_ok());
  ASSERT_TRUE(td::read_file_str((fs::path(store.directory()) / name(1)).string()).move_as_ok().ends_with("previous"));
  ASSERT_TRUE(!fs::exists(fs::path(store.directory()) / name(2)));
}
#endif

TEST(ActorStatsRecorder, writer_drains_submitted_snapshot_on_shutdown) {
  TempDirectory temporary;
  auto writer = ActorStatsFileWriter::create(temporary.path).move_as_ok();
  ASSERT_TRUE(writer->submit({"final snapshot", 123, 10}));
  writer.reset();
  auto files = snapshots(fs::path(temporary.path) / "actor-stats");
  ASSERT_EQ(1u, files.size());
  ASSERT_TRUE(td::read_file_str(files.front().string()).move_as_ok().ends_with("final snapshot"));
}

TEST(ActorStatsRecorder, periodically_saves_real_actor_statistics) {
  TempDirectory temporary;
  auto writer = ActorStatsFileWriter::create(temporary.path).move_as_ok();
  auto previous_debug = td::actor::core::need_debug();
  td::actor::set_debug(true);
  {
    td::actor::Scheduler scheduler({1});
    scheduler.run_in_context([&] {
      td::actor::create_actor<ActorStatsRecorder>("Recorder", 0.03, writer).release();
      td::actor::create_actor<ActorStatsTestWorker>("MeasuredWorker").release();
    });
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (snapshots(fs::path(temporary.path) / "actor-stats").size() < 3 &&
           std::chrono::steady_clock::now() < deadline) {
      scheduler.run(0.01);
    }
    scheduler.stop();
  }
  td::actor::set_debug(previous_debug);
  writer.reset();
  auto files = snapshots(fs::path(temporary.path) / "actor-stats");
  ASSERT_TRUE(files.size() >= 3);
  auto report = td::read_file_str(files.back().string()).move_as_ok();
  ASSERT_TRUE(report.find("ACTORS STATS") != std::string::npos);
  ASSERT_TRUE(report.find("ActorStatsTestWorker") != std::string::npos);
  ASSERT_TRUE(report.find("load_per_second") != std::string::npos);
  ASSERT_TRUE(report.find("max_delay") != std::string::npos);
  ASSERT_TRUE(report.find("# interval_seconds: 0.03") != std::string::npos);
}

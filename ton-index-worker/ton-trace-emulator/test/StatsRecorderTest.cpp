#include <chrono>
#include <filesystem>

#include "td/utils/filesystem.h"
#include "td/utils/port/path.h"
#include "td/utils/tests.h"

#include "StatsRecorder.h"

namespace {
namespace fs = std::filesystem;

class TempDirectory {
 public:
  TempDirectory() : path(td::mkdtemp("/tmp", "ton-stats-test").move_as_ok()) {
  }
  ~TempDirectory() {
    // Only this test's freshly allocated directory is removed, never a supplied path.
    std::error_code ignored;
    fs::remove_all(path, ignored);
  }
  std::string path;
};

std::string name(std::uint64_t id, bool actors = false) {
  return std::string(actors ? "actor-stats-" : "stats-") + std::to_string(id) + ".txt";
}

void write_fixture(const fs::path& path, const std::string& text = "old snapshot") {
  td::write_file(path.string(), text, {.need_sync = false, .need_lock = false}).ensure();
}

void write_pair(const fs::path& directory, std::uint64_t id) {
  write_fixture(directory / name(id));
  write_fixture(directory / name(id, true));
}

std::vector<fs::path> snapshots(const fs::path& directory, bool actors = false) {
  std::vector<fs::path> result;
  for (const auto& entry : fs::directory_iterator(directory)) {
    // The writer may rename a temporary file while this observer lists it.
    if (entry.path().extension() != ".txt" ||
        !entry.path().filename().string().starts_with(actors ? "actor-stats-" : "stats-")) {
      continue;
    }
    std::error_code error;
    if (fs::is_regular_file(entry.symlink_status(error)) && !error) {
      result.push_back(entry.path());
    }
  }
  std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
    auto left = a.filename().string(), right = b.filename().string();
    return std::stoull(left.substr(left.rfind('-') + 1)) < std::stoull(right.substr(right.rfind('-') + 1));
  });
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

TEST(StatsRecorder, interval_validation) {
  ASSERT_EQ(0.0, parse_actor_stats_interval("0").move_as_ok());
  ASSERT_EQ(10.0, parse_actor_stats_interval("10").move_as_ok());
  ASSERT_EQ(1.0, parse_actor_stats_interval("1").move_as_ok());
  ASSERT_EQ(1.5, parse_actor_stats_interval("1.5").move_as_ok());
  for (const auto* value : {"", "-1", "0.25", "0.999", "nan", "inf", "1e999", "10seconds", "1 2"}) {
    ASSERT_TRUE(parse_actor_stats_interval(td::Slice(value)).is_error());
  }
}

TEST(StatsRecorder, saves_complete_private_pair_with_shared_metadata) {
  TempDirectory temporary;
  StatsSnapshotStore store(temporary.path);
  store.initialize().ensure();
  store.save({"report body\n", "actor report body\n", 1, 10, 12.5}).ensure();
  auto files = snapshots(store.directory());
  ASSERT_EQ(1u, files.size());
  ASSERT_EQ("stats-1.txt", files.front().filename().string());
  ASSERT_EQ(1u, snapshots(store.directory(), true).size());
  auto text = td::read_file_str(files.front().string()).move_as_ok();
  ASSERT_TRUE(text.find("# captured_at_utc: 1970-01-01T00:00:01Z") != std::string::npos);
  ASSERT_TRUE(text.find("# interval_seconds: 10") != std::string::npos);
  ASSERT_TRUE(text.find("# window_seconds: 12.5") != std::string::npos);
  ASSERT_TRUE(text.find("# columns:") == std::string::npos);
  ASSERT_TRUE(text.ends_with("report body\n"));
  ASSERT_TRUE(!fs::exists(files.front().string() + ".tmp"));
  auto actor_file = fs::path(store.directory()) / "actor-stats-1.txt";
  auto actor_text = td::read_file_str(actor_file.string()).move_as_ok();
  ASSERT_TRUE(actor_text.starts_with(text.substr(0, text.find("# window_seconds:"))));
  ASSERT_TRUE(actor_text.find("# columns: 10 seconds, 10 minutes, since actor-stats startup") != std::string::npos);
  ASSERT_TRUE(actor_text.ends_with("actor report body\n"));
  ASSERT_TRUE(!fs::exists(actor_file.string() + ".tmp"));
  ASSERT_TRUE(!fs::exists(fs::path(temporary.path) / "stats.txt"));
  ASSERT_TRUE(!fs::exists(fs::path(temporary.path) / "actor-stats"));
#if TD_PORT_POSIX
  for (const auto& file : {files.front(), actor_file}) {
    auto permissions = fs::status(file).permissions();
    ASSERT_TRUE((permissions & (fs::perms::group_all | fs::perms::others_all)) == fs::perms::none);
  }
#endif
}

TEST(StatsRecorder, retains_latest_500_pairs_across_restart_and_clock_rollback) {
  TempDirectory temporary;
  auto directory = fs::path(temporary.path) / "stats";
  fs::create_directory(directory);
  for (std::uint64_t id = 1; id <= 501; ++id)
    write_pair(directory, id);

  StatsSnapshotStore first(temporary.path);
  first.initialize().ensure();
  ASSERT_EQ(500u, snapshots(directory).size());
  ASSERT_EQ(500u, snapshots(directory, true).size());
  ASSERT_TRUE(!fs::exists(directory / name(1)));
  ASSERT_TRUE(!fs::exists(directory / name(1, true)));
  first.save({"first new report", "first actor report", 502, 10, 10}).ensure();
  ASSERT_EQ(500u, snapshots(directory).size());
  ASSERT_EQ(500u, snapshots(directory, true).size());
  ASSERT_TRUE(!fs::exists(directory / name(2)));
  ASSERT_TRUE(!fs::exists(directory / name(2, true)));

  StatsSnapshotStore restarted(temporary.path);
  restarted.initialize().ensure();
  restarted.save({"after restart and backwards clock", "after restart actor report", 1, 10, 10}).ensure();
  auto files = snapshots(directory);
  ASSERT_EQ(500u, files.size());
  auto actor_files = snapshots(directory, true);
  ASSERT_EQ(500u, actor_files.size());
  ASSERT_EQ(name(4), files.front().filename().string());
  ASSERT_EQ(name(503), files.back().filename().string());
  ASSERT_EQ(name(4, true), actor_files.front().filename().string());
  ASSERT_EQ(name(503, true), actor_files.back().filename().string());
  ASSERT_TRUE(
      td::read_file_str((directory / name(503)).string()).move_as_ok().ends_with("after restart and backwards clock"));
}

TEST(StatsRecorder, rotation_preserves_unrelated_files_directories_and_symlinks) {
  TempDirectory temporary;
  StatsSnapshotStore store(temporary.path);
  store.initialize().ensure();
  auto directory = fs::path(store.directory());
  for (std::uint64_t id = 1; id <= 500; ++id)
    write_pair(directory, id);
  write_fixture(directory / "notes.txt", "keep notes");
  write_fixture(directory / "actor-stats-manual.txt", "keep manual");
  write_fixture(directory / "stats-manual.txt", "keep stats manual");
  write_fixture(directory / "stats-18446744073709551616.txt", "keep overflowing id");
  write_fixture(directory / "stats-0001.txt", "keep noncanonical name");
  write_fixture(directory / "actor-stats-0.txt.tmp", "keep unrelated temp");
  fs::create_directory(directory / name(9999));
  write_fixture(directory / name(9999) / "inside", "keep nested");
#if TD_PORT_POSIX
  write_fixture(fs::path(temporary.path) / "outside", "keep target");
  fs::create_symlink(fs::path(temporary.path) / "outside", directory / name(0));
#endif
  store.save({"new", "new actor report", 501, 1, 1}).ensure();
  ASSERT_TRUE(!fs::exists(directory / name(1)));
  ASSERT_TRUE(!fs::exists(directory / name(1, true)));
  ASSERT_EQ("keep notes", td::read_file_str((directory / "notes.txt").string()).move_as_ok());
  ASSERT_EQ("keep manual", td::read_file_str((directory / "actor-stats-manual.txt").string()).move_as_ok());
  ASSERT_EQ("keep stats manual", td::read_file_str((directory / "stats-manual.txt").string()).move_as_ok());
  ASSERT_TRUE(fs::exists(directory / "stats-18446744073709551616.txt"));
  ASSERT_TRUE(fs::exists(directory / "stats-0001.txt"));
  ASSERT_TRUE(fs::exists(directory / "actor-stats-0.txt.tmp"));
  ASSERT_EQ("keep nested", td::read_file_str((directory / name(9999) / "inside").string()).move_as_ok());
#if TD_PORT_POSIX
  ASSERT_TRUE(fs::is_symlink(fs::symlink_status(directory / name(0))));
  ASSERT_EQ("keep target", td::read_file_str((fs::path(temporary.path) / "outside").string()).move_as_ok());
#endif
}

#if TD_PORT_POSIX
TEST(StatsRecorder, refuses_symlink_directory_and_cleans_up_failed_pair) {
  TempDirectory temporary;
  auto outside = fs::path(temporary.path) / "outside";
  fs::create_directory(outside);
  auto working = fs::path(temporary.path) / "work";
  fs::create_directory(working);
  fs::create_directory_symlink(outside, working / "stats");
  StatsSnapshotStore symlink_store(working.string());
  ASSERT_TRUE(symlink_store.initialize().is_error());
  ASSERT_TRUE(fs::is_empty(outside));

  StatsSnapshotStore store(temporary.path);
  store.initialize().ensure();
  store.save({"previous", "previous actor report", 1, 10, 10}).ensure();
  write_fixture(outside / "target", "must survive");
  auto actor_temporary = fs::path(store.directory()) / (name(2, true) + ".tmp");
  fs::create_symlink(outside / "target", actor_temporary);
  ASSERT_TRUE(store.save({"new", "new actor report", 2, 10, 10}).is_error());
  ASSERT_EQ("must survive", td::read_file_str((outside / "target").string()).move_as_ok());
  ASSERT_TRUE(td::read_file_str((fs::path(store.directory()) / name(1)).string()).move_as_ok().ends_with("previous"));
  ASSERT_TRUE(!fs::exists(fs::path(store.directory()) / name(2)));
  ASSERT_TRUE(!fs::exists(fs::path(store.directory()) / name(2, true)));
  ASSERT_TRUE(!fs::exists(fs::path(store.directory()) / (name(2) + ".tmp")));
  ASSERT_TRUE(fs::is_symlink(fs::symlink_status(actor_temporary)));

  // A collision at the final actor path must also leave no half pair.
  fs::create_symlink(outside / "missing", fs::path(store.directory()) / name(3, true));
  ASSERT_TRUE(store.save({"new", "new actor report", 3, 10, 10}).is_error());
  ASSERT_TRUE(!fs::exists(fs::path(store.directory()) / name(3)));
  ASSERT_TRUE(!fs::exists(fs::path(store.directory()) / (name(3) + ".tmp")));
  ASSERT_TRUE(fs::is_symlink(fs::symlink_status(fs::path(store.directory()) / name(3, true))));
}
#endif

TEST(StatsRecorder, writer_drains_submitted_pair_on_shutdown) {
  TempDirectory temporary;
  auto writer = StatsFileWriter::create(temporary.path).move_as_ok();
  ASSERT_TRUE(writer->submit({"final snapshot", "final actor snapshot", 123, 10, 10}));
  writer.reset();
  auto files = snapshots(fs::path(temporary.path) / "stats");
  ASSERT_EQ(1u, files.size());
  ASSERT_TRUE(td::read_file_str(files.front().string()).move_as_ok().ends_with("final snapshot"));
  auto actor_files = snapshots(fs::path(temporary.path) / "stats", true);
  ASSERT_EQ(1u, actor_files.size());
  ASSERT_TRUE(td::read_file_str(actor_files.front().string()).move_as_ok().ends_with("final actor snapshot"));
}

TEST(StatsRecorder, periodically_saves_synchronized_application_and_real_actor_statistics) {
  TempDirectory temporary;
  auto writer = StatsFileWriter::create(temporary.path).move_as_ok();
  std::size_t collections = 0;
  auto previous_debug = td::actor::core::need_debug();
  td::actor::set_debug(true);
  {
    td::actor::Scheduler scheduler({1});
    scheduler.run_in_context([&] {
      td::actor::create_actor<StatsRecorder>("Recorder", 1.0, true, writer, [&] {
        return "application report " + std::to_string(++collections);
      }).release();
      td::actor::create_actor<ActorStatsTestWorker>("MeasuredWorker").release();
    });
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(6);
    while (snapshots(fs::path(temporary.path) / "stats", true).size() < 3 &&
           std::chrono::steady_clock::now() < deadline) {
      scheduler.run(0.01);
    }
    scheduler.stop();
  }
  td::actor::set_debug(previous_debug);
  writer.reset();
  auto files = snapshots(fs::path(temporary.path) / "stats", true);
  ASSERT_TRUE(files.size() >= 3);
  auto application_files = snapshots(fs::path(temporary.path) / "stats");
  ASSERT_EQ(files.size(), application_files.size());
  ASSERT_EQ(collections, application_files.size());
  for (std::size_t i = 0; i < files.size(); ++i) {
    ASSERT_EQ("actor-" + application_files[i].filename().string(), files[i].filename().string());
    auto application_report = td::read_file_str(application_files[i].string()).move_as_ok();
    auto actor_report = td::read_file_str(files[i].string()).move_as_ok();
    ASSERT_TRUE(actor_report.starts_with(application_report.substr(0, application_report.find("# window_seconds:"))));
    ASSERT_TRUE(application_report.ends_with("application report " + std::to_string(i + 1)));
    auto window_start = application_report.find("# window_seconds: ");
    ASSERT_TRUE(window_start != std::string::npos);
    ASSERT_TRUE(std::stod(application_report.substr(window_start + 18)) > 0);
  }
  auto report = td::read_file_str(files.back().string()).move_as_ok();
  ASSERT_TRUE(report.find("ACTORS STATS") != std::string::npos);
  ASSERT_TRUE(report.find("ActorStatsTestWorker") != std::string::npos);
  ASSERT_TRUE(report.find("load_per_second") != std::string::npos);
  ASSERT_TRUE(report.find("max_delay") != std::string::npos);
  ASSERT_TRUE(report.find("# interval_seconds: 1\n") != std::string::npos);
}

TEST(StatsRecorder, application_statistics_continue_when_actor_stats_are_disabled) {
  TempDirectory temporary;
  auto writer = StatsFileWriter::create(temporary.path).move_as_ok();
  std::size_t collections = 0;
  {
    td::actor::Scheduler scheduler({1});
    scheduler.run_in_context([&] {
      td::actor::create_actor<StatsRecorder>("Recorder", 1.0, false, writer, [&] {
        return "application only " + std::to_string(++collections);
      }).release();
    });
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(6);
    while (snapshots(fs::path(temporary.path) / "stats").size() < 3 && std::chrono::steady_clock::now() < deadline) {
      scheduler.run(0.01);
    }
    scheduler.stop();
  }
  writer.reset();
  auto files = snapshots(fs::path(temporary.path) / "stats");
  ASSERT_TRUE(files.size() >= 3);
  ASSERT_EQ(collections, files.size());
  ASSERT_TRUE(snapshots(fs::path(temporary.path) / "stats", true).empty());
  ASSERT_TRUE(td::read_file_str(files.back().string())
                  .move_as_ok()
                  .ends_with("application only " + std::to_string(collections)));
}

TEST(StatsRecorder, rotation_handles_application_only_and_incomplete_snapshots) {
  TempDirectory temporary;
  auto directory = fs::path(temporary.path) / "stats";
  fs::create_directory(directory);
  write_fixture(directory / name(1, true));  // An incomplete pair left by an interrupted publication.
  for (std::uint64_t id = 2; id <= 501; ++id)
    write_fixture(directory / name(id));
  StatsSnapshotStore store(temporary.path);
  store.initialize().ensure();
  ASSERT_TRUE(!fs::exists(directory / name(1, true)));
  store.save({"application only", std::nullopt, 502, 60, 60}).ensure();
  ASSERT_EQ(500u, snapshots(directory).size());
  ASSERT_TRUE(snapshots(directory, true).empty());
  ASSERT_TRUE(!fs::exists(directory / name(2)));
  store.save({"both", "actor report", 503, 10, 10}).ensure();
  ASSERT_EQ(500u, snapshots(directory).size());
  ASSERT_EQ(1u, snapshots(directory, true).size());
  ASSERT_TRUE(!fs::exists(directory / name(3)));
}

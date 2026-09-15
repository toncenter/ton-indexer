#include "SyntheticTrace.h"
#include "TraceProcessor.h"
#include "RedisTransport.h"
#include "Statistics.h"
#include "EnginePrep.h"
#include "td/actor/core/ActorTypeStat.h"
#include "td/utils/port/Clocks.h"
#include "crypto/vm/cp0.h"

#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <sys/resource.h>
#include <time.h>

namespace {
template <class T> bool failed(const td::Result<T>& result) {
  static std::atomic<int> reported{0};
  if (result.is_error() && reported.fetch_add(1) < 5) LOG(ERROR) << result.error();
  return result.is_error();
}

struct Options {
  std::string redis, output, mode = "mixed";
  int blocks = 20, block_ms = 250, traces = 100, nodes = 15, accounts = 4096;
  int payload = 32, threads = 8, workers = mch::EmuClassifierConfig{}.workers, retention_ms = 3000, settle_ms = 1000;
  int drain_seconds = 30, seed = 1;
  int fragment_txs = 0;
  bool measurements = true;

  int stages() const { return fragment_txs ? (nodes + fragment_txs - 1) / fragment_txs : 1 + (nodes > 1); }
  int total_blocks() const { return blocks + stages() - 1; }
};

Options options(int argc, char** argv) {
  Options o;
  const std::map<std::string, int*> integers{
      {"--blocks", &o.blocks}, {"--block-ms", &o.block_ms}, {"--traces-per-block", &o.traces},
      {"--nodes-per-trace", &o.nodes}, {"--accounts", &o.accounts}, {"--payload-bytes", &o.payload},
      {"--threads", &o.threads}, {"--mch-workers", &o.workers}, {"--retention-ms", &o.retention_ms},
      {"--settle-ms", &o.settle_ms}, {"--drain-seconds", &o.drain_seconds}, {"--seed", &o.seed},
      {"--fragment-txs", &o.fragment_txs}};
  for (int i = 1; i < argc; ++i) {
    const std::string key = argv[i];
    if (key == "--help") {
      std::cout << "Use bench/run.py; see bench/README.md for options and measurement scope.\n";
      std::exit(0);
    }
    if (key == "--no-measurements") { o.measurements = false; continue; }
    if (++i == argc) throw std::runtime_error("Missing value for " + key);
    const std::string value = argv[i];
    if (key == "--redis") o.redis = value;
    else if (key == "--output-dir") o.output = value;
    else if (key == "--mode") o.mode = value;
    else if (auto it = integers.find(key); it != integers.end()) {
      std::size_t consumed;
      *it->second = std::stoi(value, &consumed);
      if (consumed != value.size() || *it->second < 0) throw std::runtime_error("Invalid " + key);
    } else throw std::runtime_error("Unknown option " + key);
  }
  if (o.redis.empty() || o.output.empty()) throw std::runtime_error("--redis and --output-dir are required");
  if (o.mode != "finalized" && o.mode != "mixed" && o.mode != "promotion")
    throw std::runtime_error("--mode must be finalized, mixed or promotion");
  if (!o.blocks || !o.block_ms || !o.traces || !o.nodes || o.nodes > TON_TRACE_BENCH_MAX_CACHED_NODES || !o.accounts ||
      o.fragment_txs > o.nodes ||
      !o.threads || o.threads > 256 || o.workers > 256 || !o.retention_ms || !o.drain_seconds ||
      o.payload > 65536 || std::uint64_t(o.blocks) * o.traces > 1000000u / o.nodes ||
      std::uint64_t(o.blocks) * o.traces * o.nodes * (o.payload + 512u) > 1000000000u)
    throw std::runtime_error("Invalid size: positive counts, nodes <= benchmark cache limit, fragment-txs <= nodes, threads/workers <= 256, "
                             "payload <= 65536, <= 1M nodes and <= 1GB estimated cell payload required");
  return o;
}

double cpu_seconds() {
  timespec t{};
  CHECK(clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t) == 0);
  return t.tv_sec + t.tv_nsec * 1e-9;
}

struct Result {
  std::ostringstream blocks, samples, actors;
  double start = 0, cpu_start = 0, elapsed = 0, cpu = 0, generation = 0, drained_at = 0;
  int done = 0, failed_blocks = 0, errors = 0, nonfinalized_errors = 0, promoted_blocks = 0, fallback_blocks = 0;
  int max_backlog = 0, outstanding = 0;
  bool timeout = false;
  std::string sample_key;
};

struct Input {
  trace_bench::SyntheticTrace trace;
  std::vector<ConfirmedTraceSnapshot> snapshots;
};

// An open-loop block clock with the production single-finalized-block commit barrier.
// All other pipeline actors are the real implementation. No TON DB / TVM is involved.
class Source final : public td::actor::Actor {
 public:
  Source(Options o, std::vector<Input> input, RedisConnectionOptions redis,
         mch::EmuClassifierConfig classifier, std::shared_ptr<Result> result)
      : o_(std::move(o)), input_(std::move(input)), redis_(std::move(redis)),
        classifier_(std::move(classifier)), r_(std::move(result)) {}

 private:
  Options o_;
  std::vector<Input> input_;
  RedisConnectionOptions redis_;
  mch::EmuClassifierConfig classifier_;
  std::shared_ptr<Result> r_;
  td::actor::ActorOwn<TraceProcessor> processor_;
  td::actor::ActorTypeStats previous_stats_;
  td::uint64 previous_ticks_ = 0;
  double previous_time_ = 0, previous_cpu_ = 0, next_sample_ = 0, last_completion_ = 0;
  int tick_ = 0, arrived_ = 0, active_ = -1, final_remaining_ = 0;
  bool block_failed_ = false, drained_ = false;
  std::vector<double> injected_at_;

  int total_blocks() const { return o_.total_blocks(); }
  double due(int tick) const { return r_->start + tick * (o_.block_ms / 1000.0); }
  Input& input(int cohort, int trace) { return input_[cohort * o_.traces + trace]; }

  template <class F> void each_update(int block, F f) {
    for (int i = 0; i < o_.traces; ++i) {
      for (int step = std::max(0, block - o_.blocks + 1); step <= std::min(block, o_.stages() - 1); ++step)
        f(input(block - step, i), step);
    }
  }

  void start_up() override {
    r_->start = td::Time::now();
    r_->cpu_start = cpu_seconds();
    previous_time_ = r_->start;
    previous_cpu_ = r_->cpu_start;
    previous_ticks_ = td::Clocks::rdtsc();
    previous_stats_ = td::actor::ActorTypeStatManager::get_stats(1.0);
    next_sample_ = r_->start + 1;
    TraceRetentionConfig retention;
    retention.completed_seconds = o_.retention_ms / 1000.0;
    processor_ = td::actor::create_actor<TraceProcessor>("TraceProcessor", redis_, retention, classifier_);
    r_->blocks << "block,arrival_s,start_s,completion_s,lag_ms,source_lateness_ms,success,expected_max_trace_nodes\n";
    r_->samples << "elapsed_s,arrived_blocks,completed_blocks,block_backlog,outstanding_callbacks,process_cpu_cores\n";
    r_->actors << "elapsed_s,interval_s,actor,busy_cores,max_message_ms,max_execution_ms,max_mailbox_delay_ms\n";
    alarm();
  }

  void nonfinalized(int block) {
    each_update(block, [&](Input& entry, int step) {
      if (step == 0) {
        stamp(entry.trace.pending, "pending");
        ++r_->outstanding;
        td::actor::send_closure(processor_, &TraceProcessor::process_trace_update, std::move(entry.trace.pending),
            td::PromiseCreator::lambda([self = actor_id(this)](td::Result<td::Unit> result) {
              td::actor::send_closure(self, &Source::nonfinalized_done, failed(result));
            }));
      }
      ++r_->outstanding;
      stamp(entry.trace.confirmed[step], "confirmed");
      td::actor::send_closure(processor_, &TraceProcessor::process_confirmed_trace_update,
          std::move(entry.trace.confirmed[step]),
          td::PromiseCreator::lambda([self = actor_id(this), index = &entry - input_.data(), step](td::Result<ConfirmedTraceSnapshot> result) {
            td::actor::send_closure(self, &Source::confirmed_done, index, step, std::move(result));
          }));
    });
  }

  void nonfinalized_done(bool error) {
    --r_->outstanding;
    r_->errors += error;
    r_->nonfinalized_errors += error;
    check_done();
  }

  void confirmed_done(std::ptrdiff_t index, int step, td::Result<ConfirmedTraceSnapshot> result) {
    const bool error = failed(result);
    if (!error) input_[index].snapshots[step] = result.move_as_ok();
    nonfinalized_done(error);
  }

  void start_block() {
    if (active_ >= 0 || r_->done == arrived_) return;
    active_ = r_->done;
    block_failed_ = false;
    block_start_ = td::Time::now() - r_->start;
    std::vector<ton::BlockId> closed;
    each_update(active_, [&](Input& in, int step) {
      const auto id = in.trace.finalized[step].fragments.front().root->block_id;
      if (std::find(closed.begin(), closed.end(), id) == closed.end()) closed.push_back(id);
    });
    td::actor::send_closure(processor_, &TraceProcessor::discard_confirmed_updates, std::move(closed));
    std::vector<ConfirmedTraceSnapshot> snapshots;
    bool ready = o_.mode == "promotion";
    each_update(active_, [&](Input& in, int step) {
      const auto& snapshot = in.snapshots[step];
      ready &= bool(snapshot);
      if (snapshot) snapshots.push_back(snapshot);
    });
    if (ready) {
      ++r_->outstanding;
      td::actor::send_closure(processor_, &TraceProcessor::promote_confirmed, std::move(snapshots),
          ton::BlockSeqno(1000 + active_),
          td::PromiseCreator::lambda([self = actor_id(this)](td::Result<td::Unit> result) {
            td::actor::send_closure(self, &Source::promotion_done, result.is_error(), td::Time::now());
          }));
    } else {
      if (o_.mode == "promotion") ++r_->fallback_blocks;
      ordinary_finalized();
    }
  }
  double block_start_ = 0;

  void ordinary_finalized() {
    final_remaining_ = 0;
    each_update(active_, [&](Input& in, int step) {
      stamp(in.trace.finalized[step], "finalized");
      ++final_remaining_;
      ++r_->outstanding;
      td::actor::send_closure(processor_, &TraceProcessor::process_trace_update,
          std::move(in.trace.finalized[step]),
          td::PromiseCreator::lambda([self = actor_id(this)](td::Result<td::Unit> result) {
            td::actor::send_closure(self, &Source::finalized_done, failed(result), td::Time::now());
          }));
    });
  }

  void promotion_done(bool error, double completed) {
    --r_->outstanding;
    if (error) {
      ++r_->fallback_blocks;
      ordinary_finalized();
    } else {
      ++r_->promoted_blocks;
      block_done(completed);
    }
  }

  void finalized_done(bool error, double completed) {
    --r_->outstanding;
    r_->errors += error;
    block_failed_ |= error;
    last_completion_ = std::max(last_completion_, completed);
    if (--final_remaining_ == 0) block_done(last_completion_);
  }

  void block_done(double completed) {
    r_->blocks << active_ << ',' << due(active_ + 1) - r_->start << ',' << block_start_ << ','
               << completed - r_->start << ',' << 1000 * (completed - due(active_ + 1)) << ','
               << 1000 * (injected_at_[active_] - due(active_ + 1)) << ',' << !block_failed_ << ','
               << (o_.fragment_txs ? std::min(o_.nodes, (active_ + 1) * o_.fragment_txs) : o_.nodes) << '\n';
    r_->failed_blocks += block_failed_;
    // Release synthetic inputs and promotion snapshots once no longer needed.
    each_update(active_, [](Input& in, int step) {
      in.trace.finalized[step] = {};
      in.snapshots[step].reset();
    });
    ++r_->done;
    active_ = -1;
    start_block();
    check_done();
  }

  void sample() {
    const auto now = td::Time::now();
    const auto ticks = td::Clocks::rdtsc();
    const double interval = now - previous_time_;
    if (interval < 0.001) return;
    auto stats = td::actor::ActorTypeStatManager::get_stats(1.0);
    const double seconds_per_tick = interval / double(ticks - previous_ticks_);
    for (const auto& [type, stat] : stats.stats) {
      const double busy = (stat.seconds - previous_stats_.stats[type].seconds) * seconds_per_tick / interval;
      r_->actors << now - r_->start << ',' << interval << ','
                 << td::actor::ActorTypeStatManager::get_class_name(type.name()) << ',' << busy << ','
                 << stat.max_message_seconds.value_forever * seconds_per_tick * 1000 << ','
                 << stat.max_execute_seconds.value_forever * seconds_per_tick * 1000 << ','
                 << stat.max_delay_seconds.value_forever * seconds_per_tick * 1000 << '\n';
    }
    const auto cpu = cpu_seconds();
    r_->samples << now - r_->start << ',' << arrived_ << ',' << r_->done << ',' << arrived_ - r_->done
                << ',' << r_->outstanding << ',' << (cpu - previous_cpu_) / interval << '\n';
    previous_stats_ = std::move(stats);
    previous_time_ = now;
    previous_ticks_ = ticks;
    previous_cpu_ = cpu;
    next_sample_ = now + 1;
  }

  void stamp(TraceUpdate& update, const char* finality) {
    if (!o_.measurements) return;
    update.measurement = std::make_shared<Measurement>();
    update.measurement->set_source("benchmark").set_finality(finality);
  }

  void check_done() {
    if (!drained_ && r_->done == total_blocks() && r_->outstanding == 0) {
      drained_ = true;
      r_->drained_at = td::Time::now() - r_->start;
      sample();
      alarm_timestamp() = td::Timestamp::in(o_.settle_ms / 1000.0);
    }
  }

  void alarm() override {
    const auto now = td::Time::now();
    if (drained_ || now > due(total_blocks()) + o_.drain_seconds) {
      r_->timeout = !drained_;
      sample();
      r_->elapsed = td::Time::now() - r_->start;
      r_->cpu = cpu_seconds() - r_->cpu_start;
      td::actor::SchedulerContext::get().stop();
      return;
    }
    if (now >= next_sample_) sample();
    // One tick per turn: late arrivals keep their original due time. Catch-up
    // cannot shift the offered load clock or hide source/scheduler starvation.
    if (tick_ <= total_blocks() && now >= due(tick_)) {
      if (tick_ > 0) {
        ++arrived_;
        injected_at_.push_back(now);
        r_->max_backlog = std::max(r_->max_backlog, arrived_ - r_->done);
        start_block();
      }
      if (o_.mode != "finalized") {
        if (tick_ < total_blocks()) nonfinalized(tick_);
      }
      ++tick_;
    }
    alarm_timestamp() = td::Timestamp::at(std::min(next_sample_, tick_ <= total_blocks() ? due(tick_) :
                                                  due(total_blocks()) + o_.drain_seconds + 0.001));
  }
};

void write(const std::filesystem::path& path, const std::string& contents) {
  std::ofstream out(path);
  out << contents;
  if (!out) throw std::runtime_error("Cannot write " + path.string());
}
}  // namespace

int main(int argc, char** argv) {
  try {
    const auto o = options(argc, argv);
    SET_VERBOSITY_LEVEL(verbosity_INFO);
    vm::init_op_cp0();
    auto redis = parse_redis_connection_options(o.redis).move_as_ok();
    mch::EmuClassifierConfig classifier;
    classifier.workers = std::max(1, o.workers);
    classifier.tier2 = false;
    if (o.workers) classifier.prep = mch::make_engine_prep().move_as_ok();
    auto result = std::make_shared<Result>();
    auto generation_start = td::Time::now();
    std::vector<Input> input;
    input.reserve(o.blocks * o.traces);
    for (int block = 0; block < o.blocks; ++block) {
      for (int trace = 0; trace < o.traces; ++trace) {
        auto data = o.fragment_txs
            ? trace_bench::generate_growing(block * o.traces + trace, o.nodes, o.fragment_txs, o.accounts,
                                            o.payload, 1000 + block, o.mode != "finalized", o.seed)
            : trace_bench::generate(block * o.traces + trace, o.nodes, o.accounts, o.payload,
                                    1000 + block, o.mode != "finalized", o.seed);
        input.push_back({std::move(data), std::vector<ConfirmedTraceSnapshot>(o.stages())});
      }
    }
    result->generation = td::Time::now() - generation_start;
    result->sample_key = input.back().trace.key;
    td::actor::set_debug(true);
    {
      td::actor::Scheduler scheduler({std::size_t(o.threads)});
      scheduler.run_in_context([&] {
        td::actor::create_actor<Source>("BenchmarkSource", o, std::move(input), std::move(redis),
                                        classifier, result).release();
      });
      scheduler.run();
    }
    std::filesystem::create_directories(o.output);
    write(std::filesystem::path(o.output) / "blocks.csv", result->blocks.str());
    write(std::filesystem::path(o.output) / "samples.csv", result->samples.str());
    write(std::filesystem::path(o.output) / "actors.csv", result->actors.str());
    write(std::filesystem::path(o.output) / "application-stats.txt", g_statistics.generate_report_and_reset());
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::ostringstream summary;
    summary << "metric,value\n"
            << "generation_seconds," << result->generation << '\n'
            << "elapsed_seconds," << result->elapsed << '\n'
            << "drained_at_seconds," << result->drained_at << '\n'
            << "process_cpu_seconds," << result->cpu << '\n'
            << "peak_rss_kib_including_generation," << usage.ru_maxrss << '\n'
            << "completed_blocks," << result->done << '\n'
            << "failed_blocks," << result->failed_blocks << '\n'
            << "callback_errors," << result->errors << '\n'
            << "nonfinalized_errors," << result->nonfinalized_errors << '\n'
            << "promoted_blocks," << result->promoted_blocks << '\n'
            << "promotion_fallback_blocks," << result->fallback_blocks << '\n'
            << "max_block_backlog," << result->max_backlog << '\n'
            << "outstanding_callbacks," << result->outstanding << '\n'
            << "timed_out," << result->timeout << '\n'
            << "benchmark_node_limit," << TON_TRACE_BENCH_MAX_CACHED_NODES << '\n'
            << "nodes_per_trace," << o.nodes << '\n'
            << "fragment_txs," << o.fragment_txs << '\n'
            << "updates_per_trace," << o.stages() << '\n'
            << "offered_finalized_transactions," << o.blocks * o.traces * o.nodes << '\n'
            << "offered_steady_transactions_per_second," << o.traces * 1000.0 / o.block_ms *
                   (o.fragment_txs ? std::min(o.nodes, std::min(o.blocks, o.stages()) * o.fragment_txs) : o.nodes) << '\n'
            << "offered_mean_transactions_per_second," << o.blocks * o.traces * o.nodes /
                   (o.total_blocks() * (o.block_ms / 1000.0)) << '\n'
            << "input_horizon_seconds," << o.total_blocks() * (o.block_ms / 1000.0) << '\n'
            << "sample_trace_key," << result->sample_key << '\n';
    write(std::filesystem::path(o.output) / "summary.csv", summary.str());
    return result->timeout || result->failed_blocks ? 1 : 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 2;
  }
}

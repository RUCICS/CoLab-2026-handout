#include "benchmark/event_log.hpp"
#include "benchmark/metrics.hpp"
#include "benchmark/scoring.hpp"
#include "benchmark/schedulers.hpp"
#include "benchmark/workload_discovery.hpp"
#include "benchmark/workload_interpreter.hpp"
#include "simulator/engine.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

#include "runtime/worker.hpp"
#include "schedlab/baseline_scheduler.hpp"
#include "scheduler.h"

namespace {

std::optional<std::string> value_after(const std::vector<std::string>& args,
                                       const std::string& flag) {
  for (std::size_t i = 0; i + 1 < args.size(); ++i) {
    if (args[i] == flag) {
      return args[i + 1];
    }
  }
  return std::nullopt;
}

bool has_flag(const std::vector<std::string>& args, const std::string& flag) {
  for (const std::string& arg : args) {
    if (arg == flag) {
      return true;
    }
  }
  return false;
}

bool write_all(int fd, std::string_view text) {
  const char* data = text.data();
  std::size_t remaining = text.size();
  while (remaining > 0) {
    const ssize_t written = ::write(fd, data, remaining);
    if (written <= 0) {
      return false;
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }
  return true;
}

struct ReleaseSlot {
  schedlab::benchmark::SchedulerKind kind = schedlab::benchmark::SchedulerKind::Student;
  bool is_candidate = false;
};

enum class EngineKind {
  Real,
  Sim,
};

constexpr uint64_t kBenchmarkSimDefaultComputeChunkUnits = 1;
constexpr uint64_t kBenchmarkSimDefaultTickIntervalUs = 1;

const char* engine_name(EngineKind engine) noexcept {
  switch (engine) {
  case EngineKind::Real:
    return "real";
  case EngineKind::Sim:
    return "sim";
  }
  return "unknown";
}

std::optional<EngineKind> parse_engine_token(std::string_view token) {
  if (token == "real") {
    return EngineKind::Real;
  }
  if (token == "sim") {
    return EngineKind::Sim;
  }
  return std::nullopt;
}

std::array<ReleaseSlot, 2>
release_slots_for(int repetition, schedlab::benchmark::SchedulerKind candidate_scheduler,
                  schedlab::benchmark::SchedulerKind baseline_scheduler) {
  if ((repetition % 2) == 0) {
    return {{
        ReleaseSlot{
            .kind = candidate_scheduler,
            .is_candidate = true,
        },
        ReleaseSlot{
            .kind = baseline_scheduler,
            .is_candidate = false,
        },
    }};
  }
  return {{
      ReleaseSlot{
          .kind = baseline_scheduler,
          .is_candidate = false,
      },
      ReleaseSlot{
          .kind = candidate_scheduler,
          .is_candidate = true,
      },
  }};
}

void usage() {
  (void)write_all(
      STDERR_FILENO,
      "usage: runner --list-workloads\n"
      "       runner --mode <debug|release> [--suite <public|hidden>] "
      "[--role <gate|leaderboard>] "
      "(--scenario <id>|--track <cpu_bound|io_bound|mixed|throughput|latency|fairness>) "
      "[--repetitions N] [--workers N] [--scheduler student|baseline] "
      "[--engine <real|sim>] [--jsonl]\n");
}

const char* track_name(schedlab::benchmark::dsl::Track track) noexcept {
  switch (track) {
  case schedlab::benchmark::dsl::Track::CpuBound:
    return "cpu_bound";
  case schedlab::benchmark::dsl::Track::IoBound:
    return "io_bound";
  case schedlab::benchmark::dsl::Track::Mixed:
    return "mixed";
  case schedlab::benchmark::dsl::Track::Throughput:
    return "throughput";
  case schedlab::benchmark::dsl::Track::Latency:
    return "latency";
  case schedlab::benchmark::dsl::Track::Fairness:
    return "fairness";
  }
  return "unknown";
}

const char* suite_name(schedlab::benchmark::WorkloadSuite suite) noexcept {
  switch (suite) {
  case schedlab::benchmark::WorkloadSuite::Public:
    return "public";
  case schedlab::benchmark::WorkloadSuite::Hidden:
    return "hidden";
  }
  return "unknown";
}

const char* scorer_name(schedlab::benchmark::dsl::ScorerKind scorer) noexcept {
  switch (scorer) {
  case schedlab::benchmark::dsl::ScorerKind::ThroughputMakespan:
    return "throughput_makespan";
  case schedlab::benchmark::dsl::ScorerKind::ThroughputSustainedRate:
    return "throughput_sustained_rate";
  case schedlab::benchmark::dsl::ScorerKind::LatencyWakeupP99:
    return "latency_wakeup_p99";
  case schedlab::benchmark::dsl::ScorerKind::LatencyFlowP99:
    return "latency_flow_p99";
  case schedlab::benchmark::dsl::ScorerKind::FairnessShareSkew:
    return "fairness_share_skew";
  }
  return "unknown";
}

const char* role_name(schedlab::benchmark::dsl::ScenarioRole role) noexcept {
  switch (role) {
  case schedlab::benchmark::dsl::ScenarioRole::Leaderboard:
    return "leaderboard";
  case schedlab::benchmark::dsl::ScenarioRole::Gate:
    return "gate";
  }
  return "unknown";
}

std::optional<schedlab::benchmark::dsl::Track> parse_track_token(std::string_view token) {
  if (token == "cpu_bound") {
    return schedlab::benchmark::dsl::Track::CpuBound;
  }
  if (token == "io_bound") {
    return schedlab::benchmark::dsl::Track::IoBound;
  }
  if (token == "mixed") {
    return schedlab::benchmark::dsl::Track::Mixed;
  }
  if (token == "throughput") {
    return schedlab::benchmark::dsl::Track::Throughput;
  }
  if (token == "latency") {
    return schedlab::benchmark::dsl::Track::Latency;
  }
  if (token == "fairness") {
    return schedlab::benchmark::dsl::Track::Fairness;
  }
  return std::nullopt;
}

std::optional<schedlab::benchmark::dsl::ScenarioRole> parse_role_token(std::string_view token) {
  if (token == "leaderboard") {
    return schedlab::benchmark::dsl::ScenarioRole::Leaderboard;
  }
  if (token == "gate") {
    return schedlab::benchmark::dsl::ScenarioRole::Gate;
  }
  return std::nullopt;
}

schedlab::benchmark::dsl::ScorerKind
default_scorer_for_track(schedlab::benchmark::dsl::Track track) noexcept {
  switch (track) {
  case schedlab::benchmark::dsl::Track::CpuBound:
  case schedlab::benchmark::dsl::Track::Mixed:
  case schedlab::benchmark::dsl::Track::Throughput:
    return schedlab::benchmark::dsl::ScorerKind::ThroughputSustainedRate;
  case schedlab::benchmark::dsl::Track::IoBound:
  case schedlab::benchmark::dsl::Track::Latency:
    return schedlab::benchmark::dsl::ScorerKind::LatencyWakeupP99;
  case schedlab::benchmark::dsl::Track::Fairness:
    return schedlab::benchmark::dsl::ScorerKind::FairnessShareSkew;
  }
  return schedlab::benchmark::dsl::ScorerKind::ThroughputSustainedRate;
}

std::optional<schedlab::benchmark::WorkloadSuite> parse_suite_token(std::string_view token) {
  if (token == "public") {
    return schedlab::benchmark::WorkloadSuite::Public;
  }
  if (token == "hidden") {
    return schedlab::benchmark::WorkloadSuite::Hidden;
  }
  return std::nullopt;
}

std::string format_diagnostic(const schedlab::benchmark::DiscoveryDiagnostic& diagnostic) {
  std::string line = "runner: discovery error " + diagnostic.path.string();
  if (diagnostic.line.has_value()) {
    line += ":" + std::to_string(*diagnostic.line);
  }
  line += ": " + diagnostic.message + "\n";
  return line;
}

std::string json_escape(std::string_view text) {
  std::string escaped;
  escaped.reserve(text.size() + 8);
  for (const char ch : text) {
    switch (ch) {
    case '\\':
      escaped += "\\\\";
      break;
    case '"':
      escaped += "\\\"";
      break;
    case '\n':
      escaped += "\\n";
      break;
    case '\r':
      escaped += "\\r";
      break;
    case '\t':
      escaped += "\\t";
      break;
    default:
      escaped += ch;
      break;
    }
  }
  return escaped;
}

void append_json_separator(std::string* output, bool* first_field) {
  if (!*first_field) {
    *output += ",";
  }
  *first_field = false;
}

void append_json_string_field(std::string* output, bool* first_field, std::string_view key,
                              std::string_view value) {
  append_json_separator(output, first_field);
  *output += "\"";
  *output += json_escape(key);
  *output += "\":\"";
  *output += json_escape(value);
  *output += "\"";
}

void append_json_uint_field(std::string* output, bool* first_field, std::string_view key,
                            uint64_t value) {
  append_json_separator(output, first_field);
  *output += "\"";
  *output += json_escape(key);
  *output += "\":";
  *output += std::to_string(value);
}

void append_json_double_field(std::string* output, bool* first_field, std::string_view key,
                              double value) {
  append_json_separator(output, first_field);
  *output += "\"";
  *output += json_escape(key);
  *output += "\":";
  *output += std::to_string(value);
}

void append_json_bool_field(std::string* output, bool* first_field, std::string_view key,
                            bool value) {
  append_json_separator(output, first_field);
  *output += "\"";
  *output += json_escape(key);
  *output += "\":";
  *output += value ? "true" : "false";
}

void append_json_raw_field(std::string* output, bool* first_field, std::string_view key,
                           const std::string& value) {
  append_json_separator(output, first_field);
  *output += "\"";
  *output += json_escape(key);
  *output += "\":";
  *output += value;
}

std::string json_string_array(const std::vector<std::string>& values) {
  std::string output = "[";
  bool first = true;
  for (const auto& value : values) {
    if (!first) {
      output += ",";
    }
    first = false;
    output += "\"";
    output += json_escape(value);
    output += "\"";
  }
  output += "]";
  return output;
}

std::string metrics_json(const schedlab::benchmark::RunMetrics& metrics) {
  std::string output = "{";
  bool first_field = true;
  append_json_uint_field(&output, &first_field, "completed_tasks", metrics.completed_tasks);
  append_json_uint_field(&output, &first_field, "elapsed_time_us", metrics.elapsed_time_us);
  append_json_double_field(&output, &first_field, "throughput_tasks_per_sec",
                           metrics.throughput_tasks_per_sec);
  append_json_uint_field(&output, &first_field, "p99_wakeup_latency_us",
                         metrics.p99_wakeup_latency_us);
  append_json_uint_field(&output, &first_field, "task_observation_count",
                         metrics.task_observations.size());
  append_json_uint_field(&output, &first_field, "group_observation_count",
                         metrics.group_observations.size());
  output += "}";
  return output;
}

std::string fairness_diagnostics_json(const schedlab::benchmark::FairnessDiagnostics& diagnostics) {
  std::string output = "{";
  bool first_field = true;
  append_json_double_field(&output, &first_field, "max_share_skew", diagnostics.max_share_skew);
  append_json_double_field(&output, &first_field, "share_balance_ratio",
                           diagnostics.share_balance_ratio);
  append_json_separator(&output, &first_field);
  output += "\"groups\":[";
  bool first_group = true;
  for (const auto& group : diagnostics.groups) {
    if (!first_group) {
      output += ",";
    }
    first_group = false;
    output += "{";
    bool first_group_field = true;
    append_json_string_field(&output, &first_group_field, "group_name", group.group_name);
    append_json_uint_field(&output, &first_group_field, "weight", group.weight);
    append_json_double_field(&output, &first_group_field, "share_distortion",
                             group.share_distortion);
    output += "}";
  }
  output += "]";
  output += "}";
  return output;
}

bool emit_jsonl_record(const std::string& record) {
  return write_all(STDOUT_FILENO, record) && write_all(STDOUT_FILENO, "\n");
}

bool emit_discovery_warning_record(const schedlab::benchmark::DiscoveryDiagnostic& diagnostic) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "discovery_warning");
  append_json_string_field(&output, &first_field, "path", diagnostic.path.string());
  if (diagnostic.line.has_value()) {
    append_json_uint_field(&output, &first_field, "line", *diagnostic.line);
  }
  append_json_string_field(&output, &first_field, "message", diagnostic.message);
  output += "}";
  return emit_jsonl_record(output);
}

bool write_discovery_diagnostics(const schedlab::benchmark::DiscoveryResult& discovered,
                                 bool jsonl_mode) {
  for (const auto& diagnostic : discovered.diagnostics) {
    if (jsonl_mode) {
      if (!emit_discovery_warning_record(diagnostic)) {
        return false;
      }
      continue;
    }
    if (!write_all(STDERR_FILENO, format_diagnostic(diagnostic))) {
      return false;
    }
  }
  return true;
}

class CountingSingleWorkerInstaller final : public schedlab::benchmark::WorkloadInstaller {
public:
  explicit CountingSingleWorkerInstaller(schedlab::runtime::Worker& worker) noexcept
      : worker_(&worker) {}

  uint64_t spawn(schedlab::benchmark::WorkloadTaskMain main, void* arg,
                 schedlab::TaskAttrs attrs) override {
    ++spawn_count_;
    return worker_->spawn(main, arg, attrs);
  }

  uint64_t spawn_count() const noexcept {
    return spawn_count_;
  }

private:
  schedlab::runtime::Worker* worker_ = nullptr;
  uint64_t spawn_count_ = 0;
};

class CountingWorkerPoolInstaller final : public schedlab::benchmark::WorkloadInstaller {
public:
  explicit CountingWorkerPoolInstaller(schedlab::runtime::WorkerPool& pool) noexcept
      : pool_(&pool) {}

  uint64_t spawn(schedlab::benchmark::WorkloadTaskMain main, void* arg,
                 schedlab::TaskAttrs attrs) override {
    ++spawn_count_;
    return pool_->spawn(main, arg, attrs);
  }

  uint64_t spawn_count() const noexcept {
    return spawn_count_;
  }

private:
  schedlab::runtime::WorkerPool* pool_ = nullptr;
  uint64_t spawn_count_ = 0;
};

void append_metrics_fields(std::string* output, const schedlab::benchmark::RunMetrics& metrics) {
  *output += " completed_tasks=" + std::to_string(metrics.completed_tasks);
  *output += " elapsed_time_us=" + std::to_string(metrics.elapsed_time_us);
  *output += " throughput_tasks_per_sec=" + std::to_string(metrics.throughput_tasks_per_sec);
  *output += " p99_wakeup_latency_us=" + std::to_string(metrics.p99_wakeup_latency_us);
}

void record_task_and_group_observations(
    schedlab::benchmark::MetricsCollector* collector,
    const schedlab::benchmark::InterpretedScenario& scenario,
    const schedlab::runtime::Worker::ObservedMetrics& observed) {
  const auto& groups = scenario.group_metadata();
  const auto& task_metadata = scenario.task_metadata();
  std::vector<std::vector<uint64_t>> task_ids_by_group(groups.size());
  for (std::size_t task_offset = 0; task_offset < task_metadata.size(); ++task_offset) {
    const uint64_t task_id = static_cast<uint64_t>(task_offset + 1);
    const auto task_it = observed.task_metrics_by_id.find(task_id);
    const auto& metadata = task_metadata[task_offset];
    if (metadata.group_index < task_ids_by_group.size()) {
      task_ids_by_group[metadata.group_index].push_back(task_id);
    }
    if (task_it == observed.task_metrics_by_id.end()) {
      continue;
    }
    collector->record_task_observation(schedlab::benchmark::TaskObservation{
        .task_id = task_id,
        .flow_id = metadata.flow_id,
        .group_index = metadata.group_index,
        .release_time_us = task_it->second.release_time_us,
        .completion_time_us = task_it->second.completion_time_us,
        .cpu_runtime_us = task_it->second.cpu_runtime_us,
    });
  }

  for (std::size_t group_index = 0; group_index < groups.size(); ++group_index) {
    const auto& group = groups[group_index];
    schedlab::benchmark::GroupObservation group_observation{
        .group_name = group.name,
        .weight = group.weight,
    };
    group_observation.task_ids = std::move(task_ids_by_group[group_index]);
    collector->record_group_observation(std::move(group_observation));
  }

  for (const auto& event : observed.runnable_events) {
    collector->record_group_runnable_event(schedlab::benchmark::GroupRunnableEvent{
        .time_us = event.time_us,
        .group_index = static_cast<std::size_t>(event.group_id),
        .delta = event.delta,
    });
  }
  for (const auto& slice : observed.service_slices) {
    collector->record_group_service_slice(schedlab::benchmark::GroupServiceSlice{
        .start_time_us = slice.start_time_us,
        .end_time_us = slice.end_time_us,
        .group_index = static_cast<std::size_t>(slice.group_id),
    });
  }
}

struct LoadedScenario {
  std::string id;
  schedlab::benchmark::dsl::Track track = schedlab::benchmark::dsl::Track::CpuBound;
  schedlab::benchmark::dsl::ScorerKind scorer =
      schedlab::benchmark::dsl::ScorerKind::ThroughputSustainedRate;
  double scenario_weight = 1.0;
  schedlab::benchmark::dsl::WorkloadSpec spec;
};

struct ScenarioRunResult {
  schedlab::benchmark::dsl::Track track = schedlab::benchmark::dsl::Track::CpuBound;
  schedlab::benchmark::RunMetrics metrics;
};

struct ReleaseExecutionResult {
  schedlab::benchmark::ScoreSummary score_summary;
};

struct GateEvaluation {
  bool passed = true;
  std::vector<std::string> failed_scenarios;
};

std::optional<ScenarioRunResult>
run_loaded_scenario(const LoadedScenario& scenario,
                    schedlab::benchmark::SchedulerKind scheduler_kind, EngineKind engine_kind,
                    int worker_override, const schedlab::runtime::Worker::Config& worker_config) {
  if (!scenario.spec.workers.has_value()) {
    return std::nullopt;
  }
  const int worker_count =
      (worker_override > 0) ? worker_override : static_cast<int>(*scenario.spec.workers);
  if (worker_count <= 0) {
    return std::nullopt;
  }

  auto scheduler = make_scheduler(scheduler_kind);
  if (!scheduler) {
    return std::nullopt;
  }

  schedlab::benchmark::MetricsCollector collector;

  schedlab::benchmark::InterpretedScenario interpreted(scenario.spec);
  if (engine_kind == EngineKind::Sim) {
    schedlab::simulator::Engine::Config sim_config{
        .worker_count = worker_count,
        .compute_chunk_units = worker_config.compute_chunk_units,
        .tick_interval_us = worker_config.tick_interval_us,
        .switch_cost_us = interpreted.execution_config().switch_cost_us,
        .migration_cost_us = interpreted.execution_config().migration_cost_us,
    };
    if (sim_config.compute_chunk_units == schedlab::runtime::Worker::Config{}.compute_chunk_units) {
      sim_config.compute_chunk_units = kBenchmarkSimDefaultComputeChunkUnits;
    }
    if (sim_config.tick_interval_us == 0) {
      sim_config.tick_interval_us = kBenchmarkSimDefaultTickIntervalUs;
    }
    auto metrics = schedlab::simulator::Engine(*scheduler, interpreted, sim_config).run();
    if (!metrics.has_value()) {
      return std::nullopt;
    }
    return ScenarioRunResult{
        .track = scenario.track,
        .metrics = std::move(*metrics),
    };
  }

  uint64_t spawned_tasks = 0;
  schedlab::runtime::Worker::ObservedMetrics observed;
  const auto start = std::chrono::steady_clock::now();
  if (worker_count == 1) {
    schedlab::runtime::Worker worker(*scheduler, 0, worker_config);
    CountingSingleWorkerInstaller installer(worker);
    interpreted.install(installer);
    worker.run();
    spawned_tasks = installer.spawn_count();
    observed = worker.observed_metrics();
    for (const uint64_t latency_us : observed.wakeup_latencies_us) {
      collector.record_wakeup_latency_us(latency_us);
    }
    for (const auto& [worker_id, idle_time_us] : observed.worker_idle_time_us) {
      collector.record_worker_idle_time_us(worker_id, idle_time_us);
    }
  } else {
    schedlab::runtime::WorkerPool pool(*scheduler, worker_count, worker_config);
    CountingWorkerPoolInstaller installer(pool);
    interpreted.install(installer);
    pool.run();
    spawned_tasks = installer.spawn_count();
    observed = pool.observed_metrics();
    for (const uint64_t latency_us : observed.wakeup_latencies_us) {
      collector.record_wakeup_latency_us(latency_us);
    }
    for (const auto& [worker_id, idle_time_us] : observed.worker_idle_time_us) {
      collector.record_worker_idle_time_us(worker_id, idle_time_us);
    }
  }
  const auto end = std::chrono::steady_clock::now();

  const uint64_t elapsed_time_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  collector.record_task_completion(spawned_tasks);
  collector.set_elapsed_time_us(elapsed_time_us);
  record_task_and_group_observations(&collector, interpreted, observed);

  return ScenarioRunResult{
      .track = scenario.track,
      .metrics = collector.finish(),
  };
}

struct SelectedScenarios {
  schedlab::benchmark::WorkloadSuite suite = schedlab::benchmark::WorkloadSuite::Public;
  std::optional<schedlab::benchmark::dsl::Track> requested_track;
  std::optional<schedlab::benchmark::dsl::ScenarioRole> requested_role;
  std::vector<schedlab::benchmark::DiscoveredScenario> scenarios;
};

std::optional<SelectedScenarios> resolve_selected_scenarios(
    const std::vector<std::string>& args, bool jsonl_mode,
    std::optional<schedlab::benchmark::dsl::ScenarioRole> default_role_for_track,
    std::string* error) {
  const std::optional<std::string> suite_arg = value_after(args, "--suite");
  schedlab::benchmark::WorkloadSuite suite = schedlab::benchmark::WorkloadSuite::Public;
  if (suite_arg.has_value()) {
    const auto parsed_suite = parse_suite_token(*suite_arg);
    if (!parsed_suite.has_value()) {
      *error = "runner: unsupported suite\n";
      return std::nullopt;
    }
    suite = *parsed_suite;
  }

  const std::optional<std::string> scenario_arg = value_after(args, "--scenario");
  const std::optional<std::string> track_arg = value_after(args, "--track");
  const std::optional<std::string> role_arg = value_after(args, "--role");
  std::optional<schedlab::benchmark::dsl::ScenarioRole> requested_role;
  if (role_arg.has_value()) {
    requested_role = parse_role_token(*role_arg);
    if (!requested_role.has_value()) {
      *error = "runner: unsupported role\n";
      return std::nullopt;
    }
  }
  if (scenario_arg.has_value() && track_arg.has_value()) {
    *error = "runner: use either --scenario or --track, not both\n";
    return std::nullopt;
  }
  if (!scenario_arg.has_value() && !track_arg.has_value()) {
    *error = "runner: missing scenario selector (--scenario or --track)\n";
    return std::nullopt;
  }

  if (scenario_arg.has_value()) {
    std::string scenario_id = *scenario_arg;
    if (scenario_id.starts_with("public/")) {
      if (suite_arg.has_value() && suite != schedlab::benchmark::WorkloadSuite::Public) {
        *error = "runner: scenario suite prefix conflicts with --suite\n";
        return std::nullopt;
      }
      suite = schedlab::benchmark::WorkloadSuite::Public;
      scenario_id = scenario_id.substr(std::string("public/").size());
    } else if (scenario_id.starts_with("hidden/")) {
      if (suite_arg.has_value() && suite != schedlab::benchmark::WorkloadSuite::Hidden) {
        *error = "runner: scenario suite prefix conflicts with --suite\n";
        return std::nullopt;
      }
      suite = schedlab::benchmark::WorkloadSuite::Hidden;
      scenario_id = scenario_id.substr(std::string("hidden/").size());
    }

    const auto discovered = schedlab::benchmark::discover_workload_scenarios(suite);
    if (!discovered.diagnostics.empty()) {
      if (!write_discovery_diagnostics(discovered, jsonl_mode)) {
        *error = "runner: failed to write diagnostics\n";
        return std::nullopt;
      }
    }

    const auto it =
        std::find_if(discovered.scenarios.begin(), discovered.scenarios.end(),
                     [&scenario_id](const schedlab::benchmark::DiscoveredScenario& scenario) {
                       return scenario.id == scenario_id;
                     });
    if (it == discovered.scenarios.end()) {
      *error = "runner: unknown scenario '" + scenario_id + "'\n";
      return std::nullopt;
    }

    return SelectedScenarios{
        .suite = suite,
        .requested_track = std::nullopt,
        .requested_role = std::nullopt,
        .scenarios = {*it},
    };
  }

  const auto parsed_track = parse_track_token(*track_arg);
  if (!parsed_track.has_value()) {
    *error = "runner: unsupported track\n";
    return std::nullopt;
  }
  const auto effective_role = requested_role.has_value() ? requested_role : default_role_for_track;
  const auto discovered =
      schedlab::benchmark::discover_workload_scenarios(suite, *parsed_track, effective_role);
  if (!discovered.diagnostics.empty()) {
    if (!write_discovery_diagnostics(discovered, jsonl_mode)) {
      *error = "runner: failed to write diagnostics\n";
      return std::nullopt;
    }
  }
  if (discovered.scenarios.empty()) {
    *error = "runner: no scenarios matched requested track and suite\n";
    return std::nullopt;
  }

  return SelectedScenarios{
      .suite = suite,
      .requested_track = *parsed_track,
      .requested_role = effective_role,
      .scenarios = discovered.scenarios,
  };
}

std::optional<std::vector<LoadedScenario>> load_scenario_specs(const SelectedScenarios& selected,
                                                               std::string* error) {
  std::vector<LoadedScenario> loaded;
  loaded.reserve(selected.scenarios.size());
  for (const auto& scenario : selected.scenarios) {
    const auto parsed = schedlab::benchmark::load_workload_scenario(scenario);
    if (parsed.error.has_value()) {
      *error = "runner: failed to parse scenario '" + scenario.id + "'";
      if (parsed.error->line > 0) {
        *error += " line " + std::to_string(parsed.error->line);
      }
      *error += ": " + parsed.error->message + "\n";
      return std::nullopt;
    }
    if (!parsed.spec.has_value() || !parsed.spec->track.has_value()) {
      *error = "runner: parsed scenario missing required track '" + scenario.id + "'\n";
      return std::nullopt;
    }
    loaded.push_back(LoadedScenario{
        .id = scenario.id,
        .track = *parsed.spec->track,
        .scorer = parsed.spec->scorer.value_or(default_scorer_for_track(*parsed.spec->track)),
        .scenario_weight = parsed.spec->scenario_weight.value_or(1.0),
        .spec = *parsed.spec,
    });
  }
  return loaded;
}

std::optional<std::vector<LoadedScenario>> load_track_role_scenarios(
    schedlab::benchmark::WorkloadSuite suite, schedlab::benchmark::dsl::Track track,
    schedlab::benchmark::dsl::ScenarioRole role, bool jsonl_mode, std::string* error) {
  const auto discovered = schedlab::benchmark::discover_workload_scenarios(suite, track, role);
  if (!discovered.diagnostics.empty()) {
    if (!write_discovery_diagnostics(discovered, jsonl_mode)) {
      *error = "runner: failed to write diagnostics\n";
      return std::nullopt;
    }
  }
  const SelectedScenarios selected{
      .suite = suite,
      .requested_track = track,
      .requested_role = role,
      .scenarios = discovered.scenarios,
  };
  return load_scenario_specs(selected, error);
}

struct ListingFilter {
  std::optional<schedlab::benchmark::WorkloadSuite> suite;
  std::optional<schedlab::benchmark::dsl::Track> track;
  std::optional<schedlab::benchmark::dsl::ScenarioRole> role;
};

bool emit_run_started_record(std::string_view mode, std::optional<EngineKind> engine,
                             schedlab::benchmark::WorkloadSuite suite,
                             std::optional<schedlab::benchmark::dsl::Track> requested_track,
                             std::optional<schedlab::benchmark::dsl::ScenarioRole> requested_role,
                             std::size_t scenario_count, int repetitions) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "run_started");
  append_json_string_field(&output, &first_field, "mode", mode);
  if (engine.has_value()) {
    append_json_string_field(&output, &first_field, "engine", engine_name(*engine));
  }
  append_json_string_field(&output, &first_field, "suite", suite_name(suite));
  if (requested_track.has_value()) {
    append_json_string_field(&output, &first_field, "track", track_name(*requested_track));
  }
  if (requested_role.has_value()) {
    append_json_string_field(&output, &first_field, "role", role_name(*requested_role));
  }
  append_json_uint_field(&output, &first_field, "scenario_count", scenario_count);
  append_json_uint_field(&output, &first_field, "repetitions", repetitions);
  output += "}";
  return emit_jsonl_record(output);
}

bool emit_run_finished_record(bool ok, std::optional<std::string_view> error = std::nullopt) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "run_finished");
  append_json_bool_field(&output, &first_field, "ok", ok);
  if (error.has_value()) {
    append_json_string_field(&output, &first_field, "error", *error);
  }
  output += "}";
  return emit_jsonl_record(output);
}

bool emit_workload_listed_record(schedlab::benchmark::WorkloadSuite suite,
                                 const schedlab::benchmark::DiscoveredScenario& scenario) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "workload_listed");
  append_json_string_field(&output, &first_field, "suite", suite_name(suite));
  append_json_string_field(&output, &first_field, "scenario", scenario.id);
  append_json_string_field(&output, &first_field, "track", track_name(scenario.track));
  append_json_string_field(&output, &first_field, "role", role_name(scenario.role));
  if (scenario.scorer.has_value()) {
    append_json_string_field(&output, &first_field, "scorer", scorer_name(*scenario.scorer));
  }
  if (scenario.scenario_weight.has_value()) {
    append_json_double_field(&output, &first_field, "scenario_weight", *scenario.scenario_weight);
  }
  if (scenario.variant_name.has_value()) {
    append_json_string_field(&output, &first_field, "variant", *scenario.variant_name);
  }
  output += "}";
  return emit_jsonl_record(output);
}

bool emit_scenario_started_record(std::string_view mode, EngineKind engine,
                                  std::string_view scenario_id,
                                  schedlab::benchmark::dsl::Track track,
                                  schedlab::benchmark::SchedulerKind scheduler, int repetition,
                                  std::string_view run_kind,
                                  std::optional<std::string_view> standalone_group = std::nullopt) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "scenario_started");
  append_json_string_field(&output, &first_field, "mode", mode);
  append_json_string_field(&output, &first_field, "engine", engine_name(engine));
  append_json_string_field(&output, &first_field, "scenario", scenario_id);
  append_json_string_field(&output, &first_field, "track", track_name(track));
  append_json_string_field(&output, &first_field, "scheduler",
                           schedlab::benchmark::scheduler_kind_name(scheduler));
  append_json_uint_field(&output, &first_field, "repetition", repetition);
  append_json_string_field(&output, &first_field, "run_kind", run_kind);
  if (standalone_group.has_value()) {
    append_json_string_field(&output, &first_field, "standalone_group", *standalone_group);
  }
  output += "}";
  return emit_jsonl_record(output);
}

bool emit_scenario_finished_record(
    std::string_view mode, EngineKind engine, std::string_view scenario_id,
    schedlab::benchmark::dsl::Track track, schedlab::benchmark::SchedulerKind scheduler,
    int repetition, std::string_view run_kind, const schedlab::benchmark::RunMetrics& metrics,
    std::optional<std::string_view> standalone_group = std::nullopt) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "scenario_finished");
  append_json_string_field(&output, &first_field, "mode", mode);
  append_json_string_field(&output, &first_field, "engine", engine_name(engine));
  append_json_string_field(&output, &first_field, "scenario", scenario_id);
  append_json_string_field(&output, &first_field, "track", track_name(track));
  append_json_string_field(&output, &first_field, "scheduler",
                           schedlab::benchmark::scheduler_kind_name(scheduler));
  append_json_uint_field(&output, &first_field, "repetition", repetition);
  append_json_string_field(&output, &first_field, "run_kind", run_kind);
  if (standalone_group.has_value()) {
    append_json_string_field(&output, &first_field, "standalone_group", *standalone_group);
  }
  append_json_raw_field(&output, &first_field, "metrics", metrics_json(metrics));
  output += "}";
  return emit_jsonl_record(output);
}

bool emit_scenario_scored_record(const schedlab::benchmark::ScenarioScore& score) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "scenario_scored");
  append_json_string_field(&output, &first_field, "scenario", score.scenario_id);
  append_json_string_field(&output, &first_field, "track", track_name(score.track));
  append_json_string_field(&output, &first_field, "scorer", scorer_name(score.scorer));
  append_json_double_field(&output, &first_field, "weight", score.weight);
  append_json_bool_field(&output, &first_field, "correctness_passed", score.correctness_passed);
  append_json_double_field(&output, &first_field, "student_quality", score.student_quality);
  append_json_double_field(&output, &first_field, "baseline_quality", score.baseline_quality);
  append_json_double_field(&output, &first_field, "score", score.score);
  if (score.student_fairness.has_value()) {
    append_json_raw_field(&output, &first_field, "student_fairness",
                          fairness_diagnostics_json(*score.student_fairness));
  }
  if (score.baseline_fairness.has_value()) {
    append_json_raw_field(&output, &first_field, "baseline_fairness",
                          fairness_diagnostics_json(*score.baseline_fairness));
  }
  output += "}";
  return emit_jsonl_record(output);
}

bool emit_track_scored_record(const schedlab::benchmark::TrackScore& score) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "track_scored");
  append_json_string_field(&output, &first_field, "track", track_name(score.track));
  append_json_bool_field(&output, &first_field, "correctness_passed", score.correctness_passed);
  append_json_double_field(&output, &first_field, "score", score.score);
  append_json_double_field(&output, &first_field, "display_score", score.display_score);
  append_json_uint_field(&output, &first_field, "scenario_count", score.scenario_scores.size());
  output += "}";
  return emit_jsonl_record(output);
}

bool emit_gate_started_record(schedlab::benchmark::WorkloadSuite suite,
                              schedlab::benchmark::dsl::Track track, std::size_t scenario_count) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "gate_started");
  append_json_string_field(&output, &first_field, "suite", suite_name(suite));
  append_json_string_field(&output, &first_field, "track", track_name(track));
  append_json_uint_field(&output, &first_field, "scenario_count", scenario_count);
  output += "}";
  return emit_jsonl_record(output);
}

bool emit_gate_finished_record(schedlab::benchmark::WorkloadSuite suite,
                               schedlab::benchmark::dsl::Track track, std::size_t scenario_count,
                               bool passed, const std::vector<std::string>& failed_scenarios,
                               std::optional<std::string_view> reason = std::nullopt) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "gate_finished");
  append_json_string_field(&output, &first_field, "suite", suite_name(suite));
  append_json_string_field(&output, &first_field, "track", track_name(track));
  append_json_uint_field(&output, &first_field, "scenario_count", scenario_count);
  append_json_bool_field(&output, &first_field, "passed", passed);
  append_json_raw_field(&output, &first_field, "failed_scenarios",
                        json_string_array(failed_scenarios));
  if (reason.has_value()) {
    append_json_string_field(&output, &first_field, "reason", *reason);
  }
  output += "}";
  return emit_jsonl_record(output);
}

bool emit_debug_aggregate_record(schedlab::benchmark::dsl::Track track,
                                 const schedlab::benchmark::RunMetrics& metrics) {
  std::string output = "{";
  bool first_field = true;
  append_json_string_field(&output, &first_field, "type", "debug_aggregate");
  append_json_string_field(&output, &first_field, "track", track_name(track));
  append_json_raw_field(&output, &first_field, "metrics", metrics_json(metrics));
  output += "}";
  return emit_jsonl_record(output);
}

std::optional<ReleaseExecutionResult>
execute_release_scenarios(const std::vector<LoadedScenario>& loaded, EngineKind engine, int workers,
                          int repetitions,
                          schedlab::benchmark::SchedulerKind candidate_scheduler_kind,
                          bool jsonl_mode, std::string* error) {
  constexpr auto kBaselineScheduler = schedlab::benchmark::SchedulerKind::Baseline;

  std::map<std::string, std::vector<schedlab::benchmark::RunMetrics>> student_runs_by_scenario;
  std::map<std::string, std::vector<schedlab::benchmark::RunMetrics>> baseline_runs_by_scenario;

  for (int i = 0; i < repetitions; ++i) {
    const auto order = release_slots_for(i, candidate_scheduler_kind, kBaselineScheduler);
    for (const auto& scenario : loaded) {
      for (int slot = 0; slot < 2; ++slot) {
        if (jsonl_mode &&
            !emit_scenario_started_record("release", engine, scenario.id, scenario.track,
                                          order[slot].kind, i + 1, "mixed")) {
          *error = "runner: failed to emit scenario_started record";
          return std::nullopt;
        }
        const auto result = run_loaded_scenario(scenario, order[slot].kind, engine, workers,
                                                schedlab::runtime::Worker::Config{});
        if (!result.has_value()) {
          *error = "runner: failed to execute selected scenario";
          return std::nullopt;
        }
        if (jsonl_mode &&
            !emit_scenario_finished_record("release", engine, scenario.id, scenario.track,
                                           order[slot].kind, i + 1, "mixed", result->metrics)) {
          *error = "runner: failed to emit scenario_finished record";
          return std::nullopt;
        }
        if (order[slot].is_candidate) {
          student_runs_by_scenario[scenario.id].push_back(result->metrics);
        } else {
          baseline_runs_by_scenario[scenario.id].push_back(result->metrics);
        }
      }
    }
  }

  std::vector<schedlab::benchmark::ScenarioScoreInput> score_inputs;
  for (const auto& scenario : loaded) {
    const auto student_it = student_runs_by_scenario.find(scenario.id);
    const auto baseline_it = baseline_runs_by_scenario.find(scenario.id);
    if (student_it == student_runs_by_scenario.end() ||
        baseline_it == baseline_runs_by_scenario.end()) {
      continue;
    }
    score_inputs.push_back(schedlab::benchmark::ScenarioScoreInput{
        .scenario_id = scenario.id,
        .track = scenario.track,
        .scorer = scenario.scorer,
        .score_groups = scenario.spec.score_groups,
        .weight = scenario.scenario_weight,
        .correctness_passed = true,
        .student_runs = student_it->second,
        .baseline_runs = baseline_it->second,
    });
  }
  if (score_inputs.empty()) {
    *error = "runner: failed to collect release metrics";
    return std::nullopt;
  }

  const auto score_summary = schedlab::benchmark::score_scenarios(score_inputs);
  if (score_summary.track_scores.empty()) {
    *error = "runner: failed to score selected scenarios";
    return std::nullopt;
  }

  return ReleaseExecutionResult{
      .score_summary = score_summary,
  };
}

GateEvaluation evaluate_gate(const schedlab::benchmark::ScoreSummary& score_summary) {
  GateEvaluation evaluation{
      .passed = score_summary.correctness_gate_passed,
  };
  for (const auto& track_score : score_summary.track_scores) {
    if (!track_score.correctness_passed) {
      evaluation.passed = false;
    }
    for (const auto& scenario_score : track_score.scenario_scores) {
      if (!scenario_score.correctness_passed || scenario_score.score <= 0.0) {
        evaluation.passed = false;
        evaluation.failed_scenarios.push_back(scenario_score.scenario_id);
      }
    }
  }
  return evaluation;
}

std::optional<ListingFilter> parse_listing_filter(const std::vector<std::string>& args,
                                                  std::string* error) {
  ListingFilter filter;
  if (const auto suite_arg = value_after(args, "--suite"); suite_arg.has_value()) {
    const auto parsed_suite = parse_suite_token(*suite_arg);
    if (!parsed_suite.has_value()) {
      *error = "runner: unsupported suite\n";
      return std::nullopt;
    }
    filter.suite = *parsed_suite;
  }
  if (const auto track_arg = value_after(args, "--track"); track_arg.has_value()) {
    const auto parsed_track = parse_track_token(*track_arg);
    if (!parsed_track.has_value()) {
      *error = "runner: unsupported track\n";
      return std::nullopt;
    }
    filter.track = *parsed_track;
  }
  if (const auto role_arg = value_after(args, "--role"); role_arg.has_value()) {
    const auto parsed_role = parse_role_token(*role_arg);
    if (!parsed_role.has_value()) {
      *error = "runner: unsupported role\n";
      return std::nullopt;
    }
    filter.role = *parsed_role;
  }
  return filter;
}

bool emit_workload_listing(const ListingFilter& filter, bool jsonl_mode) {
  std::string output;
  std::vector<schedlab::benchmark::WorkloadSuite> suites;
  if (filter.suite.has_value()) {
    suites.push_back(*filter.suite);
  } else {
    suites = {
        schedlab::benchmark::WorkloadSuite::Public,
        schedlab::benchmark::WorkloadSuite::Hidden,
    };
  }
  for (const auto suite : suites) {
    const auto discovered =
        schedlab::benchmark::discover_workload_scenarios(suite, filter.track, filter.role);
    if (!discovered.diagnostics.empty()) {
      if (!write_discovery_diagnostics(discovered, jsonl_mode)) {
        return false;
      }
    }
    for (const auto& scenario : discovered.scenarios) {
      if (jsonl_mode) {
        if (!emit_workload_listed_record(suite, scenario)) {
          return false;
        }
        continue;
      }
      output += std::string(suite_name(suite)) + "/" + scenario.id +
                " track=" + track_name(scenario.track) + " role=" + role_name(scenario.role) + "\n";
    }
  }

  if (jsonl_mode) {
    return true;
  }
  return write_all(STDERR_FILENO, output);
}

} // namespace

int main(int argc, char** argv) {
  std::vector<std::string> args;
  args.reserve(static_cast<std::size_t>(argc > 0 ? argc - 1 : 0));
  for (int i = 1; i < argc; ++i) {
    args.emplace_back(argv[i]);
  }

  const bool jsonl_mode = has_flag(args, "--jsonl");

  if (has_flag(args, "--list-workloads")) {
    std::string listing_error;
    const auto listing_filter = parse_listing_filter(args, &listing_error);
    if (!listing_filter.has_value()) {
      if (!listing_error.empty()) {
        (void)write_all(STDERR_FILENO, listing_error);
      } else {
        usage();
      }
      return 1;
    }
    if (jsonl_mode && !emit_run_started_record("list", std::nullopt,
                                               listing_filter->suite.value_or(
                                                   schedlab::benchmark::WorkloadSuite::Public),
                                               listing_filter->track, listing_filter->role, 0, 1)) {
      return 1;
    }
    if (!emit_workload_listing(*listing_filter, jsonl_mode)) {
      (void)write_all(STDERR_FILENO, "runner: failed to list workloads\n");
      if (jsonl_mode) {
        (void)emit_run_finished_record(false, "runner: failed to list workloads");
      }
      return 1;
    }
    if (jsonl_mode && !emit_run_finished_record(true)) {
      return 1;
    }
    return 0;
  }

  const auto mode = value_after(args, "--mode");
  if (!mode.has_value()) {
    usage();
    return 1;
  }

  std::optional<EngineKind> requested_engine;
  if (const auto engine_arg = value_after(args, "--engine"); engine_arg.has_value()) {
    requested_engine = parse_engine_token(*engine_arg);
    if (!requested_engine.has_value()) {
      if (jsonl_mode) {
        (void)emit_run_finished_record(false, "runner: unsupported engine");
      }
      (void)write_all(STDERR_FILENO, "runner: unsupported engine\n");
      return 1;
    }
  }

  int repetitions = 1;
  if (const auto repetitions_arg = value_after(args, "--repetitions");
      repetitions_arg.has_value()) {
    repetitions = std::atoi(repetitions_arg->c_str());
  }

  int workers = 0;
  if (const auto workers_arg = value_after(args, "--workers"); workers_arg.has_value()) {
    workers = std::atoi(workers_arg->c_str());
  }

  if (repetitions <= 0) {
    if (jsonl_mode) {
      (void)emit_run_finished_record(false, "runner: repetitions must be > 0");
    }
    (void)write_all(STDERR_FILENO, "runner: repetitions must be > 0\n");
    return 1;
  }

  std::string selection_error;
  const std::optional<schedlab::benchmark::dsl::ScenarioRole> default_track_role =
      (*mode == "release")
          ? std::optional<schedlab::benchmark::dsl::ScenarioRole>{schedlab::benchmark::dsl::
                                                                      ScenarioRole::Leaderboard}
          : std::nullopt;
  const auto selected =
      resolve_selected_scenarios(args, jsonl_mode, default_track_role, &selection_error);
  if (!selected.has_value()) {
    if (jsonl_mode && !selection_error.empty()) {
      (void)emit_run_finished_record(false, selection_error);
    }
    if (!selection_error.empty()) {
      (void)write_all(STDERR_FILENO, selection_error);
    } else {
      usage();
    }
    return 1;
  }

  const auto loaded = load_scenario_specs(*selected, &selection_error);
  if (!loaded.has_value()) {
    if (jsonl_mode && !selection_error.empty()) {
      (void)emit_run_finished_record(false, selection_error);
    }
    if (!selection_error.empty()) {
      (void)write_all(STDERR_FILENO, selection_error);
    } else {
      (void)write_all(STDERR_FILENO, "runner: failed to load selected scenarios\n");
    }
    return 1;
  }

  if (*mode == "debug") {
    const EngineKind engine = requested_engine.value_or(EngineKind::Real);
    const auto scheduler_visibility = schedlab::benchmark::has_private_schedulers()
                                          ? schedlab::benchmark::SchedulerVisibility::All
                                          : schedlab::benchmark::SchedulerVisibility::PublicOnly;
    const auto scheduler_kind = schedlab::benchmark::parse_scheduler_kind(
        value_after(args, "--scheduler").value_or("student"), scheduler_visibility);
    if (!scheduler_kind.has_value()) {
      if (jsonl_mode) {
        (void)emit_run_finished_record(false, "runner: unsupported scheduler");
      }
      (void)write_all(STDERR_FILENO, "runner: unsupported scheduler\n");
      return 1;
    }

    if (jsonl_mode &&
        !emit_run_started_record("debug", engine, selected->suite, selected->requested_track,
                                 selected->requested_role, loaded->size(), repetitions)) {
      return 1;
    }

    std::map<schedlab::benchmark::dsl::Track, std::vector<schedlab::benchmark::RunMetrics>>
        runs_by_track;
    std::optional<schedlab::benchmark::RunMetrics> single_run_metrics;
    std::optional<schedlab::benchmark::dsl::Track> single_run_track;
    std::string single_scenario_id;

    for (int i = 0; i < repetitions; ++i) {
      for (const auto& scenario : *loaded) {
        if (jsonl_mode &&
            !emit_scenario_started_record("debug", engine, scenario.id, scenario.track,
                                          *scheduler_kind, i + 1, "mixed")) {
          return 1;
        }
        const auto result = run_loaded_scenario(scenario, *scheduler_kind, engine, workers,
                                                schedlab::runtime::Worker::Config{});
        if (!result.has_value()) {
          if (jsonl_mode) {
            (void)emit_run_finished_record(false, "runner: failed to execute selected scenario");
          }
          (void)write_all(STDERR_FILENO, "runner: failed to execute selected scenario\n");
          return 1;
        }
        if (jsonl_mode &&
            !emit_scenario_finished_record("debug", engine, scenario.id, scenario.track,
                                           *scheduler_kind, i + 1, "mixed", result->metrics)) {
          return 1;
        }
        runs_by_track[result->track].push_back(result->metrics);
        single_run_metrics = result->metrics;
        single_run_track = result->track;
        single_scenario_id = scenario.id;
      }
    }

    const bool aggregate_output = repetitions > 1 || loaded->size() > 1 || runs_by_track.size() > 1;
    std::string output;
    output += "mode=debug";
    output += " engine=" + std::string(engine_name(engine));
    output +=
        " scheduler=" + std::string(schedlab::benchmark::scheduler_kind_name(*scheduler_kind));
    output += " suite=" + std::string(suite_name(selected->suite));
    output += " scenario_count=" + std::to_string(loaded->size());
    output += " repetitions=" + std::to_string(repetitions);
    output += " aggregate=" + std::string(aggregate_output ? "true" : "false");
    if (selected->requested_track.has_value()) {
      output += " track=" + std::string(track_name(*selected->requested_track));
    }

    schedlab::benchmark::EventLog log;
    if (jsonl_mode) {
      if (aggregate_output) {
        for (const auto& [track, runs] : runs_by_track) {
          if (!emit_debug_aggregate_record(
                  track, schedlab::benchmark::aggregate_debug_metrics(track, runs))) {
            return 1;
          }
        }
      }
      return emit_run_finished_record(true) ? 0 : 1;
    }

    output += " event_log_events=" + std::to_string(log.events().size());
    if (!aggregate_output) {
      output += " scenario=" + single_scenario_id;
      output += " track=" + std::string(track_name(*single_run_track));
      append_metrics_fields(&output, *single_run_metrics);
      output += "\n";
      return write_all(STDERR_FILENO, output) ? 0 : 1;
    }

    output += " track_count=" + std::to_string(runs_by_track.size()) + "\n";
    for (const auto& [track, runs] : runs_by_track) {
      output += "aggregate_track track=" + std::string(track_name(track));
      append_metrics_fields(&output, schedlab::benchmark::aggregate_debug_metrics(track, runs));
      output += "\n";
    }
    return write_all(STDERR_FILENO, output) ? 0 : 1;
  }

  if (*mode == "release") {
    const EngineKind engine = requested_engine.value_or(EngineKind::Sim);
    if (has_flag(args, "--scheduler")) {
      if (jsonl_mode) {
        (void)emit_run_finished_record(false,
                                       "runner: --scheduler is only supported in debug mode");
      }
      (void)write_all(STDERR_FILENO, "runner: --scheduler is only supported in debug mode\n");
      return 1;
    }

    const auto scheduler_visibility = schedlab::benchmark::has_private_schedulers()
                                          ? schedlab::benchmark::SchedulerVisibility::All
                                          : schedlab::benchmark::SchedulerVisibility::PublicOnly;
    const auto candidate_scheduler_kind = schedlab::benchmark::parse_scheduler_kind(
        value_after(args, "--candidate-scheduler").value_or("student"), scheduler_visibility);
    if (!candidate_scheduler_kind.has_value()) {
      if (jsonl_mode) {
        (void)emit_run_finished_record(false, "runner: unsupported scheduler");
      }
      (void)write_all(STDERR_FILENO, "runner: unsupported scheduler\n");
      return 1;
    }
    if (jsonl_mode &&
        !emit_run_started_record("release", engine, selected->suite, selected->requested_track,
                                 selected->requested_role, loaded->size(), repetitions)) {
      return 1;
    }
    const bool run_gate =
        selected->requested_track.has_value() &&
        selected->requested_role.value_or(schedlab::benchmark::dsl::ScenarioRole::Leaderboard) ==
            schedlab::benchmark::dsl::ScenarioRole::Leaderboard;

    if (run_gate) {
      const auto gate_loaded = load_track_role_scenarios(
          selected->suite, *selected->requested_track, schedlab::benchmark::dsl::ScenarioRole::Gate,
          jsonl_mode, &selection_error);
      if (!gate_loaded.has_value()) {
        if (jsonl_mode) {
          (void)emit_gate_finished_record(selected->suite, *selected->requested_track, 0, false, {},
                                          selection_error);
          (void)emit_run_finished_record(false, "runner: correctness gate failed");
        }
        (void)write_all(STDERR_FILENO, selection_error);
        return 1;
      }
      if (jsonl_mode && !emit_gate_started_record(selected->suite, *selected->requested_track,
                                                  gate_loaded->size())) {
        return 1;
      }
      if (!gate_loaded->empty()) {
        const auto gate_result =
            execute_release_scenarios(*gate_loaded, engine, workers, repetitions,
                                      *candidate_scheduler_kind, jsonl_mode, &selection_error);
        if (!gate_result.has_value()) {
          if (jsonl_mode) {
            (void)emit_gate_finished_record(selected->suite, *selected->requested_track,
                                            gate_loaded->size(), false, {}, selection_error);
            (void)emit_run_finished_record(false, "runner: correctness gate failed");
          }
          (void)write_all(STDERR_FILENO, selection_error + "\n");
          return 1;
        }
        const auto& gate_track_score = gate_result->score_summary.track_scores.front();
        if (jsonl_mode) {
          for (const auto& scenario_score : gate_track_score.scenario_scores) {
            if (!emit_scenario_scored_record(scenario_score)) {
              return 1;
            }
          }
        }
        const auto gate_evaluation = evaluate_gate(gate_result->score_summary);
        if (jsonl_mode && !emit_gate_finished_record(
                              selected->suite, *selected->requested_track, gate_loaded->size(),
                              gate_evaluation.passed, gate_evaluation.failed_scenarios,
                              gate_evaluation.passed
                                  ? std::nullopt
                                  : std::optional<std::string_view>{"correctness gate failed"})) {
          return 1;
        }
        if (!gate_evaluation.passed) {
          std::string output =
              std::string("mode=release ") + "engine=" + std::string(engine_name(engine)) + " " +
              "suite=" + std::string(suite_name(selected->suite)) + " " + "student_scheduler=" +
              std::string(schedlab::benchmark::scheduler_kind_name(*candidate_scheduler_kind)) +
              " baseline_scheduler=baseline " +
              "track=" + std::string(track_name(*selected->requested_track)) + " " + "gate=fail";
          if (!gate_evaluation.failed_scenarios.empty()) {
            output += " failed_scenarios=";
            for (std::size_t i = 0; i < gate_evaluation.failed_scenarios.size(); ++i) {
              if (i > 0) {
                output += ",";
              }
              output += gate_evaluation.failed_scenarios[i];
            }
          }
          output += "\n";
          if (!jsonl_mode && !write_all(STDERR_FILENO, output)) {
            return 1;
          }
          if (jsonl_mode) {
            (void)emit_run_finished_record(false, "runner: correctness gate failed");
          }
          return 1;
        }
      } else if (jsonl_mode && !emit_gate_finished_record(
                                   selected->suite, *selected->requested_track, 0, true, {})) {
        return 1;
      }
    }

    const auto release_result =
        execute_release_scenarios(*loaded, engine, workers, repetitions, *candidate_scheduler_kind,
                                  jsonl_mode, &selection_error);
    if (!release_result.has_value()) {
      if (jsonl_mode) {
        (void)emit_run_finished_record(false, selection_error);
      }
      (void)write_all(STDERR_FILENO, selection_error + "\n");
      return 1;
    }
    const auto& track_score = release_result->score_summary.track_scores.front();

    if (jsonl_mode) {
      for (const auto& scenario_score : track_score.scenario_scores) {
        if (!emit_scenario_scored_record(scenario_score)) {
          return 1;
        }
      }
      if (!emit_track_scored_record(track_score)) {
        return 1;
      }
      return emit_run_finished_record(true) ? 0 : 1;
    }

    const std::string output =
        std::string("mode=release ") + "engine=" + std::string(engine_name(engine)) + " " +
        "suite=" + std::string(suite_name(selected->suite)) +
        " scenario_count=" + std::to_string(loaded->size()) +
        " repetitions=" + std::to_string(repetitions) + " " + "student_scheduler=" +
        std::string(schedlab::benchmark::scheduler_kind_name(*candidate_scheduler_kind)) +
        " baseline_scheduler=baseline " +
        (selected->requested_track.has_value()
             ? "track=" + std::string(track_name(*selected->requested_track)) + " "
             : std::string()) +
        (run_gate ? "gate=pass " : std::string()) + "score=" + std::to_string(track_score.score) +
        " display_score=" + std::to_string(track_score.display_score) + "\n";
    return write_all(STDERR_FILENO, output) ? 0 : 1;
  }

  (void)write_all(STDERR_FILENO, "runner: unsupported mode\n");
  return 1;
}

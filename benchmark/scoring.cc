#include "benchmark/scoring.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace schedlab::benchmark {

namespace {

constexpr double kStarvationFairnessSkew = 10000.0;
constexpr double kLatencyThroughputGuardrail = 0.8;
constexpr uint64_t kFairnessMinWindowUs = 1000;
constexpr std::size_t kFairnessTargetWindowCount = 64;

std::size_t trim_each_side_for_count(std::size_t count) noexcept {
  return (count > 2) ? 1u : 0u;
}

bool debug_track_prefers_latency_metric(dsl::Track track) noexcept {
  return track == dsl::Track::IoBound || track == dsl::Track::Latency;
}

std::optional<double> flow_p99_us(const RunMetrics& metrics,
                                  const std::vector<std::string>& score_groups) {
  if (metrics.task_observations.empty()) {
    return std::nullopt;
  }

  std::unordered_set<uint64_t> included_flow_ids;
  if (!score_groups.empty()) {
    std::unordered_set<std::size_t> selected_group_indices;
    for (std::size_t group_index = 0; group_index < metrics.group_observations.size();
         ++group_index) {
      const auto& group = metrics.group_observations[group_index];
      if (std::find(score_groups.begin(), score_groups.end(), group.group_name) !=
          score_groups.end()) {
        selected_group_indices.insert(group_index);
      }
    }
    if (selected_group_indices.empty()) {
      return std::nullopt;
    }
    for (const TaskObservation& observation : metrics.task_observations) {
      if (selected_group_indices.find(observation.group_index) == selected_group_indices.end()) {
        continue;
      }
      included_flow_ids.insert((observation.flow_id != 0) ? observation.flow_id
                                                          : observation.task_id);
    }
    if (included_flow_ids.empty()) {
      return std::nullopt;
    }
  }

  struct FlowAggregate {
    uint64_t earliest_release_us = std::numeric_limits<uint64_t>::max();
    uint64_t latest_completion_us = 0;
  };

  std::unordered_map<uint64_t, FlowAggregate> flows_by_id;
  flows_by_id.reserve(metrics.task_observations.size());
  for (const TaskObservation& observation : metrics.task_observations) {
    const uint64_t flow_id = (observation.flow_id != 0) ? observation.flow_id : observation.task_id;
    if (!included_flow_ids.empty() && included_flow_ids.find(flow_id) == included_flow_ids.end()) {
      continue;
    }
    FlowAggregate& aggregate = flows_by_id[flow_id];
    aggregate.earliest_release_us =
        std::min(aggregate.earliest_release_us, observation.release_time_us);
    aggregate.latest_completion_us =
        std::max(aggregate.latest_completion_us, observation.completion_time_us);
  }

  std::vector<uint64_t> flow_times_us;
  flow_times_us.reserve(flows_by_id.size());
  for (const auto& [flow_id, aggregate] : flows_by_id) {
    (void)flow_id;
    const uint64_t flow_time_us =
        (aggregate.latest_completion_us >= aggregate.earliest_release_us)
            ? (aggregate.latest_completion_us - aggregate.earliest_release_us)
            : 0;
    flow_times_us.push_back(flow_time_us);
  }

  std::sort(flow_times_us.begin(), flow_times_us.end());
  const std::size_t index = ((flow_times_us.size() - 1) * static_cast<std::size_t>(99)) / 100;
  return static_cast<double>(flow_times_us[index]);
}

template <typename Extractor>
std::optional<double> aggregate_metric(const std::vector<RunMetrics>& runs, Extractor extractor) {
  std::vector<double> values;
  values.reserve(runs.size());
  for (const RunMetrics& run : runs) {
    const auto value = extractor(run);
    if (!value.has_value()) {
      return std::nullopt;
    }
    values.push_back(*value);
  }
  return trimmed_mean(std::move(values), trim_each_side_for_count(values.size()));
}

std::optional<double> aggregate_quality(dsl::ScorerKind scorer, const std::vector<RunMetrics>& runs,
                                        const std::vector<std::string>& score_groups) {
  switch (scorer) {
  case dsl::ScorerKind::ThroughputMakespan:
    return aggregate_metric(runs, [](const RunMetrics& run) -> std::optional<double> {
      if (run.elapsed_time_us == 0) {
        return std::nullopt;
      }
      return 1.0 / static_cast<double>(run.elapsed_time_us);
    });
  case dsl::ScorerKind::ThroughputSustainedRate:
    return aggregate_metric(runs, [](const RunMetrics& run) -> std::optional<double> {
      if (run.throughput_tasks_per_sec <= 0.0) {
        return std::nullopt;
      }
      return run.throughput_tasks_per_sec;
    });
  case dsl::ScorerKind::LatencyWakeupP99:
    return aggregate_metric(runs, [](const RunMetrics& run) -> std::optional<double> {
      const uint64_t effective_latency_us = std::max<uint64_t>(run.p99_wakeup_latency_us, 1);
      return 1.0 / static_cast<double>(effective_latency_us);
    });
  case dsl::ScorerKind::LatencyFlowP99:
    return aggregate_metric(runs, [&score_groups](const RunMetrics& run) -> std::optional<double> {
      const auto flow_p99 = flow_p99_us(run, score_groups);
      if (!flow_p99.has_value()) {
        return std::nullopt;
      }
      return 1.0 / std::max(*flow_p99, 1.0);
    });
  case dsl::ScorerKind::FairnessShareSkew:
    return std::nullopt;
  }
  return std::nullopt;
}

std::optional<double> aggregate_throughput_rate(const std::vector<RunMetrics>& runs) {
  return aggregate_metric(runs, [](const RunMetrics& run) -> std::optional<double> {
    if (run.throughput_tasks_per_sec <= 0.0) {
      return std::nullopt;
    }
    return run.throughput_tasks_per_sec;
  });
}

std::optional<double> mean_flow_time_us_for_group(const RunMetrics& run, std::size_t group_index) {
  double total_flow_time = 0.0;
  std::size_t samples = 0;
  for (const TaskObservation& task : run.task_observations) {
    if (task.group_index != group_index) {
      continue;
    }
    const uint64_t flow_time_us = (task.completion_time_us >= task.release_time_us)
                                      ? (task.completion_time_us - task.release_time_us)
                                      : 0;
    total_flow_time += static_cast<double>(flow_time_us);
    ++samples;
  }
  if (samples == 0) {
    return std::nullopt;
  }
  return total_flow_time / static_cast<double>(samples);
}

std::optional<double> mean_flow_time_us_all_tasks(const RunMetrics& run) {
  if (run.task_observations.empty()) {
    return std::nullopt;
  }
  double total_flow_time = 0.0;
  for (const TaskObservation& task : run.task_observations) {
    const uint64_t flow_time_us = (task.completion_time_us >= task.release_time_us)
                                      ? (task.completion_time_us - task.release_time_us)
                                      : 0;
    total_flow_time += static_cast<double>(flow_time_us);
  }
  return total_flow_time / static_cast<double>(run.task_observations.size());
}

uint64_t fairness_window_us(uint64_t span_us) noexcept {
  if (span_us == 0) {
    return kFairnessMinWindowUs;
  }
  const uint64_t adaptive = (span_us + static_cast<uint64_t>(kFairnessTargetWindowCount - 1)) /
                            static_cast<uint64_t>(kFairnessTargetWindowCount);
  return std::max<uint64_t>(kFairnessMinWindowUs, adaptive);
}

struct FairnessRunAnalysis {
  double max_skew = 1.0;
  std::vector<double> overall_share_distortions;
};

std::optional<FairnessRunAnalysis> analyze_windowed_fairness(const RunMetrics& run) {
  if (run.group_observations.empty()) {
    return std::nullopt;
  }

  FairnessRunAnalysis analysis;
  analysis.overall_share_distortions.assign(run.group_observations.size(), 0.0);

  struct TimedGroupDelta {
    uint64_t time_us = 0;
    std::size_t group_index = 0;
    int delta = 0;
  };

  std::vector<TimedGroupDelta> runnable_deltas;
  runnable_deltas.reserve(run.group_runnable_events.size());
  std::vector<TimedGroupDelta> service_deltas;
  service_deltas.reserve(run.group_service_slices.size() * 2);

  uint64_t min_time_us = std::numeric_limits<uint64_t>::max();
  uint64_t max_time_us = 0;
  for (const auto& event : run.group_runnable_events) {
    if (event.group_index >= run.group_observations.size() || event.delta == 0) {
      continue;
    }
    runnable_deltas.push_back(TimedGroupDelta{
        .time_us = event.time_us,
        .group_index = event.group_index,
        .delta = event.delta,
    });
    min_time_us = std::min(min_time_us, event.time_us);
    max_time_us = std::max(max_time_us, event.time_us);
  }
  for (const auto& slice : run.group_service_slices) {
    if (slice.group_index >= run.group_observations.size() ||
        slice.end_time_us <= slice.start_time_us) {
      continue;
    }
    service_deltas.push_back(TimedGroupDelta{
        .time_us = slice.start_time_us,
        .group_index = slice.group_index,
        .delta = +1,
    });
    service_deltas.push_back(TimedGroupDelta{
        .time_us = slice.end_time_us,
        .group_index = slice.group_index,
        .delta = -1,
    });
    min_time_us = std::min(min_time_us, slice.start_time_us);
    max_time_us = std::max(max_time_us, slice.end_time_us);
  }
  if (min_time_us == std::numeric_limits<uint64_t>::max() || max_time_us <= min_time_us) {
    return std::nullopt;
  }

  std::sort(runnable_deltas.begin(), runnable_deltas.end(),
            [](const TimedGroupDelta& lhs, const TimedGroupDelta& rhs) {
              if (lhs.time_us != rhs.time_us) {
                return lhs.time_us < rhs.time_us;
              }
              if (lhs.group_index != rhs.group_index) {
                return lhs.group_index < rhs.group_index;
              }
              return lhs.delta < rhs.delta;
            });
  std::sort(service_deltas.begin(), service_deltas.end(),
            [](const TimedGroupDelta& lhs, const TimedGroupDelta& rhs) {
              if (lhs.time_us != rhs.time_us) {
                return lhs.time_us < rhs.time_us;
              }
              if (lhs.group_index != rhs.group_index) {
                return lhs.group_index < rhs.group_index;
              }
              return lhs.delta < rhs.delta;
            });

  std::vector<uint64_t> boundaries;
  boundaries.reserve(runnable_deltas.size() + service_deltas.size() + kFairnessTargetWindowCount +
                     2);
  boundaries.push_back(min_time_us);
  for (const auto& delta : runnable_deltas) {
    boundaries.push_back(delta.time_us);
  }
  for (const auto& delta : service_deltas) {
    boundaries.push_back(delta.time_us);
  }
  const uint64_t window_us = fairness_window_us(max_time_us - min_time_us);
  for (uint64_t boundary = min_time_us + window_us; boundary < max_time_us; boundary += window_us) {
    boundaries.push_back(boundary);
  }
  boundaries.push_back(max_time_us);
  std::sort(boundaries.begin(), boundaries.end());
  boundaries.erase(std::unique(boundaries.begin(), boundaries.end()), boundaries.end());
  if (boundaries.size() < 2) {
    return std::nullopt;
  }

  std::vector<int> runnable_counts(run.group_observations.size(), 0);
  std::vector<int> running_counts(run.group_observations.size(), 0);
  std::vector<double> window_actual(run.group_observations.size(), 0.0);
  std::vector<double> window_target(run.group_observations.size(), 0.0);
  std::vector<double> total_actual(run.group_observations.size(), 0.0);
  std::vector<double> total_target(run.group_observations.size(), 0.0);
  std::vector<double> window_skews;

  auto calculate_skew = [&](const std::vector<double>& actual, const std::vector<double>& target) {
    double skew = 1.0;
    bool saw_target = false;
    for (std::size_t group_index = 0; group_index < target.size(); ++group_index) {
      if (target[group_index] <= 0.0) {
        continue;
      }
      saw_target = true;
      if (actual[group_index] <= 0.0) {
        skew = std::max(skew, kStarvationFairnessSkew);
        continue;
      }
      const double distortion = actual[group_index] / target[group_index];
      skew = std::max(skew, distortion > 1.0 ? distortion : (1.0 / distortion));
    }
    return saw_target ? skew : 1.0;
  };

  std::size_t runnable_index = 0;
  std::size_t service_index = 0;
  for (std::size_t boundary_index = 0; boundary_index + 1 < boundaries.size(); ++boundary_index) {
    const uint64_t current_time_us = boundaries[boundary_index];
    while (runnable_index < runnable_deltas.size() &&
           runnable_deltas[runnable_index].time_us == current_time_us) {
      runnable_counts[runnable_deltas[runnable_index].group_index] +=
          runnable_deltas[runnable_index].delta;
      ++runnable_index;
    }
    while (service_index < service_deltas.size() &&
           service_deltas[service_index].time_us == current_time_us) {
      running_counts[service_deltas[service_index].group_index] +=
          service_deltas[service_index].delta;
      ++service_index;
    }

    const uint64_t next_time_us = boundaries[boundary_index + 1];
    if (next_time_us <= current_time_us) {
      continue;
    }
    const double dt = static_cast<double>(next_time_us - current_time_us);
    double total_service_rate = 0.0;
    for (int count : running_counts) {
      total_service_rate += static_cast<double>(std::max(count, 0));
    }
    uint64_t runnable_weight = 0;
    for (std::size_t group_index = 0; group_index < runnable_counts.size(); ++group_index) {
      if (runnable_counts[group_index] > 0) {
        runnable_weight += run.group_observations[group_index].weight;
      }
    }
    if (total_service_rate > 0.0 && runnable_weight > 0) {
      for (std::size_t group_index = 0; group_index < run.group_observations.size();
           ++group_index) {
        const double actual = static_cast<double>(std::max(running_counts[group_index], 0)) * dt;
        if (actual > 0.0) {
          window_actual[group_index] += actual;
          total_actual[group_index] += actual;
        }
        if (runnable_counts[group_index] > 0) {
          const double target = total_service_rate * dt *
                                (static_cast<double>(run.group_observations[group_index].weight) /
                                 static_cast<double>(runnable_weight));
          window_target[group_index] += target;
          total_target[group_index] += target;
        }
      }
    }

    const bool is_window_end = ((next_time_us - min_time_us) % window_us) == 0 ||
                               (boundary_index + 2 == boundaries.size());
    if (is_window_end) {
      window_skews.push_back(calculate_skew(window_actual, window_target));
      std::fill(window_actual.begin(), window_actual.end(), 0.0);
      std::fill(window_target.begin(), window_target.end(), 0.0);
    }
  }

  if (!window_skews.empty()) {
    std::sort(window_skews.begin(), window_skews.end());
    const std::size_t percentile_index =
        ((window_skews.size() - 1) * static_cast<std::size_t>(90)) / 100;
    analysis.max_skew = window_skews[percentile_index];
  }

  for (std::size_t group_index = 0; group_index < total_target.size(); ++group_index) {
    if (total_target[group_index] <= 0.0) {
      continue;
    }
    if (total_actual[group_index] <= 0.0) {
      analysis.overall_share_distortions[group_index] = 0.0;
      continue;
    }
    analysis.overall_share_distortions[group_index] =
        total_actual[group_index] / total_target[group_index];
  }
  return analysis;
}

struct FairnessAggregate {
  FairnessDiagnostics diagnostics;
  double quality = 0.0;
  double mean_makespan_us = 0.0;
};

std::optional<FairnessAggregate>
aggregate_fairness_quality(const std::vector<RunMetrics>& mixed_runs) {
  if (mixed_runs.empty()) {
    return std::nullopt;
  }

  const RunMetrics& representative = mixed_runs.front();
  if (representative.group_observations.empty()) {
    return std::nullopt;
  }

  FairnessAggregate aggregate;
  std::vector<double> makespans;
  makespans.reserve(mixed_runs.size());
  for (const RunMetrics& run : mixed_runs) {
    if (run.elapsed_time_us == 0) {
      return std::nullopt;
    }
    makespans.push_back(static_cast<double>(run.elapsed_time_us));
  }
  aggregate.mean_makespan_us =
      trimmed_mean(std::move(makespans), trim_each_side_for_count(mixed_runs.size()));

  std::vector<FairnessRunAnalysis> run_analyses;
  run_analyses.reserve(mixed_runs.size());
  for (const RunMetrics& run : mixed_runs) {
    if (run.group_observations.size() != representative.group_observations.size()) {
      return std::nullopt;
    }
    const auto analysis = analyze_windowed_fairness(run);
    if (!analysis.has_value()) {
      return std::nullopt;
    }
    run_analyses.push_back(*analysis);
  }

  std::vector<double> skew_samples;
  skew_samples.reserve(run_analyses.size());
  for (const auto& analysis : run_analyses) {
    skew_samples.push_back(analysis.max_skew);
  }
  const double mean_max_skew =
      trimmed_mean(std::move(skew_samples), trim_each_side_for_count(run_analyses.size()));
  double max_positive_share_distortion = 0.0;
  double min_positive_share_distortion = std::numeric_limits<double>::max();
  for (std::size_t group_index = 0; group_index < representative.group_observations.size();
       ++group_index) {
    const GroupObservation& group = representative.group_observations[group_index];
    std::vector<double> share_samples;
    share_samples.reserve(run_analyses.size());
    for (const auto& analysis : run_analyses) {
      if (group_index >= analysis.overall_share_distortions.size()) {
        return std::nullopt;
      }
      share_samples.push_back(analysis.overall_share_distortions[group_index]);
    }
    const double share_distortion =
        trimmed_mean(std::move(share_samples), trim_each_side_for_count(run_analyses.size()));
    if (share_distortion > 0.0) {
      max_positive_share_distortion = std::max(max_positive_share_distortion, share_distortion);
      min_positive_share_distortion = std::min(min_positive_share_distortion, share_distortion);
    }

    aggregate.diagnostics.groups.push_back(FairnessGroupDiagnostic{
        .group_name = group.group_name,
        .weight = group.weight,
        .share_distortion = share_distortion,
    });
  }

  if (mean_max_skew <= 0.0) {
    return std::nullopt;
  }

  aggregate.diagnostics.max_share_skew = mean_max_skew;
  if (max_positive_share_distortion > 0.0 && min_positive_share_distortion > 0.0 &&
      min_positive_share_distortion != std::numeric_limits<double>::max()) {
    aggregate.diagnostics.share_balance_ratio =
        max_positive_share_distortion / min_positive_share_distortion;
  } else {
    aggregate.diagnostics.share_balance_ratio = kStarvationFairnessSkew;
  }
  aggregate.quality = 1.0 / mean_max_skew;
  return aggregate;
}

double weighted_geometric_mean(const std::vector<ScenarioScore>& scenarios) {
  double weighted_log_total = 0.0;
  double total_weight = 0.0;
  for (const ScenarioScore& scenario : scenarios) {
    if (scenario.score <= 0.0 || scenario.weight <= 0.0) {
      return 0.0;
    }
    weighted_log_total += std::log(scenario.score) * scenario.weight;
    total_weight += scenario.weight;
  }
  if (total_weight <= 0.0) {
    return 0.0;
  }
  return std::exp(weighted_log_total / total_weight);
}

TrackScore& find_or_append_track_score(std::vector<TrackScore>* track_scores, dsl::Track track) {
  for (TrackScore& track_score : *track_scores) {
    if (track_score.track == track) {
      return track_score;
    }
  }
  track_scores->push_back(TrackScore{
      .track = track,
  });
  return track_scores->back();
}

} // namespace

double trimmed_mean(std::vector<double> values, std::size_t trim_each_side) {
  if (values.empty()) {
    return 0.0;
  }

  std::sort(values.begin(), values.end());
  if ((trim_each_side * 2) >= values.size()) {
    trim_each_side = 0;
  }

  double total = 0.0;
  const std::size_t begin = trim_each_side;
  const std::size_t end = values.size() - trim_each_side;
  for (std::size_t i = begin; i < end; ++i) {
    total += values[i];
  }

  return total / static_cast<double>(end - begin);
}

RunMetrics aggregate_debug_metrics(dsl::Track track, const std::vector<RunMetrics>& runs) {
  RunMetrics aggregate;
  if (runs.empty()) {
    return aggregate;
  }

  for (const RunMetrics& run : runs) {
    aggregate.completed_tasks += run.completed_tasks;
    aggregate.elapsed_time_us += run.elapsed_time_us;
  }

  if (debug_track_prefers_latency_metric(track)) {
    std::vector<double> values;
    values.reserve(runs.size());
    for (const RunMetrics& run : runs) {
      values.push_back(static_cast<double>(run.p99_wakeup_latency_us));
    }
    aggregate.p99_wakeup_latency_us = static_cast<uint64_t>(
        std::llround(trimmed_mean(std::move(values), trim_each_side_for_count(runs.size()))));
  } else {
    std::vector<double> values;
    values.reserve(runs.size());
    for (const RunMetrics& run : runs) {
      values.push_back(run.throughput_tasks_per_sec);
    }
    aggregate.throughput_tasks_per_sec =
        trimmed_mean(std::move(values), trim_each_side_for_count(runs.size()));
  }

  return aggregate;
}

ScoreSummary score_scenarios(const std::vector<ScenarioScoreInput>& inputs) {
  ScoreSummary summary;
  summary.track_scores.reserve(inputs.size());

  for (const ScenarioScoreInput& input : inputs) {
    TrackScore& track_score = find_or_append_track_score(&summary.track_scores, input.track);

    ScenarioScore scenario{
        .scenario_id = input.scenario_id,
        .track = input.track,
        .scorer = input.scorer,
        .weight = input.weight,
        .correctness_passed = input.correctness_passed,
    };

    if (!input.correctness_passed || input.student_runs.empty() || input.baseline_runs.empty()) {
      scenario.score = 0.0;
      scenario.correctness_passed = false;
      track_score.correctness_passed = false;
      summary.correctness_gate_passed = false;
      track_score.scenario_scores.push_back(std::move(scenario));
      continue;
    }

    if (input.scorer == dsl::ScorerKind::FairnessShareSkew) {
      const auto student_fairness = aggregate_fairness_quality(input.student_runs);
      const auto baseline_fairness = aggregate_fairness_quality(input.baseline_runs);
      if (!student_fairness.has_value() || !baseline_fairness.has_value() ||
          student_fairness->quality <= 0.0 || baseline_fairness->quality <= 0.0) {
        scenario.correctness_passed = false;
        scenario.score = 0.0;
        track_score.correctness_passed = false;
        summary.correctness_gate_passed = false;
        track_score.scenario_scores.push_back(std::move(scenario));
        continue;
      }
      scenario.student_fairness = student_fairness->diagnostics;
      scenario.baseline_fairness = baseline_fairness->diagnostics;
      scenario.student_quality = student_fairness->quality;
      scenario.baseline_quality = baseline_fairness->quality;
      scenario.score = scenario.student_quality / scenario.baseline_quality;
      if (student_fairness->mean_makespan_us > (2.0 * baseline_fairness->mean_makespan_us)) {
        scenario.score = std::min(scenario.score, 1.0);
      }
      track_score.scenario_scores.push_back(std::move(scenario));
      continue;
    }

    const auto student_quality =
        aggregate_quality(input.scorer, input.student_runs, input.score_groups);
    const auto baseline_quality =
        aggregate_quality(input.scorer, input.baseline_runs, input.score_groups);
    if (!student_quality.has_value() || !baseline_quality.has_value() || *student_quality <= 0.0 ||
        *baseline_quality <= 0.0) {
      scenario.correctness_passed = false;
      scenario.score = 0.0;
      track_score.correctness_passed = false;
      summary.correctness_gate_passed = false;
      track_score.scenario_scores.push_back(std::move(scenario));
      continue;
    }

    scenario.student_quality = *student_quality;
    scenario.baseline_quality = *baseline_quality;
    scenario.score = scenario.student_quality / scenario.baseline_quality;
    if (input.scorer == dsl::ScorerKind::LatencyWakeupP99 ||
        input.scorer == dsl::ScorerKind::LatencyFlowP99) {
      const auto student_throughput = aggregate_throughput_rate(input.student_runs);
      const auto baseline_throughput = aggregate_throughput_rate(input.baseline_runs);
      if (student_throughput.has_value() && baseline_throughput.has_value() &&
          *baseline_throughput > 0.0 &&
          (*student_throughput / *baseline_throughput) < kLatencyThroughputGuardrail) {
        scenario.score = std::min(scenario.score, (*student_throughput / *baseline_throughput) /
                                                      kLatencyThroughputGuardrail);
      }
    }
    track_score.scenario_scores.push_back(std::move(scenario));
  }

  for (TrackScore& track_score : summary.track_scores) {
    track_score.score = weighted_geometric_mean(track_score.scenario_scores);
    track_score.display_score = 1000.0 * track_score.score;
  }

  return summary;
}

} // namespace schedlab::benchmark

#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "benchmark/metrics.hpp"
#include "benchmark/workload_dsl.hpp"

namespace schedlab::benchmark {

struct FairnessGroupDiagnostic {
  std::string group_name;
  uint64_t weight = 1;
  double share_distortion = 0.0;
};

struct FairnessDiagnostics {
  double max_share_skew = 0.0;
  double share_balance_ratio = 0.0;
  std::vector<FairnessGroupDiagnostic> groups;
};

struct ScenarioScoreInput {
  std::string scenario_id;
  dsl::Track track = dsl::Track::CpuBound;
  dsl::ScorerKind scorer = dsl::ScorerKind::ThroughputSustainedRate;
  std::vector<std::string> score_groups;
  double weight = 1.0;
  bool correctness_passed = true;
  std::vector<RunMetrics> student_runs;
  std::vector<RunMetrics> baseline_runs;
};

struct ScenarioScore {
  std::string scenario_id;
  dsl::Track track = dsl::Track::CpuBound;
  dsl::ScorerKind scorer = dsl::ScorerKind::ThroughputSustainedRate;
  double weight = 0.0;
  bool correctness_passed = true;
  double student_quality = 0.0;
  double baseline_quality = 0.0;
  double score = 0.0;
  std::optional<FairnessDiagnostics> student_fairness;
  std::optional<FairnessDiagnostics> baseline_fairness;
};

struct TrackScore {
  dsl::Track track = dsl::Track::CpuBound;
  bool correctness_passed = true;
  double score = 0.0;
  double display_score = 0.0;
  std::vector<ScenarioScore> scenario_scores;
};

struct ScoreSummary {
  bool correctness_gate_passed = true;
  std::vector<TrackScore> track_scores;
};

double trimmed_mean(std::vector<double> values, std::size_t trim_each_side);
RunMetrics aggregate_debug_metrics(dsl::Track track, const std::vector<RunMetrics>& runs);
ScoreSummary score_scenarios(const std::vector<ScenarioScoreInput>& inputs);

} // namespace schedlab::benchmark

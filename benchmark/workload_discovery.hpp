#pragma once

#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "benchmark/workload_dsl.hpp"

namespace schedlab::benchmark {

enum class WorkloadSuite {
  Public,
  Hidden,
};

struct DiscoveredScenario {
  std::string id;
  std::filesystem::path path;
  dsl::Track track = dsl::Track::CpuBound;
  std::optional<dsl::ScorerKind> scorer;
  dsl::ScenarioRole role = dsl::ScenarioRole::Leaderboard;
  std::optional<double> scenario_weight;
  std::optional<std::string> variant_name;
};

struct DiscoveryDiagnostic {
  std::filesystem::path path;
  std::optional<std::size_t> line;
  std::string message;
};

struct DiscoveryResult {
  std::vector<DiscoveredScenario> scenarios;
  std::vector<DiscoveryDiagnostic> diagnostics;
};

DiscoveryResult
discover_workload_scenarios(WorkloadSuite suite,
                            std::optional<dsl::Track> filter_track = std::nullopt,
                            std::optional<dsl::ScenarioRole> filter_role = std::nullopt);

DiscoveryResult
discover_workload_scenarios_in_root(const std::filesystem::path& root,
                                    std::optional<dsl::Track> filter_track = std::nullopt,
                                    std::optional<dsl::ScenarioRole> filter_role = std::nullopt);

dsl::ParseResult load_workload_scenario(const std::filesystem::path& path);
dsl::ParseResult load_workload_scenario(const DiscoveredScenario& scenario);

} // namespace schedlab::benchmark

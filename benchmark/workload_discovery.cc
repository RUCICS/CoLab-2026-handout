#include "benchmark/workload_discovery.hpp"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <system_error>
#include <variant>

namespace schedlab::benchmark {

namespace {

std::filesystem::path source_root() {
  const std::filesystem::path here = std::filesystem::path(__FILE__);
  return here.parent_path().parent_path();
}

std::filesystem::path suite_root_path(WorkloadSuite suite) {
  const std::filesystem::path root = source_root() / "workloads";
  switch (suite) {
  case WorkloadSuite::Public:
    return root / "public";
  case WorkloadSuite::Hidden:
    return root / "hidden";
  }
  return root;
}

std::optional<std::string> read_text_file(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    return std::nullopt;
  }
  std::ostringstream content;
  content << input.rdbuf();
  return content.str();
}

} // namespace

DiscoveryResult discover_workload_scenarios(WorkloadSuite suite,
                                            std::optional<dsl::Track> filter_track,
                                            std::optional<dsl::ScenarioRole> filter_role) {
  return discover_workload_scenarios_in_root(suite_root_path(suite), filter_track, filter_role);
}

DiscoveryResult discover_workload_scenarios_in_root(const std::filesystem::path& root,
                                                    std::optional<dsl::Track> filter_track,
                                                    std::optional<dsl::ScenarioRole> filter_role) {
  DiscoveryResult result;
  std::error_code ec;
  if (!std::filesystem::exists(root, ec)) {
    result.diagnostics.push_back(DiscoveryDiagnostic{
        .path = root,
        .line = std::nullopt,
        .message = "discovery root does not exist",
    });
    return result;
  }
  if (ec) {
    result.diagnostics.push_back(DiscoveryDiagnostic{
        .path = root,
        .line = std::nullopt,
        .message = "failed to check discovery root",
    });
    return result;
  }

  std::filesystem::recursive_directory_iterator end;
  std::filesystem::recursive_directory_iterator it(root, ec);
  if (ec) {
    result.diagnostics.push_back(DiscoveryDiagnostic{
        .path = root,
        .line = std::nullopt,
        .message = "failed to open discovery root",
    });
    return result;
  }

  for (; it != end; it.increment(ec)) {
    if (ec) {
      result.diagnostics.push_back(DiscoveryDiagnostic{
          .path = root,
          .line = std::nullopt,
          .message = "failed while iterating discovery root",
      });
      break;
    }
    if (ec) {
      result.diagnostics.push_back(DiscoveryDiagnostic{
          .path = root,
          .line = std::nullopt,
          .message = "filesystem iteration error",
      });
      break;
    }
    if (!it->is_regular_file(ec)) {
      if (ec) {
        result.diagnostics.push_back(DiscoveryDiagnostic{
            .path = it->path(),
            .line = std::nullopt,
            .message = "failed to inspect filesystem entry",
        });
        ec.clear();
      }
      continue;
    }
    if (it->path().extension() != ".sched") {
      continue;
    }

    const dsl::ParseResult parsed = load_workload_scenario(it->path());
    if (parsed.error.has_value()) {
      result.diagnostics.push_back(DiscoveryDiagnostic{
          .path = it->path(),
          .line = parsed.error->line == 0 ? std::nullopt
                                          : std::optional<std::size_t>{parsed.error->line},
          .message = "parse error: " + parsed.error->message,
      });
      continue;
    }
    if (!parsed.spec.has_value()) {
      result.diagnostics.push_back(DiscoveryDiagnostic{
          .path = it->path(),
          .line = std::nullopt,
          .message = "parsed scenario missing spec",
      });
      continue;
    }
    if (!parsed.spec->track.has_value()) {
      result.diagnostics.push_back(DiscoveryDiagnostic{
          .path = it->path(),
          .line = std::nullopt,
          .message = "parsed scenario missing track",
      });
      continue;
    }
    if (filter_track.has_value() && *parsed.spec->track != *filter_track) {
      continue;
    }
    const dsl::ScenarioRole role = parsed.spec->role.value_or(dsl::ScenarioRole::Leaderboard);
    if (filter_role.has_value() && role != *filter_role) {
      continue;
    }

    const std::filesystem::path relative = std::filesystem::relative(it->path(), root, ec);
    if (ec) {
      result.diagnostics.push_back(DiscoveryDiagnostic{
          .path = it->path(),
          .line = std::nullopt,
          .message = "failed to compute scenario id from relative path",
      });
      ec.clear();
      continue;
    }
    std::filesystem::path id_path = relative;
    id_path.replace_extension();

    if (parsed.spec->variants.empty()) {
      result.scenarios.push_back(DiscoveredScenario{
          .id = id_path.generic_string(),
          .path = it->path(),
          .track = *parsed.spec->track,
          .scorer = parsed.spec->scorer,
          .role = role,
          .scenario_weight = parsed.spec->scenario_weight,
          .variant_name = std::nullopt,
      });
      continue;
    }

    for (const auto& variant : parsed.spec->variants) {
      std::filesystem::path variant_id = id_path;
      variant_id /= variant.name;
      result.scenarios.push_back(DiscoveredScenario{
          .id = variant_id.generic_string(),
          .path = it->path(),
          .track = *parsed.spec->track,
          .scorer = parsed.spec->scorer,
          .role = role,
          .scenario_weight = parsed.spec->scenario_weight,
          .variant_name = variant.name,
      });
    }
  }

  std::sort(
      result.scenarios.begin(), result.scenarios.end(),
      [](const DiscoveredScenario& lhs, const DiscoveredScenario& rhs) { return lhs.id < rhs.id; });
  return result;
}

dsl::ParseResult load_workload_scenario(const std::filesystem::path& path) {
  const auto content = read_text_file(path);
  if (!content.has_value()) {
    return dsl::ParseResult{
        .spec = std::nullopt,
        .error =
            dsl::ParseError{
                .line = 0,
                .message = "failed to read scenario file",
            },
    };
  }
  return dsl::parse_workload_dsl(*content);
}

dsl::ParseResult load_workload_scenario(const DiscoveredScenario& scenario) {
  dsl::ParseResult parsed = load_workload_scenario(scenario.path);
  if (!parsed.spec.has_value() || !scenario.variant_name.has_value()) {
    return parsed;
  }
  auto& spec = *parsed.spec;
  const auto variant_it = std::find_if(
      spec.variants.begin(), spec.variants.end(),
      [&](const dsl::VariantSpec& variant) { return variant.name == *scenario.variant_name; });
  if (variant_it == spec.variants.end()) {
    return dsl::ParseResult{
        .spec = std::nullopt,
        .error =
            dsl::ParseError{
                .line = 0,
                .message = "selected scenario variant not found in source workload",
            },
    };
  }

  for (const auto& override_entry : variant_it->overrides) {
    const auto param_it =
        std::find_if(spec.params.begin(), spec.params.end(), [&](const dsl::ParamSpec& param) {
          return param.name == override_entry.name;
        });
    if (param_it == spec.params.end()) {
      return dsl::ParseResult{
          .spec = std::nullopt,
          .error =
              dsl::ParseError{
                  .line = 0,
                  .message = "variant override references unknown parameter in bound scenario",
              },
      };
    }
    param_it->default_value = override_entry.value;
  }
  spec.variants.clear();
  return parsed;
}

} // namespace schedlab::benchmark

#include "benchmark/workload_interpreter.hpp"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <functional>
#include <limits>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <type_traits>
#include <utility>
#include <vector>
#include <variant>

namespace schedlab::benchmark {

namespace {

std::chrono::microseconds to_microseconds(const dsl::Duration& duration) {
  switch (duration.unit) {
  case dsl::DurationUnit::Microseconds:
    return std::chrono::microseconds(duration.value);
  case dsl::DurationUnit::Milliseconds:
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::milliseconds(duration.value));
  case dsl::DurationUnit::Seconds:
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::seconds(duration.value));
  }

  assert(false && "unknown duration unit");
  return std::chrono::microseconds(0);
}

uint64_t to_microseconds_count(const dsl::Duration& duration) {
  const auto micros = to_microseconds(duration);
  assert(micros.count() >= 0);
  return static_cast<uint64_t>(micros.count());
}

[[noreturn]] void fail_unexpanded_template_op(const char* message) {
  throw std::logic_error(message);
}

uint64_t arrival_offset_us(const std::optional<dsl::ArrivalPolicy>& arrival, uint64_t task_index) {
  if (!arrival.has_value()) {
    return 0;
  }
  const uint64_t duration_us = to_microseconds_count(arrival->duration);
  switch (arrival->policy) {
  case dsl::ArrivalPolicyKind::Delay:
    return duration_us;
  case dsl::ArrivalPolicyKind::Stagger:
  case dsl::ArrivalPolicyKind::Interval:
    return task_index * duration_us;
  case dsl::ArrivalPolicyKind::Burst:
    assert(arrival->burst_size.has_value());
    assert(*arrival->burst_size > 0);
    return (task_index / *arrival->burst_size) * duration_us;
  }
  assert(false && "unknown arrival policy");
  return 0;
}

uint64_t mix_hash(uint64_t a, uint64_t b) {
  a ^= b + 0x9e3779b97f4a7c15ULL + (a << 6) + (a >> 2);
  return a;
}

uint64_t hash_string(const std::string& input) {
  return std::hash<std::string>{}(input);
}

uint64_t hash_operation(const dsl::Operation& operation) {
  return std::visit(
      [](const auto& op) {
        using OpType = std::decay_t<decltype(op)>;
        uint64_t value = 0;
        if constexpr (std::is_same_v<OpType, dsl::ComputeOp>) {
          value = mix_hash(0x1, op.units);
        } else if constexpr (std::is_same_v<OpType, dsl::CallOp>) {
          value = mix_hash(0x2, std::hash<uint64_t>{}(op.service_units));
          value = mix_hash(value, hash_string(op.device));
        } else if constexpr (std::is_same_v<OpType, dsl::SleepOp>) {
          value = mix_hash(0x3, op.duration.value);
          value = mix_hash(value, static_cast<uint64_t>(op.duration.unit));
        } else {
          value = 0xFFFFFFFFFFFFFFFFULL;
        }
        return value;
      },
      operation);
}

bool params_equal(const std::vector<dsl::Param>& a, const std::vector<dsl::Param>& b) {
  return a.size() == b.size() &&
         std::equal(a.begin(), a.end(), b.begin(), [](const auto& a_param, const auto& b_param) {
           return a_param.key == b_param.key && a_param.value == b_param.value;
         });
}

bool program_equal(const std::vector<dsl::Operation>& lhs, const std::vector<dsl::Operation>& rhs);

bool operations_equal(const dsl::Operation& lhs, const dsl::Operation& rhs) {
  return std::visit(
      [](const auto& a, const auto& b) {
        using AL = std::decay_t<decltype(a)>;
        using BL = std::decay_t<decltype(b)>;
        if constexpr (!std::is_same_v<AL, BL>) {
          return false;
        } else if constexpr (std::is_same_v<AL, dsl::ComputeOp>) {
          return a.units == b.units && params_equal(a.params, b.params);
        } else if constexpr (std::is_same_v<AL, dsl::CallOp>) {
          return a.device == b.device && a.service_units == b.service_units &&
                 params_equal(a.params, b.params);
        } else if constexpr (std::is_same_v<AL, dsl::SleepOp>) {
          return a.duration.value == b.duration.value && a.duration.unit == b.duration.unit &&
                 params_equal(a.params, b.params);
        } else if constexpr (std::is_same_v<AL, dsl::ChoiceOp>) {
          if (a.branches.size() != b.branches.size()) {
            return false;
          }
          for (size_t index = 0; index < a.branches.size(); ++index) {
            if (a.branches[index].weight != b.branches[index].weight ||
                !program_equal(a.branches[index].body, b.branches[index].body)) {
              return false;
            }
          }
          return true;
        } else if constexpr (std::is_same_v<AL, dsl::RepeatOpPtr>) {
          if (a == nullptr || b == nullptr) {
            return false;
          }
          return a->count == b->count && program_equal(a->body, b->body);
        } else {
          return std::addressof(a) == std::addressof(b);
        }
      },
      lhs, rhs);
}

bool program_equal(const std::vector<dsl::Operation>& lhs, const std::vector<dsl::Operation>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (!operations_equal(lhs[i], rhs[i])) {
      return false;
    }
  }
  return true;
}

uint64_t hash_program(const std::vector<dsl::Operation>& program) {
  uint64_t fingerprint = 0x9e3779b97f4a7c15ULL;
  for (const dsl::Operation& op : program) {
    fingerprint = mix_hash(fingerprint, hash_operation(op));
  }
  return fingerprint;
}

std::shared_ptr<const std::vector<dsl::Operation>>
lowered_program_with_offset(const std::vector<dsl::Operation>& program, uint64_t offset_us) {
  if (offset_us == 0) {
    return std::make_shared<const std::vector<dsl::Operation>>(program);
  }

  std::vector<dsl::Operation> lowered;
  lowered.reserve(program.size() + 1);
  lowered.push_back(dsl::SleepOp{
      .duration =
          dsl::Duration{
              .value = offset_us,
              .unit = dsl::DurationUnit::Microseconds,
          },
  });
  lowered.insert(lowered.end(), program.begin(), program.end());
  return std::make_shared<const std::vector<dsl::Operation>>(std::move(lowered));
}

std::string group_completion_latch_name(std::string_view group_name) {
  return "__group_done__:" + std::string(group_name);
}

std::string group_instance_completion_event_name(std::string_view group_name, uint64_t task_index) {
  return "__group_done__:" + std::string(group_name) + ":" + std::to_string(task_index);
}

std::string join_dependency_latch_name(std::string_view downstream_group, uint64_t task_index) {
  return "__group_done__:" + std::string(downstream_group) + ":join:" + std::to_string(task_index);
}

bool uses_reserved_group_completion_prefix(std::string_view name) {
  constexpr std::string_view kPrefix = "__group_done__:";
  return name.starts_with(kPrefix);
}

} // namespace

std::optional<uint64_t> InterpretedScenario::consume_jitter_param(std::vector<dsl::Param>& params) {
  const auto it = std::find_if(params.begin(), params.end(),
                               [](const dsl::Param& param) { return param.key == "jitter"; });
  if (it == params.end()) {
    return std::nullopt;
  }
  const auto duplicate_it = std::find_if(
      std::next(it), params.end(), [](const dsl::Param& param) { return param.key == "jitter"; });
  if (duplicate_it != params.end()) {
    throw std::logic_error("duplicate jitter parameter");
  }
  uint64_t value = 0;
  const auto [ptr, ec] =
      std::from_chars(it->value.data(), it->value.data() + it->value.size(), value);
  if (ec != std::errc() || ptr != it->value.data() + it->value.size()) {
    throw std::logic_error("jitter parameter must be an integer");
  }
  params.erase(it);
  return value;
}

std::optional<bool>
InterpretedScenario::consume_compute_mem_param(std::vector<dsl::Param>& params) {
  const auto it = std::find_if(params.begin(), params.end(),
                               [](const dsl::Param& param) { return param.key == "mem"; });
  if (it == params.end()) {
    return std::nullopt;
  }
  const auto duplicate_it = std::find_if(
      std::next(it), params.end(), [](const dsl::Param& param) { return param.key == "mem"; });
  if (duplicate_it != params.end()) {
    throw std::logic_error("duplicate mem parameter");
  }
  const bool high_mem = it->value == "high";
  if (!high_mem && it->value != "low") {
    throw std::logic_error("mem parameter must be 'low' or 'high'");
  }
  params.erase(it);
  return high_mem;
}

uint64_t InterpretedScenario::apply_jitter_value(uint64_t base_value, uint64_t jitter,
                                                 LoweringState* lowering_state) {
  assert(lowering_state != nullptr && "jitter lowering requires task-local state");
  if (jitter == 0) {
    return base_value;
  }
  if (jitter > (std::numeric_limits<uint64_t>::max() - 1) / 2) {
    throw std::logic_error("jitter value exceeds supported range");
  }
  const uint64_t range = 2 * jitter + 1;
  const uint64_t visit = lowering_state->jitter_visit_index++;
  uint64_t hash = mix_hash(lowering_state->task_index, visit);
  hash = mix_hash(hash, base_value);
  const uint64_t mod = hash % range;
  if (mod <= jitter) {
    const uint64_t decrease = jitter - mod;
    if (base_value <= decrease) {
      return 1;
    }
    return base_value - decrease;
  }

  const uint64_t increase = mod - jitter;
  if (increase > std::numeric_limits<uint64_t>::max() - base_value) {
    return std::numeric_limits<uint64_t>::max();
  }
  return base_value + increase;
}

InterpretedScenario::InterpretedScenario(const dsl::WorkloadSpec& spec) {
  assert(spec.cpu_rate.has_value());
  assert(spec.track.has_value());
  track_ = *spec.track;
  scorer_ = spec.scorer;
  scenario_weight_ = spec.scenario_weight.value_or(1.0);
  ExecutionConfig config{
      .cpu_rate = *spec.cpu_rate,
      .switch_cost_us = spec.switch_cost_us.value_or(0),
      .migration_cost_us = spec.migration_cost_us.value_or(0),
      .migration_cost_local_us =
          spec.migration_cost_local_us.value_or(spec.migration_cost_us.value_or(0)),
      .migration_cost_remote_us =
          spec.migration_cost_remote_us.value_or(spec.migration_cost_us.value_or(0)),
  };
  if (spec.workers.has_value()) {
    config.worker_to_node.assign(static_cast<std::size_t>(*spec.workers), 0);
  }
  for (std::size_t node_index = 0; node_index < spec.nodes.size(); ++node_index) {
    const dsl::NodeSpec& node = spec.nodes[node_index];
    ExecutionConfig::NodeTopology topology{
        .name = node.name,
    };
    for (uint64_t worker = node.worker_begin; worker <= node.worker_end; ++worker) {
      topology.workers.push_back(static_cast<int>(worker));
      if (worker < config.worker_to_node.size()) {
        config.worker_to_node[static_cast<std::size_t>(worker)] = static_cast<int>(node_index);
      }
    }
    config.nodes.push_back(std::move(topology));
  }
  if (config.nodes.empty() && spec.workers.has_value()) {
    ExecutionConfig::NodeTopology topology{
        .name = "node0",
    };
    topology.workers.reserve(static_cast<std::size_t>(*spec.workers));
    for (uint64_t worker = 0; worker < *spec.workers; ++worker) {
      topology.workers.push_back(static_cast<int>(worker));
    }
    config.nodes.push_back(std::move(topology));
  }
  TemplateSubstitutions global_param_substitutions;
  global_param_substitutions.reserve(spec.params.size());
  for (const dsl::ParamSpec& param : spec.params) {
    const auto [_, inserted] = global_param_substitutions.emplace(param.name, param.default_value);
    if (!inserted) {
      throw std::logic_error("duplicate param '" + param.name + "'");
    }
  }

  auto device_ids = std::make_shared<std::unordered_map<std::string, DeviceId>>();
  for (std::size_t index = 0; index < spec.devices.size(); ++index) {
    device_ids->emplace(spec.devices[index].name, static_cast<DeviceId>(index));
    const dsl::DeviceSpec& device = spec.devices[index];
    if (device.rate.has_value()) {
      assert(*device.rate > 0);
      config.device_rates_by_id[static_cast<DeviceId>(index)] = *device.rate;
    }
    config.device_parallelisms_by_id[static_cast<DeviceId>(index)] = device.parallelism;
  }
  for (const dsl::SyncEvent& event : spec.events) {
    config.event_names.push_back(event.name);
  }
  for (const dsl::SyncLatch& latch : spec.latches) {
    config.latches.push_back(latch);
  }

  std::unordered_map<std::string, uint64_t> group_counts;
  std::unordered_map<std::string, uint64_t> group_ids;
  std::vector<std::pair<std::string, uint64_t>> ordered_groups;
  for (const dsl::GroupSpec& group : spec.groups) {
    if (group.count == 0) {
      throw std::logic_error("group '" + group.name + "' must have positive count");
    }
    if (uses_reserved_group_completion_prefix(group.name)) {
      throw std::logic_error("group name '" + group.name + "' uses a reserved internal prefix");
    }
    if (!group_counts.emplace(group.name, group.count).second) {
      throw std::logic_error("duplicate group name '" + group.name + "'");
    }
    group_ids.emplace(group.name, group_metadata_.size());
    group_metadata_.push_back(GroupMetadata{
        .name = group.name,
        .count = group.count,
        .weight = group.weight,
    });
    ordered_groups.emplace_back(group.name, group.count);
  }
  for (const dsl::PhaseSpec& phase : spec.phases) {
    for (const dsl::GroupSpec& group : phase.groups) {
      if (group.count == 0) {
        throw std::logic_error("group '" + group.name + "' must have positive count");
      }
      if (uses_reserved_group_completion_prefix(group.name)) {
        throw std::logic_error("group name '" + group.name + "' uses a reserved internal prefix");
      }
      if (!group_counts.emplace(group.name, group.count).second) {
        throw std::logic_error("duplicate group name '" + group.name + "'");
      }
      group_ids.emplace(group.name, group_metadata_.size());
      group_metadata_.push_back(GroupMetadata{
          .name = group.name,
          .count = group.count,
          .weight = group.weight,
      });
      ordered_groups.emplace_back(group.name, group.count);
    }
  }

  std::unordered_set<std::string> event_names;
  for (const dsl::SyncEvent& event : spec.events) {
    if (uses_reserved_group_completion_prefix(event.name)) {
      throw std::logic_error("sync name '" + event.name + "' uses a reserved internal prefix");
    }
    if (group_counts.contains(event.name)) {
      throw std::logic_error("sync name '" + event.name + "' collides with a group name");
    }
    if (!event_names.insert(event.name).second) {
      throw std::logic_error("duplicate event name '" + event.name + "'");
    }
  }
  std::unordered_set<std::string> latch_names;
  for (const dsl::SyncLatch& latch : spec.latches) {
    if (uses_reserved_group_completion_prefix(latch.name)) {
      throw std::logic_error("sync name '" + latch.name + "' uses a reserved internal prefix");
    }
    if (group_counts.contains(latch.name)) {
      throw std::logic_error("sync name '" + latch.name + "' collides with a group name");
    }
    if (event_names.contains(latch.name)) {
      throw std::logic_error("sync name '" + latch.name + "' collides with another sync object");
    }
    if (!latch_names.insert(latch.name).second) {
      throw std::logic_error("duplicate latch name '" + latch.name + "'");
    }
  }
  std::unordered_set<std::string> referenced_group_completion_targets;
  std::unordered_set<std::string> referenced_group_instance_events;
  struct JoinConsumerConfig {
    std::string downstream_group;
    uint64_t fanin = 0;
  };
  std::unordered_map<std::string, std::vector<JoinConsumerConfig>> join_consumers_by_upstream_group;
  auto collect_hidden_sync_requirements = [&](const dsl::GroupSpec& group) {
    if (!group.dependency.has_value()) {
      return;
    }
    const dsl::DependencyExpr& dependency = *group.dependency;
    switch (dependency.kind) {
    case dsl::DependencyKind::None:
      return;
    case dsl::DependencyKind::Name:
    case dsl::DependencyKind::All:
      for (const std::string& target : dependency.targets) {
        if (group_counts.contains(target)) {
          referenced_group_completion_targets.insert(target);
        }
      }
      return;
    case dsl::DependencyKind::Each:
    case dsl::DependencyKind::Fanout:
      if (!dependency.targets.empty() && group_counts.contains(dependency.targets.front())) {
        referenced_group_instance_events.insert(dependency.targets.front());
      }
      return;
    case dsl::DependencyKind::Join:
      if (!dependency.targets.empty() && group_counts.contains(dependency.targets.front())) {
        const std::string& upstream_group = dependency.targets.front();
        const uint64_t upstream_count = group_counts.at(upstream_group);
        join_consumers_by_upstream_group[upstream_group].push_back(JoinConsumerConfig{
            .downstream_group = group.name,
            .fanin = upstream_count / group.count,
        });
      }
      return;
    }
  };
  for (const dsl::GroupSpec& group : spec.groups) {
    collect_hidden_sync_requirements(group);
  }
  for (const dsl::PhaseSpec& phase : spec.phases) {
    for (const dsl::GroupSpec& group : phase.groups) {
      collect_hidden_sync_requirements(group);
    }
  }

  std::unordered_map<std::string, std::string> group_completion_latches;
  for (const auto& [group_name, count] : ordered_groups) {
    if (referenced_group_completion_targets.contains(group_name)) {
      const std::string completion_name = group_completion_latch_name(group_name);
      group_completion_latches.emplace(group_name, completion_name);
      config.latches.push_back(dsl::SyncLatch{
          .name = completion_name,
          .count = count,
      });
      latch_names.insert(completion_name);
    }
    if (referenced_group_instance_events.contains(group_name)) {
      for (uint64_t task_index = 0; task_index < count; ++task_index) {
        const std::string event_name = group_instance_completion_event_name(group_name, task_index);
        config.event_names.push_back(event_name);
        event_names.insert(event_name);
      }
    }
    const auto join_consumers_it = join_consumers_by_upstream_group.find(group_name);
    if (join_consumers_it == join_consumers_by_upstream_group.end()) {
      continue;
    }
    for (const JoinConsumerConfig& consumer : join_consumers_it->second) {
      const uint64_t downstream_count = group_counts.at(consumer.downstream_group);
      for (uint64_t task_index = 0; task_index < downstream_count; ++task_index) {
        const std::string latch_name =
            join_dependency_latch_name(consumer.downstream_group, task_index);
        config.latches.push_back(dsl::SyncLatch{
            .name = latch_name,
            .count = consumer.fanin,
        });
        latch_names.insert(latch_name);
      }
    }
  }

  auto validate_name_target = [&](std::string_view target) {
    const std::string name(target);
    const bool is_group = group_counts.contains(name);
    const bool is_event = event_names.contains(name);
    const bool is_latch = latch_names.contains(name);
    if (!is_group && !is_event && !is_latch) {
      throw std::logic_error("unknown dependency target '" + name + "'");
    }
    const int matches =
        static_cast<int>(is_group) + static_cast<int>(is_event) + static_cast<int>(is_latch);
    if (matches > 1) {
      throw std::logic_error("dependency target '" + name + "' is ambiguous");
    }
  };

  auto require_group_target = [&](std::string_view target) -> uint64_t {
    const auto group_it = group_counts.find(std::string(target));
    if (group_it == group_counts.end()) {
      throw std::logic_error("dependency target '" + std::string(target) +
                             "' must reference a group");
    }
    return group_it->second;
  };

  auto validate_dependency = [&](const dsl::GroupSpec& group) {
    if (!group.dependency.has_value()) {
      return;
    }
    const dsl::DependencyExpr& dependency = *group.dependency;
    switch (dependency.kind) {
    case dsl::DependencyKind::None:
      return;
    case dsl::DependencyKind::Name:
    case dsl::DependencyKind::All:
      for (const std::string& target : dependency.targets) {
        validate_name_target(target);
        if (group_counts.contains(target) && target == group.name) {
          throw std::logic_error("group '" + group.name + "' cannot depend on its own completion");
        }
      }
      return;
    case dsl::DependencyKind::Each: {
      if (dependency.targets.empty()) {
        throw std::logic_error("each(...) requires at least one target");
      }
      if (dependency.targets.size() != 1) {
        throw std::logic_error("each(...) requires exactly one target");
      }
      if (dependency.targets.front() == group.name) {
        throw std::logic_error("group '" + group.name + "' cannot depend on itself via each(...)");
      }
      const uint64_t upstream_count = require_group_target(dependency.targets.front());
      if (group.count != upstream_count) {
        throw std::logic_error("each(...) requires equal upstream and downstream group counts");
      }
      return;
    }
    case dsl::DependencyKind::Fanout: {
      if (dependency.targets.empty()) {
        throw std::logic_error("fanout(...) requires at least one target");
      }
      if (dependency.targets.size() != 1) {
        throw std::logic_error("fanout(...) requires exactly one target");
      }
      if (dependency.targets.front() == group.name) {
        throw std::logic_error("group '" + group.name +
                               "' cannot depend on itself via fanout(...)");
      }
      const uint64_t upstream_count = require_group_target(dependency.targets.front());
      if (upstream_count == 0 || group.count % upstream_count != 0) {
        throw std::logic_error(
            "fanout(...) requires downstream count to be a multiple of upstream count");
      }
      return;
    }
    case dsl::DependencyKind::Join: {
      if (dependency.targets.empty()) {
        throw std::logic_error("join(...) requires at least one target");
      }
      if (dependency.targets.size() != 1) {
        throw std::logic_error("join(...) requires exactly one target");
      }
      if (dependency.targets.front() == group.name) {
        throw std::logic_error("group '" + group.name + "' cannot depend on itself via join(...)");
      }
      const uint64_t upstream_count = require_group_target(dependency.targets.front());
      if (group.count == 0 || upstream_count % group.count != 0) {
        throw std::logic_error(
            "join(...) requires upstream count to be a multiple of downstream count");
      }
      return;
    }
    }
  };

  for (const dsl::GroupSpec& group : spec.groups) {
    validate_dependency(group);
  }
  for (const dsl::PhaseSpec& phase : spec.phases) {
    for (const dsl::GroupSpec& group : phase.groups) {
      validate_dependency(group);
    }
  }

  std::unordered_map<std::string, std::vector<std::string>> dependency_graph;
  auto record_dependency_edges = [&](const dsl::GroupSpec& group) {
    auto& edges = dependency_graph[group.name];
    if (!group.dependency.has_value()) {
      return;
    }
    const dsl::DependencyExpr& dependency = *group.dependency;
    switch (dependency.kind) {
    case dsl::DependencyKind::None:
      return;
    case dsl::DependencyKind::Name:
    case dsl::DependencyKind::All:
      for (const std::string& target : dependency.targets) {
        if (group_counts.contains(target)) {
          edges.push_back(target);
        }
      }
      return;
    case dsl::DependencyKind::Each:
    case dsl::DependencyKind::Fanout:
    case dsl::DependencyKind::Join:
      if (!dependency.targets.empty() && group_counts.contains(dependency.targets.front())) {
        edges.push_back(dependency.targets.front());
      }
      return;
    }
  };
  for (const dsl::GroupSpec& group : spec.groups) {
    record_dependency_edges(group);
  }
  for (const dsl::PhaseSpec& phase : spec.phases) {
    for (const dsl::GroupSpec& group : phase.groups) {
      record_dependency_edges(group);
    }
  }
  std::unordered_map<std::string, uint8_t> visit_state;
  std::function<void(const std::string&)> validate_acyclic = [&](const std::string& group_name) {
    const uint8_t state = visit_state[group_name];
    if (state == 2) {
      return;
    }
    if (state == 1) {
      throw std::logic_error("cyclic group dependency involving '" + group_name + "'");
    }
    visit_state[group_name] = 1;
    const auto edges_it = dependency_graph.find(group_name);
    if (edges_it != dependency_graph.end()) {
      for (const std::string& target : edges_it->second) {
        validate_acyclic(target);
      }
    }
    visit_state[group_name] = 2;
  };
  for (const auto& [group_name, _] : group_counts) {
    validate_acyclic(group_name);
  }

  execution_config_ = std::make_shared<const ExecutionConfig>(std::move(config));
  device_ids_ = std::move(device_ids);

  for (const dsl::TemplateSpec& templ : spec.templates) {
    template_map_.emplace(templ.name, templ);
  }

  auto validate_sync_operations = [&](const std::vector<dsl::Operation>& body,
                                      const auto& self) -> void {
    for (const dsl::Operation& operation : body) {
      std::visit(
          [&](const auto& op) {
            using OpType = std::decay_t<decltype(op)>;
            if constexpr (std::is_same_v<OpType, dsl::WaitOp>) {
              const bool is_event = event_names.contains(op.target);
              const bool is_latch = latch_names.contains(op.target);
              if (!is_event && !is_latch) {
                throw std::logic_error("wait target '" + op.target +
                                       "' must reference an event or latch");
              }
              if (is_event && is_latch) {
                throw std::logic_error("wait target '" + op.target + "' is ambiguous");
              }
            } else if constexpr (std::is_same_v<OpType, dsl::SignalOp>) {
              if (!event_names.contains(op.target)) {
                throw std::logic_error("signal target '" + op.target + "' must reference an event");
              }
            } else if constexpr (std::is_same_v<OpType, dsl::ArriveOp>) {
              if (!latch_names.contains(op.target)) {
                throw std::logic_error("arrive target '" + op.target + "' must reference a latch");
              }
            } else if constexpr (std::is_same_v<OpType, dsl::RepeatOpPtr>) {
              assert(op != nullptr && "repeat op must be non-null");
              self(op->body, self);
            }
          },
          operation);
    }
  };

  std::unordered_map<std::string, std::vector<uint64_t>> flow_ids_by_group;
  uint64_t next_flow_id = 1;

  // Assign a deterministic scenario-wide lowering ordinal so jitter varies
  // across groups/phases as well as within a single group.
  uint64_t next_task_ordinal = 0;
  auto lower_group_tasks = [&](const dsl::GroupSpec& group, uint64_t phase_base_offset_us) {
    const std::size_t group_index = group_ids.at(group.name);
    auto& group_flow_ids = flow_ids_by_group[group.name];
    group_flow_ids.reserve(group.count);
    for (uint64_t task_index = 0; task_index < group.count; ++task_index) {
      const uint64_t offset_us =
          phase_base_offset_us + arrival_offset_us(group.arrival, task_index);
      uint64_t flow_id = next_flow_id++;
      if (group.dependency.has_value()) {
        const dsl::DependencyExpr& dependency = *group.dependency;
        switch (dependency.kind) {
        case dsl::DependencyKind::None:
        case dsl::DependencyKind::Name:
        case dsl::DependencyKind::All:
          break;
        case dsl::DependencyKind::Each: {
          const auto& upstream_flow_ids = flow_ids_by_group.at(dependency.targets.front());
          flow_id = upstream_flow_ids.at(task_index);
          break;
        }
        case dsl::DependencyKind::Fanout: {
          const std::string& target = dependency.targets.front();
          const uint64_t upstream_count = group_counts.at(target);
          const uint64_t fanout = group.count / upstream_count;
          const auto& upstream_flow_ids = flow_ids_by_group.at(target);
          flow_id = upstream_flow_ids.at(task_index / fanout);
          break;
        }
        case dsl::DependencyKind::Join: {
          const std::string& target = dependency.targets.front();
          const uint64_t upstream_count = group_counts.at(target);
          const uint64_t fanin = upstream_count / group.count;
          const auto& upstream_flow_ids = flow_ids_by_group.at(target);
          flow_id = upstream_flow_ids.at(task_index * fanin);
          break;
        }
        }
      }
      group_flow_ids.push_back(flow_id);
      LoweringState lowering_state{
          .task_index = next_task_ordinal++,
          .choice_visit_index = task_index,
      };
      std::vector<dsl::Operation> expanded_body;
      if (group.dependency.has_value()) {
        const dsl::DependencyExpr& dependency = *group.dependency;
        switch (dependency.kind) {
        case dsl::DependencyKind::None:
          break;
        case dsl::DependencyKind::Name:
        case dsl::DependencyKind::All:
          for (const std::string& target : dependency.targets) {
            const auto group_it = group_completion_latches.find(target);
            assert(group_it != group_completion_latches.end() || !group_counts.contains(target));
            expanded_body.push_back(dsl::WaitOp{
                .target = group_it != group_completion_latches.end() ? group_it->second : target,
            });
          }
          break;
        case dsl::DependencyKind::Each: {
          const std::string& target = dependency.targets.front();
          expanded_body.push_back(dsl::WaitOp{
              .target = group_instance_completion_event_name(target, task_index),
          });
          break;
        }
        case dsl::DependencyKind::Fanout: {
          const std::string& target = dependency.targets.front();
          const uint64_t upstream_count = group_counts.at(target);
          const uint64_t fanout = group.count / upstream_count;
          expanded_body.push_back(dsl::WaitOp{
              .target = group_instance_completion_event_name(target, task_index / fanout),
          });
          break;
        }
        case dsl::DependencyKind::Join:
          expanded_body.push_back(dsl::WaitOp{
              .target = join_dependency_latch_name(group.name, task_index),
          });
          break;
        }
      }
      const auto expanded_group_body =
          expand_program(group.body, &global_param_substitutions, &lowering_state);
      expanded_body.insert(expanded_body.end(), expanded_group_body.begin(),
                           expanded_group_body.end());
      validate_sync_operations(expanded_body, validate_sync_operations);
      ProgramKey key{
          .offset = offset_us,
          .fingerprint = hash_program(expanded_body),
          .length = expanded_body.size(),
      };
      std::shared_ptr<const std::vector<dsl::Operation>> lowered_body;
      auto& bucket = lowered_cache_[key];
      for (const auto& candidate : bucket) {
        if (program_equal(expanded_body, *candidate)) {
          lowered_body = candidate;
          break;
        }
      }
      if (!lowered_body) {
        lowered_body = lowered_program_with_offset(expanded_body, offset_us);
        bucket.push_back(lowered_body);
      }
      programs_.push_back(lowered_body);
      std::vector<CompletionAction> completion_actions;
      if (const auto it = group_completion_latches.find(group.name);
          it != group_completion_latches.end()) {
        completion_actions.push_back(CompletionAction{
            .kind = CompletionAction::Kind::ArriveLatch,
            .target = it->second,
        });
      }
      if (referenced_group_instance_events.contains(group.name)) {
        completion_actions.push_back(CompletionAction{
            .kind = CompletionAction::Kind::SignalEvent,
            .target = group_instance_completion_event_name(group.name, task_index),
        });
      }
      if (const auto join_it = join_consumers_by_upstream_group.find(group.name);
          join_it != join_consumers_by_upstream_group.end()) {
        for (const JoinConsumerConfig& consumer : join_it->second) {
          completion_actions.push_back(CompletionAction{
              .kind = CompletionAction::Kind::ArriveLatch,
              .target = join_dependency_latch_name(consumer.downstream_group,
                                                   task_index / consumer.fanin),
          });
        }
      }
      task_templates_.push_back(TaskTemplate{
          .program = lowered_body,
          .device_ids = device_ids_,
          .attrs =
              schedlab::TaskAttrs{
                  .group_id = group_index,
                  .weight = group.weight,
              },
          .completion_actions = std::move(completion_actions),
          .high_mem = lowering_state.has_high_mem_compute,
      });
      task_metadata_.push_back(TaskMetadata{
          .group_index = group_index,
          .flow_id = flow_id,
      });
    }
  };

  // Top-level groups behave like an implicit phase at 0us.
  for (const dsl::GroupSpec& group : spec.groups) {
    lower_group_tasks(group, 0);
  }

  for (const dsl::PhaseSpec& phase : spec.phases) {
    const uint64_t phase_base_offset_us = to_microseconds_count(phase.at);
    for (const dsl::GroupSpec& group : phase.groups) {
      lower_group_tasks(group, phase_base_offset_us);
    }
  }
}

const InterpretedScenario::ExecutionConfig& InterpretedScenario::execution_config() const noexcept {
  assert(execution_config_);
  return *execution_config_;
}

dsl::Track InterpretedScenario::track() const noexcept {
  return track_;
}

std::optional<dsl::ScorerKind> InterpretedScenario::scorer() const noexcept {
  return scorer_;
}

double InterpretedScenario::scenario_weight() const noexcept {
  return scenario_weight_;
}

const std::vector<InterpretedScenario::GroupMetadata>&
InterpretedScenario::group_metadata() const noexcept {
  return group_metadata_;
}

const std::vector<InterpretedScenario::TaskMetadata>&
InterpretedScenario::task_metadata() const noexcept {
  return task_metadata_;
}

const std::vector<InterpretedScenario::LoweredTask>&
InterpretedScenario::lowered_tasks() const noexcept {
  return task_templates_;
}

const std::unordered_map<std::string, DeviceId>& InterpretedScenario::device_ids() const noexcept {
  assert(device_ids_ != nullptr);
  return *device_ids_;
}

void InterpretedScenario::install(WorkloadInstaller& installer) {
  for (const TaskTemplate& task_template : task_templates_) {
    // Installed tasks own detached invocation state so they remain valid
    // even after the InterpretedScenario used to install them is destroyed.
    auto* invocation = new TaskInvocation{
        .program = task_template.program,
        .device_ids = task_template.device_ids,
        .execution_config = execution_config_,
        .attrs = task_template.attrs,
        .completion_actions = task_template.completion_actions,
    };
    installer.spawn(&InterpretedScenario::task_main, invocation, task_template.attrs);
  }
}

void InterpretedScenario::task_main(WorkloadContext& workload, void* raw) {
  assert(raw != nullptr && "task invocation must be non-null");
  std::unique_ptr<TaskInvocation> invocation(static_cast<TaskInvocation*>(raw));
  assert(invocation->execution_config && "execution config must be present");
  for (const auto& [device, rate] : invocation->execution_config->device_rates_by_id) {
    workload.configure_device_rate(device, rate);
  }
  for (const auto& [device, parallel] : invocation->execution_config->device_parallelisms_by_id) {
    workload.configure_device_parallelism(device, parallel);
  }
  for (const std::string& event_name : invocation->execution_config->event_names) {
    workload.declare_event(event_name);
  }
  for (const dsl::SyncLatch& latch : invocation->execution_config->latches) {
    workload.declare_latch(latch.name, latch.count);
  }
  workload.configure_cpu_rate(invocation->execution_config->cpu_rate);
  execute_program(workload, *invocation->program, *invocation->device_ids);
  for (const CompletionAction& action : invocation->completion_actions) {
    switch (action.kind) {
    case CompletionAction::Kind::SignalEvent:
      workload.signal_event(action.target);
      break;
    case CompletionAction::Kind::ArriveLatch:
      workload.arrive_latch(action.target);
      break;
    }
  }
}

void InterpretedScenario::execute_program(
    WorkloadContext& workload, const std::vector<dsl::Operation>& program,
    const std::unordered_map<std::string, DeviceId>& device_ids) {
  for (const dsl::Operation& operation : program) {
    execute_operation(workload, operation, device_ids);
  }
}

void InterpretedScenario::execute_operation(
    WorkloadContext& workload, const dsl::Operation& operation,
    const std::unordered_map<std::string, DeviceId>& device_ids) {
  std::visit(
      [&](const auto& op) {
        using OpType = std::decay_t<decltype(op)>;
        if constexpr (std::is_same_v<OpType, dsl::ComputeOp>) {
          if (op.units_param_ref.has_value()) {
            fail_unexpanded_template_op(
                "unexpanded compute template parameter reached interpreter");
          }
          workload.compute_for(op.units);
        } else if constexpr (std::is_same_v<OpType, dsl::CallOp>) {
          if (op.device_param_ref.has_value()) {
            fail_unexpanded_template_op(
                "unexpanded call device template parameter reached interpreter");
          }
          if (op.service_units_param_ref.has_value()) {
            fail_unexpanded_template_op(
                "unexpanded call service template parameter reached interpreter");
          }
          const auto iter = device_ids.find(op.device);
          assert(iter != device_ids.end() && "call references unknown device");
          schedlab::DeviceRequest request;
          request.service_units = op.service_units;
          static_cast<void>(workload.device_call(iter->second, request));
        } else if constexpr (std::is_same_v<OpType, dsl::SleepOp>) {
          if (op.duration_param_ref.has_value()) {
            fail_unexpanded_template_op("unexpanded sleep template parameter reached interpreter");
          }
          workload.sleep_for(to_microseconds(op.duration));
        } else if constexpr (std::is_same_v<OpType, dsl::WaitOp>) {
          workload.wait_sync(op.target);
        } else if constexpr (std::is_same_v<OpType, dsl::SignalOp>) {
          workload.signal_event(op.target);
        } else if constexpr (std::is_same_v<OpType, dsl::ArriveOp>) {
          workload.arrive_latch(op.target);
        } else if constexpr (std::is_same_v<OpType, dsl::UseOp>) {
          fail_unexpanded_template_op("unexpanded use op reached interpreter");
        } else if constexpr (std::is_same_v<OpType, dsl::ChoiceOp>) {
          fail_unexpanded_template_op("unexpanded choice op reached interpreter");
        } else if constexpr (std::is_same_v<OpType, dsl::RepeatOpPtr>) {
          assert(op != nullptr && "repeat op must be non-null");
          for (uint64_t count = 0; count < op->count; ++count) {
            execute_program(workload, op->body, device_ids);
          }
        }
      },
      operation);
}

std::vector<dsl::Operation>
InterpretedScenario::expand_program(const std::vector<dsl::Operation>& program,
                                    const TemplateSubstitutions* substitutions,
                                    LoweringState* lowering_state) {
  std::vector<dsl::Operation> expanded;
  expanded.reserve(program.size());
  for (const dsl::Operation& operation : program) {
    append_expanded_operation(operation, expanded, substitutions, lowering_state);
  }
  return expanded;
}

void InterpretedScenario::append_expanded_operation(const dsl::Operation& operation,
                                                    std::vector<dsl::Operation>& out,
                                                    const TemplateSubstitutions* substitutions,
                                                    LoweringState* lowering_state) {
  std::visit(
      [&](const auto& op) {
        using OpType = std::decay_t<decltype(op)>;
        if constexpr (std::is_same_v<OpType, dsl::ComputeOp>) {
          dsl::ComputeOp copy = op;
          if (copy.units_param_ref.has_value()) {
            const dsl::TemplateArgValue& units_value =
                resolve_template_argument(*copy.units_param_ref, substitutions, "compute units");
            copy.units = extract_integer_param(units_value, "compute units");
            copy.units_param_ref.reset();
          }
          if (auto jitter = consume_jitter_param(copy.params); jitter.has_value()) {
            if (lowering_state == nullptr) {
              throw std::logic_error("jitter requires task-local state");
            }
            copy.units = apply_jitter_value(copy.units, *jitter, lowering_state);
          }
          if (auto high_mem = consume_compute_mem_param(copy.params);
              high_mem.has_value() && *high_mem && lowering_state != nullptr) {
            lowering_state->has_high_mem_compute = true;
          }
          out.push_back(copy);
        } else if constexpr (std::is_same_v<OpType, dsl::CallOp>) {
          dsl::CallOp copy = op;
          if (copy.device_param_ref.has_value()) {
            const dsl::TemplateArgValue& device_value =
                resolve_template_argument(*copy.device_param_ref, substitutions, "call device");
            copy.device = extract_string_param(device_value, "call device");
            copy.device_param_ref.reset();
          }
          if (copy.service_units_param_ref.has_value()) {
            const dsl::TemplateArgValue& service_units_value = resolve_template_argument(
                *copy.service_units_param_ref, substitutions, "call service units");
            copy.service_units = extract_integer_param(service_units_value, "call service units");
            copy.service_units_param_ref.reset();
          }
          if (auto jitter = consume_jitter_param(copy.params); jitter.has_value()) {
            if (lowering_state == nullptr) {
              throw std::logic_error("jitter requires task-local state");
            }
            copy.service_units = apply_jitter_value(copy.service_units, *jitter, lowering_state);
          }
          out.push_back(copy);
        } else if constexpr (std::is_same_v<OpType, dsl::SleepOp>) {
          dsl::SleepOp copy = op;
          if (copy.duration_param_ref.has_value()) {
            const dsl::TemplateArgValue& duration_value = resolve_template_argument(
                *copy.duration_param_ref, substitutions, "sleep duration");
            copy.duration = extract_duration_param(duration_value, "sleep duration");
            copy.duration_param_ref.reset();
          }
          out.push_back(copy);
        } else if constexpr (std::is_same_v<OpType, dsl::WaitOp>) {
          out.push_back(op);
        } else if constexpr (std::is_same_v<OpType, dsl::SignalOp>) {
          out.push_back(op);
        } else if constexpr (std::is_same_v<OpType, dsl::ArriveOp>) {
          out.push_back(op);
        } else if constexpr (std::is_same_v<OpType, dsl::UseOp>) {
          if (template_map_.find(op.template_name) != template_map_.end()) {
            const std::vector<dsl::Operation> expanded =
                expand_use_operation(op, substitutions, lowering_state);
            out.insert(out.end(), expanded.begin(), expanded.end());
          } else {
            out.push_back(op);
          }
        } else if constexpr (std::is_same_v<OpType, dsl::ChoiceOp>) {
          if (lowering_state == nullptr) {
            throw std::logic_error("choice lowering requires task-local state");
          }
          uint64_t total_weight = 0;
          for (const dsl::WeightedBranch& branch : op.branches) {
            total_weight += branch.weight;
          }
          if (total_weight == 0) {
            throw std::logic_error("choice must declare at least one branch");
          }
          const uint64_t selection = lowering_state->choice_visit_index % total_weight;
          lowering_state->choice_visit_index += 1;
          uint64_t cursor = 0;
          const dsl::WeightedBranch* selected = nullptr;
          for (const dsl::WeightedBranch& branch : op.branches) {
            if (selection < cursor + branch.weight) {
              selected = &branch;
              break;
            }
            cursor += branch.weight;
          }
          assert(selected != nullptr && "choice branch selection out of bounds");
          const std::vector<dsl::Operation> expanded_branch =
              expand_program(selected->body, substitutions, lowering_state);
          out.insert(out.end(), expanded_branch.begin(), expanded_branch.end());
        } else if constexpr (std::is_same_v<OpType, dsl::RepeatOpPtr>) {
          assert(op != nullptr && "repeat op must be non-null");
          for (uint64_t iteration = 0; iteration < op->count; ++iteration) {
            const std::vector<dsl::Operation> expanded_iteration =
                expand_program(op->body, substitutions, lowering_state);
            out.insert(out.end(), expanded_iteration.begin(), expanded_iteration.end());
          }
        }
      },
      operation);
}

std::vector<dsl::Operation>
InterpretedScenario::expand_use_operation(const dsl::UseOp& use,
                                          const TemplateSubstitutions* parent_substitutions,
                                          LoweringState* lowering_state) {
  const auto it = template_map_.find(use.template_name);
  if (it == template_map_.end()) {
    throw std::logic_error("unknown template '" + use.template_name + "'");
  }
  if (std::find(template_expansion_stack_.begin(), template_expansion_stack_.end(),
                use.template_name) != template_expansion_stack_.end()) {
    throw std::logic_error("recursive template expansion '" + use.template_name + "'");
  }
  const dsl::TemplateSpec& templ = it->second;
  if (templ.params.size() != use.args.size()) {
    throw std::logic_error("template arity mismatch for '" + use.template_name + "'");
  }
  TemplateSubstitutions substitutions;
  if (parent_substitutions != nullptr) {
    substitutions = *parent_substitutions;
  }
  substitutions.reserve(substitutions.size() + templ.params.size());
  for (std::size_t index = 0; index < templ.params.size(); ++index) {
    substitutions[templ.params[index]] = use.args[index];
  }
  template_expansion_stack_.push_back(use.template_name);
  std::vector<dsl::Operation> expanded;
  for (const dsl::Operation& operation : templ.body) {
    append_expanded_operation(operation, expanded, &substitutions, lowering_state);
  }
  template_expansion_stack_.pop_back();
  return expanded;
}

uint64_t InterpretedScenario::extract_integer_param(const dsl::TemplateArgValue& value,
                                                    const char* context) {
  if (const auto integer = std::get_if<uint64_t>(&value); integer) {
    return *integer;
  }
  throw std::logic_error(std::string(context) + " requires an integer template argument");
}

dsl::Duration InterpretedScenario::extract_duration_param(const dsl::TemplateArgValue& value,
                                                          const char* context) {
  if (const auto duration = std::get_if<dsl::Duration>(&value); duration) {
    return *duration;
  }
  throw std::logic_error(std::string(context) + " requires a duration template argument");
}

std::string InterpretedScenario::extract_string_param(const dsl::TemplateArgValue& value,
                                                      const char* context) {
  if (const auto string_value = std::get_if<std::string>(&value); string_value) {
    return *string_value;
  }
  throw std::logic_error(std::string(context) + " requires an identifier template argument");
}

const dsl::TemplateArgValue&
InterpretedScenario::resolve_template_argument(const dsl::TemplateParamRef& param_ref,
                                               const TemplateSubstitutions* substitutions,
                                               const char* context) {
  if (substitutions == nullptr) {
    throw std::logic_error(std::string(context) + " may only reference template parameters");
  }
  const auto it = substitutions->find(param_ref.name);
  if (it == substitutions->end()) {
    throw std::logic_error("unknown template parameter '" + param_ref.name + "' in " +
                           std::string(context));
  }
  return it->second;
}

} // namespace schedlab::benchmark

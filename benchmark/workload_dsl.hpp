#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace schedlab::benchmark::dsl {

enum class Track {
  CpuBound,
  IoBound,
  Mixed,
  Throughput,
  Latency,
  Fairness,
};

enum class ScorerKind {
  ThroughputMakespan,
  ThroughputSustainedRate,
  LatencyWakeupP99,
  LatencyFlowP99,
  FairnessShareSkew,
};

enum class ScenarioRole {
  Leaderboard,
  Gate,
};

enum class DurationUnit {
  Microseconds,
  Milliseconds,
  Seconds,
};

struct Duration {
  uint64_t value = 0;
  DurationUnit unit = DurationUnit::Microseconds;
};

struct Param {
  std::string key;
  std::string value;
};

struct TemplateParamRef {
  std::string name;
};

struct ComputeOp {
  uint64_t units = 0;
  std::optional<TemplateParamRef> units_param_ref;
  std::vector<Param> params;
};

struct CallOp {
  std::string device;
  std::optional<TemplateParamRef> device_param_ref;
  uint64_t service_units = 0;
  std::optional<TemplateParamRef> service_units_param_ref;
  std::vector<Param> params;
};

struct SleepOp {
  Duration duration{};
  std::optional<TemplateParamRef> duration_param_ref;
  std::vector<Param> params;
};

struct WaitOp {
  std::string target;
};

struct SignalOp {
  std::string target;
};

struct ArriveOp {
  std::string target;
};

using TemplateArgValue = std::variant<uint64_t, Duration, std::string>;

struct UseOp {
  std::string template_name;
  std::vector<TemplateArgValue> args;
};

struct ChoiceOp;

struct RepeatOp;
using RepeatOpPtr = std::shared_ptr<RepeatOp>;
using Operation = std::variant<ComputeOp, CallOp, SleepOp, UseOp, ChoiceOp, RepeatOpPtr, WaitOp,
                               SignalOp, ArriveOp>;

struct SyncEvent {
  std::string name;
};

struct SyncLatch {
  std::string name;
  uint64_t count = 0;
};

enum class DependencyKind { None, Name, All, Each, Fanout, Join };

struct DependencyExpr {
  DependencyKind kind = DependencyKind::None;
  std::vector<std::string> targets;
};

struct RepeatOp {
  uint64_t count = 0;
  std::vector<Operation> body;
};

struct WeightedBranch {
  uint64_t weight = 0;
  std::vector<Operation> body;
};

struct ChoiceOp {
  std::vector<WeightedBranch> branches;
};

struct DeviceSpec {
  std::string name;
  std::string model;
  std::optional<uint64_t> rate;
  uint64_t parallelism = 1;
  std::vector<Param> params;
};

struct NodeSpec {
  std::string name;
  uint64_t worker_begin = 0;
  uint64_t worker_end = 0;
};

enum class ArrivalPolicyKind {
  Delay,
  Stagger,
  Interval,
  Burst,
};

struct ArrivalPolicy {
  ArrivalPolicyKind policy = ArrivalPolicyKind::Delay;
  Duration duration{};
  std::optional<uint64_t> burst_size;
};

struct GroupSpec {
  std::string name;
  uint64_t count = 0;
  uint64_t weight = 1;
  std::optional<ArrivalPolicy> arrival;
  std::vector<Operation> body;
  std::optional<DependencyExpr> dependency;
};

struct PhaseSpec {
  std::string name;
  Duration at{};
  std::vector<GroupSpec> groups;
};

struct TemplateSpec {
  std::string name;
  std::vector<std::string> params;
  std::vector<Operation> body;
};

struct ParamSpec {
  std::string name;
  TemplateArgValue default_value;
};

struct VariantOverride {
  std::string name;
  TemplateArgValue value;
};

struct VariantSpec {
  std::string name;
  std::vector<VariantOverride> overrides;
};

struct WorkloadSpec {
  std::optional<Track> track;
  std::optional<ScorerKind> scorer;
  std::vector<std::string> score_groups;
  std::optional<ScenarioRole> role;
  std::optional<double> scenario_weight;
  std::optional<uint64_t> workers;
  std::optional<uint64_t> cpu_rate;
  std::optional<uint64_t> switch_cost_us;
  std::optional<uint64_t> migration_cost_us;
  std::optional<uint64_t> migration_cost_local_us;
  std::optional<uint64_t> migration_cost_remote_us;
  std::vector<NodeSpec> nodes;
  std::vector<DeviceSpec> devices;
  std::vector<SyncEvent> events;
  std::vector<SyncLatch> latches;
  std::vector<ParamSpec> params;
  std::vector<VariantSpec> variants;
  std::vector<TemplateSpec> templates;
  std::vector<PhaseSpec> phases;
  std::vector<GroupSpec> groups;
};

struct ParseError {
  std::size_t line = 0;
  std::string message;
};

struct ParseResult {
  std::optional<WorkloadSpec> spec;
  std::optional<ParseError> error;
};

ParseResult parse_workload_dsl(std::string_view source);

} // namespace schedlab::benchmark::dsl

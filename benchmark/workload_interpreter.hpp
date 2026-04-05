#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "benchmark/workload_install.hpp"
#include "benchmark/workload_dsl.hpp"
#include "schedlab/task_view.hpp"

namespace schedlab::benchmark {

class InterpretedScenario {
public:
  struct GroupMetadata {
    std::string name;
    uint64_t count = 0;
    uint64_t weight = 1;
  };

  struct TaskMetadata {
    std::size_t group_index = 0;
    uint64_t flow_id = 0;
  };

  struct CompletionAction {
    enum class Kind {
      SignalEvent,
      ArriveLatch,
    };

    Kind kind = Kind::ArriveLatch;
    std::string target;
  };

  struct LoweredTask {
    std::shared_ptr<const std::vector<dsl::Operation>> program;
    std::shared_ptr<const std::unordered_map<std::string, DeviceId>> device_ids;
    schedlab::TaskAttrs attrs;
    std::vector<CompletionAction> completion_actions;
    bool high_mem = false;
  };

  struct ExecutionConfig {
    struct NodeTopology {
      std::string name;
      std::vector<int> workers;
    };

    uint64_t cpu_rate = 0;
    uint64_t switch_cost_us = 0;
    uint64_t migration_cost_us = 0;
    uint64_t migration_cost_local_us = 0;
    uint64_t migration_cost_remote_us = 0;
    std::vector<NodeTopology> nodes;
    std::vector<int> worker_to_node;
    std::unordered_map<DeviceId, uint64_t> device_rates_by_id;
    std::unordered_map<DeviceId, uint64_t> device_parallelisms_by_id;
    std::vector<std::string> event_names;
    std::vector<dsl::SyncLatch> latches;
  };

  explicit InterpretedScenario(const dsl::WorkloadSpec& spec);

  // install() detaches per-task invocation state so spawned task args stay
  // valid even if this scenario object is destroyed immediately afterwards.
  void install(WorkloadInstaller& installer);

  // Scenario-level execution settings intended for runtime wiring
  // (e.g. cpu/device rate plumbing). Exposed so runner/runtime code can
  // consume these settings without re-parsing workload specs.
  const ExecutionConfig& execution_config() const noexcept;
  dsl::Track track() const noexcept;
  std::optional<dsl::ScorerKind> scorer() const noexcept;
  double scenario_weight() const noexcept;
  const std::vector<GroupMetadata>& group_metadata() const noexcept;
  const std::vector<TaskMetadata>& task_metadata() const noexcept;
  const std::vector<LoweredTask>& lowered_tasks() const noexcept;
  const std::unordered_map<std::string, DeviceId>& device_ids() const noexcept;

private:
  using TemplateSubstitutions = std::unordered_map<std::string, dsl::TemplateArgValue>;

  struct LoweringState {
    // Deterministic scenario-wide lowering ordinal for the current task.
    // Choice selection keeps its own local counter so jitter heterogeneity can
    // span groups/phases without perturbing existing choice behavior.
    uint64_t task_index = 0;
    uint64_t choice_visit_index = 0;
    uint64_t jitter_visit_index = 0;
    bool has_high_mem_compute = false;
  };

  std::vector<dsl::Operation> expand_program(const std::vector<dsl::Operation>& program,
                                             const TemplateSubstitutions* substitutions = nullptr,
                                             LoweringState* lowering_state = nullptr);
  void append_expanded_operation(const dsl::Operation& operation, std::vector<dsl::Operation>& out,
                                 const TemplateSubstitutions* substitutions,
                                 LoweringState* lowering_state);
  std::vector<dsl::Operation>
  expand_use_operation(const dsl::UseOp& use, const TemplateSubstitutions* parent_substitutions,
                       LoweringState* lowering_state);
  static uint64_t extract_integer_param(const dsl::TemplateArgValue& value, const char* context);
  static dsl::Duration extract_duration_param(const dsl::TemplateArgValue& value,
                                              const char* context);
  static std::string extract_string_param(const dsl::TemplateArgValue& value, const char* context);
  static const dsl::TemplateArgValue&
  resolve_template_argument(const dsl::TemplateParamRef& param_ref,
                            const TemplateSubstitutions* substitutions, const char* context);
  static std::optional<uint64_t> consume_jitter_param(std::vector<dsl::Param>& params);
  static std::optional<bool> consume_compute_mem_param(std::vector<dsl::Param>& params);
  static uint64_t apply_jitter_value(uint64_t base_value, uint64_t jitter,
                                     LoweringState* lowering_state);

  using TaskTemplate = LoweredTask;

  struct TaskInvocation {
    std::shared_ptr<const std::vector<dsl::Operation>> program;
    std::shared_ptr<const std::unordered_map<std::string, DeviceId>> device_ids;
    std::shared_ptr<const ExecutionConfig> execution_config;
    schedlab::TaskAttrs attrs;
    std::vector<CompletionAction> completion_actions;
  };

  static void task_main(WorkloadContext& workload, void* raw);

  static void execute_program(WorkloadContext& workload, const std::vector<dsl::Operation>& program,
                              const std::unordered_map<std::string, DeviceId>& device_ids);

  static void execute_operation(WorkloadContext& workload, const dsl::Operation& operation,
                                const std::unordered_map<std::string, DeviceId>& device_ids);

  struct ProgramKey {
    uint64_t offset = 0;
    uint64_t fingerprint = 0;
    std::size_t length = 0;

    bool operator==(const ProgramKey& other) const noexcept {
      return offset == other.offset && fingerprint == other.fingerprint && length == other.length;
    }
  };

  struct ProgramKeyHash {
    std::size_t operator()(const ProgramKey& key) const noexcept {
      const uint64_t mix =
          key.offset ^ (key.fingerprint << 1) ^ (static_cast<uint64_t>(key.length) << 2);
      return static_cast<std::size_t>(mix ^ (mix >> 32));
    }
  };

  std::unordered_map<std::string, dsl::TemplateSpec> template_map_;
  std::vector<std::string> template_expansion_stack_;
  std::unordered_map<ProgramKey, std::vector<std::shared_ptr<const std::vector<dsl::Operation>>>,
                     ProgramKeyHash>
      lowered_cache_;

  std::shared_ptr<const ExecutionConfig> execution_config_;
  std::shared_ptr<const std::unordered_map<std::string, DeviceId>> device_ids_;
  std::vector<std::shared_ptr<const std::vector<dsl::Operation>>> programs_;
  std::vector<TaskTemplate> task_templates_;
  dsl::Track track_ = dsl::Track::CpuBound;
  std::optional<dsl::ScorerKind> scorer_;
  double scenario_weight_ = 1.0;
  std::vector<GroupMetadata> group_metadata_;
  std::vector<TaskMetadata> task_metadata_;
#ifdef SCHEDLAB_TESTING
public:
  const std::vector<std::shared_ptr<const std::vector<dsl::Operation>>>&
  lowered_programs_for_tests() const noexcept {
    return programs_;
  }
#endif
};

} // namespace schedlab::benchmark

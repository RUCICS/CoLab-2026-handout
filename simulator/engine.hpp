#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "devices/device_model.hpp"
#include "benchmark/metrics.hpp"
#include "benchmark/workload_interpreter.hpp"
#include "runtime/sync_registry.hpp"
#include "runtime/task_runtime.hpp"
#include "runtime/tick_source.hpp"
#include "schedlab/scheduler.hpp"

namespace schedlab::simulator {

class EngineSystemView;

class Engine {
public:
  struct Config {
    int worker_count = 1;
    uint64_t compute_chunk_units = 32;
    uint64_t tick_interval_us = 0;
    uint64_t switch_cost_us = 0;
    uint64_t migration_cost_us = 0;
  };

  Engine(Scheduler& scheduler, const benchmark::InterpretedScenario& scenario, Config config);
  ~Engine();

  std::optional<benchmark::RunMetrics> run();

private:
  struct Event {
    enum class Kind {
      ComputeChunkComplete,
      Ready,
      DeviceDue,
    };

    uint64_t time_us = 0;
    Kind kind = Kind::Ready;
    uint64_t ordinal = 0;
    uint64_t task_id = 0;
    int worker_id = -1;
    uint64_t chunk_units = 0;
    uint64_t runtime_us = 0;
    ReadyContext ready_context{};
    uint64_t generation = 0;
  };

  struct EventCompare {
    bool operator()(const Event& lhs, const Event& rhs) const noexcept;
  };

  struct WorkerState {
    int worker_id = -1;
    uint64_t vtime_us = 0;
    uint64_t idle_time_us = 0;
    std::optional<uint64_t> current_task_id;
    std::optional<uint64_t> last_ran_task_id;
    std::deque<uint64_t> local_ready_queue;
    bool preempt_requested = false;
    bool has_dispatched = false;
    runtime::TickSource tick_source{0};
  };

  struct TaskRecord {
    runtime::RuntimeTask runtime;
    std::shared_ptr<const std::vector<benchmark::dsl::Operation>> program;
    const std::unordered_map<std::string, DeviceId>* device_ids = nullptr;
    std::vector<benchmark::InterpretedScenario::CompletionAction> completion_actions;
    std::size_t group_index = 0;
    uint64_t flow_id = 0;
    std::size_t pc = 0;
    uint64_t remaining_compute_units = 0;
    uint64_t accumulated_cache_warmth = 0;
    uint64_t ready_time_us = 0;
    uint64_t blocked_since_us = 0;
    bool has_release_time = false;
    uint64_t release_time_us = 0;
    uint64_t completion_time_us = 0;
    bool high_mem = false;
    ReadyContext last_ready_context{};

    TaskRecord(uint64_t task_id, TaskAttrs attrs);
  };

  uint64_t to_runtime_us(const TaskRecord& task, int worker_id, uint64_t cpu_units) const noexcept;
  uint64_t duration_to_us(const benchmark::dsl::Duration& duration) const noexcept;
  EngineSystemView& system_view() const;
  int worker_node(int worker_id) const noexcept;
  void mark_task_running(const WorkerState& worker, const TaskRecord& task);
  void mark_task_not_running(const WorkerState& worker, const TaskRecord& task);

  bool initialize();
  bool dispatch_idle_workers();
  void enqueue_local_ready(int worker_id, uint64_t task_id);
  std::optional<uint64_t> remove_local_ready_task(int worker_id, uint64_t task_id);
  std::optional<uint64_t> remove_default_steal_task(int victim_worker_id);
  std::optional<uint64_t> remove_specific_steal_task(int victim_worker_id, uint64_t task_id);
  bool dispatch_one(WorkerState& worker, uint64_t task_id);
  bool execute_until_pause(WorkerState& worker, TaskRecord& task);
  bool handle_signal_or_arrive(WorkerState& worker, TaskRecord& task,
                               const benchmark::dsl::Operation& op);
  bool maybe_preempt_current(WorkerState& worker, TaskRecord& task);
  bool process_next_event_batch();
  bool process_event(const Event& event);
  bool process_ready_event(const Event& event);
  bool process_compute_event(const Event& event);
  bool process_device_due_event(const Event& event);

  void enqueue_event(Event event);
  void enqueue_ready_event(uint64_t task_id, uint64_t ready_time_us, ReadyContext context);
  void schedule_next_device_due_event();
  void align_devices_to(uint64_t time_us);
  void record_group_runnable_delta(uint64_t time_us, std::size_t group_index, int delta);
  void record_group_service_slice(uint64_t start_time_us, uint64_t end_time_us,
                                  std::size_t group_index);

  friend class EngineSystemView;

  Scheduler* scheduler_ = nullptr;
  const benchmark::InterpretedScenario* scenario_ = nullptr;
  Config config_{};
  devices::FifoDeviceModel devices_;
  std::unique_ptr<runtime::SyncRegistry> sync_registry_;
  std::vector<WorkerState> workers_;
  std::unordered_map<uint64_t, TaskRecord> tasks_;
  std::priority_queue<Event, std::vector<Event>, EventCompare> events_;
  uint64_t next_event_ordinal_ = 1;
  uint64_t device_event_generation_ = 0;
  uint64_t device_time_us_ = 0;
  uint64_t completed_tasks_ = 0;
  std::vector<int> node_high_mem_count_;
  std::vector<benchmark::GroupRunnableEvent> group_runnable_events_;
  std::vector<benchmark::GroupServiceSlice> group_service_slices_;
  std::vector<uint64_t> wakeup_latencies_us_;
  mutable std::unique_ptr<EngineSystemView> system_view_;
};

} // namespace schedlab::simulator

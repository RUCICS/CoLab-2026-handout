#include "simulator/engine.hpp"

#include <algorithm>
#include <cassert>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

namespace schedlab::simulator {

namespace {

uint64_t ceil_div_u64(uint64_t numerator, uint64_t denominator) noexcept {
  if (numerator == 0 || denominator == 0) {
    return 0;
  }
  return 1 + ((numerator - 1) / denominator);
}

} // namespace

class EngineSystemView final : public SystemView {
public:
  explicit EngineSystemView(const Engine& engine)
      : engine_(engine), switch_cost_us_(engine.config_.switch_cost_us),
        migration_cost_us_(engine.config_.migration_cost_us),
        migration_cost_local_us_(engine.scenario_->execution_config().migration_cost_local_us),
        migration_cost_remote_us_(engine.scenario_->execution_config().migration_cost_remote_us) {
    worker_states_.resize(engine_.workers_.size());
    const auto& topology = engine_.scenario_->execution_config();
    node_workers_.resize(topology.nodes.size());
    for (const auto& worker : engine_.workers_) {
      const auto node = worker_node(worker.worker_id);
      if (node.has_value() && *node >= 0 &&
          static_cast<std::size_t>(*node) < node_workers_.size()) {
        node_workers_[static_cast<std::size_t>(*node)].push_back(worker.worker_id);
      }
    }
    queue_contexts_.resize(engine_.workers_.size());
    queue_views_.reserve(engine_.workers_.size());
    for (std::size_t index = 0; index < engine_.workers_.size(); ++index) {
      queue_contexts_[index] = QueueContext{
          .view = this,
          .worker_index = index,
      };
      queue_views_.push_back(QueueView(&queue_contexts_[index], &EngineSystemView::queue_size,
                                       &EngineSystemView::queue_get));
    }
    invalidate();
  }

  void invalidate() const noexcept {
    const auto& topology = engine_.scenario_->execution_config();
    for (std::size_t index = 0; index < engine_.workers_.size(); ++index) {
      const auto& worker = engine_.workers_[index];
      const int topology_node =
          worker.worker_id >= 0 &&
                  static_cast<std::size_t>(worker.worker_id) < topology.worker_to_node.size()
              ? topology.worker_to_node[static_cast<std::size_t>(worker.worker_id)]
              : 0;
      worker_states_[index] = SystemView::WorkerState{
          .worker_id = worker.worker_id,
          .topology_node = topology_node,
          .local_queue_size = static_cast<uint32_t>(worker.local_ready_queue.size()),
          .is_idle = !worker.current_task_id.has_value(),
          .running_task_id = worker.current_task_id,
      };
    }
  }

  uint32_t total_ready_count() const override {
    uint32_t total = 0;
    for (const auto& worker : engine_.workers_) {
      total += static_cast<uint32_t>(worker.local_ready_queue.size());
    }
    return total;
  }

  uint32_t total_worker_count() const override {
    return static_cast<uint32_t>(engine_.workers_.size());
  }

  uint32_t node_count() const override {
    return static_cast<uint32_t>(engine_.scenario_->execution_config().nodes.size());
  }

  uint64_t switch_cost() const override {
    return switch_cost_us_;
  }

  uint64_t migration_cost(int from_worker, int to_worker) const override {
    if (from_worker == to_worker) {
      return 0;
    }
    const auto& topology = engine_.scenario_->execution_config();
    if (topology.nodes.size() <= 1) {
      return migration_cost_us_;
    }
    const auto from = worker_node(from_worker);
    const auto to = worker_node(to_worker);
    if (!from.has_value() || !to.has_value()) {
      return migration_cost_us_;
    }
    return *from == *to ? migration_cost_local_us_ : migration_cost_remote_us_;
  }

  std::span<const WorkerState> worker_states() const override {
    return worker_states_;
  }

  std::span<const int> workers_in_node(int node) const override {
    if (node < 0 || static_cast<std::size_t>(node) >= node_workers_.size()) {
      return {};
    }
    const auto& workers = node_workers_[static_cast<std::size_t>(node)];
    return std::span<const int>(workers.data(), workers.size());
  }

  const QueueView& queue(int worker_id) const override {
    static const QueueView kEmpty;
    if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= engine_.workers_.size()) {
      return kEmpty;
    }
    return queue_views_[static_cast<std::size_t>(worker_id)];
  }

  TaskViewRef running_task(int worker_id) const override {
    if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= engine_.workers_.size()) {
      return {};
    }
    const auto& worker = engine_.workers_[static_cast<std::size_t>(worker_id)];
    if (!worker.current_task_id.has_value()) {
      return {};
    }
    return engine_.tasks_.at(*worker.current_task_id).runtime.view_ref();
  }

  TaskViewRef task(uint64_t task_id) const override {
    const auto it = engine_.tasks_.find(task_id);
    if (it == engine_.tasks_.end()) {
      return {};
    }
    return it->second.runtime.view_ref();
  }

private:
  struct QueueContext {
    const EngineSystemView* view = nullptr;
    std::size_t worker_index = 0;
  };

  static std::size_t queue_size(const void* context) noexcept {
    const auto* queue = static_cast<const QueueContext*>(context);
    return queue->view->engine_.workers_[queue->worker_index].local_ready_queue.size();
  }

  static const TaskView& queue_get(const void* context, std::size_t index) noexcept {
    const auto* queue = static_cast<const QueueContext*>(context);
    const auto& worker = queue->view->engine_.workers_[queue->worker_index];
    const uint64_t task_id = worker.local_ready_queue[index];
    return queue->view->engine_.tasks_.at(task_id).runtime.view_ref();
  }

  std::optional<int> worker_node(int worker_id) const {
    const auto& topology = engine_.scenario_->execution_config();
    if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= topology.worker_to_node.size()) {
      return std::nullopt;
    }
    return topology.worker_to_node[static_cast<std::size_t>(worker_id)];
  }

  const Engine& engine_;
  uint64_t switch_cost_us_ = 0;
  uint64_t migration_cost_us_ = 0;
  uint64_t migration_cost_local_us_ = 0;
  uint64_t migration_cost_remote_us_ = 0;
  mutable std::vector<SystemView::WorkerState> worker_states_;
  mutable std::vector<QueueContext> queue_contexts_;
  mutable std::vector<QueueView> queue_views_;
  mutable std::vector<std::vector<int>> node_workers_;
};

bool Engine::EventCompare::operator()(const Event& lhs, const Event& rhs) const noexcept {
  auto priority = [](Event::Kind kind) noexcept {
    switch (kind) {
    case Event::Kind::ComputeChunkComplete:
      return 0;
    case Event::Kind::Ready:
      return 1;
    case Event::Kind::DeviceDue:
      return 2;
    }
    return 3;
  };
  if (lhs.time_us != rhs.time_us) {
    return lhs.time_us > rhs.time_us;
  }
  if (priority(lhs.kind) != priority(rhs.kind)) {
    return priority(lhs.kind) > priority(rhs.kind);
  }
  return lhs.ordinal > rhs.ordinal;
}

Engine::TaskRecord::TaskRecord(uint64_t task_id, TaskAttrs attrs) : runtime(task_id, attrs) {}

Engine::Engine(Scheduler& scheduler, const benchmark::InterpretedScenario& scenario, Config config)
    : scheduler_(&scheduler), scenario_(&scenario), config_(config) {}

Engine::~Engine() = default;

uint64_t Engine::to_runtime_us(const TaskRecord& task, int worker_id,
                               uint64_t cpu_units) const noexcept {
  uint64_t runtime_us = ceil_div_u64(cpu_units, scenario_->execution_config().cpu_rate);
  if (!task.high_mem) {
    return runtime_us;
  }
  const int node = worker_node(worker_id);
  if (node < 0 || static_cast<std::size_t>(node) >= node_high_mem_count_.size()) {
    return runtime_us;
  }
  const int high_mem_neighbors = node_high_mem_count_[static_cast<std::size_t>(node)];
  if (high_mem_neighbors <= 1) {
    return runtime_us;
  }
  return runtime_us * static_cast<uint64_t>(100 + 20 * (high_mem_neighbors - 1)) / 100;
}

uint64_t Engine::duration_to_us(const benchmark::dsl::Duration& duration) const noexcept {
  switch (duration.unit) {
  case benchmark::dsl::DurationUnit::Microseconds:
    return duration.value;
  case benchmark::dsl::DurationUnit::Milliseconds:
    return duration.value * 1000ULL;
  case benchmark::dsl::DurationUnit::Seconds:
    return duration.value * 1000ULL * 1000ULL;
  }
  return 0;
}

EngineSystemView& Engine::system_view() const {
  if (!system_view_) {
    system_view_ = std::make_unique<EngineSystemView>(*this);
  }
  system_view_->invalidate();
  return *system_view_;
}

int Engine::worker_node(int worker_id) const noexcept {
  const auto& topology = scenario_->execution_config();
  if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= topology.worker_to_node.size()) {
    return 0;
  }
  return topology.worker_to_node[static_cast<std::size_t>(worker_id)];
}

void Engine::mark_task_running(const WorkerState& worker, const TaskRecord& task) {
  if (!task.high_mem) {
    return;
  }
  const int node = worker_node(worker.worker_id);
  if (node < 0 || static_cast<std::size_t>(node) >= node_high_mem_count_.size()) {
    return;
  }
  ++node_high_mem_count_[static_cast<std::size_t>(node)];
}

void Engine::mark_task_not_running(const WorkerState& worker, const TaskRecord& task) {
  if (!task.high_mem) {
    return;
  }
  const int node = worker_node(worker.worker_id);
  if (node < 0 || static_cast<std::size_t>(node) >= node_high_mem_count_.size()) {
    return;
  }
  int& count = node_high_mem_count_[static_cast<std::size_t>(node)];
  if (count > 0) {
    --count;
  }
}

std::optional<benchmark::RunMetrics> Engine::run() {
  if (scheduler_ == nullptr || scenario_ == nullptr) {
    return std::nullopt;
  }
  if (config_.worker_count <= 0 || config_.compute_chunk_units == 0) {
    return std::nullopt;
  }

  devices_ = devices::FifoDeviceModel{};
  sync_registry_ = std::make_unique<runtime::SyncRegistry>();
  workers_.clear();
  tasks_.clear();
  wakeup_latencies_us_.clear();
  group_runnable_events_.clear();
  group_service_slices_.clear();
  while (!events_.empty()) {
    events_.pop();
  }
  next_event_ordinal_ = 1;
  device_event_generation_ = 0;
  device_time_us_ = 0;
  completed_tasks_ = 0;
  node_high_mem_count_.clear();

  if (!initialize()) {
    return std::nullopt;
  }

  while (completed_tasks_ < tasks_.size()) {
    if (!dispatch_idle_workers()) {
      return std::nullopt;
    }
    if (completed_tasks_ == tasks_.size()) {
      break;
    }
    if (!process_next_event_batch()) {
      return std::nullopt;
    }
  }

  benchmark::MetricsCollector collector;
  collector.record_task_completion(completed_tasks_);
  uint64_t makespan_us = 0;
  for (const auto& worker : workers_) {
    collector.record_worker_idle_time_us(worker.worker_id, worker.idle_time_us);
    makespan_us = std::max(makespan_us, worker.vtime_us);
  }
  collector.set_elapsed_time_us(makespan_us);

  const auto& group_metadata = scenario_->group_metadata();
  std::vector<std::vector<uint64_t>> task_ids_by_group(group_metadata.size());
  for (const auto& [task_id, task] : tasks_) {
    if (task.group_index < task_ids_by_group.size()) {
      task_ids_by_group[task.group_index].push_back(task_id);
    }
    collector.record_task_observation(benchmark::TaskObservation{
        .task_id = task_id,
        .flow_id = task.flow_id,
        .group_index = task.group_index,
        .release_time_us = task.release_time_us,
        .completion_time_us = task.completion_time_us,
        .cpu_runtime_us = task.runtime.view().total_runtime_us,
    });
  }
  for (std::size_t group_index = 0; group_index < group_metadata.size(); ++group_index) {
    benchmark::GroupObservation observation{
        .group_name = group_metadata[group_index].name,
        .weight = group_metadata[group_index].weight,
        .task_ids = std::move(task_ids_by_group[group_index]),
    };
    collector.record_group_observation(std::move(observation));
  }
  for (uint64_t latency_us : wakeup_latencies_us_) {
    collector.record_wakeup_latency_us(latency_us);
  }
  for (const auto& event : group_runnable_events_) {
    collector.record_group_runnable_event(event);
  }
  for (const auto& slice : group_service_slices_) {
    collector.record_group_service_slice(slice);
  }
  return collector.finish();
}

void Engine::record_group_runnable_delta(uint64_t time_us, std::size_t group_index, int delta) {
  group_runnable_events_.push_back(benchmark::GroupRunnableEvent{
      .time_us = time_us,
      .group_index = group_index,
      .delta = delta,
  });
}

void Engine::record_group_service_slice(uint64_t start_time_us, uint64_t end_time_us,
                                        std::size_t group_index) {
  if (end_time_us <= start_time_us) {
    return;
  }
  group_service_slices_.push_back(benchmark::GroupServiceSlice{
      .start_time_us = start_time_us,
      .end_time_us = end_time_us,
      .group_index = group_index,
  });
}

bool Engine::initialize() {
  const auto& exec = scenario_->execution_config();
  node_high_mem_count_.assign(std::max<std::size_t>(exec.nodes.size(), 1), 0);
  for (const auto& [device_id, rate] : exec.device_rates_by_id) {
    devices_.configure_device_rate(device_id, rate);
  }
  for (const auto& [device_id, parallelism] : exec.device_parallelisms_by_id) {
    devices_.configure_device_parallelism(device_id, parallelism);
  }
  for (const auto& event_name : exec.event_names) {
    if (!sync_registry_->declare_event(event_name)) {
      return false;
    }
  }
  for (const auto& latch : exec.latches) {
    if (!sync_registry_->declare_latch(latch.name, latch.count)) {
      return false;
    }
  }

  workers_.reserve(static_cast<std::size_t>(config_.worker_count));
  for (int worker_id = 0; worker_id < config_.worker_count; ++worker_id) {
    workers_.push_back(WorkerState{
        .worker_id = worker_id,
        .tick_source = runtime::TickSource(config_.tick_interval_us),
    });
  }
  scheduler_->init(system_view());

  const auto& lowered_tasks = scenario_->lowered_tasks();
  const auto& task_metadata = scenario_->task_metadata();
  if (lowered_tasks.size() != task_metadata.size()) {
    return false;
  }

  for (std::size_t index = 0; index < lowered_tasks.size(); ++index) {
    const uint64_t task_id = static_cast<uint64_t>(index + 1);
    const auto& lowered_task = lowered_tasks[index];
    const auto& metadata = task_metadata[index];

    TaskRecord task(task_id, lowered_task.attrs);
    task.program = lowered_task.program;
    task.device_ids = lowered_task.device_ids.get();
    task.completion_actions = lowered_task.completion_actions;
    task.group_index = metadata.group_index;
    task.flow_id = metadata.flow_id;
    task.high_mem = lowered_task.high_mem;

    if (!task.program) {
      return false;
    }

    if (!task.program->empty()) {
      const auto* sleep_op = std::get_if<benchmark::dsl::SleepOp>(&task.program->front());
      if (sleep_op != nullptr) {
        task.pc = 1;
        enqueue_ready_event(task_id, duration_to_us(sleep_op->duration),
                            ReadyContext{
                                .reason = ReadyReason::Spawn,
                                .source_worker_id = -1,
                                .previous_worker_id = -1,
                                .ready_time_us = duration_to_us(sleep_op->duration),
                            });
      } else {
        enqueue_ready_event(task_id, 0,
                            ReadyContext{
                                .reason = ReadyReason::Spawn,
                                .source_worker_id = -1,
                                .previous_worker_id = -1,
                                .ready_time_us = 0,
                            });
      }
    } else {
      enqueue_ready_event(task_id, 0,
                          ReadyContext{
                              .reason = ReadyReason::Spawn,
                              .source_worker_id = -1,
                              .previous_worker_id = -1,
                              .ready_time_us = 0,
                          });
    }
    tasks_.emplace(task_id, std::move(task));
  }

  return true;
}

bool Engine::dispatch_idle_workers() {
  bool progressed = true;
  while (progressed) {
    progressed = false;
    for (auto& worker : workers_) {
      if (worker.current_task_id.has_value()) {
        continue;
      }

      auto& system = system_view();
      std::optional<uint64_t> next =
          scheduler_->pick_next(worker.worker_id, system.queue(worker.worker_id), system);
      if (next.has_value()) {
        next = remove_local_ready_task(worker.worker_id, *next);
      }
      if (!next.has_value()) {
        const auto steal = scheduler_->steal(worker.worker_id, system);
        if (steal.has_value()) {
          if (steal->task_id.has_value()) {
            next = remove_specific_steal_task(steal->victim_worker_id, *steal->task_id);
          } else {
            next = remove_default_steal_task(steal->victim_worker_id);
          }
        }
      }
      if (!next.has_value()) {
        continue;
      }
      if (!dispatch_one(worker, *next)) {
        return false;
      }
      progressed = true;
    }
  }
  return true;
}

void Engine::enqueue_local_ready(int worker_id, uint64_t task_id) {
  if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= workers_.size()) {
    return;
  }
  workers_[static_cast<std::size_t>(worker_id)].local_ready_queue.push_back(task_id);
}

std::optional<uint64_t> Engine::remove_local_ready_task(int worker_id, uint64_t task_id) {
  if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= workers_.size()) {
    return std::nullopt;
  }
  auto& queue = workers_[static_cast<std::size_t>(worker_id)].local_ready_queue;
  if (queue.empty()) {
    return std::nullopt;
  }
  if (queue.front() == task_id) {
    queue.pop_front();
    return task_id;
  }
  if (queue.back() == task_id) {
    queue.pop_back();
    return task_id;
  }
  const auto it = std::find(queue.begin(), queue.end(), task_id);
  if (it == queue.end()) {
    return std::nullopt;
  }
  const uint64_t selected = *it;
  queue.erase(it);
  return selected;
}

std::optional<uint64_t> Engine::remove_default_steal_task(int victim_worker_id) {
  if (victim_worker_id < 0 || static_cast<std::size_t>(victim_worker_id) >= workers_.size()) {
    return std::nullopt;
  }
  auto& queue = workers_[static_cast<std::size_t>(victim_worker_id)].local_ready_queue;
  if (queue.empty()) {
    return std::nullopt;
  }
  const uint64_t task_id = queue.back();
  queue.pop_back();
  return task_id;
}

std::optional<uint64_t> Engine::remove_specific_steal_task(int victim_worker_id, uint64_t task_id) {
  return remove_local_ready_task(victim_worker_id, task_id);
}

bool Engine::dispatch_one(WorkerState& worker, uint64_t task_id) {
  auto task_it = tasks_.find(task_id);
  if (task_it == tasks_.end()) {
    return false;
  }
  TaskRecord& task = task_it->second;
  if (task.runtime.state() != runtime::TaskState::Ready) {
    return false;
  }

  if (worker.vtime_us < task.ready_time_us) {
    worker.idle_time_us += task.ready_time_us - worker.vtime_us;
    worker.vtime_us = task.ready_time_us;
  }

  if (worker.has_dispatched && worker.last_ran_task_id != task_id) {
    worker.vtime_us += config_.switch_cost_us;
  }
  if (task.runtime.view().last_worker_id >= 0 &&
      task.runtime.view().last_worker_id != worker.worker_id) {
    const auto& topology = scenario_->execution_config();
    const int from = task.runtime.view().last_worker_id;
    const int to = worker.worker_id;
    uint64_t base_migration_cost = 0;
    if (topology.nodes.size() <= 1) {
      base_migration_cost = config_.migration_cost_us;
    } else if (from >= 0 && to >= 0 &&
               static_cast<std::size_t>(from) < topology.worker_to_node.size() &&
               static_cast<std::size_t>(to) < topology.worker_to_node.size() &&
               topology.worker_to_node[static_cast<std::size_t>(from)] ==
                   topology.worker_to_node[static_cast<std::size_t>(to)]) {
      base_migration_cost = topology.migration_cost_local_us;
    } else if (topology.migration_cost_remote_us != 0) {
      base_migration_cost = topology.migration_cost_remote_us;
    } else {
      base_migration_cost = config_.migration_cost_us;
    }
    const uint64_t hotness_penalty = std::min<uint64_t>(50, task.accumulated_cache_warmth / 100);
    worker.vtime_us += base_migration_cost + hotness_penalty;
    task.accumulated_cache_warmth = 0;
  }

  if (!task.runtime.on_dispatch(worker.worker_id)) {
    return false;
  }
  worker.current_task_id = task_id;
  worker.last_ran_task_id = task_id;
  worker.preempt_requested = false;
  worker.has_dispatched = true;
  mark_task_running(worker, task);
  if (task.last_ready_context.reason == ReadyReason::Wakeup) {
    wakeup_latencies_us_.push_back(worker.vtime_us - task.ready_time_us);
  }
  return execute_until_pause(worker, task);
}

bool Engine::execute_until_pause(WorkerState& worker, TaskRecord& task) {
  while (true) {
    if (task.pc >= task.program->size()) {
      for (const auto& action : task.completion_actions) {
        benchmark::dsl::Operation synthetic;
        if (action.kind == benchmark::InterpretedScenario::CompletionAction::Kind::SignalEvent) {
          synthetic = benchmark::dsl::SignalOp{.target = action.target};
        } else {
          synthetic = benchmark::dsl::ArriveOp{.target = action.target};
        }
        if (!handle_signal_or_arrive(worker, task, synthetic)) {
          return false;
        }
        if (!worker.current_task_id.has_value()) {
          return true;
        }
      }
      if (!task.runtime.on_exit(worker.worker_id)) {
        return false;
      }
      mark_task_not_running(worker, task);
      record_group_runnable_delta(worker.vtime_us, task.group_index, -1);
      task.completion_time_us = worker.vtime_us;
      worker.current_task_id.reset();
      ++completed_tasks_;
      scheduler_->on_task_exited(task.runtime.view(), worker.worker_id);
      return true;
    }

    const benchmark::dsl::Operation& operation = (*task.program)[task.pc];
    if (const auto* compute = std::get_if<benchmark::dsl::ComputeOp>(&operation)) {
      if (task.remaining_compute_units == 0) {
        task.remaining_compute_units = compute->units;
      }
      const uint64_t chunk_units =
          std::min(task.remaining_compute_units, config_.compute_chunk_units);
      const uint64_t runtime_us = to_runtime_us(task, worker.worker_id, chunk_units);
      enqueue_event(Event{
          .time_us = worker.vtime_us + runtime_us,
          .kind = Event::Kind::ComputeChunkComplete,
          .ordinal = next_event_ordinal_++,
          .task_id = task.runtime.view().task_id,
          .worker_id = worker.worker_id,
          .chunk_units = chunk_units,
          .runtime_us = runtime_us,
      });
      return true;
    }

    if (const auto* sleep = std::get_if<benchmark::dsl::SleepOp>(&operation)) {
      if (!task.runtime.on_block(worker.worker_id)) {
        return false;
      }
      mark_task_not_running(worker, task);
      record_group_runnable_delta(worker.vtime_us, task.group_index, -1);
      scheduler_->on_task_blocked(task.runtime.view(), worker.worker_id);
      task.blocked_since_us = worker.vtime_us;
      ++task.pc;
      worker.current_task_id.reset();
      enqueue_ready_event(task.runtime.view().task_id,
                          worker.vtime_us + duration_to_us(sleep->duration),
                          ReadyContext{
                              .reason = ReadyReason::Wakeup,
                              .source_worker_id = worker.worker_id,
                              .previous_worker_id = worker.worker_id,
                              .ready_time_us = worker.vtime_us + duration_to_us(sleep->duration),
                          });
      return true;
    }

    if (const auto* call = std::get_if<benchmark::dsl::CallOp>(&operation)) {
      if (task.device_ids == nullptr) {
        return false;
      }
      const auto device_it = task.device_ids->find(call->device);
      if (device_it == task.device_ids->end()) {
        return false;
      }
      if (!task.runtime.on_block(worker.worker_id)) {
        return false;
      }
      mark_task_not_running(worker, task);
      record_group_runnable_delta(worker.vtime_us, task.group_index, -1);
      scheduler_->on_task_blocked(task.runtime.view(), worker.worker_id);
      task.blocked_since_us = worker.vtime_us;
      ++task.pc;
      worker.current_task_id.reset();
      align_devices_to(worker.vtime_us);
      devices_.submit(task.runtime.view().task_id, device_it->second,
                      DeviceRequest{
                          .service_units = call->service_units,
                      });
      schedule_next_device_due_event();
      return true;
    }

    if (const auto* wait = std::get_if<benchmark::dsl::WaitOp>(&operation)) {
      const auto result = sync_registry_->wait(wait->target, task.runtime.view().task_id, [&]() {
        return task.runtime.on_block(worker.worker_id);
      });
      ++task.pc;
      if (result == runtime::SyncWaitResult::Ready) {
        continue;
      }
      if (result != runtime::SyncWaitResult::Blocked) {
        return false;
      }
      mark_task_not_running(worker, task);
      record_group_runnable_delta(worker.vtime_us, task.group_index, -1);
      scheduler_->on_task_blocked(task.runtime.view(), worker.worker_id);
      task.blocked_since_us = worker.vtime_us;
      worker.current_task_id.reset();
      return true;
    }

    if (std::holds_alternative<benchmark::dsl::SignalOp>(operation) ||
        std::holds_alternative<benchmark::dsl::ArriveOp>(operation)) {
      if (!handle_signal_or_arrive(worker, task, operation)) {
        return false;
      }
      if (!worker.current_task_id.has_value()) {
        return true;
      }
      ++task.pc;
      continue;
    }

    return false;
  }
}

bool Engine::handle_signal_or_arrive(WorkerState& worker, TaskRecord& task,
                                     const benchmark::dsl::Operation& operation) {
  auto wake_fn = [&](uint64_t woken_task_id) {
    enqueue_ready_event(
        woken_task_id, worker.vtime_us,
        ReadyContext{
            .reason = ReadyReason::Wakeup,
            .source_worker_id = worker.worker_id,
            .previous_worker_id = static_cast<int>(task.runtime.view().last_worker_id),
            .ready_time_us = worker.vtime_us,
        });
  };

  if (const auto* signal = std::get_if<benchmark::dsl::SignalOp>(&operation)) {
    if (sync_registry_->signal_event(signal->target, wake_fn) !=
        runtime::SyncActionResult::Applied) {
      return false;
    }
  } else if (const auto* arrive = std::get_if<benchmark::dsl::ArriveOp>(&operation)) {
    if (sync_registry_->arrive_latch(arrive->target, wake_fn) !=
        runtime::SyncActionResult::Applied) {
      return false;
    }
  } else {
    return false;
  }

  return maybe_preempt_current(worker, task);
}

bool Engine::maybe_preempt_current(WorkerState& worker, TaskRecord& task) {
  if (!worker.preempt_requested) {
    return true;
  }
  worker.preempt_requested = false;
  if (!task.runtime.on_preempt(worker.worker_id)) {
    return false;
  }
  mark_task_not_running(worker, task);
  task.ready_time_us = worker.vtime_us;
  task.last_ready_context = ReadyContext{
      .reason = ReadyReason::Wakeup,
      .source_worker_id = worker.worker_id,
      .previous_worker_id = worker.worker_id,
      .ready_time_us = worker.vtime_us,
  };
  enqueue_local_ready(worker.worker_id, task.runtime.view().task_id);
  worker.current_task_id.reset();
  scheduler_->on_task_preempted(task.runtime.view(), worker.worker_id);
  return true;
}

bool Engine::process_next_event_batch() {
  if (events_.empty()) {
    return false;
  }
  const uint64_t next_time_us = events_.top().time_us;
  std::vector<Event> batch;
  while (!events_.empty() && events_.top().time_us == next_time_us) {
    batch.push_back(events_.top());
    events_.pop();
  }
  for (const auto& event : batch) {
    if (!process_event(event)) {
      return false;
    }
  }
  return true;
}

bool Engine::process_event(const Event& event) {
  switch (event.kind) {
  case Event::Kind::Ready:
    return process_ready_event(event);
  case Event::Kind::ComputeChunkComplete:
    return process_compute_event(event);
  case Event::Kind::DeviceDue:
    return process_device_due_event(event);
  }
  return false;
}

bool Engine::process_ready_event(const Event& event) {
  auto task_it = tasks_.find(event.task_id);
  if (task_it == tasks_.end()) {
    return false;
  }
  TaskRecord& task = task_it->second;
  if (task.runtime.state() == runtime::TaskState::New) {
    if (!task.runtime.on_spawn_ready()) {
      return false;
    }
  } else {
    if (!task.runtime.on_wakeup_ready()) {
      return false;
    }
  }
  if (!task.has_release_time) {
    task.has_release_time = true;
    task.release_time_us = event.time_us;
  }
  task.ready_time_us = event.time_us;
  task.last_ready_context = event.ready_context;
  record_group_runnable_delta(event.time_us, task.group_index, +1);
  if (event.ready_context.reason == ReadyReason::Wakeup && event.time_us >= task.blocked_since_us) {
    task.blocked_since_us = 0;
  }

  int target_worker =
      scheduler_->select_worker(task.runtime.view(), event.ready_context, system_view());
  if (target_worker < 0 || static_cast<std::size_t>(target_worker) >= workers_.size()) {
    return false;
  }
  enqueue_local_ready(target_worker, task.runtime.view().task_id);

  WorkerState& target = workers_[static_cast<std::size_t>(target_worker)];
  if (target.current_task_id.has_value()) {
    TaskRecord& current = tasks_.at(*target.current_task_id);
    if (scheduler_->should_preempt(task.runtime.view(), current.runtime.view(), target.worker_id,
                                   system_view())) {
      target.preempt_requested = true;
    }
  }
  return true;
}

bool Engine::process_compute_event(const Event& event) {
  if (event.worker_id < 0 || static_cast<std::size_t>(event.worker_id) >= workers_.size()) {
    return false;
  }
  WorkerState& worker = workers_[static_cast<std::size_t>(event.worker_id)];
  if (!worker.current_task_id.has_value() || *worker.current_task_id != event.task_id) {
    return true;
  }
  TaskRecord& task = tasks_.at(event.task_id);
  record_group_service_slice(event.time_us - event.runtime_us, event.time_us, task.group_index);
  if (!task.runtime.on_runtime_advance(event.runtime_us)) {
    return false;
  }
  worker.vtime_us = event.time_us;
  worker.tick_source.advance_by(event.runtime_us);
  if (std::numeric_limits<uint64_t>::max() - task.accumulated_cache_warmth < event.runtime_us) {
    task.accumulated_cache_warmth = std::numeric_limits<uint64_t>::max();
  } else {
    task.accumulated_cache_warmth += event.runtime_us;
  }
  if (task.remaining_compute_units < event.chunk_units) {
    return false;
  }
  task.remaining_compute_units -= event.chunk_units;

  if (worker.tick_source.consume_pending_tick()) {
    const TickAction action =
        scheduler_->on_tick(task.runtime.view(), worker.worker_id, system_view());
    if (action == TickAction::RequestResched) {
      worker.preempt_requested = true;
    }
  }

  if (task.remaining_compute_units > 0) {
    if (!maybe_preempt_current(worker, task)) {
      return false;
    }
    if (!worker.current_task_id.has_value()) {
      return true;
    }
    return execute_until_pause(worker, task);
  }

  task.remaining_compute_units = 0;
  ++task.pc;
  if (!maybe_preempt_current(worker, task)) {
    return false;
  }
  if (!worker.current_task_id.has_value()) {
    return true;
  }
  return execute_until_pause(worker, task);
}

bool Engine::process_device_due_event(const Event& event) {
  if (event.generation != device_event_generation_) {
    return true;
  }
  align_devices_to(event.time_us);
  const auto completions = devices_.pop_completed();
  for (const auto& completion : completions) {
    auto task_it = tasks_.find(completion.task_token);
    if (task_it == tasks_.end()) {
      return false;
    }
    TaskRecord& task = task_it->second;
    if (!task.runtime.on_device_completion(completion.result)) {
      return false;
    }
    static_cast<void>(task.runtime.take_device_result());
    enqueue_ready_event(
        completion.task_token, event.time_us,
        ReadyContext{
            .reason = ReadyReason::Wakeup,
            .source_worker_id = -1,
            .previous_worker_id = static_cast<int>(task.runtime.view().last_worker_id),
            .ready_time_us = event.time_us,
        });
  }
  schedule_next_device_due_event();
  return true;
}

void Engine::enqueue_event(Event event) {
  events_.push(std::move(event));
}

void Engine::enqueue_ready_event(uint64_t task_id, uint64_t ready_time_us, ReadyContext context) {
  enqueue_event(Event{
      .time_us = ready_time_us,
      .kind = Event::Kind::Ready,
      .ordinal = next_event_ordinal_++,
      .task_id = task_id,
      .worker_id = context.source_worker_id,
      .ready_context = context,
  });
}

void Engine::schedule_next_device_due_event() {
  const auto next_due = devices_.next_completion_at_us();
  if (!next_due.has_value()) {
    return;
  }
  ++device_event_generation_;
  enqueue_event(Event{
      .time_us = *next_due,
      .kind = Event::Kind::DeviceDue,
      .ordinal = next_event_ordinal_++,
      .generation = device_event_generation_,
  });
}

void Engine::align_devices_to(uint64_t time_us) {
  if (time_us < device_time_us_) {
    return;
  }
  devices_.advance_by(time_us - device_time_us_);
  device_time_us_ = time_us;
}

} // namespace schedlab::simulator

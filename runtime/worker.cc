#include "runtime/worker.hpp"

#include <algorithm>
#include <chrono>
#include <utility>

namespace schedlab::runtime {

uint64_t steady_now_us() noexcept {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch())
          .count());
}

class ScopedActivityCounter {
public:
  explicit ScopedActivityCounter(std::atomic<uint64_t>& counter) noexcept : counter_(&counter) {
    counter_->fetch_add(1, std::memory_order_acq_rel);
  }

  ~ScopedActivityCounter() {
    counter_->fetch_sub(1, std::memory_order_acq_rel);
  }

  ScopedActivityCounter(const ScopedActivityCounter&) = delete;
  ScopedActivityCounter& operator=(const ScopedActivityCounter&) = delete;

private:
  std::atomic<uint64_t>* counter_ = nullptr;
};

class WorkerSystemView final : public SystemView {
public:
  explicit WorkerSystemView(const Worker& worker) : worker_(&worker) {
    worker_ids_.push_back(worker.worker_id_);
    worker_states_.push_back(SystemView::WorkerState{
        .worker_id = worker.worker_id_,
        .topology_node = 0,
        .local_queue_size = static_cast<uint32_t>(worker.local_ready_queue_.size()),
        .is_idle = !worker.current_task_id_.has_value(),
        .running_task_id = worker.current_task_id_,
    });
    queue_view_ = QueueView(this, &WorkerSystemView::queue_size, &WorkerSystemView::queue_get);
  }

  void invalidate() noexcept {
    worker_states_.front().local_queue_size =
        static_cast<uint32_t>(worker_->local_ready_queue_.size());
    worker_states_.front().is_idle = !worker_->current_task_id_.has_value();
    worker_states_.front().running_task_id = worker_->current_task_id_;
  }

  uint32_t total_ready_count() const override {
    return worker_states_.front().local_queue_size;
  }
  uint32_t total_worker_count() const override {
    return 1;
  }
  uint32_t node_count() const override {
    return 1;
  }
  uint64_t switch_cost() const override {
    return 0;
  }
  uint64_t migration_cost(int from_worker, int to_worker) const override {
    return from_worker == to_worker ? 0 : 0;
  }
  std::span<const WorkerState> worker_states() const override {
    return worker_states_;
  }
  std::span<const int> workers_in_node(int node) const override {
    if (node != 0) {
      return {};
    }
    return std::span<const int>(worker_ids_.data(), worker_ids_.size());
  }
  const QueueView& queue(int worker_id) const override {
    static const QueueView kEmpty;
    return worker_id == worker_states_.front().worker_id ? queue_view_ : kEmpty;
  }
  TaskViewRef running_task(int worker_id) const override {
    if (worker_id != worker_states_.front().worker_id || !worker_->current_task_id_.has_value()) {
      return {};
    }
    return worker_->tasks_.at(*worker_->current_task_id_)->task.view_ref();
  }
  TaskViewRef task(uint64_t task_id) const override {
    const auto it = worker_->tasks_.find(task_id);
    if (it == worker_->tasks_.end()) {
      return {};
    }
    return it->second->task.view_ref();
  }

private:
  static std::size_t queue_size(const void* context) noexcept {
    const auto* view = static_cast<const WorkerSystemView*>(context);
    return view->worker_->local_ready_queue_.size();
  }

  static const TaskView& queue_get(const void* context, std::size_t index) noexcept {
    const auto* view = static_cast<const WorkerSystemView*>(context);
    const uint64_t task_id = view->worker_->local_ready_queue_[index];
    return view->worker_->tasks_.at(task_id)->task.view_ref();
  }

  const Worker* worker_ = nullptr;
  std::vector<int> worker_ids_;
  std::vector<SystemView::WorkerState> worker_states_;
  QueueView queue_view_;
};

class WorkerPoolSystemView final : public SystemView {
public:
  explicit WorkerPoolSystemView(const WorkerPool& pool) : pool_(&pool) {
    worker_ids_.reserve(pool.worker_states_.size());
    worker_states_.reserve(pool.worker_states_.size());
    queue_contexts_.resize(pool.worker_states_.size());
    queue_views_.reserve(pool.worker_states_.size());

    for (std::size_t index = 0; index < pool.worker_states_.size(); ++index) {
      const auto& state = *pool.worker_states_[index];
      worker_ids_.push_back(state.worker_id);
      worker_states_.push_back(SystemView::WorkerState{
          .worker_id = state.worker_id,
          .topology_node = 0,
          .local_queue_size = static_cast<uint32_t>(state.local_ready_queue.size()),
          .is_idle = !state.current_task_id.has_value(),
          .running_task_id = state.current_task_id,
      });
      queue_contexts_[index] = QueueContext{
          .view = this,
          .worker_index = index,
      };
      queue_views_.push_back(QueueView(&queue_contexts_[index], &WorkerPoolSystemView::queue_size,
                                       &WorkerPoolSystemView::queue_get));
    }
  }

  void invalidate() noexcept {
    for (std::size_t index = 0; index < pool_->worker_states_.size(); ++index) {
      const auto& state = *pool_->worker_states_[index];
      worker_states_[index].local_queue_size =
          static_cast<uint32_t>(state.local_ready_queue.size());
      worker_states_[index].is_idle = !state.current_task_id.has_value();
      worker_states_[index].running_task_id = state.current_task_id;
    }
  }

  uint32_t total_ready_count() const override {
    uint32_t total = 0;
    for (const auto& worker : worker_states_) {
      total += worker.local_queue_size;
    }
    return total;
  }

  uint32_t total_worker_count() const override {
    return static_cast<uint32_t>(worker_states_.size());
  }

  uint32_t node_count() const override {
    return 1;
  }
  uint64_t switch_cost() const override {
    return 0;
  }
  uint64_t migration_cost(int from_worker, int to_worker) const override {
    return from_worker == to_worker ? 0 : 0;
  }
  std::span<const WorkerState> worker_states() const override {
    return worker_states_;
  }
  std::span<const int> workers_in_node(int node) const override {
    if (node != 0) {
      return {};
    }
    return std::span<const int>(worker_ids_.data(), worker_ids_.size());
  }

  const QueueView& queue(int worker_id) const override {
    static const QueueView kEmpty;
    if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= queue_views_.size()) {
      return kEmpty;
    }
    return queue_views_[static_cast<std::size_t>(worker_id)];
  }

  TaskViewRef running_task(int worker_id) const override {
    if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= pool_->worker_states_.size()) {
      return {};
    }
    const auto& state = *pool_->worker_states_[static_cast<std::size_t>(worker_id)];
    if (!state.current_task_id.has_value()) {
      return {};
    }
    return pool_->tasks_.at(*state.current_task_id)->task.view_ref();
  }

  TaskViewRef task(uint64_t task_id) const override {
    const auto it = pool_->tasks_.find(task_id);
    if (it == pool_->tasks_.end()) {
      return {};
    }
    return it->second->task.view_ref();
  }

private:
  struct QueueContext {
    const WorkerPoolSystemView* view = nullptr;
    std::size_t worker_index = 0;
  };

  static std::size_t queue_size(const void* context) noexcept {
    const auto* queue = static_cast<const QueueContext*>(context);
    return queue->view->pool_->worker_states_[queue->worker_index]->local_ready_queue.size();
  }

  static const TaskView& queue_get(const void* context, std::size_t index) noexcept {
    const auto* queue = static_cast<const QueueContext*>(context);
    const auto& state = *queue->view->pool_->worker_states_[queue->worker_index];
    const uint64_t task_id = state.local_ready_queue[index];
    return queue->view->pool_->tasks_.at(task_id)->task.view_ref();
  }

  const WorkerPool* pool_ = nullptr;
  std::vector<int> worker_ids_;
  std::vector<SystemView::WorkerState> worker_states_;
  std::vector<QueueContext> queue_contexts_;
  std::vector<QueueView> queue_views_;
};

Worker::ExecutionContext::ExecutionContext(Worker& worker) noexcept : worker_(&worker) {}

void Worker::ExecutionContext::advance_runtime_time(uint64_t runtime_us) noexcept {
  worker_->timer_queue_.advance_by(runtime_us);
  worker_->devices_.advance_by(runtime_us);
  worker_->tick_source_.advance_by(runtime_us);
  worker_->drain_due_events();
}

void Worker::ExecutionContext::record_compute_service(uint64_t elapsed_us) noexcept {
  if (!worker_->current_task_id_.has_value()) {
    return;
  }
  const TaskRecord& record = worker_->lookup_task_or_fail(*worker_->current_task_id_);
  const uint64_t end_time_us = steady_now_us();
  const uint64_t start_time_us = (end_time_us > elapsed_us) ? (end_time_us - elapsed_us) : 0;
  worker_->record_service_slice(start_time_us, end_time_us, record.task.view().group_id);
}

int Worker::ExecutionContext::current_worker_id() const noexcept {
  return worker_->worker_id_;
}

bool Worker::ExecutionContext::schedule_sleep(uint64_t task_token, uint64_t delay_us) noexcept {
  return worker_->timer_queue_.schedule_sleep(task_token, delay_us);
}

bool Worker::ExecutionContext::submit_device_call(uint64_t task_token, DeviceId device,
                                                  const DeviceRequest& request) noexcept {
  return worker_->devices_.submit(task_token, device, request);
}

void Worker::ExecutionContext::configure_device_rate(DeviceId device, uint64_t rate) noexcept {
  worker_->devices_.configure_device_rate(device, rate);
}

void Worker::ExecutionContext::configure_device_parallelism(DeviceId device,
                                                            uint64_t parallel) noexcept {
  worker_->devices_.configure_device_parallelism(device, parallel);
}

bool Worker::ExecutionContext::declare_event(std::string_view name) noexcept {
  return worker_->sync_registry_.declare_event(name);
}

bool Worker::ExecutionContext::declare_latch(std::string_view name, uint64_t count) noexcept {
  return worker_->sync_registry_.declare_latch(name, count);
}

SyncWaitResult Worker::ExecutionContext::wait_sync(RuntimeTask& task,
                                                   std::string_view name) noexcept {
  return worker_->sync_registry_.wait(name, task.view().task_id,
                                      [&]() { return task.on_block(worker_->worker_id_); });
}

SyncActionResult Worker::ExecutionContext::signal_event(std::string_view name) noexcept {
  return worker_->sync_registry_.signal_event(
      name, [&](uint64_t task_id) { worker_->wake_sync_task(task_id); });
}

SyncActionResult Worker::ExecutionContext::arrive_latch(std::string_view name) noexcept {
  return worker_->sync_registry_.arrive_latch(
      name, [&](uint64_t task_id) { worker_->wake_sync_task(task_id); });
}

Worker::RescheduleSource::RescheduleSource(Worker& worker) noexcept : worker_(&worker) {}

bool Worker::RescheduleSource::consume_reschedule_request() noexcept {
  return worker_->consume_tick_reschedule_request();
}

Worker::TaskRecord::TaskRecord(uint64_t task_id, TaskMain main_fn, void* raw_arg, TaskAttrs attrs,
                               RescheduleSource& reschedule_source,
                               ExecutionContext& execution_context, uint64_t compute_chunk_units)
    : task(task_id, attrs), main(main_fn), arg(raw_arg),
      workload(task, reschedule_source, execution_context, compute_chunk_units), frame{},
      context(&Worker::task_entry, &frame), pending_wakeup_steady_us(std::nullopt),
      pending_sync_wakeup(false), pending_sync_source_worker(-1) {}

Worker::Worker(Scheduler& scheduler, int worker_id) : Worker(scheduler, worker_id, Config{}) {}

Worker::Worker(Scheduler& scheduler, int worker_id, Config config)
    : scheduler_(&scheduler), config_(config), worker_id_(worker_id), next_task_id_(1),
      current_task_id_(std::nullopt), framework_{}, timer_queue_{}, devices_{},
      reschedule_source_(*this), tick_source_(config_.tick_interval_us), execution_context_(*this),
      tasks_{} {
  if (worker_id_ < 0) {
    detail::hard_fail("Worker: worker_id must be >= 0");
  }
  if (config_.compute_chunk_units == 0) {
    detail::hard_fail("Worker: compute_chunk_units must be > 0");
  }
}

Worker::~Worker() = default;

SystemView& Worker::system_view() const {
  if (!system_view_) {
    system_view_ = std::make_unique<WorkerSystemView>(*this);
  }
  system_view_->invalidate();
  return *system_view_;
}

WorkerPool::TaskRescheduleSource::TaskRescheduleSource(WorkerPool& pool) noexcept : pool_(&pool) {}

void WorkerPool::TaskRescheduleSource::bind_task_id(uint64_t task_id) noexcept {
  task_id_.store(task_id, std::memory_order_release);
}

bool WorkerPool::TaskRescheduleSource::consume_reschedule_request() noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  if (task_id == 0) {
    return false;
  }
  return pool_->consume_tick_reschedule_request(task_id);
}

WorkerPool::TaskExecutionContext::TaskExecutionContext(WorkerPool& pool) noexcept : pool_(&pool) {}

void WorkerPool::TaskExecutionContext::bind_task_id(uint64_t task_id) noexcept {
  task_id_.store(task_id, std::memory_order_release);
}

void WorkerPool::TaskExecutionContext::advance_runtime_time(uint64_t runtime_us) noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  WorkerState& state = pool_->state_for_task_or_fail(task_id);
  state.timer_queue.advance_by(runtime_us);
  state.devices.advance_by(runtime_us);
  state.tick_source.advance_by(runtime_us);
  pool_->drain_due_events(state);
  pool_->refresh_pending_events(state);
}

void WorkerPool::TaskExecutionContext::record_compute_service(uint64_t elapsed_us) noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  const TaskRecord& record = pool_->lookup_task_or_fail(task_id);
  const uint64_t end_time_us = steady_now_us();
  const uint64_t start_time_us = (end_time_us > elapsed_us) ? (end_time_us - elapsed_us) : 0;
  pool_->record_service_slice(start_time_us, end_time_us, record.task.view().group_id);
}

int WorkerPool::TaskExecutionContext::current_worker_id() const noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  return pool_->state_for_task_or_fail(task_id).worker_id;
}

bool WorkerPool::TaskExecutionContext::schedule_sleep(uint64_t task_token,
                                                      uint64_t delay_us) noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  WorkerState& state = pool_->state_for_task_or_fail(task_id);
  const bool scheduled = state.timer_queue.schedule_sleep(task_token, delay_us);
  pool_->refresh_pending_events(state);
  return scheduled;
}

bool WorkerPool::TaskExecutionContext::submit_device_call(uint64_t task_token, DeviceId device,
                                                          const DeviceRequest& request) noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  WorkerState& state = pool_->state_for_task_or_fail(task_id);
  const bool submitted = state.devices.submit(task_token, device, request);
  pool_->refresh_pending_events(state);
  return submitted;
}

void WorkerPool::TaskExecutionContext::configure_device_rate(DeviceId device,
                                                             uint64_t rate) noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  WorkerState& state = pool_->state_for_task_or_fail(task_id);
  state.devices.configure_device_rate(device, rate);
}

void WorkerPool::TaskExecutionContext::configure_device_parallelism(DeviceId device,
                                                                    uint64_t parallel) noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  WorkerState& state = pool_->state_for_task_or_fail(task_id);
  state.devices.configure_device_parallelism(device, parallel);
}

bool WorkerPool::TaskExecutionContext::declare_event(std::string_view name) noexcept {
  return pool_->sync_registry_.declare_event(name);
}

bool WorkerPool::TaskExecutionContext::declare_latch(std::string_view name,
                                                     uint64_t count) noexcept {
  return pool_->sync_registry_.declare_latch(name, count);
}

SyncWaitResult WorkerPool::TaskExecutionContext::wait_sync(RuntimeTask& task,
                                                           std::string_view name) noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  WorkerState& state = pool_->state_for_task_or_fail(task_id);
  return pool_->sync_registry_.wait(name, task.view().task_id,
                                    [&]() { return task.on_block(state.worker_id); });
}

SyncActionResult WorkerPool::TaskExecutionContext::signal_event(std::string_view name) noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  const int source_worker_id = pool_->state_for_task_or_fail(task_id).worker_id;
  return pool_->sync_registry_.signal_event(name, [&](uint64_t waiting_task_id) {
    pool_->wake_sync_task(waiting_task_id, source_worker_id);
  });
}

SyncActionResult WorkerPool::TaskExecutionContext::arrive_latch(std::string_view name) noexcept {
  const uint64_t task_id = task_id_.load(std::memory_order_acquire);
  const int source_worker_id = pool_->state_for_task_or_fail(task_id).worker_id;
  return pool_->sync_registry_.arrive_latch(name, [&](uint64_t waiting_task_id) {
    pool_->wake_sync_task(waiting_task_id, source_worker_id);
  });
}

WorkerPool::TaskRecord::TaskRecord(uint64_t task_id, TaskMain main_fn, void* raw_arg,
                                   TaskAttrs attrs, WorkerPool& pool, uint64_t compute_chunk_units)
    : task(task_id, attrs), main(main_fn), arg(raw_arg), reschedule_source(pool),
      execution_context(pool),
      workload(task, reschedule_source, execution_context, compute_chunk_units), frame{},
      context(&WorkerPool::task_entry, &frame) {
  frame.record = this;
  reschedule_source.bind_task_id(task_id);
  execution_context.bind_task_id(task_id);
}

WorkerPool::WorkerPool(Scheduler& scheduler, int worker_count, Worker::Config config)
    : scheduler_(&scheduler), config_(config), worker_states_(), worker_threads_(), tasks_(),
      next_task_id_(1), total_tasks_(0), exited_tasks_(0), progress_epoch_(0), started_(false),
      stop_requested_(false), task_mu_() {
  if (worker_count <= 0) {
    detail::hard_fail("WorkerPool: worker_count must be > 0");
  }
  if (config_.compute_chunk_units == 0) {
    detail::hard_fail("WorkerPool: compute_chunk_units must be > 0");
  }
  worker_states_.reserve(static_cast<std::size_t>(worker_count));
  for (int i = 0; i < worker_count; ++i) {
    auto state = std::make_unique<WorkerState>();
    state->worker_id = i;
    state->tick_source = TickSource(config_.tick_interval_us);
    worker_states_.push_back(std::move(state));
  }
}

WorkerPool::~WorkerPool() {
  stop_requested_.store(true, std::memory_order_release);
  join();
}

SystemView& WorkerPool::system_view() const {
  if (!system_view_) {
    system_view_ = std::make_unique<WorkerPoolSystemView>(*this);
  }
  system_view_->invalidate();
  return *system_view_;
}

void WorkerPool::initialize_scheduler_if_needed() {
  std::lock_guard<std::mutex> lock(task_mu_);
  if (scheduler_initialized_) {
    return;
  }
  ScopedActivityCounter scheduler_activity(scheduler_calls_in_flight_);
  scheduler_->init(system_view());
  scheduler_initialized_ = true;
}

void WorkerPool::enqueue_task_ready(uint64_t task_id, ReadyContext ctx) {
  initialize_scheduler_if_needed();
  std::lock_guard<std::mutex> lock(task_mu_);
  TaskRecord& record = *tasks_.at(task_id);
  ctx.previous_worker_id = record.task.view().last_worker_id;
  ScopedActivityCounter scheduler_activity(scheduler_calls_in_flight_);
  const int target_worker = scheduler_->select_worker(record.task.view(), ctx, system_view());
  if (target_worker < 0 || static_cast<std::size_t>(target_worker) >= worker_states_.size()) {
    detail::hard_fail("WorkerPool::enqueue_task_ready: scheduler selected invalid worker");
  }
  WorkerState& target = *worker_states_[static_cast<std::size_t>(target_worker)];
  if (std::find(target.local_ready_queue.begin(), target.local_ready_queue.end(), task_id) !=
      target.local_ready_queue.end()) {
    detail::hard_fail("WorkerPool::enqueue_task_ready: task already queued");
  }
  target.local_ready_queue.push_back(task_id);
  progress_epoch_.fetch_add(1, std::memory_order_release);

  if (target.current_task_id.has_value()) {
    const TaskRecord& current = *tasks_.at(*target.current_task_id);
    if (scheduler_->should_preempt(record.task.view(), current.task.view(), target.worker_id,
                                   system_view())) {
      target.pending_external_resched.store(true, std::memory_order_release);
    }
  }
}

void WorkerPool::requeue_local_task(WorkerState& state, uint64_t task_id) {
  std::lock_guard<std::mutex> lock(task_mu_);
  if (std::find(state.local_ready_queue.begin(), state.local_ready_queue.end(), task_id) !=
      state.local_ready_queue.end()) {
    detail::hard_fail("WorkerPool::requeue_local_task: task already queued");
  }
  state.local_ready_queue.push_back(task_id);
  progress_epoch_.fetch_add(1, std::memory_order_release);
}

std::optional<uint64_t> WorkerPool::remove_local_ready_task(WorkerState& state, uint64_t task_id) {
  std::lock_guard<std::mutex> lock(task_mu_);
  if (state.local_ready_queue.empty()) {
    return std::nullopt;
  }
  if (state.local_ready_queue.front() == task_id) {
    state.local_ready_queue.pop_front();
    return task_id;
  }
  if (state.local_ready_queue.back() == task_id) {
    state.local_ready_queue.pop_back();
    return task_id;
  }
  const auto it =
      std::find(state.local_ready_queue.begin(), state.local_ready_queue.end(), task_id);
  if (it == state.local_ready_queue.end()) {
    return std::nullopt;
  }
  state.local_ready_queue.erase(it);
  return task_id;
}

std::optional<uint64_t> WorkerPool::remove_default_steal_task(int victim_worker_id) {
  std::lock_guard<std::mutex> lock(task_mu_);
  if (victim_worker_id < 0 || static_cast<std::size_t>(victim_worker_id) >= worker_states_.size()) {
    return std::nullopt;
  }
  WorkerState& victim = *worker_states_[static_cast<std::size_t>(victim_worker_id)];
  if (victim.local_ready_queue.empty()) {
    return std::nullopt;
  }
  const uint64_t task_id = victim.local_ready_queue.back();
  victim.local_ready_queue.pop_back();
  return task_id;
}

std::optional<uint64_t> WorkerPool::remove_specific_steal_task(int victim_worker_id,
                                                               uint64_t task_id) {
  if (victim_worker_id < 0 || static_cast<std::size_t>(victim_worker_id) >= worker_states_.size()) {
    return std::nullopt;
  }
  return remove_local_ready_task(*worker_states_[static_cast<std::size_t>(victim_worker_id)],
                                 task_id);
}

std::optional<uint64_t> WorkerPool::select_next_task(WorkerState& state) {
  initialize_scheduler_if_needed();
  std::lock_guard<std::mutex> lock(task_mu_);
  ScopedActivityCounter scheduler_activity(scheduler_calls_in_flight_);
  auto& system = system_view();
  std::optional<uint64_t> next =
      scheduler_->pick_next(state.worker_id, system.queue(state.worker_id), system);
  if (next.has_value()) {
    const auto it =
        std::find(state.local_ready_queue.begin(), state.local_ready_queue.end(), *next);
    if (it == state.local_ready_queue.end()) {
      detail::hard_fail(
          "WorkerPool::select_next_task: scheduler selected a task not in the local queue");
    }
    state.local_ready_queue.erase(it);
    return next;
  }

  const std::optional<Scheduler::StealResult> steal = scheduler_->steal(state.worker_id, system);
  if (!steal.has_value()) {
    return std::nullopt;
  }
  if (steal->victim_worker_id < 0 ||
      static_cast<std::size_t>(steal->victim_worker_id) >= worker_states_.size()) {
    return std::nullopt;
  }
  WorkerState& victim = *worker_states_[static_cast<std::size_t>(steal->victim_worker_id)];
  if (steal->task_id.has_value()) {
    const auto it = std::find(victim.local_ready_queue.begin(), victim.local_ready_queue.end(),
                              *steal->task_id);
    if (it == victim.local_ready_queue.end()) {
      detail::hard_fail("WorkerPool::select_next_task: invalid explicit steal target");
    }
    const uint64_t task_id = *it;
    victim.local_ready_queue.erase(it);
    return task_id;
  }
  if (victim.local_ready_queue.empty()) {
    return std::nullopt;
  }
  const uint64_t task_id = victim.local_ready_queue.back();
  victim.local_ready_queue.pop_back();
  return task_id;
}

uint64_t WorkerPool::spawn(TaskMain main, void* arg, TaskAttrs attrs) {
  if (main == nullptr) {
    detail::hard_fail("WorkerPool::spawn: task entry must not be null");
  }
  if (started_.load(std::memory_order_acquire)) {
    detail::hard_fail("WorkerPool::spawn: spawn after start is not supported");
  }

  const uint64_t task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
  auto record =
      std::make_unique<TaskRecord>(task_id, main, arg, attrs, *this, config_.compute_chunk_units);
  record->release_time_us = steady_now_us();
  if (!record->task.on_spawn_ready()) {
    detail::hard_fail("WorkerPool::spawn: failed to transition task to ready");
  }

  {
    const std::lock_guard<std::mutex> lock(task_mu_);
    tasks_.emplace(task_id, std::move(record));
  }
  {
    const std::lock_guard<std::mutex> lock(task_mu_);
    const TaskRecord& inserted = *tasks_.at(task_id);
    record_runnable_delta(inserted.release_time_us, inserted.task.view().group_id, +1);
  }
  total_tasks_.fetch_add(1, std::memory_order_release);
  progress_epoch_.fetch_add(1, std::memory_order_release);
  enqueue_task_ready(task_id, ReadyContext{
                                  .reason = ReadyReason::Spawn,
                                  .source_worker_id = -1,
                                  .previous_worker_id = -1,
                                  .ready_time_us = 0,
                              });
  return task_id;
}

void WorkerPool::start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    detail::hard_fail("WorkerPool::start: already started");
  }
  initialize_scheduler_if_needed();
  stop_requested_.store(false, std::memory_order_release);
  worker_threads_.reserve(worker_states_.size());
  for (std::size_t i = 0; i < worker_states_.size(); ++i) {
    worker_threads_.emplace_back([this, i]() { run_worker_loop(static_cast<int>(i)); });
  }
}

void WorkerPool::join() {
  for (std::thread& worker : worker_threads_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  worker_threads_.clear();
}

void WorkerPool::run() {
  start();
  join();
}

WorkerPool::ObservedMetrics WorkerPool::observed_metrics() const {
  ObservedMetrics observed;
  for (const auto& state : worker_states_) {
    observed.worker_idle_time_us.emplace(state->worker_id, state->idle_time_us);
    observed.wakeup_latencies_us.insert(observed.wakeup_latencies_us.end(),
                                        state->wakeup_latencies_us.begin(),
                                        state->wakeup_latencies_us.end());
  }
  const std::lock_guard<std::mutex> lock(task_mu_);
  for (const auto& [task_id, record] : tasks_) {
    observed.task_metrics_by_id.emplace(task_id,
                                        Worker::ObservedTaskMetrics{
                                            .release_time_us = record->release_time_us,
                                            .completion_time_us = record->completion_time_us,
                                            .cpu_runtime_us = record->task.view().total_runtime_us,
                                        });
  }
  {
    const std::lock_guard<std::mutex> observed_lock(observed_mu_);
    observed.runnable_events = runnable_events_;
    observed.service_slices = service_slices_;
  }
  return observed;
}

void WorkerPool::record_runnable_delta(uint64_t time_us, uint64_t group_id, int delta) {
  const std::lock_guard<std::mutex> lock(observed_mu_);
  runnable_events_.push_back(Worker::ObservedRunnableEvent{
      .time_us = time_us,
      .group_id = group_id,
      .delta = delta,
  });
}

void WorkerPool::record_service_slice(uint64_t start_time_us, uint64_t end_time_us,
                                      uint64_t group_id) {
  if (end_time_us <= start_time_us) {
    return;
  }
  const std::lock_guard<std::mutex> lock(observed_mu_);
  service_slices_.push_back(Worker::ObservedServiceSlice{
      .start_time_us = start_time_us,
      .end_time_us = end_time_us,
      .group_id = group_id,
  });
}

void WorkerPool::task_entry(void* raw) {
  auto* frame = static_cast<TaskRecord::EntryFrame*>(raw);
  if (frame == nullptr || frame->record == nullptr) {
    detail::hard_fail("WorkerPool::task_entry: invalid task frame");
  }
  frame->record->main(frame->record->workload, frame->record->arg);
}

WorkerPool::TaskRecord& WorkerPool::lookup_task_or_fail(uint64_t task_id) {
  const std::lock_guard<std::mutex> lock(task_mu_);
  const auto it = tasks_.find(task_id);
  if (it == tasks_.end()) {
    detail::hard_fail("WorkerPool: unknown task id");
  }
  return *it->second;
}

const WorkerPool::TaskRecord& WorkerPool::lookup_task_or_fail(uint64_t task_id) const {
  const std::lock_guard<std::mutex> lock(task_mu_);
  const auto it = tasks_.find(task_id);
  if (it == tasks_.end()) {
    detail::hard_fail("WorkerPool: unknown task id");
  }
  return *it->second;
}

WorkerPool::WorkerState& WorkerPool::state_for_task_or_fail(uint64_t task_id) {
  TaskRecord& record = lookup_task_or_fail(task_id);
  const int worker_id = record.bound_worker_id.load(std::memory_order_acquire);
  if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= worker_states_.size()) {
    detail::hard_fail("WorkerPool: task is not bound to a valid worker");
  }
  return *worker_states_[static_cast<std::size_t>(worker_id)];
}

const WorkerPool::WorkerState& WorkerPool::state_for_task_or_fail(uint64_t task_id) const {
  const TaskRecord& record = lookup_task_or_fail(task_id);
  const int worker_id = record.bound_worker_id.load(std::memory_order_acquire);
  if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= worker_states_.size()) {
    detail::hard_fail("WorkerPool: task is not bound to a valid worker");
  }
  return *worker_states_[static_cast<std::size_t>(worker_id)];
}

bool WorkerPool::all_tasks_exited() const {
  return exited_tasks_.load(std::memory_order_acquire) ==
         total_tasks_.load(std::memory_order_acquire);
}

bool WorkerPool::any_tasks_actively_running() const {
  const std::lock_guard<std::mutex> lock(task_mu_);
  for (const auto& [task_id, record] : tasks_) {
    (void)task_id;
    if (record->actively_running.load(std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

bool WorkerPool::any_pending_events() const {
  for (const auto& state : worker_states_) {
    if (state->has_pending_events.load(std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

bool WorkerPool::dispatch_one(WorkerState& state, uint64_t task_id) {
  ScopedActivityCounter dispatch_activity(dispatches_in_flight_);
  TaskRecord& record = lookup_task_or_fail(task_id);
  const uint64_t wakeup_us = record.pending_wakeup_steady_us.exchange(0, std::memory_order_acq_rel);
  if (wakeup_us != 0) {
    const uint64_t dispatch_us = steady_now_us();
    const uint64_t latency_us = (dispatch_us > wakeup_us) ? (dispatch_us - wakeup_us) : 0;
    state.wakeup_latencies_us.push_back((latency_us > 0) ? latency_us : 1);
  }
  bool expected_running = false;
  if (!record.actively_running.compare_exchange_strong(expected_running, true,
                                                       std::memory_order_acq_rel)) {
    detail::hard_fail("WorkerPool::dispatch_one: task is already running");
  }

  {
    const std::lock_guard<std::mutex> lock(task_mu_);
    record.bound_worker_id.store(state.worker_id, std::memory_order_release);
    state.current_task_id = task_id;
  }
  if (!record.task.on_dispatch(state.worker_id)) {
    {
      const std::lock_guard<std::mutex> lock(task_mu_);
      state.current_task_id.reset();
      record.bound_worker_id.store(-1, std::memory_order_release);
      record.actively_running.store(false, std::memory_order_release);
    }
    detail::hard_fail("WorkerPool::dispatch_one: task dispatch transition failed");
  }
  progress_epoch_.fetch_add(1, std::memory_order_release);

  record.context.resume(state.framework);

  {
    const std::lock_guard<std::mutex> lock(task_mu_);
    state.current_task_id.reset();
  }

  if (record.context.finished()) {
    record.completion_time_us = steady_now_us();
    record_runnable_delta(record.completion_time_us, record.task.view().group_id, -1);
    if (!record.task.on_exit(state.worker_id)) {
      detail::hard_fail("WorkerPool::dispatch_one: task exit transition failed");
    }
    {
      const std::lock_guard<std::mutex> lock(task_mu_);
      record.bound_worker_id.store(-1, std::memory_order_release);
      record.actively_running.store(false, std::memory_order_release);
      ScopedActivityCounter scheduler_activity(scheduler_calls_in_flight_);
      scheduler_->on_task_exited(record.task.view(), state.worker_id);
    }
    exited_tasks_.fetch_add(1, std::memory_order_release);
    progress_epoch_.fetch_add(1, std::memory_order_release);
    return true;
  }

  const TaskState task_state = record.task.state();
  if (task_state == TaskState::Blocked) {
    const bool pending_sync_wakeup =
        record.pending_sync_wakeup.exchange(false, std::memory_order_acq_rel);
    if (pending_sync_wakeup) {
      const uint64_t now_us = steady_now_us();
      record_runnable_delta(now_us, record.task.view().group_id, -1);
      if (!record.task.on_wakeup_ready()) {
        detail::hard_fail("WorkerPool::dispatch_one: deferred sync wakeup transition failed");
      }
      record.pending_wakeup_steady_us.store(now_us, std::memory_order_release);
      const int source_worker_id =
          record.pending_sync_source_worker.exchange(-1, std::memory_order_acq_rel);
      {
        const std::lock_guard<std::mutex> lock(task_mu_);
        record.bound_worker_id.store(-1, std::memory_order_release);
        record.actively_running.store(false, std::memory_order_release);
      }
      record_runnable_delta(now_us, record.task.view().group_id, +1);
      enqueue_task_ready(task_id, ReadyContext{
                                      .reason = ReadyReason::Wakeup,
                                      .source_worker_id = source_worker_id,
                                      .previous_worker_id = record.task.view().last_worker_id,
                                      .ready_time_us = now_us,
                                  });
      progress_epoch_.fetch_add(1, std::memory_order_release);
      return true;
    }
    {
      const std::lock_guard<std::mutex> lock(task_mu_);
      record.bound_worker_id.store(-1, std::memory_order_release);
      record.actively_running.store(false, std::memory_order_release);
      record.blocked_since_steady_us.store(steady_now_us(), std::memory_order_release);
      ScopedActivityCounter scheduler_activity(scheduler_calls_in_flight_);
      scheduler_->on_task_blocked(record.task.view(), state.worker_id);
    }
    record_runnable_delta(record.blocked_since_steady_us.load(std::memory_order_acquire),
                          record.task.view().group_id, -1);
    progress_epoch_.fetch_add(1, std::memory_order_release);
    return true;
  }
  if (task_state == TaskState::Ready) {
    {
      const std::lock_guard<std::mutex> lock(task_mu_);
      record.bound_worker_id.store(-1, std::memory_order_release);
      record.actively_running.store(false, std::memory_order_release);
    }
    requeue_local_task(state, task_id);
    {
      const std::lock_guard<std::mutex> lock(task_mu_);
      ScopedActivityCounter scheduler_activity(scheduler_calls_in_flight_);
      scheduler_->on_task_preempted(record.task.view(), state.worker_id);
    }
    progress_epoch_.fetch_add(1, std::memory_order_release);
    return true;
  }

  {
    const std::lock_guard<std::mutex> lock(task_mu_);
    record.bound_worker_id.store(-1, std::memory_order_release);
    record.actively_running.store(false, std::memory_order_release);
  }
  detail::hard_fail("WorkerPool::dispatch_one: unexpected task state after yield");
}

bool WorkerPool::consume_tick_reschedule_request(uint64_t task_id) {
  std::lock_guard<std::mutex> lock(task_mu_);
  TaskRecord& record = *tasks_.at(task_id);
  const int worker_id = record.bound_worker_id.load(std::memory_order_acquire);
  if (worker_id < 0 || static_cast<std::size_t>(worker_id) >= worker_states_.size()) {
    return false;
  }
  WorkerState& state = *worker_states_[static_cast<std::size_t>(worker_id)];
  bool should_resched = false;
  if (state.tick_source.consume_pending_tick()) {
    ScopedActivityCounter scheduler_activity(scheduler_calls_in_flight_);
    const TickAction action = scheduler_->on_tick(record.task.view(), worker_id, system_view());
    should_resched = action == TickAction::RequestResched;
  }
  if (state.pending_external_resched.exchange(false, std::memory_order_acq_rel)) {
    should_resched = true;
  }
  if (!should_resched) {
    return false;
  }
  if (!record.task.on_preempt(worker_id)) {
    detail::hard_fail("WorkerPool::consume_tick_reschedule_request: task preempt failed");
  }
  return true;
}

void WorkerPool::drain_due_events(WorkerState& state) {
  const uint64_t now_us = steady_now_us();
  const auto due_sleepers = state.timer_queue.pop_due();
  for (const uint64_t task_id : due_sleepers) {
    uint64_t group_id = 0;
    {
      const std::lock_guard<std::mutex> lock(task_mu_);
      TaskRecord& record = *tasks_.at(task_id);
      group_id = record.task.view().group_id;
      const uint64_t blocked_since =
          record.blocked_since_steady_us.exchange(0, std::memory_order_acq_rel);
      if (blocked_since != 0) {
        record.task.record_blocked_time(now_us - blocked_since);
      }
      if (!record.task.on_wakeup_ready()) {
        detail::hard_fail("WorkerPool::drain_due_events: invalid sleep wakeup transition");
      }
      record.pending_wakeup_steady_us.store(now_us, std::memory_order_release);
    }
    record_runnable_delta(now_us, group_id, +1);
    enqueue_task_ready(task_id, ReadyContext{
                                    .reason = ReadyReason::Wakeup,
                                    .source_worker_id = state.worker_id,
                                    .previous_worker_id = -1,
                                    .ready_time_us = now_us,
                                });
  }

  const auto completions = state.devices.pop_completed();
  for (const auto& completion : completions) {
    uint64_t group_id = 0;
    {
      const std::lock_guard<std::mutex> lock(task_mu_);
      TaskRecord& record = *tasks_.at(completion.task_token);
      group_id = record.task.view().group_id;
      const uint64_t blocked_since =
          record.blocked_since_steady_us.exchange(0, std::memory_order_acq_rel);
      if (blocked_since != 0) {
        record.task.record_blocked_time(now_us - blocked_since);
      }
      if (!record.task.on_device_completion(completion.result)) {
        detail::hard_fail("WorkerPool::drain_due_events: invalid device completion");
      }
      if (!record.task.on_wakeup_ready()) {
        detail::hard_fail("WorkerPool::drain_due_events: invalid device wakeup transition");
      }
      record.pending_wakeup_steady_us.store(now_us, std::memory_order_release);
    }
    record_runnable_delta(now_us, group_id, +1);
    enqueue_task_ready(completion.task_token, ReadyContext{
                                                  .reason = ReadyReason::Wakeup,
                                                  .source_worker_id = state.worker_id,
                                                  .previous_worker_id = -1,
                                                  .ready_time_us = now_us,
                                              });
  }

  refresh_pending_events(state);
}

void WorkerPool::refresh_pending_events(WorkerState& state) {
  const bool has_events = state.timer_queue.next_wake_at_us().has_value() ||
                          state.devices.next_completion_at_us().has_value();
  state.has_pending_events.store(has_events, std::memory_order_release);
}

bool WorkerPool::advance_idle_until_progress(WorkerState& state) {
  const std::optional<uint64_t> timer_wake_at = state.timer_queue.next_wake_at_us();
  const std::optional<uint64_t> device_complete_at = state.devices.next_completion_at_us();

  std::optional<uint64_t> next_event_at;
  if (timer_wake_at.has_value() && device_complete_at.has_value()) {
    next_event_at = (*timer_wake_at < *device_complete_at) ? *timer_wake_at : *device_complete_at;
  } else if (timer_wake_at.has_value()) {
    next_event_at = *timer_wake_at;
  } else if (device_complete_at.has_value()) {
    next_event_at = *device_complete_at;
  }

  if (!next_event_at.has_value()) {
    state.has_pending_events.store(false, std::memory_order_release);
    return false;
  }

  const uint64_t current_time = state.timer_queue.now_us();
  const uint64_t delta = (*next_event_at > current_time) ? (*next_event_at - current_time) : 0;
  state.idle_time_us += delta;
  state.timer_queue.advance_by(delta);
  state.devices.advance_by(delta);
  drain_due_events(state);
  refresh_pending_events(state);
  return true;
}

void WorkerPool::wake_sync_task(uint64_t task_id, int source_worker_id) {
  bool enqueue = false;
  uint64_t ready_time_us = steady_now_us();
  uint64_t group_id = 0;
  {
    const std::lock_guard<std::mutex> lock(task_mu_);
    TaskRecord& record = *tasks_.at(task_id);
    group_id = record.task.view().group_id;
    if (record.actively_running.load(std::memory_order_acquire)) {
      record.pending_sync_source_worker.store(source_worker_id, std::memory_order_release);
      record.pending_sync_wakeup.store(true, std::memory_order_release);
      return;
    }
    const uint64_t blocked_since =
        record.blocked_since_steady_us.exchange(0, std::memory_order_acq_rel);
    if (blocked_since != 0) {
      record.task.record_blocked_time(ready_time_us - blocked_since);
    }
    if (!record.task.on_wakeup_ready()) {
      detail::hard_fail("WorkerPool::wake_sync_task: invalid sync wakeup transition");
    }
    record.pending_wakeup_steady_us.store(ready_time_us, std::memory_order_release);
    enqueue = true;
  }
  if (enqueue) {
    record_runnable_delta(ready_time_us, group_id, +1);
    enqueue_task_ready(task_id, ReadyContext{
                                    .reason = ReadyReason::Wakeup,
                                    .source_worker_id = source_worker_id,
                                    .previous_worker_id = -1,
                                    .ready_time_us = ready_time_us,
                                });
  }
}

void WorkerPool::run_worker_loop(int worker_index) {
  WorkerState& state = *worker_states_[static_cast<std::size_t>(worker_index)];
  uint64_t last_progress_epoch = progress_epoch_.load(std::memory_order_acquire);
  uint64_t idle_stall_checks = 0;
  constexpr uint64_t kMaxIdleStallChecks = 256;

  while (!stop_requested_.load(std::memory_order_acquire)) {
    drain_due_events(state);

    const std::optional<uint64_t> next = select_next_task(state);
    if (next.has_value()) {
      idle_stall_checks = 0;
      last_progress_epoch = progress_epoch_.load(std::memory_order_acquire);
      dispatch_one(state, *next);
      continue;
    }

    if (all_tasks_exited()) {
      return;
    }

    if (advance_idle_until_progress(state)) {
      idle_stall_checks = 0;
      last_progress_epoch = progress_epoch_.load(std::memory_order_acquire);
      continue;
    }

    if (all_tasks_exited()) {
      return;
    }

    if (any_tasks_actively_running() || any_pending_events() ||
        scheduler_calls_in_flight_.load(std::memory_order_acquire) != 0 ||
        dispatches_in_flight_.load(std::memory_order_acquire) != 0) {
      idle_stall_checks = 0;
      last_progress_epoch = progress_epoch_.load(std::memory_order_acquire);
      std::this_thread::yield();
      continue;
    }

    const uint64_t progress_epoch = progress_epoch_.load(std::memory_order_acquire);
    if (progress_epoch != last_progress_epoch) {
      last_progress_epoch = progress_epoch;
      idle_stall_checks = 0;
      std::this_thread::yield();
      continue;
    }

    ++idle_stall_checks;
    if (idle_stall_checks < kMaxIdleStallChecks) {
      std::this_thread::yield();
      continue;
    }

    detail::hard_fail("WorkerPool::run_worker_loop: stalled with non-exited tasks");
  }
}

} // namespace schedlab::runtime

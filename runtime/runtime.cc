#include "runtime/runtime.h"

#include <algorithm>
#include <chrono>

namespace schedlab::runtime {

namespace {

uint64_t single_worker_now_us() noexcept {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch())
          .count());
}

} // namespace

void Worker::run() {
  initialize_scheduler_if_needed();
  run_loop();
}

void Worker::run_loop() {
  while (true) {
    drain_due_events();

    std::optional<uint64_t> next = select_next_task();
    if (next.has_value()) {
      dispatch_one(*next);
      continue;
    }

    if (all_tasks_exited()) {
      return;
    }

    if (!advance_idle_until_progress()) {
      detail::hard_fail("Worker::run_loop: stalled with non-exited tasks");
    }
  }
}

Worker::ObservedMetrics Worker::observed_metrics() const {
  ObservedMetrics observed{
      .wakeup_latencies_us = wakeup_latencies_us_,
      .worker_idle_time_us =
          {
              {worker_id_, idle_time_us_},
          },
      .runnable_events = runnable_events_,
      .service_slices = service_slices_,
  };
  for (const auto& [task_id, record] : tasks_) {
    observed.task_metrics_by_id.emplace(task_id,
                                        Worker::ObservedTaskMetrics{
                                            .release_time_us = record->release_time_us,
                                            .completion_time_us = record->completion_time_us,
                                            .cpu_runtime_us = record->task.view().total_runtime_us,
                                        });
  }
  return observed;
}

void Worker::record_runnable_delta(uint64_t time_us, uint64_t group_id, int delta) {
  runnable_events_.push_back(ObservedRunnableEvent{
      .time_us = time_us,
      .group_id = group_id,
      .delta = delta,
  });
}

void Worker::record_service_slice(uint64_t start_time_us, uint64_t end_time_us, uint64_t group_id) {
  if (end_time_us <= start_time_us) {
    return;
  }
  service_slices_.push_back(ObservedServiceSlice{
      .start_time_us = start_time_us,
      .end_time_us = end_time_us,
      .group_id = group_id,
  });
}

void Worker::initialize_scheduler_if_needed() {
  if (scheduler_initialized_) {
    return;
  }
  scheduler_->init(system_view());
  scheduler_initialized_ = true;
}

void Worker::enqueue_task_ready(uint64_t task_id, ReadyContext ctx) {
  initialize_scheduler_if_needed();
  TaskRecord& record = lookup_task_or_fail(task_id);
  ctx.previous_worker_id = record.task.view().last_worker_id;
  const int target_worker = scheduler_->select_worker(record.task.view(), ctx, system_view());
  if (target_worker != worker_id_) {
    detail::hard_fail("Worker::enqueue_task_ready: scheduler selected invalid worker");
  }
  if (std::find(local_ready_queue_.begin(), local_ready_queue_.end(), task_id) !=
      local_ready_queue_.end()) {
    detail::hard_fail("Worker::enqueue_task_ready: task already queued");
  }
  local_ready_queue_.push_back(task_id);
  if (current_task_id_.has_value()) {
    const TaskRecord& current = lookup_task_or_fail(*current_task_id_);
    if (scheduler_->should_preempt(record.task.view(), current.task.view(), worker_id_,
                                   system_view())) {
      pending_external_resched_ = true;
    }
  }
}

void Worker::requeue_local_task(uint64_t task_id) {
  if (std::find(local_ready_queue_.begin(), local_ready_queue_.end(), task_id) !=
      local_ready_queue_.end()) {
    detail::hard_fail("Worker::requeue_local_task: task already queued");
  }
  local_ready_queue_.push_back(task_id);
}

std::optional<uint64_t> Worker::remove_local_ready_task(uint64_t task_id) {
  if (local_ready_queue_.empty()) {
    return std::nullopt;
  }
  if (local_ready_queue_.front() == task_id) {
    local_ready_queue_.pop_front();
    return task_id;
  }
  if (local_ready_queue_.back() == task_id) {
    local_ready_queue_.pop_back();
    return task_id;
  }
  const auto it = std::find(local_ready_queue_.begin(), local_ready_queue_.end(), task_id);
  if (it == local_ready_queue_.end()) {
    return std::nullopt;
  }
  local_ready_queue_.erase(it);
  return task_id;
}

std::optional<uint64_t> Worker::remove_default_steal_task(int victim_worker_id) {
  if (victim_worker_id != worker_id_ || local_ready_queue_.empty()) {
    return std::nullopt;
  }
  const uint64_t task_id = local_ready_queue_.back();
  local_ready_queue_.pop_back();
  return task_id;
}

std::optional<uint64_t> Worker::remove_specific_steal_task(int victim_worker_id, uint64_t task_id) {
  if (victim_worker_id != worker_id_) {
    return std::nullopt;
  }
  return remove_local_ready_task(task_id);
}

std::optional<uint64_t> Worker::select_next_task() {
  initialize_scheduler_if_needed();
  auto& system = system_view();
  std::optional<uint64_t> next =
      scheduler_->pick_next(worker_id_, system.queue(worker_id_), system);
  if (next.has_value()) {
    const auto removed = remove_local_ready_task(*next);
    if (!removed.has_value()) {
      detail::hard_fail(
          "Worker::select_next_task: scheduler selected a task not in the local queue");
    }
    return removed;
  }

  const std::optional<Scheduler::StealResult> steal = scheduler_->steal(worker_id_, system);
  if (!steal.has_value()) {
    return std::nullopt;
  }
  if (steal->task_id.has_value()) {
    const auto removed = remove_specific_steal_task(steal->victim_worker_id, *steal->task_id);
    if (!removed.has_value()) {
      detail::hard_fail("Worker::select_next_task: invalid explicit steal target");
    }
    return removed;
  }
  return remove_default_steal_task(steal->victim_worker_id);
}

uint64_t Worker::spawn(TaskMain main, void* arg, TaskAttrs attrs) {
  if (main == nullptr) {
    detail::hard_fail("Worker::spawn: task entry must not be null");
  }
  const uint64_t task_id = next_task_id_++;
  auto record = std::make_unique<TaskRecord>(task_id, main, arg, attrs, reschedule_source_,
                                             execution_context_, config_.compute_chunk_units);
  record->release_time_us = single_worker_now_us();
  record->frame.worker = this;
  record->frame.record = record.get();

  if (!record->task.on_spawn_ready()) {
    detail::hard_fail("Worker::spawn: failed to transition task to ready");
  }
  tasks_.emplace(task_id, std::move(record));
  record_runnable_delta(tasks_.at(task_id)->release_time_us,
                        tasks_.at(task_id)->task.view().group_id, +1);
  enqueue_task_ready(task_id, ReadyContext{
                                  .reason = ReadyReason::Spawn,
                                  .source_worker_id = -1,
                                  .previous_worker_id = -1,
                                  .ready_time_us = 0,
                              });
  return task_id;
}

void Worker::task_entry(void* raw) {
  auto* frame = static_cast<TaskRecord::EntryFrame*>(raw);
  if (frame == nullptr || frame->worker == nullptr || frame->record == nullptr) {
    detail::hard_fail("Worker::task_entry: invalid task frame");
  }
  frame->record->main(frame->record->workload, frame->record->arg);
}

Worker::TaskRecord& Worker::lookup_task_or_fail(uint64_t task_id) {
  const auto it = tasks_.find(task_id);
  if (it == tasks_.end()) {
    detail::hard_fail("Worker: unknown task id");
  }
  return *it->second;
}

const Worker::TaskRecord& Worker::lookup_task_or_fail(uint64_t task_id) const {
  const auto it = tasks_.find(task_id);
  if (it == tasks_.end()) {
    detail::hard_fail("Worker: unknown task id");
  }
  return *it->second;
}

bool Worker::dispatch_one(uint64_t task_id) {
  TaskRecord& record = lookup_task_or_fail(task_id);
  if (record.pending_wakeup_steady_us.has_value()) {
    const uint64_t dispatch_us = single_worker_now_us();
    const uint64_t wakeup_us = *record.pending_wakeup_steady_us;
    const uint64_t latency_us = (dispatch_us > wakeup_us) ? (dispatch_us - wakeup_us) : 0;
    wakeup_latencies_us_.push_back((latency_us > 0) ? latency_us : 1);
    record.pending_wakeup_steady_us.reset();
  }
  if (!record.task.on_dispatch(worker_id_)) {
    detail::hard_fail("Worker::dispatch_one: task dispatch transition failed");
  }

  current_task_id_ = task_id;
  record.context.resume(framework_);
  current_task_id_.reset();

  if (record.context.finished()) {
    record.completion_time_us = single_worker_now_us();
    record_runnable_delta(record.completion_time_us, record.task.view().group_id, -1);
    if (!record.task.on_exit(worker_id_)) {
      detail::hard_fail("Worker::dispatch_one: task exit transition failed");
    }
    scheduler_->on_task_exited(record.task.view(), worker_id_);
    return true;
  }

  const TaskState state = record.task.state();
  if (state == TaskState::Blocked) {
    if (record.pending_sync_wakeup) {
      const uint64_t now_us = single_worker_now_us();
      record_runnable_delta(now_us, record.task.view().group_id, -1);
      if (!record.task.on_wakeup_ready()) {
        detail::hard_fail("Worker::dispatch_one: deferred sync wakeup transition failed");
      }
      record.pending_wakeup_steady_us = now_us;
      const int source_worker_id = record.pending_sync_source_worker;
      record.pending_sync_wakeup = false;
      record.pending_sync_source_worker = -1;
      record_runnable_delta(now_us, record.task.view().group_id, +1);
      enqueue_task_ready(task_id, ReadyContext{
                                      .reason = ReadyReason::Wakeup,
                                      .source_worker_id = source_worker_id,
                                      .previous_worker_id = record.task.view().last_worker_id,
                                      .ready_time_us = now_us,
                                  });
      return true;
    }
    record.blocked_since_steady_us = single_worker_now_us();
    record_runnable_delta(record.blocked_since_steady_us, record.task.view().group_id, -1);
    scheduler_->on_task_blocked(record.task.view(), worker_id_);
    return true;
  }
  if (state == TaskState::Ready) {
    requeue_local_task(task_id);
    scheduler_->on_task_preempted(record.task.view(), worker_id_);
    return true;
  }
  detail::hard_fail("Worker::dispatch_one: unexpected task state after yield");
}

bool Worker::consume_tick_reschedule_request() {
  if (!current_task_id_.has_value()) {
    pending_external_resched_ = false;
    return false;
  }
  bool should_resched = false;
  if (tick_source_.consume_pending_tick()) {
    TaskRecord& record = lookup_task_or_fail(*current_task_id_);
    const TickAction action = scheduler_->on_tick(record.task.view(), worker_id_, system_view());
    should_resched = action == TickAction::RequestResched;
  }
  if (pending_external_resched_) {
    pending_external_resched_ = false;
    should_resched = true;
  }
  if (!should_resched) {
    return false;
  }
  TaskRecord& record = lookup_task_or_fail(*current_task_id_);
  if (!record.task.on_preempt(worker_id_)) {
    detail::hard_fail("Worker::consume_tick_reschedule_request: task preempt transition failed");
  }
  return true;
}

bool Worker::all_tasks_exited() const {
  for (const auto& [task_id, record] : tasks_) {
    (void)task_id;
    if (record->task.state() != TaskState::Exited) {
      return false;
    }
  }
  return true;
}

void Worker::drain_due_events() {
  const uint64_t now_us = single_worker_now_us();
  const auto due_sleepers = timer_queue_.pop_due();
  for (const uint64_t task_id : due_sleepers) {
    TaskRecord& record = lookup_task_or_fail(task_id);
    if (record.blocked_since_steady_us != 0) {
      record.task.record_blocked_time(now_us - record.blocked_since_steady_us);
      record.blocked_since_steady_us = 0;
    }
    if (!record.task.on_wakeup_ready()) {
      detail::hard_fail("Worker::drain_due_events: invalid sleep wakeup transition");
    }
    record.pending_wakeup_steady_us = now_us;
    record_runnable_delta(now_us, record.task.view().group_id, +1);
    enqueue_task_ready(task_id, ReadyContext{
                                    .reason = ReadyReason::Wakeup,
                                    .source_worker_id = worker_id_,
                                    .previous_worker_id = record.task.view().last_worker_id,
                                    .ready_time_us = now_us,
                                });
  }

  const auto completions = devices_.pop_completed();
  for (const auto& completion : completions) {
    TaskRecord& record = lookup_task_or_fail(completion.task_token);
    if (record.blocked_since_steady_us != 0) {
      record.task.record_blocked_time(now_us - record.blocked_since_steady_us);
      record.blocked_since_steady_us = 0;
    }
    if (!record.task.on_device_completion(completion.result)) {
      detail::hard_fail("Worker::drain_due_events: invalid device completion");
    }
    if (!record.task.on_wakeup_ready()) {
      detail::hard_fail("Worker::drain_due_events: invalid device wakeup transition");
    }
    record.pending_wakeup_steady_us = now_us;
    record_runnable_delta(now_us, record.task.view().group_id, +1);
    enqueue_task_ready(completion.task_token,
                       ReadyContext{
                           .reason = ReadyReason::Wakeup,
                           .source_worker_id = worker_id_,
                           .previous_worker_id = record.task.view().last_worker_id,
                           .ready_time_us = now_us,
                       });
  }
}

bool Worker::advance_idle_until_progress() {
  const std::optional<uint64_t> timer_wake_at = timer_queue_.next_wake_at_us();
  const std::optional<uint64_t> device_complete_at = devices_.next_completion_at_us();

  std::optional<uint64_t> next_event_at;
  if (timer_wake_at.has_value() && device_complete_at.has_value()) {
    next_event_at = (*timer_wake_at < *device_complete_at) ? *timer_wake_at : *device_complete_at;
  } else if (timer_wake_at.has_value()) {
    next_event_at = *timer_wake_at;
  } else if (device_complete_at.has_value()) {
    next_event_at = *device_complete_at;
  }

  if (!next_event_at.has_value()) {
    return false;
  }

  const uint64_t current_time = timer_queue_.now_us();
  const uint64_t delta = (*next_event_at > current_time) ? (*next_event_at - current_time) : 0;
  idle_time_us_ += delta;
  timer_queue_.advance_by(delta);
  devices_.advance_by(delta);
  drain_due_events();
  return true;
}

void Worker::wake_sync_task(uint64_t task_id) {
  TaskRecord& record = lookup_task_or_fail(task_id);
  if (current_task_id_.has_value() && *current_task_id_ == task_id) {
    record.pending_sync_wakeup = true;
    record.pending_sync_source_worker = worker_id_;
    return;
  }
  const uint64_t now_us = single_worker_now_us();
  if (record.blocked_since_steady_us != 0) {
    record.task.record_blocked_time(now_us - record.blocked_since_steady_us);
    record.blocked_since_steady_us = 0;
  }
  if (!record.task.on_wakeup_ready()) {
    detail::hard_fail("Worker::wake_sync_task: invalid sync wakeup transition");
  }
  record.pending_wakeup_steady_us = now_us;
  record_runnable_delta(now_us, record.task.view().group_id, +1);
  enqueue_task_ready(task_id, ReadyContext{
                                  .reason = ReadyReason::Wakeup,
                                  .source_worker_id = worker_id_,
                                  .previous_worker_id = record.task.view().last_worker_id,
                                  .ready_time_us = now_us,
                              });
}

} // namespace schedlab::runtime

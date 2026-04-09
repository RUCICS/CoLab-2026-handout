#include "scheduler.h"

namespace student {

void Scheduler::init(const schedlab::SystemView& system) {
  (void)system;
}

int Scheduler::select_worker(const schedlab::TaskView&, schedlab::ReadyContext ctx,
                             const schedlab::SystemView& system) {
  // Keep wakeups on the worker that previously ran the task when possible.
  if (ctx.source_worker_id >= 0) {
    return ctx.source_worker_id;
  }
  if (system.total_worker_count() == 0) {
    return 0;
  }
  // Starter policy: place new work on worker 0.
  return 0;
}

std::optional<uint64_t> Scheduler::pick_next(int, schedlab::QueueView candidates,
                                             const schedlab::SystemView&) {
  if (candidates.empty()) {
    return std::nullopt;
  }
  // Starter policy: plain FIFO among local ready tasks.
  return candidates.front().task_id;
}

schedlab::TickAction Scheduler::on_tick(const schedlab::TaskView& current, int,
                                        const schedlab::SystemView&) {
  // Starter policy: fixed 20us time slice.
  if (current.current_slice_runtime_us >= 20) {
    return schedlab::TickAction::RequestResched;
  }
  return schedlab::TickAction::Continue;
}

bool Scheduler::should_preempt(const schedlab::TaskView&, const schedlab::TaskView&, int,
                               const schedlab::SystemView&) {
  return false;
}

std::optional<schedlab::Scheduler::StealResult> Scheduler::steal(int, const schedlab::SystemView&) {
  return std::nullopt;
}

void Scheduler::on_task_preempted(const schedlab::TaskView& task, int worker_id) {
  (void)task;
  (void)worker_id;
}

void Scheduler::on_task_blocked(const schedlab::TaskView& task, int worker_id) {
  (void)task;
  (void)worker_id;
}

void Scheduler::on_task_exited(const schedlab::TaskView& task, int worker_id) {
  (void)task;
  (void)worker_id;
}

} // namespace student

#include <cassert>
#include <cstdint>
#include <deque>
#include <string>
#include <vector>

#include "runtime/worker.hpp"

namespace {

class TickRecordingScheduler final : public schedlab::Scheduler {
public:
  int select_worker(const schedlab::TaskView& task, schedlab::ReadyContext ctx,
                    const schedlab::SystemView&) override {
    events_.push_back("ready:" + std::to_string(task.task_id) + ":" +
                      (ctx.reason == schedlab::ReadyReason::Spawn ? "spawn" : "wakeup"));
    return 0;
  }

  void on_task_preempted(const schedlab::TaskView& task, int) override {
    events_.push_back("preempted:" + std::to_string(task.task_id));
  }

  void on_task_blocked(const schedlab::TaskView&, int) override {}

  void on_task_exited(const schedlab::TaskView& task, int) override {
    events_.push_back("exited:" + std::to_string(task.task_id));
  }

  schedlab::TickAction on_tick(const schedlab::TaskView& current, int,
                               const schedlab::SystemView&) override {
    events_.push_back("tick:" + std::to_string(current.task_id) + ":" +
                      std::to_string(current.current_slice_runtime_us));
    if (!requested_preemption_) {
      requested_preemption_ = true;
      return schedlab::TickAction::RequestResched;
    }
    return schedlab::TickAction::Continue;
  }

  bool should_preempt(const schedlab::TaskView&, const schedlab::TaskView&, int,
                      const schedlab::SystemView&) override {
    return false;
  }

  std::optional<uint64_t> pick_next(int, schedlab::QueueView candidates,
                                    const schedlab::SystemView&) override {
    if (candidates.empty()) {
      return std::nullopt;
    }
    const uint64_t task_id = candidates.front().task_id;
    events_.push_back("pick:" + std::to_string(task_id));
    return task_id;
  }

  std::optional<StealResult> steal(int, const schedlab::SystemView&) override {
    return std::nullopt;
  }

  const std::vector<std::string>& events() const noexcept {
    return events_;
  }

private:
  std::vector<std::string> events_;
  bool requested_preemption_ = false;
};

struct TaskArgs {
  int* completion_count = nullptr;
};

void compute_task(schedlab::WorkloadContext& workload, void* raw) {
  auto* args = static_cast<TaskArgs*>(raw);
  workload.compute_for(2);
  ++(*args->completion_count);
}

} // namespace

int main() {
  TickRecordingScheduler scheduler;
  schedlab::runtime::Worker::Config config{
      .compute_chunk_units = 1,
      .tick_interval_us = 1,
  };
  schedlab::runtime::Worker worker(scheduler, 0, config);

  int completion_count = 0;
  TaskArgs args{.completion_count = &completion_count};
  const uint64_t task_id = worker.spawn(&compute_task, &args);
  assert(task_id == 1);

  worker.run();

  assert(completion_count == 1);
  assert((scheduler.events() == std::vector<std::string>{
                                    "ready:1:spawn",
                                    "pick:1",
                                    "tick:1:1",
                                    "preempted:1",
                                    "pick:1",
                                    "tick:1:1",
                                    "exited:1",
                                }));
}

#include <cassert>
#include <chrono>
#include <cstdint>
#include <deque>
#include <string>
#include <vector>

#include "runtime/worker.hpp"

namespace {

class RecordingScheduler final : public schedlab::Scheduler {
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

  void on_task_blocked(const schedlab::TaskView& task, int) override {
    events_.push_back("blocked:" + std::to_string(task.task_id));
  }

  void on_task_exited(const schedlab::TaskView& task, int) override {
    events_.push_back("exited:" + std::to_string(task.task_id));
  }

  schedlab::TickAction on_tick(const schedlab::TaskView&, int,
                               const schedlab::SystemView&) override {
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
};

struct SleepTaskArgs {
  std::vector<std::string>* completions = nullptr;
};

void sleep_task(schedlab::WorkloadContext& workload, void* raw) {
  auto* args = static_cast<SleepTaskArgs*>(raw);
  workload.compute_for(2);
  workload.sleep_for(std::chrono::microseconds(5));
  workload.compute_for(1);
  args->completions->push_back("sleep");
}

struct DeviceTaskArgs {
  std::vector<std::string>* completions = nullptr;
};

void device_task(schedlab::WorkloadContext& workload, void* raw) {
  auto* args = static_cast<DeviceTaskArgs*>(raw);
  const schedlab::DeviceResult result = workload.device_call(7, schedlab::DeviceRequest{
                                                                    .service_units = 2,
                                                                    .opcode = 0,
                                                                    .arg0 = 42,
                                                                    .arg1 = 84,
                                                                });
  args->completions->push_back("device:" + std::to_string(result.value0) + ":" +
                               std::to_string(result.value1));
}

} // namespace

int main() {
  RecordingScheduler scheduler;
  schedlab::runtime::Worker worker(scheduler, 0);

  std::vector<std::string> completions;
  SleepTaskArgs sleep_args{.completions = &completions};
  DeviceTaskArgs device_args{.completions = &completions};

  const uint64_t sleep_task_id = worker.spawn(&sleep_task, &sleep_args);
  const uint64_t device_task_id = worker.spawn(&device_task, &device_args);

  assert(sleep_task_id == 1);
  assert(device_task_id == 2);

  worker.run();

  assert((completions == std::vector<std::string>{
                             "device:42:84",
                             "sleep",
                         }));
  assert((scheduler.events() == std::vector<std::string>{
                                    "ready:1:spawn",
                                    "ready:2:spawn",
                                    "pick:1",
                                    "blocked:1",
                                    "pick:2",
                                    "blocked:2",
                                    "ready:2:wakeup",
                                    "pick:2",
                                    "exited:2",
                                    "ready:1:wakeup",
                                    "pick:1",
                                    "exited:1",
                                }));
}

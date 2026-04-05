#pragma once

#include <cstdint>
#include <optional>

#include "schedlab/device.hpp"
#include "runtime/task_state.hpp"
#include "schedlab/task_view.hpp"

namespace schedlab::runtime {

class RuntimeTask {
public:
  explicit RuntimeTask(uint64_t task_id, TaskAttrs attrs = {});

  TaskState state() const;
  TaskView view() const;
  const TaskView& view_ref() const;

  bool on_spawn_ready();
  bool on_dispatch(int worker_id);
  bool on_runtime_advance(uint64_t runtime_us);
  bool on_wakeup_ready();
  bool on_device_completion(const DeviceResult& result);
  bool on_preempt(int worker_id);
  bool on_block(int worker_id);
  bool on_exit(int worker_id);
  std::optional<DeviceResult> take_device_result();
  void record_blocked_time(uint64_t blocked_time_us);

private:
  bool transition_to(TaskState next_state);
  bool finish_running_slice(int worker_id, StopReason reason);

  TaskState state_ = TaskState::New;
  TaskView view_{};
  std::optional<DeviceResult> pending_device_result_;
  uint64_t completed_slice_count_ = 0;
};

} // namespace schedlab::runtime

#pragma once

#include <cstdint>

namespace schedlab {

enum class StopReason {
  None,
  Preempted,
  Blocked,
  Exited,
};

enum class ReadyReason {
  Spawn,
  Wakeup,
};

enum class TickAction {
  Continue,
  RequestResched,
};

struct TaskAttrs {
  uint64_t group_id = 0;
  uint64_t weight = 1;
};

struct TaskView {
  uint64_t task_id = 0;
  uint64_t group_id = 0;
  uint64_t weight = 1;
  StopReason last_stop_reason = StopReason::None;

  uint64_t total_runtime_us = 0;
  uint64_t current_slice_runtime_us = 0;
  uint64_t last_slice_time_us = 0;
  int last_worker_id = -1;

  uint32_t voluntary_block_count = 0;
  uint32_t involuntary_preempt_count = 0;
  uint64_t total_blocked_time_us = 0;
  uint64_t avg_slice_us = 0;
};

class TaskViewRef {
public:
  TaskViewRef() = default;
  TaskViewRef(std::nullptr_t) noexcept {}
  TaskViewRef(const TaskView& view) noexcept : ptr_(&view) {}

  [[nodiscard]] bool has_value() const noexcept {
    return ptr_ != nullptr;
  }
  [[nodiscard]] explicit operator bool() const noexcept {
    return has_value();
  }

  [[nodiscard]] const TaskView& value() const noexcept {
    return *ptr_;
  }
  [[nodiscard]] const TaskView& operator*() const noexcept {
    return *ptr_;
  }
  [[nodiscard]] const TaskView* operator->() const noexcept {
    return ptr_;
  }

private:
  const TaskView* ptr_ = nullptr;
};

} // namespace schedlab

#pragma once

namespace schedlab::runtime {

enum class TaskState {
  New,
  Ready,
  Running,
  Blocked,
  Exited,
};

bool can_transition(TaskState from, TaskState to);

} // namespace schedlab::runtime

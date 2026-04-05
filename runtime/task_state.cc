#include "runtime/task_state.hpp"

namespace schedlab::runtime {

bool can_transition(TaskState from, TaskState to) {
  switch (from) {
  case TaskState::New:
    return to == TaskState::Ready;
  case TaskState::Ready:
    return to == TaskState::Running;
  case TaskState::Running:
    return to == TaskState::Ready || to == TaskState::Blocked || to == TaskState::Exited;
  case TaskState::Blocked:
    return to == TaskState::Ready;
  case TaskState::Exited:
    return false;
  }
  return false;
}

} // namespace schedlab::runtime

#include "runtime/task_runtime.hpp"

namespace schedlab::runtime {

RuntimeTask::RuntimeTask(uint64_t task_id, TaskAttrs attrs) {
  view_.task_id = task_id;
  view_.group_id = attrs.group_id;
  view_.weight = attrs.weight;
}

TaskState RuntimeTask::state() const {
  return state_;
}

TaskView RuntimeTask::view() const {
  return view_;
}

const TaskView& RuntimeTask::view_ref() const {
  return view_;
}

bool RuntimeTask::transition_to(TaskState next_state) {
  if (!can_transition(state_, next_state)) {
    return false;
  }
  state_ = next_state;
  return true;
}

bool RuntimeTask::finish_running_slice(int worker_id, StopReason reason) {
  if (state_ != TaskState::Running) {
    return false;
  }
  view_.last_slice_time_us = view_.current_slice_runtime_us;
  ++completed_slice_count_;
  if (completed_slice_count_ == 1) {
    view_.avg_slice_us = view_.last_slice_time_us;
  } else {
    const uint64_t accumulated =
        view_.avg_slice_us * (completed_slice_count_ - 1) + view_.last_slice_time_us;
    view_.avg_slice_us = accumulated / completed_slice_count_;
  }
  view_.current_slice_runtime_us = 0;
  view_.last_worker_id = worker_id;
  view_.last_stop_reason = reason;
  return true;
}

bool RuntimeTask::on_spawn_ready() {
  return transition_to(TaskState::Ready);
}

bool RuntimeTask::on_dispatch(int) {
  if (!transition_to(TaskState::Running)) {
    return false;
  }
  view_.current_slice_runtime_us = 0;
  return true;
}

bool RuntimeTask::on_runtime_advance(uint64_t runtime_us) {
  if (state_ != TaskState::Running) {
    return false;
  }
  view_.current_slice_runtime_us += runtime_us;
  view_.total_runtime_us += runtime_us;
  return true;
}

bool RuntimeTask::on_wakeup_ready() {
  return transition_to(TaskState::Ready);
}

bool RuntimeTask::on_device_completion(const DeviceResult& result) {
  if (state_ != TaskState::Blocked || pending_device_result_.has_value()) {
    return false;
  }
  pending_device_result_ = result;
  return true;
}

bool RuntimeTask::on_preempt(int worker_id) {
  if (!finish_running_slice(worker_id, StopReason::Preempted)) {
    return false;
  }
  ++view_.involuntary_preempt_count;
  return transition_to(TaskState::Ready);
}

bool RuntimeTask::on_block(int worker_id) {
  if (!finish_running_slice(worker_id, StopReason::Blocked)) {
    return false;
  }
  ++view_.voluntary_block_count;
  return transition_to(TaskState::Blocked);
}

bool RuntimeTask::on_exit(int worker_id) {
  if (!finish_running_slice(worker_id, StopReason::Exited)) {
    return false;
  }
  return transition_to(TaskState::Exited);
}

std::optional<DeviceResult> RuntimeTask::take_device_result() {
  std::optional<DeviceResult> result = pending_device_result_;
  pending_device_result_.reset();
  return result;
}

void RuntimeTask::record_blocked_time(uint64_t blocked_time_us) {
  view_.total_blocked_time_us += blocked_time_us;
}

} // namespace schedlab::runtime

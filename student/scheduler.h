#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "schedlab/scheduler.hpp"

namespace student {

class Scheduler final : public schedlab::Scheduler {
 public:
  void init(const schedlab::SystemView& system) override;

  int select_worker(
      const schedlab::TaskView& task,
      schedlab::ReadyContext ctx,
      const schedlab::SystemView& system) override;

  std::optional<uint64_t> pick_next(
      int worker_id,
      schedlab::QueueView candidates,
      const schedlab::SystemView& system) override;

  schedlab::TickAction on_tick(
      const schedlab::TaskView& current,
      int worker_id,
      const schedlab::SystemView& system) override;

  bool should_preempt(
      const schedlab::TaskView& waking,
      const schedlab::TaskView& current,
      int worker_id,
      const schedlab::SystemView& system) override;

  std::optional<StealResult> steal(
      int thief_worker_id,
      const schedlab::SystemView& system) override;

  void on_task_preempted(const schedlab::TaskView& task, int worker_id) override;
  void on_task_blocked(const schedlab::TaskView& task, int worker_id) override;
  void on_task_exited(const schedlab::TaskView& task, int worker_id) override;

 private:
  struct TaskState {
    uint64_t last_ready_time_us = 0;
    int wakeup_boost = 0;
    int latency_credit = 0;
    uint64_t observed_burst_us = 8;
  };

  TaskState& state_for(uint64_t task_id);
  const TaskState& state_for(uint64_t task_id) const;
  int worker_count_ = 0;
  mutable std::mutex mu_;
  std::unordered_map<uint64_t, TaskState> task_states_;
};

}  // namespace student

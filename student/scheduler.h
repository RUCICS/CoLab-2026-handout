#pragma once

#include <cstdint>
#include <optional>

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
};

}  // namespace student

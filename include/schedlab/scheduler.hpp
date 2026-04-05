#pragma once

#include <cstdint>
#include <optional>

#include "schedlab/queue_view.hpp"
#include "schedlab/system_view.hpp"
#include "schedlab/task_view.hpp"

namespace schedlab {

struct ReadyContext {
  ReadyReason reason = ReadyReason::Spawn;
  int source_worker_id = -1;
  int previous_worker_id = -1;
  uint64_t ready_time_us = 0;
};

class Scheduler {
public:
  virtual ~Scheduler() = default;

  virtual void init(const SystemView&) {}

  virtual int select_worker(const TaskView& task, ReadyContext ctx, const SystemView& system) = 0;

  virtual std::optional<uint64_t> pick_next(int worker_id, QueueView candidates,
                                            const SystemView& system) = 0;

  virtual TickAction on_tick(const TaskView& current, int worker_id, const SystemView& system) = 0;

  virtual bool should_preempt(const TaskView& waking, const TaskView& current, int worker_id,
                              const SystemView& system) = 0;

  struct StealResult {
    int victim_worker_id = -1;
    std::optional<uint64_t> task_id;
  };

  virtual std::optional<StealResult> steal(int thief_worker_id, const SystemView& system) = 0;

  virtual void on_task_preempted(const TaskView&, int) {}
  virtual void on_task_blocked(const TaskView&, int) {}
  virtual void on_task_exited(const TaskView&, int) {}
};

} // namespace schedlab

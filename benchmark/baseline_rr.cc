#include "schedlab/baseline_scheduler.hpp"

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>

namespace schedlab::benchmark {

namespace {

// ---------------------------------------------------------------------------
// Baseline Round-Robin Scheduler (v4)
//
// Design intent: a correct, functioning scheduler that is clearly beatable
// on every axis.  It should feel like "the simplest thing that works" —
// good enough that students can't dismiss it as broken, but weak enough
// that real scheduling strategies yield visible improvement.
//
// Calibration history:
//   v1: quantum=1   → switch_cost dominates, throughput pathologically bad
//   v2: quantum=100 + preempt + longest-steal + affinity → too good overall
//   v3: quantum=30 + no preempt + simple steal → throughput still too good
//       (rr_steal 1.013x, baseline already near-optimal due to steal)
//   v4: quantum=20 + no preempt + NO steal + no affinity  ← current
//
// Throughput room (quantum=20, switch_cost≈8):
//   Baseline efficiency ≈ 20/(20+8) = 71%.
//   Optimal large-quantum efficiency ≈ 200/(200+8) = 96%.
//   Quantum improvement alone: 96/71 = 1.35×.
//   Plus: no stealing means work accumulates on busy workers while others
//   idle.  A good work-stealing strategy adds another significant factor.
//
// Latency room:
//   should_preempt returns false → wakeup tasks wait up to a full quantum
//   behind whatever is currently running.
//   MLFQ + aggressive preempt-on-ready eliminates this wait entirely.
//
// Fairness room:
//   No weight awareness.  All tasks treated equally regardless of group
//   weight.  CFS-style weighted vruntime scheduling dominates.
//
// Work distribution without steal:
//   Spawned tasks go to a global queue, drained by any worker's pick_next.
//   Wakeup tasks go to the source worker's queue.  No rebalancing ever
//   happens — once work lands on a worker, it stays there.  This is the
//   main source of throughput suboptimality and is the intended target
//   for student optimization.
// ---------------------------------------------------------------------------

class BaselineRoundRobinScheduler final : public Scheduler {
public:
  static constexpr uint64_t kTimeQuantum = 20;

  int select_worker(const TaskView&, ReadyContext ctx, const SystemView& system) override {
    const std::lock_guard<std::mutex> lock(mu_);
    if (ctx.reason == ReadyReason::Spawn) {
      if (system.total_worker_count() == 0) {
        return 0;
      }
      const int worker = next_spawn_worker_ % static_cast<int>(system.total_worker_count());
      next_spawn_worker_ = (worker + 1) % static_cast<int>(system.total_worker_count());
      return worker;
    }
    return normalize(ctx.source_worker_id);
  }

  void on_task_preempted(const TaskView&, int) override {}

  void on_task_blocked(const TaskView&, int) override {}

  void on_task_exited(const TaskView&, int) override {}

  TickAction on_tick(const TaskView& current, int worker_id, const SystemView& system) override {
    if (current.current_slice_runtime_us >= kTimeQuantum) {
      if (!system.queue(normalize(worker_id)).empty()) {
        return TickAction::RequestResched;
      }
      return TickAction::RequestResched;
    }
    return TickAction::Continue;
  }

  bool should_preempt(const TaskView& /* waking */, const TaskView& /* current */,
                      int /* worker_id */, const SystemView& /* system */) override {
    return false;
  }

  std::optional<uint64_t> pick_next(int, QueueView candidates, const SystemView&) override {
    if (candidates.empty()) {
      return std::nullopt;
    }
    return candidates.front().task_id;
  }

  std::optional<StealResult> steal(int, const SystemView&) override {
    return std::nullopt;
  }

private:
  static int normalize(int worker_id) noexcept {
    return worker_id >= 0 ? worker_id : 0;
  }

  std::mutex mu_;
  int next_spawn_worker_ = 0;
};

} // namespace

std::unique_ptr<Scheduler> make_baseline_rr_scheduler() {
  return std::make_unique<BaselineRoundRobinScheduler>();
}

} // namespace schedlab::benchmark

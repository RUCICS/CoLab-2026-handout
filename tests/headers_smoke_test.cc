#include "schedlab/debug_api.hpp"
#include "schedlab/device.hpp"
#include "schedlab/queue_view.hpp"
#include "schedlab/scheduler.hpp"
#include "schedlab/system_view.hpp"
#include "schedlab/task_view.hpp"
#include "schedlab/workload_api.hpp"
#include "scheduler.h"

namespace {

class TrivialScheduler final : public schedlab::Scheduler {
public:
  int select_worker(const schedlab::TaskView&, schedlab::ReadyContext,
                    const schedlab::SystemView&) override {
    return 0;
  }

  std::optional<uint64_t> pick_next(int, schedlab::QueueView candidates,
                                    const schedlab::SystemView&) override {
    if (candidates.empty()) {
      return std::nullopt;
    }
    return candidates.front().task_id;
  }

  schedlab::TickAction on_tick(const schedlab::TaskView&, int,
                               const schedlab::SystemView&) override {
    return schedlab::TickAction::Continue;
  }

  bool should_preempt(const schedlab::TaskView&, const schedlab::TaskView&, int,
                      const schedlab::SystemView&) override {
    return false;
  }

  std::optional<schedlab::Scheduler::StealResult> steal(int, const schedlab::SystemView&) override {
    return std::nullopt;
  }
};

} // namespace

int main() {
  student::Scheduler scheduler;
  TrivialScheduler trivial;
  schedlab::DebugEventKind kind = schedlab::DebugEventKind::Unknown;
  const char* name = schedlab::debug_event_kind_name(schedlab::DebugEventKind::TaskReady);
  const bool parsed = schedlab::parse_debug_event_kind("TaskPick", &kind);
  (void)scheduler;
  (void)trivial;
  (void)name;
  (void)parsed;
  (void)kind;
  return 0;
}

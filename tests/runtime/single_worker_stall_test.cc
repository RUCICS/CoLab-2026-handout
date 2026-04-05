#include <cassert>
#include <csignal>
#include <cstdint>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "runtime/worker.hpp"

namespace {

class DropReadyScheduler final : public schedlab::Scheduler {
public:
  int select_worker(const schedlab::TaskView&, schedlab::ReadyContext,
                    const schedlab::SystemView&) override {
    return 0;
  }
  void on_task_preempted(const schedlab::TaskView&, int) override {}
  void on_task_blocked(const schedlab::TaskView&, int) override {}
  void on_task_exited(const schedlab::TaskView&, int) override {}

  schedlab::TickAction on_tick(const schedlab::TaskView&, int,
                               const schedlab::SystemView&) override {
    return schedlab::TickAction::Continue;
  }

  bool should_preempt(const schedlab::TaskView&, const schedlab::TaskView&, int,
                      const schedlab::SystemView&) override {
    return false;
  }

  std::optional<uint64_t> pick_next(int, schedlab::QueueView,
                                    const schedlab::SystemView&) override {
    return std::nullopt;
  }

  std::optional<StealResult> steal(int, const schedlab::SystemView&) override {
    return std::nullopt;
  }
};

void no_op_task(schedlab::WorkloadContext&, void*) {}

} // namespace

int main() {
  pid_t child = fork();
  assert(child >= 0);

  if (child == 0) {
    rlimit core_limit{
        .rlim_cur = 0,
        .rlim_max = 0,
    };
    setrlimit(RLIMIT_CORE, &core_limit);

    DropReadyScheduler scheduler;
    schedlab::runtime::Worker worker(scheduler, 0);
    worker.spawn(&no_op_task, nullptr);
    worker.run();
    _exit(0);
  }

  int status = 0;
  for (int attempt = 0; attempt < 5000; ++attempt) {
    const pid_t waited = waitpid(child, &status, WNOHANG);
    assert(waited >= 0);
    if (waited == child) {
      assert(WIFSIGNALED(status));
      assert(WTERMSIG(status) == SIGABRT);
      return 0;
    }
    usleep(1000);
  }

  kill(child, SIGKILL);
  waitpid(child, &status, 0);
  assert(false && "worker stalled instead of aborting");
}

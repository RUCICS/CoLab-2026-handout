#include "benchmark/schedulers.hpp"

namespace schedlab::benchmark {

std::unique_ptr<schedlab::Scheduler> make_private_scheduler(SchedulerKind) {
  return nullptr;
}

bool has_private_schedulers() noexcept {
  return false;
}

} // namespace schedlab::benchmark

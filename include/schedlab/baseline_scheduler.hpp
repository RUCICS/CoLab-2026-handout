#pragma once

#include <memory>

#include "schedlab/scheduler.hpp"

namespace schedlab::benchmark {

std::unique_ptr<Scheduler> make_baseline_rr_scheduler();

} // namespace schedlab::benchmark

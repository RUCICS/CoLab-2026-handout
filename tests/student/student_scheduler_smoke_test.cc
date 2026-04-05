#include <cassert>
#include <string_view>

#include "benchmark/workload_dsl.hpp"
#include "benchmark/workload_interpreter.hpp"
#include "scheduler.h"
#include "simulator/engine.hpp"

int main() {
  constexpr std::string_view source = R"(
track throughput
score throughput_makespan
workers 2
cpu_rate 1

group batch * 4:
  compute 2
)";

  const auto parsed = schedlab::benchmark::dsl::parse_workload_dsl(source);
  assert(parsed.spec.has_value());

  schedlab::benchmark::InterpretedScenario scenario(*parsed.spec);
  student::Scheduler scheduler;
  schedlab::simulator::Engine engine(scheduler, scenario,
                                     schedlab::simulator::Engine::Config{
                                         .worker_count = 2,
                                         .compute_chunk_units = 1,
                                         .tick_interval_us = 1,
                                     });

  const auto metrics = engine.run();
  assert(metrics.has_value());
  assert(metrics->completed_tasks == 4);
  assert(metrics->task_observations.size() == 4);
  assert(metrics->elapsed_time_us >= 4);
}

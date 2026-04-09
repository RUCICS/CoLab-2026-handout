set(SCHEDLAB_CONTEXT_IMPL_SOURCES
  runtime/context.cc
  runtime/task_entry.cc
)

set(SCHEDLAB_SINGLE_WORKER_IMPL_SOURCES
  runtime/runtime.cc
  runtime/worker.cc
)

set(SCHEDLAB_RUNTIME_TASK_SOURCES
  runtime/task_state.cc
  runtime/task_runtime.cc
)

set(SCHEDLAB_RUNTIME_WORKLOAD_SOURCES
  ${SCHEDLAB_RUNTIME_TASK_SOURCES}
  runtime/runtime_workload_context.cc
  runtime/cpu_burner.cc
)

set(SCHEDLAB_RUNTIME_TIMED_WORKLOAD_SOURCES
  ${SCHEDLAB_RUNTIME_WORKLOAD_SOURCES}
  runtime/timer_queue.cc
  runtime/tick_source.cc
)

set(SCHEDLAB_RUNTIME_DEVICE_SOURCES
  devices/fifo_device.cc
)

set(SCHEDLAB_RUNTIME_SINGLE_WORKER_TEST_SOURCES
  ${SCHEDLAB_RUNTIME_TIMED_WORKLOAD_SOURCES}
  ${SCHEDLAB_CONTEXT_IMPL_SOURCES}
  ${SCHEDLAB_SINGLE_WORKER_IMPL_SOURCES}
  ${SCHEDLAB_RUNTIME_DEVICE_SOURCES}
)

set(SCHEDLAB_SIMULATOR_COMMON_SOURCES
  simulator/engine.cc
  benchmark/metrics.cc
  benchmark/workload_dsl.cc
  benchmark/workload_interpreter.cc
  ${SCHEDLAB_RUNTIME_TASK_SOURCES}
  runtime/tick_source.cc
  ${SCHEDLAB_RUNTIME_DEVICE_SOURCES}
)

set(SCHEDLAB_REFERENCE_SCHEDULER_TEST_SOURCES
  benchmark/schedulers_public.cc
  ${SCHEDLAB_PRIVATE_SCHEDULER_SOURCE}
  ${SCHEDLAB_REFERENCE_SCHEDULER_SOURCES}
  benchmark/baseline_rr.cc
  student/scheduler.cc
)

set(SCHEDLAB_RUNNER_SOURCES
  benchmark/runner_main.cc
  benchmark/metrics.cc
  benchmark/schedulers_public.cc
  ${SCHEDLAB_PRIVATE_SCHEDULER_SOURCE}
  ${SCHEDLAB_REFERENCE_SCHEDULER_SOURCES}
  benchmark/scoring.cc
  benchmark/event_log.cc
  benchmark/workload_dsl.cc
  benchmark/workload_discovery.cc
  benchmark/workload_interpreter.cc
  simulator/engine.cc
  benchmark/baseline_rr.cc
  ${SCHEDLAB_SINGLE_WORKER_IMPL_SOURCES}
  ${SCHEDLAB_RUNTIME_WORKLOAD_SOURCES}
  ${SCHEDLAB_CONTEXT_IMPL_SOURCES}
  runtime/timer_queue.cc
  runtime/tick_source.cc
  ${SCHEDLAB_RUNTIME_DEVICE_SOURCES}
  student/scheduler.cc
)

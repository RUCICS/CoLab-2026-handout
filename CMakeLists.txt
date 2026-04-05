cmake_minimum_required(VERSION 3.20)
project(schedlab LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(SchedlabOptions)
include(SchedlabSources)
include(SchedlabTargets)

add_library(student_scheduler
  student/scheduler.cc
)
target_include_directories(student_scheduler
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/student
)

schedlab_add_test_target(headers_smoke_test
  WITH_STUDENT
  SOURCES tests/headers_smoke_test.cc
  LIBRARIES student_scheduler
)

schedlab_add_test_target(student_runtime_surface_test
  WITH_STUDENT
  SOURCES tests/student/student_runtime_surface_test.cc
  LIBRARIES student_scheduler
)

schedlab_add_test_target(task_state_test
  INTERNAL_ONLY
  SOURCES
    tests/runtime/task_state_test.cc
    ${SCHEDLAB_RUNTIME_TASK_SOURCES}
)

schedlab_add_test_target(context_test
  SOURCES
    tests/runtime/context_test.cc
    ${SCHEDLAB_CONTEXT_IMPL_SOURCES}
)

schedlab_add_test_target(compute_for_test
  INTERNAL_ONLY
  SOURCES
    tests/runtime/compute_for_test.cc
    ${SCHEDLAB_RUNTIME_WORKLOAD_SOURCES}
    runtime/timer_queue.cc
    ${SCHEDLAB_CONTEXT_IMPL_SOURCES}
)

schedlab_add_test_target(sleep_for_test
  INTERNAL_ONLY
  SOURCES
    tests/runtime/sleep_for_test.cc
    ${SCHEDLAB_RUNTIME_WORKLOAD_SOURCES}
    runtime/timer_queue.cc
    ${SCHEDLAB_CONTEXT_IMPL_SOURCES}
)

schedlab_add_test_target(fifo_device_test
  INTERNAL_ONLY
  SOURCES
    tests/devices/fifo_device_test.cc
    ${SCHEDLAB_RUNTIME_WORKLOAD_SOURCES}
    runtime/timer_queue.cc
    ${SCHEDLAB_CONTEXT_IMPL_SOURCES}
    ${SCHEDLAB_RUNTIME_DEVICE_SOURCES}
)

schedlab_add_test_target(single_worker_loop_test
  SOURCES
    tests/runtime/single_worker_loop_test.cc
    ${SCHEDLAB_RUNTIME_SINGLE_WORKER_TEST_SOURCES}
)

schedlab_add_test_target(sync_primitives_test
  INTERNAL_ONLY
  SOURCES
    tests/runtime/sync_primitives_test.cc
    ${SCHEDLAB_RUNTIME_SINGLE_WORKER_TEST_SOURCES}
)

schedlab_add_test_target(single_worker_stall_test
  SOURCES
    tests/runtime/single_worker_stall_test.cc
    ${SCHEDLAB_RUNTIME_SINGLE_WORKER_TEST_SOURCES}
)

schedlab_add_test_target(tick_preemption_test
  SOURCES
    tests/runtime/tick_preemption_test.cc
    ${SCHEDLAB_RUNTIME_SINGLE_WORKER_TEST_SOURCES}
)

schedlab_add_test_target(student_scheduler_smoke_test
  WITH_STUDENT
  SOURCES
    tests/student/student_scheduler_smoke_test.cc
    ${SCHEDLAB_SIMULATOR_COMMON_SOURCES}
    student/scheduler.cc
)

schedlab_add_test_target(replay_test
  INTERNAL_ONLY
  SOURCES
    tests/benchmark/replay_test.cc
    benchmark/event_log.cc
    tools/replay.cc
    tools/dump_timeline.cc
)

schedlab_add_test_target(multi_worker_test
  INTERNAL_ONLY
  WITH_STUDENT
  SOURCES
    tests/runtime/multi_worker_test.cc
    benchmark/baseline_rr.cc
    student/scheduler.cc
    ${SCHEDLAB_RUNTIME_WORKLOAD_SOURCES}
    runtime/timer_queue.cc
    runtime/tick_source.cc
    ${SCHEDLAB_CONTEXT_IMPL_SOURCES}
    ${SCHEDLAB_SINGLE_WORKER_IMPL_SOURCES}
    ${SCHEDLAB_RUNTIME_DEVICE_SOURCES}
)

schedlab_add_test_target(metrics_test
  INTERNAL_ONLY
  SOURCES
    tests/benchmark/metrics_test.cc
    benchmark/metrics.cc
    benchmark/scoring.cc
    benchmark/workload_discovery.cc
    benchmark/workload_dsl.cc
)

schedlab_add_test_target(scoring_test
  INTERNAL_ONLY
  SOURCES
    tests/benchmark/scoring_test.cc
    benchmark/scoring.cc
)

schedlab_add_test_target(debug_aggregation_test
  INTERNAL_ONLY
  SOURCES
    tests/benchmark/debug_aggregation_test.cc
    benchmark/scoring.cc
)

schedlab_add_test_target(reference_scheduler_registry_test
  INTERNAL_ONLY
  WITH_STUDENT
  SOURCES
    tests/benchmark/reference_scheduler_registry_test.cc
    ${SCHEDLAB_REFERENCE_SCHEDULER_TEST_SOURCES}
)

schedlab_add_test_target(reference_scheduler_smoke_test
  INTERNAL_ONLY
  WITH_STUDENT
  SOURCES
    tests/benchmark/reference_scheduler_smoke_test.cc
    ${SCHEDLAB_REFERENCE_SCHEDULER_TEST_SOURCES}
)

schedlab_add_test_target(runner_cli_test
  INTERNAL_ONLY
  SOURCES tests/benchmark/runner_cli_test.cc
  DEPENDS runner
  DEFINITIONS RUNNER_BIN_PATH="$<TARGET_FILE:runner>"
)

schedlab_add_test_target(engine_smoke_test
  INTERNAL_ONLY
  SOURCES
    tests/simulator/engine_smoke_test.cc
    ${SCHEDLAB_SIMULATOR_COMMON_SOURCES}
)

schedlab_add_test_target(blocking_ops_test
  INTERNAL_ONLY
  SOURCES
    tests/simulator/blocking_ops_test.cc
    ${SCHEDLAB_SIMULATOR_COMMON_SOURCES}
)

schedlab_add_test_target(simulator_multi_worker_test
  INTERNAL_ONLY
  SOURCES
    tests/simulator/multi_worker_test.cc
    ${SCHEDLAB_SIMULATOR_COMMON_SOURCES}
)

schedlab_add_test_target(workload_dsl_test
  INTERNAL_ONLY
  SOURCES
    tests/benchmark/workload_dsl_test.cc
    benchmark/workload_dsl.cc
)

schedlab_add_test_target(workload_discovery_test
  INTERNAL_ONLY
  SOURCES
    tests/benchmark/workload_discovery_test.cc
    benchmark/workload_discovery.cc
    benchmark/workload_dsl.cc
    benchmark/metrics.cc
)

schedlab_add_test_target(workload_interpreter_test
  INTERNAL_ONLY
  SOURCES
    tests/benchmark/workload_interpreter_test.cc
    benchmark/workload_discovery.cc
    benchmark/workload_dsl.cc
    benchmark/workload_interpreter.cc
    ${SCHEDLAB_RUNTIME_WORKLOAD_SOURCES}
    ${SCHEDLAB_CONTEXT_IMPL_SOURCES}
    ${SCHEDLAB_RUNTIME_DEVICE_SOURCES}
  DEFINITIONS SCHEDLAB_TESTING=1
)

schedlab_add_binary(runner
  WITH_STUDENT
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/benchmark
  SOURCES ${SCHEDLAB_RUNNER_SOURCES}
)

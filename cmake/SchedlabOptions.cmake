set(SCHEDLAB_HAS_PRIVATE_SCHEDULER_SOURCES OFF)
if (
  EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/benchmark/schedulers_private.cc" AND
  EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/benchmark/reference.hpp"
)
  set(SCHEDLAB_HAS_PRIVATE_SCHEDULER_SOURCES ON)
endif()

set(SCHEDLAB_REFERENCE_SCHEDULER_SOURCES)
if (SCHEDLAB_HAS_PRIVATE_SCHEDULER_SOURCES)
  list(APPEND SCHEDLAB_REFERENCE_SCHEDULER_SOURCES
    benchmark/reference/rr_steal.cc
    benchmark/reference/mlfq.cc
    benchmark/reference/fair.cc
    benchmark/reference/lifo.cc
  )
endif()

option(
  SCHEDLAB_ENABLE_PRIVATE_SCHEDULERS
  "Enable internal benchmark scheduler extensions"
  ${SCHEDLAB_HAS_PRIVATE_SCHEDULER_SOURCES}
)
if (SCHEDLAB_ENABLE_PRIVATE_SCHEDULERS AND NOT SCHEDLAB_HAS_PRIVATE_SCHEDULER_SOURCES)
  message(FATAL_ERROR "SCHEDLAB_ENABLE_PRIVATE_SCHEDULERS=ON but private scheduler sources are missing")
endif()

set(SCHEDLAB_HAS_INTERNAL_TESTS OFF)
if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark/runner_cli_test.cc")
  set(SCHEDLAB_HAS_INTERNAL_TESTS ON)
endif()

set(SCHEDLAB_HAS_PUBLIC_TESTS OFF)
if (
  EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/headers_smoke_test.cc" AND
  EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/student/student_scheduler_smoke_test.cc"
)
  set(SCHEDLAB_HAS_PUBLIC_TESTS ON)
endif()

option(
  SCHEDLAB_BUILD_PUBLIC_TESTS
  "Build student-facing smoke and surface tests"
  ${SCHEDLAB_HAS_PUBLIC_TESTS}
)

option(
  SCHEDLAB_BUILD_INTERNAL_TESTS
  "Build framework/self-check tests that are not intended for the student handout"
  ${SCHEDLAB_HAS_INTERNAL_TESTS}
)

set(SCHEDLAB_PRIVATE_SCHEDULER_SOURCE benchmark/schedulers_stub.cc)
if (SCHEDLAB_ENABLE_PRIVATE_SCHEDULERS)
  set(SCHEDLAB_PRIVATE_SCHEDULER_SOURCE benchmark/schedulers_private.cc)
endif()

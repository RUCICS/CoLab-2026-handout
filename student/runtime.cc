#include "student/runtime.h"

namespace schedlab::runtime {

void Worker::run() {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::run");
}

void Worker::run_loop() {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::run_loop");
}

Worker::ObservedMetrics Worker::observed_metrics() const {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::observed_metrics");
}

void Worker::record_runnable_delta(uint64_t, uint64_t, int) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::record_runnable_delta");
}

void Worker::record_service_slice(uint64_t, uint64_t, uint64_t) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::record_service_slice");
}

void Worker::initialize_scheduler_if_needed() {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::initialize_scheduler_if_needed");
}

void Worker::enqueue_task_ready(uint64_t, ReadyContext) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::enqueue_task_ready");
}

void Worker::requeue_local_task(uint64_t) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::requeue_local_task");
}

std::optional<uint64_t> Worker::remove_local_ready_task(uint64_t) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::remove_local_ready_task");
}

std::optional<uint64_t> Worker::remove_default_steal_task(int) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::remove_default_steal_task");
}

std::optional<uint64_t> Worker::remove_specific_steal_task(int, uint64_t) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::remove_specific_steal_task");
}

std::optional<uint64_t> Worker::select_next_task() {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::select_next_task");
}

uint64_t Worker::spawn(TaskMain, void*, TaskAttrs) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::spawn");
}

void Worker::task_entry(void*) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::task_entry");
}

Worker::TaskRecord& Worker::lookup_task_or_fail(uint64_t) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::lookup_task_or_fail");
}

const Worker::TaskRecord& Worker::lookup_task_or_fail(uint64_t) const {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::lookup_task_or_fail");
}

bool Worker::dispatch_one(uint64_t) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::dispatch_one");
}

bool Worker::consume_tick_reschedule_request() {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::consume_tick_reschedule_request");
}

bool Worker::all_tasks_exited() const {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::all_tasks_exited");
}

void Worker::drain_due_events() {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::drain_due_events");
}

bool Worker::advance_idle_until_progress() {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::advance_idle_until_progress");
}

void Worker::wake_sync_task(uint64_t) {
  detail::hard_fail("student/runtime.cc: TODO implement Worker::wake_sync_task");
}

} // namespace schedlab::runtime

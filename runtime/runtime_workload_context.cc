#include "runtime/runtime_workload_context.hpp"

#include <algorithm>
#include <chrono>
#include <optional>

#include "runtime/context.hpp"

namespace schedlab::runtime {

namespace {

uint64_t ceil_div_u64(uint64_t numerator, uint64_t denominator) noexcept {
  if (numerator == 0 || denominator == 0) {
    return 0;
  }
  return 1 + ((numerator - 1) / denominator);
}

} // namespace

RuntimeWorkloadContext::RuntimeWorkloadContext(RuntimeTask& task,
                                               RescheduleRequestSource& reschedule_source,
                                               ExecutionContextSource& execution_context_source,
                                               uint64_t compute_chunk_units)
    : RuntimeWorkloadContext(task, reschedule_source, execution_context_source, compute_chunk_units,
                             RuntimeAccountingConfig{}) {}

RuntimeWorkloadContext::RuntimeWorkloadContext(RuntimeTask& task,
                                               RescheduleRequestSource& reschedule_source,
                                               ExecutionContextSource& execution_context_source,
                                               uint64_t compute_chunk_units,
                                               RuntimeAccountingConfig accounting_config)
    : task_(&task), reschedule_source_(&reschedule_source),
      execution_context_source_(&execution_context_source),
      compute_chunk_units_(compute_chunk_units), accounting_config_(accounting_config) {
  if (compute_chunk_units_ == 0) {
    detail::hard_fail("RuntimeWorkloadContext: compute_chunk_units must be > 0");
  }
  if (accounting_config_.runtime_us_per_cpu_unit == 0) {
    detail::hard_fail("RuntimeWorkloadContext: runtime_us_per_cpu_unit must be > 0");
  }
}

void RuntimeWorkloadContext::compute_for(uint64_t cpu_units) {
  execution_started_ = true;
  uint64_t remaining = cpu_units;
  while (remaining > 0) {
    const uint64_t chunk = std::min(remaining, compute_chunk_units_);
    const uint64_t runtime_us = to_runtime_us(chunk);
    const auto slice_start = std::chrono::steady_clock::now();
    burner_.burn(chunk);
    const auto slice_end = std::chrono::steady_clock::now();
    const uint64_t elapsed_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(slice_end - slice_start).count());
    if (!task_->on_runtime_advance(runtime_us)) {
      detail::hard_fail("RuntimeWorkloadContext::compute_for: task is not running");
    }
    execution_context_source_->record_compute_service(elapsed_us);
    execution_context_source_->advance_runtime_time(runtime_us);
    remaining -= chunk;

    const bool should_reschedule = reschedule_source_->consume_reschedule_request();
    if (should_reschedule) {
      yield_current_task();
    }
  }
}

void RuntimeWorkloadContext::sleep_for(std::chrono::microseconds duration) {
  execution_started_ = true;
  if (duration.count() < 0) {
    detail::hard_fail("RuntimeWorkloadContext::sleep_for: duration must be >= 0");
  }
  const int worker_id = execution_context_source_->current_worker_id();
  if (worker_id < 0) {
    detail::hard_fail("RuntimeWorkloadContext::sleep_for: invalid worker id");
  }
  if (!task_->on_block(worker_id)) {
    detail::hard_fail("RuntimeWorkloadContext::sleep_for: task is not running");
  }
  if (!execution_context_source_->schedule_sleep(task_->view().task_id,
                                                 static_cast<uint64_t>(duration.count()))) {
    detail::hard_fail("RuntimeWorkloadContext::sleep_for: task already scheduled for wakeup");
  }
  yield_current_task();
}

DeviceResult RuntimeWorkloadContext::device_call(DeviceId device, const DeviceRequest& request) {
  execution_started_ = true;
  const int worker_id = execution_context_source_->current_worker_id();
  if (worker_id < 0) {
    detail::hard_fail("RuntimeWorkloadContext::device_call: invalid worker id");
  }
  if (!task_->on_block(worker_id)) {
    detail::hard_fail("RuntimeWorkloadContext::device_call: task is not running");
  }
  const auto configured_rate = configured_device_rates_.find(device);
  if (configured_rate != configured_device_rates_.end()) {
    execution_context_source_->configure_device_rate(device, configured_rate->second);
  }
  const auto parallel_it = configured_device_parallelisms_.find(device);
  if (parallel_it != configured_device_parallelisms_.end()) {
    execution_context_source_->configure_device_parallelism(device, parallel_it->second);
  }
  if (!execution_context_source_->submit_device_call(task_->view().task_id, device, request)) {
    detail::hard_fail("RuntimeWorkloadContext::device_call: device submission failed");
  }
  yield_current_task();
  std::optional<DeviceResult> result = task_->take_device_result();
  if (!result.has_value()) {
    detail::hard_fail("RuntimeWorkloadContext::device_call: missing device result after wakeup");
  }
  return *result;
}

void RuntimeWorkloadContext::configure_cpu_rate(uint64_t cpu_rate) {
  if (cpu_rate == 0) {
    detail::hard_fail("RuntimeWorkloadContext::configure_cpu_rate: cpu_rate must be > 0");
  }
  if (execution_started_) {
    detail::hard_fail("RuntimeWorkloadContext::configure_cpu_rate: cpu_rate must be configured "
                      "before task execution");
  }
  if (cpu_rate_configured_) {
    detail::hard_fail(
        "RuntimeWorkloadContext::configure_cpu_rate: cpu_rate may only be configured once");
  }
  cpu_rate_ = cpu_rate;
  cpu_rate_configured_ = true;
}

void RuntimeWorkloadContext::configure_device_rate(DeviceId device, uint64_t rate) {
  if (rate == 0) {
    detail::hard_fail("RuntimeWorkloadContext::configure_device_rate: rate must be > 0");
  }
  if (execution_started_) {
    detail::hard_fail("RuntimeWorkloadContext::configure_device_rate: device rates must be "
                      "configured before task execution");
  }
  if (configured_device_rates_.find(device) != configured_device_rates_.end()) {
    detail::hard_fail("RuntimeWorkloadContext::configure_device_rate: device rate may only be "
                      "configured once per device");
  }
  configured_device_rates_.emplace(device, rate);
}

void RuntimeWorkloadContext::configure_device_parallelism(DeviceId device, uint64_t parallel) {
  if (parallel == 0) {
    detail::hard_fail("RuntimeWorkloadContext::configure_device_parallelism: parallel must be > 0");
  }
  if (execution_started_) {
    detail::hard_fail("RuntimeWorkloadContext::configure_device_parallelism: device parallelism "
                      "must be configured before task execution");
  }
  if (configured_device_parallelisms_.find(device) != configured_device_parallelisms_.end()) {
    detail::hard_fail("RuntimeWorkloadContext::configure_device_parallelism: device parallelism "
                      "may only be configured once");
  }
  configured_device_parallelisms_.emplace(device, parallel);
}

void RuntimeWorkloadContext::declare_event(std::string_view name) {
  if (!execution_context_source_->declare_event(name)) {
    detail::hard_fail("RuntimeWorkloadContext::declare_event: conflicting sync declaration");
  }
}

void RuntimeWorkloadContext::declare_latch(std::string_view name, uint64_t count) {
  if (count == 0) {
    detail::hard_fail("RuntimeWorkloadContext::declare_latch: count must be > 0");
  }
  if (!execution_context_source_->declare_latch(name, count)) {
    detail::hard_fail("RuntimeWorkloadContext::declare_latch: conflicting sync declaration");
  }
}

void RuntimeWorkloadContext::wait_sync(std::string_view target) {
  execution_started_ = true;
  switch (execution_context_source_->wait_sync(*task_, target)) {
  case SyncWaitResult::Ready:
    return;
  case SyncWaitResult::Blocked:
    yield_current_task();
    return;
  case SyncWaitResult::MissingTarget:
    detail::hard_fail("RuntimeWorkloadContext::wait_sync: unknown sync target");
  case SyncWaitResult::InvalidTransition:
    detail::hard_fail("RuntimeWorkloadContext::wait_sync: task wait transition failed");
  }
}

void RuntimeWorkloadContext::signal_event(std::string_view target) {
  execution_started_ = true;
  switch (execution_context_source_->signal_event(target)) {
  case SyncActionResult::Applied:
    return;
  case SyncActionResult::MissingTarget:
    detail::hard_fail("RuntimeWorkloadContext::signal_event: unknown event target");
  case SyncActionResult::WrongKind:
    detail::hard_fail("RuntimeWorkloadContext::signal_event: target must be an event");
  case SyncActionResult::InvalidState:
    detail::hard_fail("RuntimeWorkloadContext::signal_event: invalid event state");
  }
}

void RuntimeWorkloadContext::arrive_latch(std::string_view target) {
  execution_started_ = true;
  switch (execution_context_source_->arrive_latch(target)) {
  case SyncActionResult::Applied:
    return;
  case SyncActionResult::MissingTarget:
    detail::hard_fail("RuntimeWorkloadContext::arrive_latch: unknown latch target");
  case SyncActionResult::WrongKind:
    detail::hard_fail("RuntimeWorkloadContext::arrive_latch: target must be a latch");
  case SyncActionResult::InvalidState:
    detail::hard_fail("RuntimeWorkloadContext::arrive_latch: latch has already reached zero");
  }
}

uint64_t RuntimeWorkloadContext::to_runtime_us(uint64_t cpu_units) const noexcept {
  if (cpu_units == 0) {
    return 0;
  }
  const uint64_t scaled_units = cpu_units * accounting_config_.runtime_us_per_cpu_unit;
  return ceil_div_u64(scaled_units, cpu_rate_);
}

} // namespace schedlab::runtime

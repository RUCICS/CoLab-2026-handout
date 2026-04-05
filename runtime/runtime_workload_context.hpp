#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>
#include <unordered_map>

#include "runtime/cpu_burner.hpp"
#include "runtime/task_runtime.hpp"
#include "schedlab/workload_api.hpp"

namespace schedlab::runtime {

class RescheduleRequestSource {
public:
  virtual ~RescheduleRequestSource() = default;
  virtual bool consume_reschedule_request() noexcept = 0;
};

enum class SyncWaitResult {
  Ready,
  Blocked,
  MissingTarget,
  InvalidTransition,
};

enum class SyncActionResult {
  Applied,
  MissingTarget,
  WrongKind,
  InvalidState,
};

class ExecutionContextSource {
public:
  virtual ~ExecutionContextSource() = default;
  virtual void advance_runtime_time(uint64_t runtime_us) noexcept = 0;
  virtual void record_compute_service(uint64_t elapsed_us) noexcept = 0;
  virtual int current_worker_id() const noexcept = 0;
  virtual bool schedule_sleep(uint64_t task_token, uint64_t delay_us) noexcept = 0;
  virtual bool submit_device_call(uint64_t task_token, DeviceId device,
                                  const DeviceRequest& request) noexcept = 0;
  virtual void configure_device_rate(DeviceId, uint64_t) noexcept {}
  virtual void configure_device_parallelism(DeviceId, uint64_t) noexcept {}
  virtual bool declare_event(std::string_view) noexcept {
    return true;
  }
  virtual bool declare_latch(std::string_view, uint64_t) noexcept {
    return true;
  }
  virtual SyncWaitResult wait_sync(RuntimeTask&, std::string_view) noexcept {
    return SyncWaitResult::MissingTarget;
  }
  virtual SyncActionResult signal_event(std::string_view) noexcept {
    return SyncActionResult::MissingTarget;
  }
  virtual SyncActionResult arrive_latch(std::string_view) noexcept {
    return SyncActionResult::MissingTarget;
  }
};

class RuntimeWorkloadContext final : public WorkloadContext {
public:
  struct RuntimeAccountingConfig {
    uint64_t runtime_us_per_cpu_unit = 1;
  };

  RuntimeWorkloadContext(RuntimeTask& task, RescheduleRequestSource& reschedule_source,
                         ExecutionContextSource& execution_context_source,
                         uint64_t compute_chunk_units = 32);
  RuntimeWorkloadContext(RuntimeTask& task, RescheduleRequestSource& reschedule_source,
                         ExecutionContextSource& execution_context_source,
                         uint64_t compute_chunk_units, RuntimeAccountingConfig accounting_config);

  RuntimeWorkloadContext(const RuntimeWorkloadContext&) = delete;
  RuntimeWorkloadContext& operator=(const RuntimeWorkloadContext&) = delete;
  RuntimeWorkloadContext(RuntimeWorkloadContext&&) = delete;
  RuntimeWorkloadContext& operator=(RuntimeWorkloadContext&&) = delete;

  void configure_cpu_rate(uint64_t cpu_rate) override;
  void configure_device_rate(DeviceId device, uint64_t rate) override;
  void configure_device_parallelism(DeviceId device, uint64_t parallel) override;
  void declare_event(std::string_view name) override;
  void declare_latch(std::string_view name, uint64_t count) override;
  void compute_for(uint64_t cpu_units) override;
  void sleep_for(std::chrono::microseconds duration) override;
  DeviceResult device_call(DeviceId device, const DeviceRequest& request) override;
  void wait_sync(std::string_view target) override;
  void signal_event(std::string_view target) override;
  void arrive_latch(std::string_view target) override;

private:
  uint64_t to_runtime_us(uint64_t cpu_units) const noexcept;

  RuntimeTask* task_ = nullptr;
  RescheduleRequestSource* reschedule_source_ = nullptr;
  ExecutionContextSource* execution_context_source_ = nullptr;
  uint64_t compute_chunk_units_ = 0;
  RuntimeAccountingConfig accounting_config_{};
  uint64_t cpu_rate_ = 1;
  bool cpu_rate_configured_ = false;
  std::unordered_map<DeviceId, uint64_t> configured_device_rates_;
  std::unordered_map<DeviceId, uint64_t> configured_device_parallelisms_;
  bool execution_started_ = false;
  CpuBurner burner_;
};

} // namespace schedlab::runtime

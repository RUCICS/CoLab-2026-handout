#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>

#include "schedlab/device.hpp"

namespace schedlab {

class WorkloadContext {
public:
  virtual ~WorkloadContext() = default;

  // Framework-owned execution configuration hook.
  // Default implementation is a no-op for non-runtime contexts.
  virtual void configure_cpu_rate(uint64_t) {}
  virtual void configure_device_rate(DeviceId, uint64_t) {}
  virtual void configure_device_parallelism(DeviceId, uint64_t) {}
  virtual void declare_event(std::string_view) {}
  virtual void declare_latch(std::string_view, uint64_t) {}

  virtual void compute_for(uint64_t cpu_units) = 0;
  virtual void sleep_for(std::chrono::microseconds duration) = 0;
  virtual DeviceResult device_call(DeviceId device, const DeviceRequest& request) = 0;
  virtual void wait_sync(std::string_view target) = 0;
  virtual void signal_event(std::string_view target) = 0;
  virtual void arrive_latch(std::string_view target) = 0;
};

} // namespace schedlab

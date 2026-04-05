#pragma once

#include <cstdint>
#include <optional>
#include <queue>
#include <unordered_map>
#include <vector>

#include "schedlab/device.hpp"

namespace schedlab::devices {

struct DeviceCompletion {
  uint64_t task_token = 0;
  DeviceResult result{};
};

class FifoDeviceModel {
public:
  void configure_device_rate(DeviceId device, uint64_t rate) noexcept;
  std::optional<uint64_t> configured_device_rate(DeviceId device) const noexcept;
  bool submit(uint64_t task_token, DeviceId device, const DeviceRequest& request);
  void advance_by(uint64_t delta_us) noexcept;
  std::vector<DeviceCompletion> pop_completed();
  std::optional<uint64_t> next_completion_at_us() const noexcept;
  void configure_device_parallelism(DeviceId device, uint64_t parallel) noexcept;

private:
  struct PendingRequest {
    uint64_t task_token = 0;
    uint64_t service_rate = 1;
    DeviceRequest request{};
    DeviceResult result{};
  };

  struct ActiveRequest {
    uint64_t completes_at_us = 0;
    PendingRequest pending{};
  };

  struct DeviceQueueState {
    std::queue<PendingRequest> queued;
    std::vector<ActiveRequest> active;
    uint64_t max_active = 1;
  };

  static DeviceResult make_result(DeviceId device, const DeviceRequest& request) noexcept;
  static void start_request(DeviceQueueState& state, PendingRequest pending,
                            uint64_t start_at_us) noexcept;

  uint64_t now_us_ = 0;
  std::unordered_map<DeviceId, DeviceQueueState> queues_;
  std::unordered_map<DeviceId, uint64_t> device_rates_;
};

} // namespace schedlab::devices

#include "devices/device_model.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <utility>

namespace schedlab::devices {

namespace {

[[noreturn]] void hard_fail(const char* message) noexcept {
  std::fputs(message, stderr);
  std::fputc('\n', stderr);
  std::fflush(stderr);
  std::abort();
}

uint64_t ceil_div_u64(uint64_t numerator, uint64_t denominator) noexcept {
  if (numerator == 0 || denominator == 0) {
    return 0;
  }
  return 1 + ((numerator - 1) / denominator);
}

uint64_t service_time_us(const DeviceRequest& request, uint64_t service_rate) noexcept {
  if (request.service_units == 0) {
    return 0;
  }
  return ceil_div_u64(request.service_units, service_rate);
}

} // namespace

void FifoDeviceModel::configure_device_rate(DeviceId device, uint64_t rate) noexcept {
  if (rate == 0) {
    hard_fail("FifoDeviceModel::configure_device_rate: rate must be > 0");
  }
  device_rates_[device] = rate;
}

void FifoDeviceModel::configure_device_parallelism(DeviceId device, uint64_t parallel) noexcept {
  if (parallel == 0) {
    hard_fail("FifoDeviceModel::configure_device_parallelism: parallel must be > 0");
  }
  DeviceQueueState& state = queues_[device];
  state.max_active = parallel;
}

std::optional<uint64_t> FifoDeviceModel::configured_device_rate(DeviceId device) const noexcept {
  const auto it = device_rates_.find(device);
  if (it == device_rates_.end()) {
    return std::nullopt;
  }
  return it->second;
}

bool FifoDeviceModel::submit(uint64_t task_token, DeviceId device, const DeviceRequest& request) {
  const uint64_t effective_rate = configured_device_rate(device).value_or(1);
  PendingRequest pending;
  pending.task_token = task_token;
  pending.service_rate = effective_rate;
  pending.request = request;
  pending.result = make_result(device, request);

  DeviceQueueState& state = queues_[device];
  if (state.active.size() < state.max_active) {
    start_request(state, std::move(pending), now_us_);
  } else {
    state.queued.push(std::move(pending));
  }
  return true;
}

void FifoDeviceModel::advance_by(uint64_t delta_us) noexcept {
  now_us_ += delta_us;
}

std::vector<DeviceCompletion> FifoDeviceModel::pop_completed() {
  std::vector<DeviceCompletion> completions;
  for (auto& [device, state] : queues_) {
    (void)device;
    bool progress = true;
    while (progress) {
      progress = false;
      size_t index = state.active.size();
      uint64_t earliest = std::numeric_limits<uint64_t>::max();
      for (size_t i = 0; i < state.active.size(); ++i) {
        if (state.active[i].completes_at_us <= now_us_ &&
            state.active[i].completes_at_us < earliest) {
          earliest = state.active[i].completes_at_us;
          index = i;
        }
      }
      if (index < state.active.size()) {
        const uint64_t completed_at_us = state.active[index].completes_at_us;
        completions.push_back(DeviceCompletion{
            .task_token = state.active[index].pending.task_token,
            .result = state.active[index].pending.result,
        });
        state.active.erase(state.active.begin() + index);
        progress = true;
        if (!state.queued.empty() && state.active.size() < state.max_active) {
          PendingRequest next = std::move(state.queued.front());
          state.queued.pop();
          start_request(state, std::move(next), completed_at_us);
        }
      }
    }
  }
  return completions;
}

std::optional<uint64_t> FifoDeviceModel::next_completion_at_us() const noexcept {
  std::optional<uint64_t> soonest;
  for (const auto& [device, state] : queues_) {
    (void)device;
    if (state.active.empty()) {
      continue;
    }
    for (const ActiveRequest& active : state.active) {
      if (active.completes_at_us <= now_us_) {
        soonest = now_us_;
        break;
      }
      if (!soonest.has_value() || active.completes_at_us < *soonest) {
        soonest = active.completes_at_us;
      }
    }
  }
  return soonest;
}

DeviceResult FifoDeviceModel::make_result(DeviceId device, const DeviceRequest& request) noexcept {
  return DeviceResult{
      .status = device,
      .value0 = request.arg0,
      .value1 = request.arg1,
  };
}

void FifoDeviceModel::start_request(DeviceQueueState& state, PendingRequest pending,
                                    uint64_t start_at_us) noexcept {
  const uint64_t completion_at_us =
      start_at_us + service_time_us(pending.request, pending.service_rate);
  state.active.push_back(ActiveRequest{
      .completes_at_us = completion_at_us,
      .pending = std::move(pending),
  });
}

} // namespace schedlab::devices

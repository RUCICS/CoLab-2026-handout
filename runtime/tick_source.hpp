#pragma once

#include <cstdint>

namespace schedlab::runtime {

class TickSource {
public:
  explicit TickSource(uint64_t tick_interval_us) noexcept;

  void advance_by(uint64_t runtime_us) noexcept;
  bool consume_pending_tick() noexcept;

private:
  uint64_t tick_interval_us_ = 0;
  uint64_t accumulated_runtime_us_ = 0;
  bool has_pending_tick_ = false;
};

} // namespace schedlab::runtime

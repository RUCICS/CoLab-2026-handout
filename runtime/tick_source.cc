#include "runtime/tick_source.hpp"

namespace schedlab::runtime {

TickSource::TickSource(uint64_t tick_interval_us) noexcept : tick_interval_us_(tick_interval_us) {}

void TickSource::advance_by(uint64_t runtime_us) noexcept {
  if (tick_interval_us_ == 0 || runtime_us == 0) {
    return;
  }
  accumulated_runtime_us_ += runtime_us;
  if (accumulated_runtime_us_ < tick_interval_us_) {
    return;
  }
  has_pending_tick_ = true;
  accumulated_runtime_us_ %= tick_interval_us_;
}

bool TickSource::consume_pending_tick() noexcept {
  if (!has_pending_tick_) {
    return false;
  }
  has_pending_tick_ = false;
  return true;
}

} // namespace schedlab::runtime

#pragma once

#include <queue>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

namespace schedlab::runtime {

class TimerQueue {
public:
  uint64_t now_us() const noexcept;
  void advance_by(uint64_t delta_us) noexcept;
  bool schedule_sleep(uint64_t task_token, uint64_t delay_us);
  bool cancel_sleep(uint64_t task_token);
  std::vector<uint64_t> pop_due();
  std::optional<uint64_t> next_wake_at_us();

private:
  struct TimerEvent {
    uint64_t wake_at_us = 0;
    uint64_t sequence = 0;
    uint64_t task_token = 0;
  };

  struct EarlierWake {
    bool operator()(const TimerEvent& left, const TimerEvent& right) const noexcept {
      if (left.wake_at_us != right.wake_at_us) {
        return left.wake_at_us > right.wake_at_us;
      }
      return left.sequence > right.sequence;
    }
  };

  uint64_t now_us_ = 0;
  uint64_t next_sequence_ = 0;
  std::priority_queue<TimerEvent, std::vector<TimerEvent>, EarlierWake> queue_;
  std::unordered_map<uint64_t, uint64_t> active_event_sequence_;

  bool head_is_stale() const noexcept;
};

} // namespace schedlab::runtime

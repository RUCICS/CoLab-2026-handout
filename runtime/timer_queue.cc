#include "runtime/timer_queue.hpp"

namespace schedlab::runtime {

uint64_t TimerQueue::now_us() const noexcept {
  return now_us_;
}

void TimerQueue::advance_by(uint64_t delta_us) noexcept {
  now_us_ += delta_us;
}

bool TimerQueue::schedule_sleep(uint64_t task_token, uint64_t delay_us) {
  if (active_event_sequence_.contains(task_token)) {
    return false;
  }
  const uint64_t sequence = next_sequence_++;
  queue_.push(TimerEvent{
      .wake_at_us = now_us_ + delay_us,
      .sequence = sequence,
      .task_token = task_token,
  });
  active_event_sequence_.emplace(task_token, sequence);
  return true;
}

bool TimerQueue::cancel_sleep(uint64_t task_token) {
  return active_event_sequence_.erase(task_token) == 1;
}

std::vector<uint64_t> TimerQueue::pop_due() {
  while (head_is_stale()) {
    queue_.pop();
  }

  std::vector<uint64_t> due;
  while (!queue_.empty() && queue_.top().wake_at_us <= now_us_) {
    const TimerEvent event = queue_.top();
    queue_.pop();
    const auto active = active_event_sequence_.find(event.task_token);
    if (active == active_event_sequence_.end()) {
      continue;
    }
    if (active->second != event.sequence) {
      continue;
    }
    active_event_sequence_.erase(active);
    due.push_back(event.task_token);

    while (head_is_stale()) {
      queue_.pop();
    }
  }
  return due;
}

std::optional<uint64_t> TimerQueue::next_wake_at_us() {
  while (head_is_stale()) {
    queue_.pop();
  }
  if (queue_.empty()) {
    return std::nullopt;
  }
  return queue_.top().wake_at_us;
}

bool TimerQueue::head_is_stale() const noexcept {
  if (queue_.empty()) {
    return false;
  }
  const TimerEvent& event = queue_.top();
  const auto active = active_event_sequence_.find(event.task_token);
  if (active == active_event_sequence_.end()) {
    return true;
  }
  return active->second != event.sequence;
}

} // namespace schedlab::runtime

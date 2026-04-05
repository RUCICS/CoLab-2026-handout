#include "benchmark/event_log.hpp"

#include <charconv>
#include <optional>
#include <sstream>

namespace schedlab::benchmark {

namespace {

bool parse_u64(std::string_view text, uint64_t* out) noexcept {
  if (out == nullptr || text.empty()) {
    return false;
  }
  uint64_t value = 0;
  const char* begin = text.data();
  const char* end = text.data() + text.size();
  const std::from_chars_result parsed = std::from_chars(begin, end, value);
  if (parsed.ec != std::errc{} || parsed.ptr != end) {
    return false;
  }
  *out = value;
  return true;
}

bool parse_i32(std::string_view text, int* out) noexcept {
  if (out == nullptr || text.empty()) {
    return false;
  }
  int value = 0;
  const char* begin = text.data();
  const char* end = text.data() + text.size();
  const std::from_chars_result parsed = std::from_chars(begin, end, value);
  if (parsed.ec != std::errc{} || parsed.ptr != end) {
    return false;
  }
  *out = value;
  return true;
}

} // namespace

void EventLog::record(const DebugEvent& event) {
  events_.push_back(event);
}

const std::vector<DebugEvent>& EventLog::events() const noexcept {
  return events_;
}

std::string EventLog::serialize() const {
  std::string out;
  out.reserve(events_.size() * 32);
  for (const DebugEvent& event : events_) {
    out += debug_event_kind_name(event.kind);
    out += ",";
    out += std::to_string(event.timestamp_us);
    out += ",";
    out += std::to_string(event.worker_id);
    out += ",";
    out += std::to_string(event.task_id);
    out += "\n";
  }
  return out;
}

std::optional<std::vector<DebugEvent>> parse_event_log(std::string_view serialized) {
  std::vector<DebugEvent> events;
  std::istringstream stream{std::string(serialized)};
  std::string line;
  while (std::getline(stream, line)) {
    if (line.empty()) {
      continue;
    }

    std::string_view remaining(line);
    const std::size_t first = remaining.find(',');
    if (first == std::string_view::npos) {
      return std::nullopt;
    }
    const std::string_view kind_token = remaining.substr(0, first);
    remaining.remove_prefix(first + 1);

    const std::size_t second = remaining.find(',');
    if (second == std::string_view::npos) {
      return std::nullopt;
    }
    const std::string_view time_token = remaining.substr(0, second);
    remaining.remove_prefix(second + 1);

    const std::size_t third = remaining.find(',');
    if (third == std::string_view::npos) {
      return std::nullopt;
    }
    const std::string_view worker_token = remaining.substr(0, third);
    const std::string_view task_token = remaining.substr(third + 1);

    DebugEvent event;
    if (!parse_debug_event_kind(kind_token, &event.kind)) {
      return std::nullopt;
    }
    if (!parse_u64(time_token, &event.timestamp_us)) {
      return std::nullopt;
    }
    if (!parse_i32(worker_token, &event.worker_id)) {
      return std::nullopt;
    }
    if (!parse_u64(task_token, &event.task_id)) {
      return std::nullopt;
    }
    events.push_back(event);
  }
  return events;
}

std::string format_invariant_violation(std::string_view detail) {
  return "Invariant violation: " + std::string(detail);
}

} // namespace schedlab::benchmark

#include "benchmark/event_log.hpp"

namespace schedlab::tools {

std::string dump_timeline(const benchmark::EventLog& log) {
  std::string out;
  const auto& events = log.events();
  out.reserve(events.size() * 48);
  for (const DebugEvent& event : events) {
    out += "t=" + std::to_string(event.timestamp_us);
    out += " worker " + std::to_string(event.worker_id);
    out += " task " + std::to_string(event.task_id);
    out += " " + std::string(debug_event_kind_name(event.kind));
    out += "\n";
  }
  return out;
}

} // namespace schedlab::tools

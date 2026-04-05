#include "benchmark/event_log.hpp"

namespace schedlab::tools {

std::optional<std::vector<DebugEvent>> replay_events(std::string_view serialized) {
  return benchmark::parse_event_log(serialized);
}

} // namespace schedlab::tools

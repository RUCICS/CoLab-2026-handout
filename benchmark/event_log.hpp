#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "schedlab/debug_api.hpp"

namespace schedlab::benchmark {

class EventLog {
public:
  void record(const DebugEvent& event);
  const std::vector<DebugEvent>& events() const noexcept;
  std::string serialize() const;

private:
  std::vector<DebugEvent> events_;
};

std::optional<std::vector<DebugEvent>> parse_event_log(std::string_view serialized);
std::string format_invariant_violation(std::string_view detail);

} // namespace schedlab::benchmark

namespace schedlab::tools {

std::optional<std::vector<DebugEvent>> replay_events(std::string_view serialized);
std::string dump_timeline(const benchmark::EventLog& log);

} // namespace schedlab::tools

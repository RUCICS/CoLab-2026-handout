#pragma once

#include <cstdint>
#include <string_view>

namespace schedlab {

enum class DebugEventKind {
  Unknown,
  TaskReady,
  TaskPick,
  TaskExited,
};

struct DebugEvent {
  DebugEventKind kind = DebugEventKind::Unknown;
  uint64_t timestamp_us = 0;
  int worker_id = -1;
  uint64_t task_id = 0;
};

inline const char* debug_event_kind_name(DebugEventKind kind) noexcept {
  switch (kind) {
  case DebugEventKind::TaskReady:
    return "TaskReady";
  case DebugEventKind::TaskPick:
    return "TaskPick";
  case DebugEventKind::TaskExited:
    return "TaskExited";
  case DebugEventKind::Unknown:
    return "Unknown";
  }
  return "Unknown";
}

inline bool parse_debug_event_kind(std::string_view token, DebugEventKind* out) noexcept {
  if (out == nullptr) {
    return false;
  }
  if (token == "TaskReady") {
    *out = DebugEventKind::TaskReady;
    return true;
  }
  if (token == "TaskPick") {
    *out = DebugEventKind::TaskPick;
    return true;
  }
  if (token == "TaskExited") {
    *out = DebugEventKind::TaskExited;
    return true;
  }
  if (token == "Unknown") {
    *out = DebugEventKind::Unknown;
    return true;
  }
  return false;
}

} // namespace schedlab

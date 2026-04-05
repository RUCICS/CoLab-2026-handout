#pragma once

#include <array>
#include <memory>
#include <optional>
#include <string_view>

#include "schedlab/scheduler.hpp"

namespace schedlab::benchmark {

enum class SchedulerKind {
  Student,
  Baseline,
  RefRrSteal,
  RefMlfq,
  RefFair,
  RefLifo,
};

enum class SchedulerVisibility {
  PublicOnly,
  All,
};

std::optional<SchedulerKind>
parse_scheduler_kind(std::string_view token,
                     SchedulerVisibility visibility = SchedulerVisibility::All) noexcept;
const char* scheduler_kind_name(SchedulerKind kind) noexcept;
bool is_public_scheduler(SchedulerKind kind) noexcept;
bool is_private_scheduler(SchedulerKind kind) noexcept;
std::array<SchedulerKind, 2> release_scheduler_order(int repetition_index) noexcept;

std::unique_ptr<schedlab::Scheduler> make_public_scheduler(SchedulerKind kind);
std::unique_ptr<schedlab::Scheduler> make_private_scheduler(SchedulerKind kind);
std::unique_ptr<schedlab::Scheduler> make_scheduler(SchedulerKind kind);
bool has_private_schedulers() noexcept;

} // namespace schedlab::benchmark

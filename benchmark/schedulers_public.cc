#include "benchmark/schedulers.hpp"

#include <memory>

#include "schedlab/baseline_scheduler.hpp"
#include "scheduler.h"

namespace schedlab::benchmark {

std::optional<SchedulerKind> parse_scheduler_kind(std::string_view token,
                                                  SchedulerVisibility visibility) noexcept {
  if (token == "student") {
    return SchedulerKind::Student;
  }
  if (token == "baseline") {
    return SchedulerKind::Baseline;
  }
  if (visibility == SchedulerVisibility::PublicOnly) {
    return std::nullopt;
  }
  if (token == "ref:rr_steal") {
    return SchedulerKind::RefRrSteal;
  }
  if (token == "ref:mlfq") {
    return SchedulerKind::RefMlfq;
  }
  if (token == "ref:fair") {
    return SchedulerKind::RefFair;
  }
  if (token == "ref:lifo") {
    return SchedulerKind::RefLifo;
  }
  return std::nullopt;
}

const char* scheduler_kind_name(SchedulerKind kind) noexcept {
  switch (kind) {
  case SchedulerKind::Student:
    return "student";
  case SchedulerKind::Baseline:
    return "baseline";
  case SchedulerKind::RefRrSteal:
    return "ref:rr_steal";
  case SchedulerKind::RefMlfq:
    return "ref:mlfq";
  case SchedulerKind::RefFair:
    return "ref:fair";
  case SchedulerKind::RefLifo:
    return "ref:lifo";
  }
  return "unknown";
}

bool is_public_scheduler(SchedulerKind kind) noexcept {
  return kind == SchedulerKind::Student || kind == SchedulerKind::Baseline;
}

bool is_private_scheduler(SchedulerKind kind) noexcept {
  return !is_public_scheduler(kind);
}

std::array<SchedulerKind, 2> release_scheduler_order(int repetition_index) noexcept {
  if ((repetition_index % 2) == 0) {
    return {
        SchedulerKind::Student,
        SchedulerKind::Baseline,
    };
  }
  return {
      SchedulerKind::Baseline,
      SchedulerKind::Student,
  };
}

std::unique_ptr<schedlab::Scheduler> make_public_scheduler(SchedulerKind kind) {
  switch (kind) {
  case SchedulerKind::Student:
    return std::make_unique<student::Scheduler>();
  case SchedulerKind::Baseline:
    return make_baseline_rr_scheduler();
  case SchedulerKind::RefRrSteal:
  case SchedulerKind::RefMlfq:
  case SchedulerKind::RefFair:
  case SchedulerKind::RefLifo:
    return nullptr;
  }
  return nullptr;
}

std::unique_ptr<schedlab::Scheduler> make_scheduler(SchedulerKind kind) {
  if (auto scheduler = make_public_scheduler(kind)) {
    return scheduler;
  }
  return make_private_scheduler(kind);
}

} // namespace schedlab::benchmark

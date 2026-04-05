#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "runtime/runtime_workload_context.hpp"

namespace schedlab::runtime {

class SyncRegistry {
public:
  bool declare_event(std::string_view name) {
    const std::lock_guard<std::mutex> lock(mu_);
    const auto [it, inserted] = objects_.try_emplace(std::string(name), SyncObject{
                                                                            .kind = SyncKind::Event,
                                                                            .signaled = false,
                                                                            .initial_count = 0,
                                                                            .remaining = 0,
                                                                            .waiters = {},
                                                                        });
    return inserted || it->second.kind == SyncKind::Event;
  }

  bool declare_latch(std::string_view name, uint64_t count) {
    const std::lock_guard<std::mutex> lock(mu_);
    const auto [it, inserted] = objects_.try_emplace(std::string(name), SyncObject{
                                                                            .kind = SyncKind::Latch,
                                                                            .signaled = false,
                                                                            .initial_count = count,
                                                                            .remaining = count,
                                                                            .waiters = {},
                                                                        });
    if (inserted) {
      return true;
    }
    return it->second.kind == SyncKind::Latch && it->second.initial_count == count;
  }

  template <typename BlockFn>
  SyncWaitResult wait(std::string_view name, uint64_t task_token, BlockFn&& block_fn) {
    const std::lock_guard<std::mutex> lock(mu_);
    const auto it = objects_.find(std::string(name));
    if (it == objects_.end()) {
      return SyncWaitResult::MissingTarget;
    }
    SyncObject& object = it->second;
    if ((object.kind == SyncKind::Event && object.signaled) ||
        (object.kind == SyncKind::Latch && object.remaining == 0)) {
      return SyncWaitResult::Ready;
    }
    if (!block_fn()) {
      return SyncWaitResult::InvalidTransition;
    }
    object.waiters.push_back(task_token);
    return SyncWaitResult::Blocked;
  }

  template <typename WakeFn>
  SyncActionResult signal_event(std::string_view name, WakeFn&& wake_fn) {
    std::vector<uint64_t> waiters;
    {
      const std::lock_guard<std::mutex> lock(mu_);
      const auto it = objects_.find(std::string(name));
      if (it == objects_.end()) {
        return SyncActionResult::MissingTarget;
      }
      SyncObject& object = it->second;
      if (object.kind != SyncKind::Event) {
        return SyncActionResult::WrongKind;
      }
      object.signaled = true;
      waiters.swap(object.waiters);
    }
    for (const uint64_t task_token : waiters) {
      wake_fn(task_token);
    }
    return SyncActionResult::Applied;
  }

  template <typename WakeFn>
  SyncActionResult arrive_latch(std::string_view name, WakeFn&& wake_fn) {
    std::vector<uint64_t> waiters;
    {
      const std::lock_guard<std::mutex> lock(mu_);
      const auto it = objects_.find(std::string(name));
      if (it == objects_.end()) {
        return SyncActionResult::MissingTarget;
      }
      SyncObject& object = it->second;
      if (object.kind != SyncKind::Latch) {
        return SyncActionResult::WrongKind;
      }
      if (object.remaining == 0) {
        return SyncActionResult::InvalidState;
      }
      --object.remaining;
      if (object.remaining == 0) {
        waiters.swap(object.waiters);
      }
    }
    for (const uint64_t task_token : waiters) {
      wake_fn(task_token);
    }
    return SyncActionResult::Applied;
  }

private:
  enum class SyncKind {
    Event,
    Latch,
  };

  struct SyncObject {
    SyncKind kind = SyncKind::Event;
    bool signaled = false;
    uint64_t initial_count = 0;
    uint64_t remaining = 0;
    std::vector<uint64_t> waiters;
  };

  std::mutex mu_;
  std::unordered_map<std::string, SyncObject> objects_;
};

} // namespace schedlab::runtime

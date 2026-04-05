#include "runtime/context.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstdint>

namespace schedlab::runtime::detail {

namespace {
thread_local TaskContext* g_current_context = nullptr;
thread_local FrameworkContext* g_current_framework = nullptr;
} // namespace

[[noreturn]] void hard_fail(const char* message) noexcept {
  std::fputs(message, stderr);
  std::fputc('\n', stderr);
  std::abort();
}

void set_current_context(TaskContext* context) noexcept {
  g_current_context = context;
}

TaskContext* current_context() noexcept {
  return g_current_context;
}

void set_current_framework(FrameworkContext* framework) noexcept {
  g_current_framework = framework;
}

FrameworkContext* current_framework() noexcept {
  return g_current_framework;
}

void task_entry_trampoline(int lo, int hi) noexcept {
  uintptr_t raw = (static_cast<uintptr_t>(static_cast<unsigned int>(hi)) << 32) |
                  static_cast<uintptr_t>(static_cast<unsigned int>(lo));
  auto* context = reinterpret_cast<TaskContext*>(raw);
  set_current_context(context);
  context->run_entry();
}

} // namespace schedlab::runtime::detail

namespace schedlab::runtime {

void yield_current_task() {
  TaskContext* context = detail::current_context();
  if (context == nullptr) {
    detail::hard_fail("yield_current_task: no active task context");
  }
  context->yield();
}

} // namespace schedlab::runtime

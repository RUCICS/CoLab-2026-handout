#define _XOPEN_SOURCE 700
#include "runtime/context.h"

#include <cstdlib>
#include <cstdint>
#include <ucontext.h>
#include <utility>
#include <vector>

namespace schedlab::runtime {

struct FrameworkContext::Impl {
  ucontext_t context{};
};

struct TaskContext::Impl {
  ucontext_t task_ctx{};
  std::vector<char> stack;
  Entry entry = nullptr;
  void* arg = nullptr;
  bool finished = false;
};

FrameworkContext::FrameworkContext() : impl_(std::make_unique<Impl>()) {}

FrameworkContext::~FrameworkContext() = default;

TaskContext::TaskContext(Entry entry, void* arg, std::size_t stack_size)
    : impl_(std::make_unique<Impl>()) {
  if (entry == nullptr) {
    detail::hard_fail("TaskContext: entry must not be null");
  }
  if (stack_size == 0) {
    detail::hard_fail("TaskContext: stack_size must be > 0");
  }
  impl_->stack.resize(stack_size);
  impl_->entry = entry;
  impl_->arg = arg;

  if (getcontext(&impl_->task_ctx) != 0) {
    std::abort();
  }

  impl_->task_ctx.uc_stack.ss_sp = impl_->stack.data();
  impl_->task_ctx.uc_stack.ss_size = impl_->stack.size();
  impl_->task_ctx.uc_link = nullptr;

  uintptr_t raw = reinterpret_cast<uintptr_t>(this);
  int lo = static_cast<int>(raw & 0xFFFFFFFFu);
  int hi = static_cast<int>((raw >> 32) & 0xFFFFFFFFu);
  makecontext(&impl_->task_ctx, reinterpret_cast<void (*)()>(detail::task_entry_trampoline), 2, lo,
              hi);
}

TaskContext::~TaskContext() = default;

bool TaskContext::finished() const noexcept {
  return impl_->finished;
}

void TaskContext::resume(FrameworkContext& framework) {
  if (impl_->finished) {
    return;
  }
  if (detail::current_context() != nullptr || detail::current_framework() != nullptr) {
    detail::hard_fail("TaskContext::resume: runtime context already active");
  }
  impl_->task_ctx.uc_link = &framework.impl_->context;
  detail::set_current_context(this);
  detail::set_current_framework(&framework);
  if (swapcontext(&framework.impl_->context, &impl_->task_ctx) != 0) {
    std::abort();
  }
  detail::set_current_framework(nullptr);
  detail::set_current_context(nullptr);
}

void TaskContext::yield() {
  FrameworkContext* framework = detail::current_framework();
  if (framework == nullptr) {
    detail::hard_fail("TaskContext::yield: no active framework context");
  }
  if (swapcontext(&impl_->task_ctx, &framework->impl_->context) != 0) {
    std::abort();
  }
}

void TaskContext::run_entry() noexcept {
  impl_->entry(impl_->arg);
  impl_->finished = true;
  yield();
}

} // namespace schedlab::runtime

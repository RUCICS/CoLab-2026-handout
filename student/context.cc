#include "student/context.h"

namespace schedlab::runtime {

struct FrameworkContext::Impl {};

struct TaskContext::Impl {
  bool finished = false;
};

FrameworkContext::FrameworkContext() : impl_(std::make_unique<Impl>()) {}

FrameworkContext::~FrameworkContext() = default;

TaskContext::TaskContext(Entry entry, void* arg, std::size_t stack_size)
    : impl_(std::make_unique<Impl>()) {
  (void)entry;
  (void)arg;
  (void)stack_size;
}

TaskContext::~TaskContext() = default;

bool TaskContext::finished() const noexcept {
  return impl_->finished;
}

void TaskContext::resume(FrameworkContext&) {
  detail::hard_fail("student/context.cc: TODO implement TaskContext::resume");
}

void TaskContext::yield() {
  detail::hard_fail("student/context.cc: TODO implement TaskContext::yield");
}

void TaskContext::run_entry() noexcept {
  detail::hard_fail("student/context.cc: TODO implement TaskContext::run_entry");
}

} // namespace schedlab::runtime

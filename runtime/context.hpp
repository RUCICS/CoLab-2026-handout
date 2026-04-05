#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

namespace schedlab::runtime {

class TaskContext;
class FrameworkContext;

namespace detail {

[[noreturn]] void hard_fail(const char* message) noexcept;

void set_current_context(TaskContext* context) noexcept;
TaskContext* current_context() noexcept;
void set_current_framework(FrameworkContext* framework) noexcept;
FrameworkContext* current_framework() noexcept;
void task_entry_trampoline(int lo, int hi) noexcept;

} // namespace detail

class FrameworkContext {
public:
  FrameworkContext();
  ~FrameworkContext();

  FrameworkContext(const FrameworkContext&) = delete;
  FrameworkContext& operator=(const FrameworkContext&) = delete;
  FrameworkContext(FrameworkContext&&) = delete;
  FrameworkContext& operator=(FrameworkContext&&) = delete;

private:
  friend class TaskContext;

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class TaskContext {
public:
  using Entry = void (*)(void*);

  explicit TaskContext(Entry entry, void* arg, std::size_t stack_size = 64 * 1024);
  ~TaskContext();

  TaskContext(const TaskContext&) = delete;
  TaskContext& operator=(const TaskContext&) = delete;
  TaskContext(TaskContext&&) = delete;
  TaskContext& operator=(TaskContext&&) = delete;

  bool finished() const noexcept;
  void resume(FrameworkContext& framework);

private:
  friend void detail::task_entry_trampoline(int lo, int hi) noexcept;
  friend void yield_current_task();

  void yield();

  void run_entry() noexcept;

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

void yield_current_task();

} // namespace schedlab::runtime

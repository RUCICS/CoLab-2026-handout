#include <cassert>
#include <cstdint>
#include <vector>

#include "runtime/context.hpp"

namespace {

void task_entry(void* arg) {
  auto* steps = static_cast<std::vector<int>*>(arg);
  steps->push_back(1);
  schedlab::runtime::yield_current_task();
  steps->push_back(2);
}

} // namespace

int main() {
  std::vector<int> steps;
  schedlab::runtime::FrameworkContext framework;
  schedlab::runtime::TaskContext context(task_entry, &steps);

  assert(!context.finished());
  context.resume(framework);
  assert(!context.finished());
  assert((steps == std::vector<int>{1}));

  context.resume(framework);
  assert(context.finished());
  assert((steps == std::vector<int>{1, 2}));

  context.resume(framework);
  assert((steps == std::vector<int>{1, 2}));
}

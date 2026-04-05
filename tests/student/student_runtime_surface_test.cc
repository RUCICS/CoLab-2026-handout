#include <type_traits>

#include "student/context.h"
#include "student/runtime.h"

static_assert(std::is_same_v<student::FrameworkContext, schedlab::runtime::FrameworkContext>);
static_assert(std::is_same_v<student::TaskContext, schedlab::runtime::TaskContext>);
static_assert(std::is_same_v<student::SingleWorkerRuntime, schedlab::runtime::Worker>);

int main() {
  return 0;
}

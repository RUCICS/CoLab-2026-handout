#pragma once

#include <cstdint>

#include "schedlab/task_view.hpp"
#include "schedlab/workload_api.hpp"

namespace schedlab::benchmark {

using WorkloadTaskMain = void (*)(WorkloadContext& workload, void* arg);

class WorkloadInstaller {
public:
  virtual ~WorkloadInstaller() = default;
  virtual uint64_t spawn(WorkloadTaskMain main, void* arg, schedlab::TaskAttrs attrs = {}) = 0;
};

} // namespace schedlab::benchmark

#pragma once

#include <cstdint>
#include <span>

#include "schedlab/queue_view.hpp"

namespace schedlab {

class SystemView {
public:
  struct WorkerState {
    int worker_id = -1;
    int topology_node = 0;
    uint32_t local_queue_size = 0;
    bool is_idle = true;
    std::optional<uint64_t> running_task_id;
  };

  virtual ~SystemView() = default;

  [[nodiscard]] virtual uint32_t total_ready_count() const = 0;
  [[nodiscard]] virtual uint32_t total_worker_count() const = 0;
  [[nodiscard]] virtual uint32_t node_count() const = 0;

  [[nodiscard]] virtual uint64_t switch_cost() const = 0;
  [[nodiscard]] virtual uint64_t migration_cost(int from_worker, int to_worker) const = 0;

  [[nodiscard]] virtual std::span<const WorkerState> worker_states() const = 0;
  [[nodiscard]] virtual std::span<const int> workers_in_node(int node) const = 0;

  [[nodiscard]] virtual const QueueView& queue(int worker_id) const = 0;
  [[nodiscard]] virtual TaskViewRef running_task(int worker_id) const = 0;
  [[nodiscard]] virtual TaskViewRef task(uint64_t task_id) const = 0;
};

} // namespace schedlab

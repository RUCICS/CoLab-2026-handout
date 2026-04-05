#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "benchmark/schedulers.hpp"

namespace schedlab::benchmark {

struct TaskObservation {
  uint64_t task_id = 0;
  uint64_t flow_id = 0;
  std::size_t group_index = 0;
  uint64_t release_time_us = 0;
  uint64_t completion_time_us = 0;
  uint64_t cpu_runtime_us = 0;
};

struct GroupRunnableEvent {
  uint64_t time_us = 0;
  std::size_t group_index = 0;
  int delta = 0;
};

struct GroupServiceSlice {
  uint64_t start_time_us = 0;
  uint64_t end_time_us = 0;
  std::size_t group_index = 0;
};

struct GroupObservation {
  std::string group_name;
  uint64_t weight = 1;
  std::vector<uint64_t> task_ids;
  uint64_t cpu_runtime_us = 0;
};

struct RunMetrics {
  uint64_t completed_tasks = 0;
  uint64_t elapsed_time_us = 0;
  double throughput_tasks_per_sec = 0.0;
  uint64_t p99_wakeup_latency_us = 0;
  std::unordered_map<int, uint64_t> worker_idle_time_us;
  std::vector<TaskObservation> task_observations;
  std::vector<GroupObservation> group_observations;
  std::vector<GroupRunnableEvent> group_runnable_events;
  std::vector<GroupServiceSlice> group_service_slices;
};

class MetricsCollector {
public:
  void record_task_completion(uint64_t count = 1);
  void set_elapsed_time_us(uint64_t elapsed_time_us);
  void record_wakeup_latency_us(uint64_t latency_us);
  void record_worker_idle_time_us(int worker_id, uint64_t idle_time_us);
  void record_task_observation(TaskObservation observation);
  void record_group_observation(GroupObservation observation);
  void record_group_runnable_event(GroupRunnableEvent event);
  void record_group_service_slice(GroupServiceSlice slice);
  RunMetrics finish() const;

private:
  uint64_t completed_tasks_ = 0;
  uint64_t elapsed_time_us_ = 0;
  std::vector<uint64_t> wakeup_latencies_us_;
  std::unordered_map<int, uint64_t> worker_idle_time_us_;
  std::vector<TaskObservation> task_observations_;
  std::vector<GroupObservation> group_observations_;
  std::vector<GroupRunnableEvent> group_runnable_events_;
  std::vector<GroupServiceSlice> group_service_slices_;
};

} // namespace schedlab::benchmark

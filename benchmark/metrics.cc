#include "benchmark/metrics.hpp"

#include <algorithm>

namespace schedlab::benchmark {

void MetricsCollector::record_task_completion(uint64_t count) {
  completed_tasks_ += count;
}

void MetricsCollector::set_elapsed_time_us(uint64_t elapsed_time_us) {
  elapsed_time_us_ = elapsed_time_us;
}

void MetricsCollector::record_wakeup_latency_us(uint64_t latency_us) {
  wakeup_latencies_us_.push_back(latency_us);
}

void MetricsCollector::record_worker_idle_time_us(int worker_id, uint64_t idle_time_us) {
  worker_idle_time_us_[worker_id] += idle_time_us;
}

void MetricsCollector::record_task_observation(TaskObservation observation) {
  task_observations_.push_back(std::move(observation));
}

void MetricsCollector::record_group_observation(GroupObservation observation) {
  group_observations_.push_back(std::move(observation));
}

void MetricsCollector::record_group_runnable_event(GroupRunnableEvent event) {
  group_runnable_events_.push_back(std::move(event));
}

void MetricsCollector::record_group_service_slice(GroupServiceSlice slice) {
  group_service_slices_.push_back(std::move(slice));
}

RunMetrics MetricsCollector::finish() const {
  RunMetrics metrics;
  metrics.completed_tasks = completed_tasks_;
  metrics.elapsed_time_us = elapsed_time_us_;
  metrics.worker_idle_time_us = worker_idle_time_us_;
  metrics.task_observations = task_observations_;
  metrics.group_observations = group_observations_;
  metrics.group_runnable_events = group_runnable_events_;
  metrics.group_service_slices = group_service_slices_;

  for (const TaskObservation& task : metrics.task_observations) {
    if (task.group_index >= metrics.group_observations.size()) {
      continue;
    }
    metrics.group_observations[task.group_index].cpu_runtime_us += task.cpu_runtime_us;
  }

  if (elapsed_time_us_ != 0) {
    metrics.throughput_tasks_per_sec =
        static_cast<double>(completed_tasks_) * 1000000.0 / static_cast<double>(elapsed_time_us_);
  }

  if (!wakeup_latencies_us_.empty()) {
    std::vector<uint64_t> sorted = wakeup_latencies_us_;
    std::sort(sorted.begin(), sorted.end());
    const std::size_t index = ((sorted.size() - 1) * static_cast<std::size_t>(99)) / 100;
    metrics.p99_wakeup_latency_us = sorted[index];
  }

  return metrics;
}

} // namespace schedlab::benchmark

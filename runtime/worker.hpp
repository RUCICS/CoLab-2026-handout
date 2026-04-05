#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "devices/device_model.hpp"
#include "runtime/context.hpp"
#include "runtime/runtime_workload_context.hpp"
#include "runtime/sync_registry.hpp"
#include "runtime/task_runtime.hpp"
#include "runtime/tick_source.hpp"
#include "runtime/timer_queue.hpp"
#include "schedlab/scheduler.hpp"
#include "schedlab/workload_api.hpp"

namespace schedlab::runtime {

class WorkerSystemView;
class WorkerPoolSystemView;

class Worker {
public:
  struct ObservedTaskMetrics {
    uint64_t release_time_us = 0;
    uint64_t completion_time_us = 0;
    uint64_t cpu_runtime_us = 0;
  };

  struct ObservedRunnableEvent {
    uint64_t time_us = 0;
    uint64_t group_id = 0;
    int delta = 0;
  };

  struct ObservedServiceSlice {
    uint64_t start_time_us = 0;
    uint64_t end_time_us = 0;
    uint64_t group_id = 0;
  };

  struct ObservedMetrics {
    std::vector<uint64_t> wakeup_latencies_us;
    std::unordered_map<int, uint64_t> worker_idle_time_us;
    std::unordered_map<uint64_t, ObservedTaskMetrics> task_metrics_by_id;
    std::vector<ObservedRunnableEvent> runnable_events;
    std::vector<ObservedServiceSlice> service_slices;
  };

  using TaskMain = void (*)(WorkloadContext& workload, void* arg);
  struct Config {
    uint64_t compute_chunk_units = 32;
    uint64_t tick_interval_us = 0;
  };

  explicit Worker(Scheduler& scheduler, int worker_id);
  Worker(Scheduler& scheduler, int worker_id, Config config);
  ~Worker();

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;
  Worker(Worker&&) = delete;
  Worker& operator=(Worker&&) = delete;

  uint64_t spawn(TaskMain main, void* arg, TaskAttrs attrs = {});
  void run();
  ObservedMetrics observed_metrics() const;

private:
  friend class WorkerSystemView;

  class RescheduleSource final : public RescheduleRequestSource {
  public:
    explicit RescheduleSource(Worker& worker) noexcept;
    bool consume_reschedule_request() noexcept override;

  private:
    Worker* worker_ = nullptr;
  };

  class ExecutionContext final : public ExecutionContextSource {
  public:
    explicit ExecutionContext(Worker& worker) noexcept;

    void advance_runtime_time(uint64_t runtime_us) noexcept override;
    void record_compute_service(uint64_t elapsed_us) noexcept override;
    int current_worker_id() const noexcept override;
    bool schedule_sleep(uint64_t task_token, uint64_t delay_us) noexcept override;
    bool submit_device_call(uint64_t task_token, DeviceId device,
                            const DeviceRequest& request) noexcept override;
    void configure_device_rate(DeviceId device, uint64_t rate) noexcept override;
    void configure_device_parallelism(DeviceId device, uint64_t parallel) noexcept override;
    bool declare_event(std::string_view name) noexcept override;
    bool declare_latch(std::string_view name, uint64_t count) noexcept override;
    SyncWaitResult wait_sync(RuntimeTask& task, std::string_view name) noexcept override;
    SyncActionResult signal_event(std::string_view name) noexcept override;
    SyncActionResult arrive_latch(std::string_view name) noexcept override;

  private:
    Worker* worker_ = nullptr;
  };

  struct TaskRecord {
    struct EntryFrame {
      Worker* worker = nullptr;
      TaskRecord* record = nullptr;
    };

    TaskRecord(uint64_t task_id, TaskMain main, void* arg, TaskAttrs attrs,
               RescheduleSource& reschedule_source, ExecutionContext& execution_context,
               uint64_t compute_chunk_units);

    RuntimeTask task;
    TaskMain main = nullptr;
    void* arg = nullptr;
    RuntimeWorkloadContext workload;
    EntryFrame frame{};
    TaskContext context;
    uint64_t release_time_us = 0;
    uint64_t completion_time_us = 0;
    uint64_t blocked_since_steady_us = 0;
    std::optional<uint64_t> pending_wakeup_steady_us;
    bool pending_sync_wakeup = false;
    int pending_sync_source_worker = -1;
  };

  static void task_entry(void* raw);

  TaskRecord& lookup_task_or_fail(uint64_t task_id);
  const TaskRecord& lookup_task_or_fail(uint64_t task_id) const;
  SystemView& system_view() const;

  void run_loop();
  bool dispatch_one(uint64_t task_id);
  void initialize_scheduler_if_needed();
  void enqueue_task_ready(uint64_t task_id, ReadyContext ctx);
  void requeue_local_task(uint64_t task_id);
  std::optional<uint64_t> select_next_task();
  std::optional<uint64_t> remove_local_ready_task(uint64_t task_id);
  std::optional<uint64_t> remove_default_steal_task(int victim_worker_id);
  std::optional<uint64_t> remove_specific_steal_task(int victim_worker_id, uint64_t task_id);
  bool consume_tick_reschedule_request();
  bool all_tasks_exited() const;
  void drain_due_events();
  bool advance_idle_until_progress();
  void wake_sync_task(uint64_t task_id);
  void record_runnable_delta(uint64_t time_us, uint64_t group_id, int delta);
  void record_service_slice(uint64_t start_time_us, uint64_t end_time_us, uint64_t group_id);

  Scheduler* scheduler_ = nullptr;
  Config config_{};
  int worker_id_ = -1;
  uint64_t next_task_id_ = 1;
  std::optional<uint64_t> current_task_id_;
  FrameworkContext framework_;
  TimerQueue timer_queue_;
  devices::FifoDeviceModel devices_;
  RescheduleSource reschedule_source_;
  TickSource tick_source_;
  ExecutionContext execution_context_;
  SyncRegistry sync_registry_;
  std::unordered_map<uint64_t, std::unique_ptr<TaskRecord>> tasks_;
  std::deque<uint64_t> local_ready_queue_;
  std::vector<uint64_t> wakeup_latencies_us_;
  std::vector<Worker::ObservedRunnableEvent> runnable_events_;
  std::vector<Worker::ObservedServiceSlice> service_slices_;
  uint64_t idle_time_us_ = 0;
  bool pending_external_resched_ = false;
  bool scheduler_initialized_ = false;
  mutable std::unique_ptr<WorkerSystemView> system_view_;
};

class WorkerPool {
public:
  using ObservedMetrics = Worker::ObservedMetrics;
  using TaskMain = Worker::TaskMain;

  WorkerPool(Scheduler& scheduler, int worker_count, Worker::Config config = {});
  ~WorkerPool();

  WorkerPool(const WorkerPool&) = delete;
  WorkerPool& operator=(const WorkerPool&) = delete;
  WorkerPool(WorkerPool&&) = delete;
  WorkerPool& operator=(WorkerPool&&) = delete;

  uint64_t spawn(TaskMain main, void* arg, TaskAttrs attrs = {});
  void start();
  void join();
  void run();
  ObservedMetrics observed_metrics() const;

private:
  friend class WorkerPoolSystemView;

  class TaskRescheduleSource final : public RescheduleRequestSource {
  public:
    explicit TaskRescheduleSource(WorkerPool& pool) noexcept;
    bool consume_reschedule_request() noexcept override;

    void bind_task_id(uint64_t task_id) noexcept;

  private:
    WorkerPool* pool_ = nullptr;
    std::atomic<uint64_t> task_id_{0};
  };

  class TaskExecutionContext final : public ExecutionContextSource {
  public:
    explicit TaskExecutionContext(WorkerPool& pool) noexcept;

    void bind_task_id(uint64_t task_id) noexcept;

    void advance_runtime_time(uint64_t runtime_us) noexcept override;
    void record_compute_service(uint64_t elapsed_us) noexcept override;
    int current_worker_id() const noexcept override;
    bool schedule_sleep(uint64_t task_token, uint64_t delay_us) noexcept override;
    bool submit_device_call(uint64_t task_token, DeviceId device,
                            const DeviceRequest& request) noexcept override;
    void configure_device_rate(DeviceId device, uint64_t rate) noexcept override;
    void configure_device_parallelism(DeviceId device, uint64_t parallel) noexcept override;
    bool declare_event(std::string_view name) noexcept override;
    bool declare_latch(std::string_view name, uint64_t count) noexcept override;
    SyncWaitResult wait_sync(RuntimeTask& task, std::string_view name) noexcept override;
    SyncActionResult signal_event(std::string_view name) noexcept override;
    SyncActionResult arrive_latch(std::string_view name) noexcept override;

  private:
    WorkerPool* pool_ = nullptr;
    std::atomic<uint64_t> task_id_{0};
  };

  struct TaskRecord {
    struct EntryFrame {
      TaskRecord* record = nullptr;
    };

    TaskRecord(uint64_t task_id, TaskMain main_fn, void* raw_arg, TaskAttrs attrs, WorkerPool& pool,
               uint64_t compute_chunk_units);

    RuntimeTask task;
    TaskMain main = nullptr;
    void* arg = nullptr;
    TaskRescheduleSource reschedule_source;
    TaskExecutionContext execution_context;
    RuntimeWorkloadContext workload;
    EntryFrame frame{};
    TaskContext context;
    uint64_t release_time_us = 0;
    uint64_t completion_time_us = 0;
    std::atomic<uint64_t> blocked_since_steady_us{0};
    std::atomic<int> bound_worker_id{-1};
    std::atomic<bool> actively_running{false};
    std::atomic<uint64_t> pending_wakeup_steady_us{0};
    std::atomic<bool> pending_sync_wakeup{false};
    std::atomic<int> pending_sync_source_worker{-1};
  };

  struct WorkerState {
    int worker_id = -1;
    FrameworkContext framework;
    TimerQueue timer_queue;
    devices::FifoDeviceModel devices;
    TickSource tick_source{0};
    std::optional<uint64_t> current_task_id;
    std::atomic<bool> has_pending_events{false};
    std::vector<uint64_t> wakeup_latencies_us;
    uint64_t idle_time_us = 0;
    std::deque<uint64_t> local_ready_queue;
    std::atomic<bool> pending_external_resched{false};
  };

  static void task_entry(void* raw);

  TaskRecord& lookup_task_or_fail(uint64_t task_id);
  const TaskRecord& lookup_task_or_fail(uint64_t task_id) const;
  SystemView& system_view() const;

  bool all_tasks_exited() const;
  bool any_tasks_actively_running() const;
  bool any_pending_events() const;
  bool dispatch_one(WorkerState& state, uint64_t task_id);
  void initialize_scheduler_if_needed();
  void enqueue_task_ready(uint64_t task_id, ReadyContext ctx);
  void requeue_local_task(WorkerState& state, uint64_t task_id);
  std::optional<uint64_t> select_next_task(WorkerState& state);
  std::optional<uint64_t> remove_local_ready_task(WorkerState& state, uint64_t task_id);
  std::optional<uint64_t> remove_default_steal_task(int victim_worker_id);
  std::optional<uint64_t> remove_specific_steal_task(int victim_worker_id, uint64_t task_id);
  bool consume_tick_reschedule_request(uint64_t task_id);
  void drain_due_events(WorkerState& state);
  void refresh_pending_events(WorkerState& state);
  bool advance_idle_until_progress(WorkerState& state);
  void run_worker_loop(int worker_index);
  void wake_sync_task(uint64_t task_id, int source_worker_id);
  void record_runnable_delta(uint64_t time_us, uint64_t group_id, int delta);
  void record_service_slice(uint64_t start_time_us, uint64_t end_time_us, uint64_t group_id);

  WorkerState& state_for_task_or_fail(uint64_t task_id);
  const WorkerState& state_for_task_or_fail(uint64_t task_id) const;

  Scheduler* scheduler_ = nullptr;
  Worker::Config config_{};
  std::vector<std::unique_ptr<WorkerState>> worker_states_;
  std::vector<std::thread> worker_threads_;
  std::unordered_map<uint64_t, std::unique_ptr<TaskRecord>> tasks_;
  SyncRegistry sync_registry_;
  std::atomic<uint64_t> next_task_id_{1};
  std::atomic<uint64_t> total_tasks_{0};
  std::atomic<uint64_t> exited_tasks_{0};
  std::atomic<uint64_t> progress_epoch_{0};
  std::atomic<uint64_t> scheduler_calls_in_flight_{0};
  std::atomic<uint64_t> dispatches_in_flight_{0};
  std::atomic<bool> started_{false};
  std::atomic<bool> stop_requested_{false};
  mutable std::mutex task_mu_;
  mutable std::mutex observed_mu_;
  std::vector<Worker::ObservedRunnableEvent> runnable_events_;
  std::vector<Worker::ObservedServiceSlice> service_slices_;
  bool scheduler_initialized_ = false;
  mutable std::unique_ptr<WorkerPoolSystemView> system_view_;
};

} // namespace schedlab::runtime

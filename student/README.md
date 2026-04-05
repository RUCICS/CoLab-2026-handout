# Student Implementation Module

This directory is the student-owned implementation area for `Schedlab`.

It now contains both:
- Phase 1 mechanism code for the coroutine/runtime path
- Phase 2 scheduler policy code for the simulator-facing scheduler interface

## What You Own

You may change anything under `student/`, including adding helper headers or
source files for:
- coroutine context management
- single-worker runtime loop and wakeup logic
- runqueue data structures
- per-task scheduling metadata
- placement and steal policies
- locking or atomic schemes inside your scheduler

## What You Do Not Own

Do not modify:
- `include/schedlab/`
- `runtime/`
- `devices/`
- `benchmark/`
- `workloads/`
- `tools/`

The framework owns task lifetime, stacks, blocking and wakeup semantics,
preemption delivery, worker threads, and correctness checks.

## Suggested Development Order

1. Make `student/context.*` correct.
2. Make `student/runtime.*` pass the single-worker tests.
3. Implement a simple single-worker policy in `student/scheduler.*`.
4. Extend `student/scheduler.*` for multi-worker correctness and optimization.

See [student-guide.md](/home/starrydream/ICS2/CoLab-2026/docs/student-guide.md) for the full contract and [ta-debugging.md](/home/starrydream/ICS2/CoLab-2026/docs/ta-debugging.md) for the invariant-driven debugging workflow used by the course staff.

# CoLab 2026：构建你自己的多核调度器

> 操作系统每秒做出数百万次调度决策，决定哪个任务在哪个核上运行。这些决策看似简单——不就是从队列里挑一个任务吗？但当你面对多个核心、不同优先级的任务、以及吞吐量和响应延迟之间不可调和的矛盾时，你会发现这个问题远比想象中有趣。
>
> 在这个实验中，你将从零搭建一个用户态协程运行时，然后在其上实现自己的多核调度策略，并在公开的 benchmark 上与同学们一较高下。

> [!WARNING]
> 如果你发现了 Bug，请提交 Issue；有任何疑问，请在讨论区提出。
>
> 也欢迎你参与改进框架代码或帮助同学答疑，这部分贡献可被计入额外加分。

> [!NOTE]
> 本实验为期 4 周。Phase 1（运行时实现）建议在前 1–2 周完成，Phase 2（调度策略与打榜）在后 2–3 周进行。

## 这个实验在做什么

你在 CSAPP 中学过异常控制流——中断、信号、上下文切换——这些概念告诉你操作系统"能做什么"。但你可能从未亲手实现过一次上下文切换，也没有真正体会过"切换一次要花多少时间"意味着什么。

CoLab 把这件事拆成两层：

**第一层是机制。** 你要用 `ucontext` 为每个任务建立独立的栈，实现 framework 和 task 之间的上下文切换，搭建一个能让多个协程轮转执行的 worker 循环。这一层做完之后，你会对"上下文切换到底保存和恢复了什么"有非常具体的理解——不再是教科书上的示意图，而是你自己写的、能跑的代码。

**第二层是策略。** 运行时搭好之后，你要在一个确定性仿真引擎（DES）上实现自己的多核调度算法，挑选一个赛道参加打榜竞赛。三条赛道分别考察吞吐量、延迟和公平性——它们之间存在结构性的矛盾，没有一个调度器能同时在三条赛道上称霸。你需要选定一个方向，做出自己的 trade-off。

这两层的关系并不是"先做完一个再做另一个然后忘掉"。Phase 1 中你亲手实现的上下文切换和 worker 循环，正是 Phase 2 中调度器运行的基础设施。你在 Phase 1 中建立的对 switch cost、task 状态转换、run loop 结构的理解，会直接影响你在 Phase 2 中做策略设计时的判断力。

## 实验结构

CoLab 分为两个阶段，Phase 1 是 Phase 2 的前置门槛。

### Phase 1：运行时（Runtime）

你需要实现两个文件：

- **`student/context.cc`**——上下文切换。为每个 task 分配独立的栈，实现 framework 到 task 的切换（`resume`）和 task 回到 framework 的让出（`yield`），以及 task 正常返回后的 finished 语义。核心是理解 `ucontext` 的 `makecontext` / `swapcontext`，以及 entry trampoline 的作用。

- **`student/runtime.cc`**——Worker 运行循环。一个 worker 要不断地从 ready 队列中选任务、切换过去执行、处理任务的阻塞和唤醒、响应 tick 驱动的抢占请求、推进时间事件。这些函数合在一起构成了一个完整的用户态调度 loop。

你不需要处理真实的多线程同步，不需要写信号处理函数，也不需要改动 `runtime/` 目录下的框架代码。Phase 1 的关注点是单个 worker 内部的执行逻辑。

**Phase 1 的完成标志是通过全部 runtime 测试。** 测试从最基本的编译检查开始，逐步验证上下文切换、单 worker 循环、阻塞唤醒、tick 抢占等功能。建议按以下顺序逐个击破：

| 顺序 | 测试 | 验证内容 |
|:---:|------|----------|
| 1 | `headers_smoke_test` | 头文件能正确编译 |
| 2 | `student_runtime_surface_test` | API 接口签名正确 |
| 3 | `context_test` | 上下文切换：resume、yield、task 返回 |
| 4 | `single_worker_loop_test` | Worker 主循环能驱动任务执行完毕 |
| 5 | `single_worker_stall_test` | 任务阻塞与唤醒的正确处理 |
| 6 | `tick_preemption_test` | Tick 驱动的抢占机制 |
| 7 | `student_scheduler_smoke_test` | 调度器基本功能验证 |

通过全部 7 个测试后，Phase 2 的打榜功能将解锁。

> [!TIP]
> 建议先专注于 `context.cc`，让 `context_test` 通过后再进入 `runtime.cc`。上下文切换是整个运行时的地基——如果 `resume` 和 `yield` 不正确，后面的所有测试都不可能通过。

### Phase 2：调度策略（Scheduling）

Phase 2 在一个确定性仿真引擎上进行。仿真引擎接收你的调度器和一组 workload 描述文件，以虚拟时间驱动任务的创建、执行、阻塞、唤醒和完成，最终计算出你的调度器在各项指标上的得分。因为仿真是完全确定性的，相同的调度器对相同的 workload 总是产生相同的结果——这让你可以放心地做对比实验和回归测试。

你需要实现 `student/scheduler.cc` 中的调度策略。调度器通过以下接口与运行时交互：

| 接口 | 决策 |
|------|------|
| `select_worker` | 一个新就绪的 task 应该被放到哪个 worker 的队列上？ |
| `pick_next` | 当前 worker 接下来应该运行队列中的哪个 task？ |
| `on_tick` | 时钟中断到来时，是否应该抢占当前正在运行的 task？ |
| `should_preempt` | 一个 task 刚被唤醒，它是否应该立刻抢占当前 worker 上正在运行的 task？ |
| `steal` | 当前 worker 空闲了，能否从别的 worker 的队列里偷一个 task 过来？ |

通过 `TaskView` 你可以观察到每个 task 的运行时统计（累计运行时间、当前 time slice、权重、阻塞次数等），通过 `SystemView` 你可以看到全局状态（各 worker 的队列长度、拓扑节点、迁移开销等）。接口的完整定义在 `include/schedlab/scheduler.hpp`，数据结构定义在 `include/schedlab/` 下的对应头文件中。

**选择你的赛道。** 三条赛道分别考察不同的调度目标：

- **Throughput（吞吐量）**——在给定的 workload 下尽快完成所有任务。考验你的负载均衡、work stealing、减少核心空闲时间的能力。
- **Latency（延迟）**——降低交互型任务的响应延迟（尤其是尾部延迟 P99）。考验你识别和优先处理短任务、交互任务的能力，但要小心不要让吞吐量掉得太多——延迟赛道有吞吐量的保底约束。
- **Fairness（公平性）**——让不同权重的任务按比例获得 CPU 时间。考验你对 weighted fair scheduling 的理解，但过度追求公平可能会牺牲整体效率——如果你的 makespan 超过 baseline 的 2 倍，分数会被封顶。

你只需要选择一条赛道参加。每条赛道下有多个 workload 场景，你的赛道得分是各场景得分的加权几何平均值。每个场景的得分是你的调度器的指标与 baseline（Round-Robin）调度器指标的比值——也就是说，你需要做得比最朴素的 RR 更好。

**运行 benchmark：**

```bash
# 查看所有可用的 workload
python tools/bench.py list

# 调试模式：运行单个场景，查看详细输出
python tools/bench.py debug --scenario public/throughput/batch_mix

# 正式评分：运行整条赛道
python tools/bench.py release --track throughput
```

> [!IMPORTANT]
> 正式评分模式（`release`）会先运行 correctness gate 场景。如果你的调度器在 gate 场景上产生了错误的行为（如死锁、任务饿死），整条赛道会直接判定为失败。请确保你的调度器至少在基本功能上是正确的，再去追求分数。

## 环境要求

- 操作系统：x86-64 架构的 Linux（推荐 Ubuntu 22.04 或更高版本）
  - Windows 用户请使用 WSL 2
  - macOS 用户请使用 Linux 虚拟机或课程提供的服务器环境
- 编译器：支持 C++17 的 g++ 或 clang
- CMake 3.16+
- Python 3.8+
- Make, Git

## 快速开始

```bash
# 克隆仓库（替换为你的仓库地址）
git clone your-assignment-repo-url
cd CoLab

# 构建项目
mkdir -p build && cd build
cmake ..
make -j$(nproc)

# 运行 Phase 1 测试（此时应全部失败，这是正常的）
ctest --test-dir build

# 等你实现了 Phase 1 之后，试试 Phase 2
python tools/bench.py list
```

## 项目结构

```
CoLab/
├── student/                  # 你需要完成的代码（唯一允许修改的目录）
│   ├── context.h / .cc       # Phase 1：上下文切换
│   ├── runtime.h / .cc       # Phase 1：Worker 运行循环
│   └── scheduler.h / .cc     # Phase 2：调度策略
├── include/schedlab/         # 公开接口定义（只读，请仔细阅读）
│   ├── scheduler.hpp         # 调度器接口
│   ├── task_view.hpp         # 任务状态视图
│   ├── queue_view.hpp        # 队列只读视图
│   └── system_view.hpp       # 全局系统状态视图
├── tests/                    # 测试用例
├── runtime/                  # 运行时框架（只读）
├── simulator/                # DES 仿真引擎（只读）
├── benchmark/                # Benchmark runner
├── workloads/public/         # 公开的 workload 场景
│   ├── throughput/           # 吞吐量赛道的场景
│   ├── latency/              # 延迟赛道的场景
│   └── fairness/             # 公平性赛道的场景
├── tools/                    # 辅助工具（bench.py 等）
└── docs/                     # 详细文档
    ├── phase1.md             # Phase 1 详细指导
    └── phase2.md             # Phase 2 详细指导
```

`student/` 是你唯一需要修改的目录。`include/schedlab/` 下的头文件定义了你要实现的接口和你能观察到的数据结构，这些文件值得反复阅读。`workloads/public/` 下的场景文件用一种声明式 DSL 描述——你不需要掌握它的全部语法，但能读懂场景的结构（有几组任务、每组做什么、用什么拓扑）会帮助你理解 benchmark 的行为。

## 计分标准

总分由 Phase 1 和 Phase 2 两部分组成：

$$
\text{总分} = \text{Phase 1 分数} \times 40\% + \text{Phase 2 分数} \times 60\%
$$

**Phase 1（40%）** 按测试通过情况计分。7 个测试各有权重，全部通过即获得 Phase 1 满分。

**Phase 2（60%）** 按你选定赛道的打榜得分计算。打榜得分的计算方式是：你的赛道分数（各场景得分的加权几何平均值，乘以 1000）根据排名映射到最终分数。具体的排名-分数映射规则将在打榜开放时公布。

> [!IMPORTANT]
> **Phase 1 是 Phase 2 的硬性前置条件。** 只有 Phase 1 的全部测试通过后，你的 Phase 2 打榜成绩才会被计入。如果 Phase 1 未完成，Phase 2 部分计 0 分。

## 时间规划

| 周次 | 建议目标 |
|:---:|---------|
| 第 1 周 | 完成 `context.cc`，通过 `context_test` |
| 第 2 周 | 完成 `runtime.cc`，通过全部 Phase 1 测试；开始阅读调度器接口和 workload 场景 |
| 第 3 周 | 实现调度策略，在选定赛道上反复调优 |
| 第 4 周 | 最终调优与提交 |

这只是建议节奏。如果你对 `ucontext` 已经有经验，Phase 1 可能在几天内就能完成；如果你是第一次接触协程和上下文切换，建议给 Phase 1 留足时间——在这一阶段建立的理解会让 Phase 2 事半功倍。

## 学术诚信与 AI 使用

### 关于协作的边界

讨论思路、交流对系统机制的理解、一起分析某个 bug 的成因——这些都是正常的、被鼓励的学习方式。但直接复制他人的调度策略代码、或将自己的代码提供给他人复制，违背了实验的初衷。我们会对提交的代码进行查重检测。

### 关于 AI 工具

我们鼓励你使用 AI 工具。在面对一个你从未接触过的 API（比如 `ucontext`）时，让 AI 解释它的工作原理、帮你理解报错信息、讨论设计方案的优劣——这些都是高效的学习方式，也是你未来工作中会持续使用的技能。

但你需要意识到一个现实：AI 生成的 `ucontext` 代码经常在边界条件上出错——栈对齐、entry trampoline 的返回语义、`makecontext` 的参数传递——这些细节如果不理解原理，调试起来会非常痛苦。Phase 1 的设计意图就是让你在这个过程中建立对上下文切换机制的真实理解。如果你跳过了这个过程，Phase 2 中你对 switch cost 和 migration cost 的直觉就缺少了根基。

至于 Phase 2 的调度策略，我们更关注的是你的策略背后的思考——为什么选择这个赛道、为什么采用这种队列结构、在什么场景下你做了什么 trade-off。好的调度策略不是从某个地方抄来的参数组合，而是你对 workload 特征的分析和对调度机制的理解的产物。

## 参考资料

1. Remzi H. Arpaci-Dusseau and Andrea C. Arpaci-Dusseau, *Operating Systems: Three Easy Pieces* (OSTEP), Chapters on Scheduling
2. Randal E. Bryant and David R. O'Hallaron, *Computer Systems: A Programmer's Perspective* (CSAPP), Chapter 8: Exceptional Control Flow
3. `man ucontext` / `man makecontext` / `man swapcontext`
4. Humphries et al., *ghOSt: Fast & Flexible User-Space Delegation of Linux Scheduling*, SOSP 2021
5. Linux `sched_ext` framework documentation

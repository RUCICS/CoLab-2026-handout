# CoLab 2026：构建你自己的多核调度器

> 操作系统每秒做出数百万次调度决策，决定哪个任务在哪个核上运行。这些决策看似简单——不就是从队列里挑一个任务吗？但当你面对多个核心、不同优先级的任务、以及吞吐量和响应延迟之间不可调和的矛盾时，你会发现这个问题远比想象中有趣。
>
> 在这个实验中，你将实现自己的多核调度策略，并在公开的 benchmark 上与同学们一较高下。

> [!WARNING]
> 如果你发现了 Bug，请提交 Issue；有任何疑问，请在讨论区提出。
>
> 也欢迎你参与改进框架代码或帮助同学答疑，这部分贡献可被计入额外加分。

## 这个实验在做什么

你在 CSAPP 中学过异常控制流——中断、信号、上下文切换——这些概念告诉你操作系统"能做什么"。CoLab 让你把这些知识用起来：在一个确定性仿真引擎（DES）上实现自己的多核调度算法，然后选一条赛道参加打榜竞赛。

> 什么是确定性仿真引擎（DES）？
> 真机环境里存在大量扰动：后台进程抢占、CPU 频率变化、容器资源争用、网络和 I/O 抖动都会改变程序行为。DES 提供一个可重放、可控制、可验证的执行环境。相同的 workload、相同的初始状态、相同的事件顺序会得到一致的结果，所以它很适合调试调度策略、做回归实验和比较不同算法。

三条赛道分别考察吞吐量、延迟和公平性——它们之间存在结构性的矛盾，没有一个调度器能同时在三条赛道上称霸。你需要选定一个方向，做出自己的 trade-off。

## 基本模型

每个 `.sched` workload 会定义一组 worker 和一批 task。worker 可以理解成模拟出来的 CPU 核，task 是调度器要决定放置、运行、抢占和迁移的执行单元。

task 的创建、阻塞、唤醒和完成都由 framework 与 simulator 驱动；你的调度器负责回答几个关键问题：新 task 进入哪个 worker、本地队列里先跑谁、时钟到来时是否抢占、唤醒时是否立刻抢占、空闲 worker 是否去偷任务。

worker 数量、`cpu_rate`、`switch_cost`、`migration_cost`、拓扑 `node`、设备参数，以及各类 task 的行为模式，都写在 workload 的 `.sched` 文件里。`switch_cost` 表示同一 worker 上切换任务的开销，`migration_cost` 表示 task 在 worker 之间迁移的额外代价；它们会直接影响不同调度策略的得分表现。

## 调度器接口

你需要实现 `student/scheduler.h` 和 `student/scheduler.cc` 中的调度策略。这两个文件是你唯一需要修改的文件。

`benchmark/baseline_rr.cc` 中的 `BaselineRoundRobinScheduler` 是一个完整、正确、可以直接运行的参考实现。它的目标是给你一个清晰的起点，而更高的分数来自你自己的策略设计。

调度器通过以下接口与运行时交互：

| 接口 | 决策 |
|------|------|
| `select_worker` | 一个新就绪的 task 应该放到哪个 worker 的队列上？ |
| `pick_next` | 当前 worker 接下来应该运行队列中的哪个 task？ |
| `on_tick` | 时钟中断到来时，是否应该抢占当前正在运行的 task？ |
| `should_preempt` | 一个 task 刚被唤醒，它是否应该立刻抢占当前 worker 上正在运行的 task？ |
| `steal` | 当前 worker 空闲了，能否从别的 worker 的队列里偷一个 task 过来？ |

通过 `TaskView` 你可以观察到每个 task 的运行时统计（累计运行时间、当前 time slice、权重、阻塞次数等），通过 `SystemView` 你可以看到全局状态（各 worker 的队列长度、拓扑节点、迁移开销等）。接口的完整定义在 `include/schedlab/scheduler.hpp`，数据结构定义在 `include/schedlab/` 下的对应头文件中。

接口、视图结构、参考实现入口和常见调度思路见 `docs/scheduler-guide.md`。

## 赛道

三条赛道分别考察不同的调度目标：

- **Throughput（吞吐量）**——在给定的 workload 下尽快完成所有任务。考验你的负载均衡、work stealing、减少核心空闲时间的能力。
- **Latency（延迟）**——降低交互型任务的响应延迟（尤其是尾部延迟 P99）。考验你识别和优先处理短任务、交互任务的能力，但要注意延迟赛道有吞吐量的保底约束。
- **Fairness（公平性）**——让不同权重的任务按比例获得 CPU 时间。考验你对 weighted fair scheduling 的理解，但如果你的 makespan 超过 baseline 的 2 倍，分数会被封顶。

你只需要选择一条赛道参加。每条赛道下有多个 workload 场景，你的赛道得分是各场景得分的加权几何平均值。每个场景的得分是你的调度器指标与 baseline（Round-Robin）调度器指标的比值——也就是说，你需要做得比最朴素的 RR 更好。

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

# 查看所有可用的 workload 场景
python tools/bench.py list

# 调试模式：运行单个场景，查看详细输出
python tools/bench.py debug --scenario public/throughput/batch_mix

# 正式评分：运行整条赛道
python tools/bench.py release --track throughput
```

> [!IMPORTANT]
> 正式评分模式（`release`）会先运行 correctness gate 场景。如果你的调度器在 gate 场景上产生了错误行为（如死锁、任务饿死），整条赛道会直接判定为失败。请确保你的调度器在基本功能上是正确的，再去追求分数。

## 项目结构

```
CoLab/
├── student/                  # 你需要完成的代码（唯一允许修改的目录）
│   └── scheduler.h / .cc    # 调度策略
├── include/schedlab/         # 公开接口定义（只读，请仔细阅读）
│   ├── scheduler.hpp         # 调度器接口
│   ├── task_view.hpp         # 任务状态视图
│   ├── queue_view.hpp        # 队列只读视图
│   └── system_view.hpp       # 全局系统状态视图
├── runtime/                  # 运行时框架（只读）
├── simulator/                # DES 仿真引擎（只读）
├── workloads/public/         # 公开的 workload 场景
│   ├── throughput/           # 吞吐量赛道的场景
│   ├── latency/              # 延迟赛道的场景
│   └── fairness/             # 公平性赛道的场景
├── tools/                    # 辅助工具（bench.py 等）
├── colab                     # 打榜平台 CLI 工具
└── docs/                     # 详细文档
```

`include/schedlab/` 下的头文件定义了你要实现的接口和你能观察到的数据结构，这些文件值得反复阅读。`workloads/public/` 下的场景文件用一种声明式 DSL 描述——你读懂场景的结构、arrival 模式、同步关系和代价模型之后，调度器的 trade-off 会清晰很多。

公开 workload 的阅读方法、`.sched` 基本语法、常见字段含义和例子见 `docs/workload-guide.md`。

## 打榜平台

CoLab 使用统一的打榜平台 LabKit 进行提交和排名。项目根目录下的 `colab` 是与平台交互的命令行工具。

### 认证

首次使用前，需要将当前设备绑定到你的课程账号：

```bash
./colab auth
```

CLI 会生成本地密钥对，并在终端显示一个验证码和浏览器链接。打开链接，输入验证码并使用微人大登录，即可完成绑定。认证只需做一次，后续提交和查询都会自动使用已绑定的身份。

### 选择赛道

确定你要参加的赛道后，用 `track` 命令声明：

```bash
./colab track throughput
./colab track latency
./colab track fairness
```

### 提交

按要求准备好文件后，用 `submit` 命令提交：

```bash
./colab submit student/scheduler.h student/scheduler.cc
```

提交后 CLI 会自动等待评测完成并输出结果。如果不想等，可以加 `--detach`，之后再用 `history` 查看。

### 查看排行榜

```bash
./colab board
```

### 查看提交历史

```bash
# 列出所有提交记录
./colab history

# 查看某次提交的详情
./colab history <submission-id>
```

### 修改昵称

```bash
./colab nick your-name
```

### 其他

运行 `./colab --help` 可以查看所有可用命令。

## 计分标准

课程总评由性能分和打榜分组成：

$$
\text{总分} = 0.85 \times \text{性能分} + 0.15 \times \text{打榜分}
$$

其中，选定赛道的最终评测倍率记为 `r`；赛道内排名百分位记为 `p`，`p = 0` 表示第一名，`p = 1` 表示最后一名。LabKit 显示的赛道分数仍为 `1000 × r`。

### 评测说明

> [!IMPORTANT]
> - 最终榜上的评测会使用非 `public` 目录中的隐藏用例。调度策略应当面向赛道目标和 workload 结构设计。
> - 打榜开放后，每位同学每天有提交次数限制。建议先在本地完成正确性验证和针对性调优，再进行正式提交。

### 性能分（85%）

性能分根据选定赛道的最终评测倍率 `r` 计算。相邻锚点之间采用线性插值。

#### Throughput

| 赛道倍率 `r` | 性能分 |
| --- | ---: |
| `r <= 0.80` | `0` |
| `r = 1.00` | `35` |
| `r = 1.15` | `55` |
| `r = 1.30` | `72` |
| `r = 1.45` | `85` |
| `r = 1.60` | `93` |
| `r >= 1.85` | `100` |

#### Latency

| 赛道倍率 `r` | 性能分 |
| --- | ---: |
| `r <= 0.90` | `0` |
| `r = 1.00` | `40` |
| `r = 1.10` | `62` |
| `r = 1.20` | `80` |
| `r = 1.30` | `92` |
| `r >= 1.40` | `100` |

#### Fairness

| 赛道倍率 `r` | 性能分 |
| --- | ---: |
| `r <= 0.50` | `0` |
| `r = 0.80` | `20` |
| `r = 1.00` | `38` |
| `r = 1.20` | `58` |
| `r = 1.40` | `75` |
| `r = 1.60` | `88` |
| `r >= 1.90` | `100` |

### 打榜分（15%）

打榜分根据选定赛道内的排名百分位 `p` 计算：

| 排名百分位 `p` | 打榜分 |
| --- | ---: |
| `0 <= p <= 0.10` | `100` |
| `0.10 < p <= 0.30` | 从 `100` 线性插值到 `85` |
| `0.30 < p <= 0.60` | 从 `85` 线性插值到 `60` |
| `0.60 < p <= 1.00` | 从 `60` 线性插值到 `0` |

## 学术诚信与 AI 使用

### 关于协作

讨论思路、交流对调度机制的理解、一起分析某个 bug 的成因——这些都是被鼓励的学习方式。但直接复制他人的调度策略代码、或将自己的代码提供给他人复制，违背了实验的初衷。我们会对提交的代码进行查重检测。

### 关于 AI 工具

我们鼓励你使用 AI 工具。让 AI 解释调度算法的工作原理、帮你理解报错信息、讨论设计方案的优劣——这些都是高效的学习方式，也是你未来工作中会持续使用的技能。

但我们更关注的是你的策略背后的思考——为什么选择这个赛道、为什么采用这种队列结构、在什么场景下你做了什么 trade-off。好的调度策略不是从某处抄来的参数组合，而是你对 workload 特征的分析和对调度机制的理解的产物。

## 参考资料

1. Remzi H. Arpaci-Dusseau and Andrea C. Arpaci-Dusseau, *Operating Systems: Three Easy Pieces* (OSTEP), Chapters on Scheduling
2. Randal E. Bryant and David R. O'Hallaron, *Computer Systems: A Programmer's Perspective* (CSAPP), Chapter 8: Exceptional Control Flow
3. Humphries et al., *ghOSt: Fast & Flexible User-Space Delegation of Linux Scheduling*, SOSP 2021
4. Linux `sched_ext` framework documentation

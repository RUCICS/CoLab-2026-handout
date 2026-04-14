# CoLab 2026：构建你自己的多核调度器

> 操作系统每秒做出数百万次调度决策，决定哪个任务在哪个核上运行。这些决策看似简单——不就是从队列里挑一个任务吗？但当你面对多个核心、不同优先级的任务、以及吞吐量和响应延迟之间不可调和的矛盾时，你会发现这个问题远比想象中有趣。
>
> 在这个实验中，你将在一个确定性仿真引擎上实现自己的多核调度策略，并在公开的 benchmark 上与同学们一较高下。

> [!WARNING]
> 如果你发现了 Bug，请提交 Issue；有任何疑问，请在讨论区提出。
>
> 也欢迎你参与改进框架代码或帮助同学答疑，这部分贡献可被计入额外加分。

## 这个实验在做什么

你在 CSAPP 中学过异常控制流——中断、信号、上下文切换——这些概念告诉你操作系统"能做什么"。CoLab 让你把这些知识用起来：在一个确定性仿真引擎（DES）上实现自己的多核调度算法，然后选一条赛道参加打榜竞赛。

> 什么是确定性仿真引擎（DES）？
> 真机环境天然存在大量不可控扰动。操作系统调度、后台进程抢占、网络瞬时抖动、CPU频率变化、容器资源争用都会改变程序行为，尤其在并发场景下，同一份代码可能出现“这次能复现、下次复现不了”的问题。确定性仿真引擎可以被理解为一种“可重放、可控制、可验证”的执行环境。它的核心特征是确定性，也就是在相同输入、相同初始状态、相同事件顺序下，每次运行都得到完全一致的结果，从而方便各位调试。

三条赛道分别考察吞吐量、延迟和公平性——它们之间存在结构性的矛盾，没有一个调度器能同时在三条赛道上称霸。你需要选定一个方向，做出自己的 trade-off。


## 介绍一下基本模型

多少个worker
多少个task

写一下在什么里面有定义

解读一下sched文件中的每一项的涵义
- task在worker迁移的开销


## 调度器接口

你需要实现 `student/scheduler.h` 和 `student/scheduler.cc` 中的调度策略。这两个文件是你唯一需要修改的文件。

再提醒一下class BaselineRoundRobinScheduler final : public Scheduler 是个参考能跑的实现。



调度器通过以下接口与运行时交互：

| 接口 | 决策 |
|------|------|
| `select_worker` | 一个新就绪的 task 应该放到哪个 worker 的队列上？ |
| `pick_next` | 当前 worker 接下来应该运行队列中的哪个 task？ |
| `on_tick` | 时钟中断到来时，是否应该抢占当前正在运行的 task？ |
| `should_preempt` | 一个 task 刚被唤醒，它是否应该立刻抢占当前 worker 上正在运行的 task？ |
| `steal` | 当前 worker 空闲了，能否从别的 worker 的队列里偷一个 task 过来？ |

文档


通过 `TaskView` 你可以观察到每个 task 的运行时统计（累计运行时间、当前 time slice、权重、阻塞次数等），通过 `SystemView` 你可以看到全局状态（各 worker 的队列长度、拓扑节点、迁移开销等）。接口的完整定义在 `include/schedlab/scheduler.hpp`，数据结构定义在 `include/schedlab/` 下的对应头文件中。

## 赛道

三条赛道分别考察不同的调度目标：

- **Throughput（吞吐量）**——在给定的 workload 下尽快完成所有任务。考验你的负载均衡、work stealing、减少核心空闲时间的能力。
- **Latency（延迟）**——降低交互型任务的响应延迟（尤其是尾部延迟 P99）。考验你识别和优先处理短任务、交互任务的能力，但要注意延迟赛道有吞吐量的保底约束。
- **Fairness（公平性）**——让不同权重的任务按比例获得 CPU 时间。考验你对 weighted fair scheduling 的理解，但如果你的 makespan 超过 baseline 的 2 倍，分数会被封顶。

你只需要选择一条赛道参加。每条赛道下有多个 workload 场景，你的赛道得分是各场景得分的加权几何平均值。每个场景的得分是你的调度器指标与 baseline（Round-Robin）调度器指标的比值——也就是说，你需要做得比最朴素的 RR 更好。


补充一下：public和private的问题，榜是纯按private


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


补一个评分规则

60分正确性+30分性能分数（划一条线）+10分的排名打榜分数（学生的ranking）

30分：按比例衰减

10分的：前50%满分，下面按比例衰减


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
├── workloads/public/         # 公开的 workload 场景 （只读）
│   ├── throughput/           # 吞吐量赛道的场景
│   ├── latency/              # 延迟赛道的场景
│   └── fairness/             # 公平性赛道的场景
├── tools/                    # 辅助工具（bench.py 等）
├── colab                     # 打榜平台 CLI 工具
└── docs/                     # 详细文档
```

`include/schedlab/` 下的头文件定义了你要实现的接口和你能观察到的数据结构，这些文件值得反复阅读。

详细讲一下接口和数据结构


`workloads/public/` 下的场景文件用一种声明式 DSL 描述——你不需要掌握它的全部语法，但能读懂场景的结构会帮助你理解 benchmark 的行为。

详细讲一下DSL规则，举一些简单的例子

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

打榜得分的计算方式：你的赛道分数（各场景得分的加权几何平均值，乘以 1000）根据排名映射到最终分数。具体的排名-分数映射规则将在打榜开放时公布。


得改


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
# Student Implementation Area

`student/` 是你实现调度策略的工作区。

handout 的起始代码只包含两个文件：
- `student/scheduler.h`
- `student/scheduler.cc`

你可以直接在这两个文件里实现策略，也可以在 `student/` 下添加自己的辅助头文件和源文件来组织代码。

## 你负责的内容

- worker 放置策略
- 本地队列选取策略
- tick 抢占策略
- 唤醒抢占策略
- work stealing 策略
- 调度器维护的私有元数据

## framework 负责的内容

- task 生命周期
- ready queue 的实际维护
- 阻塞与唤醒
- 设备完成事件
- correctness gate
- benchmark 计分

## 建议阅读顺序

1. 先读仓库根目录的 `README.md`
2. 再读 `../docs/scheduler-guide.md`
3. 然后选一个公开 workload，对照 `../docs/workload-guide.md` 理解它在测试什么
4. 从 `benchmark/baseline_rr.cc` 开始读一个完整参考实现

## 建议开发顺序

1. 先写一个正确、简单、稳定的策略
2. 用公开 workload 跑通 correctness gate
3. 选定一条赛道再做针对性优化
4. 用公开 workload 做对比实验，再提交到排行榜

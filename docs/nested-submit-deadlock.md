# 嵌套提交死锁排查报告：满池时外层任务向同一个池投递子任务

## 1. 现象

采集编排器在外层任务（已占用池 worker）里向**同一个池**并发投递子任务。当外层任务占满全部
worker 后，所有嵌套提交永久阻塞：无错误返回、无日志输出，采集流水线假死。复现测试
（`nested_submit_test.go` 中 `TestNestedSubmitSamePool`，池容量 2、2 个外层任务、共 8 个
子任务）在改造前 3 秒后已执行子任务数为 **0/8**。

## 2. 死锁机理与容量账目的关系

### 2.1 容量账目

池的容量账目由三个计数构成（`ants.go` 中 `poolCommon`）：

- `capacity`：池容量，固定值（有限池）；
- `running`：当前存活的 worker 协程数，`worker.run()` 时 +1，worker 退出时 -1；
- 空闲 worker 队列 `workers`：已完成任务、等待复用的 worker。

`retrieveWorker()`（`ants.go`）是唯一的 worker 分配入口，按序尝试：

1. 从空闲队列 `detach` 一个 worker；
2. 若 `capacity == -1 || capacity > running`，新建 worker（`running` 在 `run()` 内 +1）；
3. 否则（**池已满**：`running == capacity` 且空闲队列为空），非阻塞配置下报错，否则
   `cond.Wait()` 挂起，等待 `revertWorker()`（worker 完成任务归还时）或 worker 退出时的
   `cond.Signal()` 唤醒。

### 2.2 循环等待的形成

关键事实：**worker 从被取出到被归还的整个区间都计入 `running`**，而外层任务在 worker
上执行的期间，该 worker 既不在空闲队列、名额也仍占着 `running`。

死锁时序（以容量 N 为例）：

1. N 个外层任务被投递，`running` 升至 N，N 个 worker 全部被检出执行外层任务；
2. 每个外层任务在执行中调用 `pool.Submit(子任务)`，进入 `retrieveWorker()`：
   空闲队列空、`running == capacity`，走到 `cond.Wait()` 挂起；
3. 此时**全部 N 个 worker 协程都阻塞在 `cond.Wait()` 里**，它们持有的 worker 名额永远
   不会被归还——归还动作 `revertWorker()` 只能由 worker 协程自己在任务完成后执行，
   而它们已无法执行到那一步；
4. `cond.Signal()` 的唯一来源（`revertWorker`、worker 退出、purge 清理空闲 worker）
   全部失效：没有 worker 会完成任务，也没有空闲 worker 可清理（purge 只扫空闲队列，
   且 `isDormant` 要求 `running == 0` 或全部空闲，均不成立）。

四个死锁必要条件齐备：worker 名额互斥、外层任务**持有名额并等待**新名额、名额不可抢占、
N 个 worker 构成循环等待。这是一个典型的"持有并等待"型死锁，且**无任何超时与日志**，
表现为流水线假死。

### 2.3 与容量账目的关系

死锁不是容量配置错误，而是容量账目的**记账粒度**问题：账目把"正在执行任务的 worker"
与"阻塞在嵌套提交里的 worker"记为同一类（都占 `running`），等待机制无法区分"池忙但
在推进"与"池忙且已无人推进"。只要存在嵌套提交，`running == capacity` 就不再是
"稍等即有 worker 归还"的充分条件，阻塞等待的前提被破坏。

## 3. 触发条件与影响面

触发条件（需同时满足）：

1. 有限容量池（`capacity > 0`），且未开启 `Nonblocking`、未触及 `MaxBlockingTasks` 上限；
2. 任务在执行期间向**同一个池**提交新任务（嵌套提交）；
3. 嵌套提交发生时池已满（`running == capacity`、空闲队列为空），且外层任务会等待
   子任务完成（或外层任务数量足够多，使所有 worker 都进入等待）。

影响面：

- `Pool.Submit`、`PoolWithFunc.Invoke`、`PoolWithFuncGeneric[T].Invoke` 共用
  `poolCommon.retrieveWorker()`，三条提交路径同等受影响；
- `MultiPool` 若子池间交叉提交且各池皆满，同样可死锁；
- 死锁后 `Release()` 可解除（`cond.Broadcast` 唤醒、返回 `ErrPoolClosed`），但正常
  运行中的流水线无外力介入则永久假死；
- 默认池（`DefaultAntsPoolSize`，容量无限）不受影响；`WithNonblocking(true)` 的池
  不会死锁（快速失败），但嵌套任务直接丢失。

## 4. 方案取舍

候选方案：

| 方案 | 说明 | 问题 |
| --- | --- | --- |
| A. 仅文档/约定 | 要求用户改用无限池或 `Nonblocking` | 不满足"改造为可工作的形态"；`Nonblocking` 下子任务直接报错丢失 |
| B. caller-runs 内联执行 | 嵌套提交在满池时由当前 worker 协程同步执行子任务 | 引入新的死锁面：若先提交的子任务 A 等待后提交的子任务 B，A 内联执行时 B 尚未提交，外层协程被 A 永久卡住；且深层嵌套退化为串行 |
| C. **临时扩容（overflow worker）** | 检测到提交者是本池 worker 且池已满时，为其派生一个临时超额 worker 执行子任务 | 容量上限被暂时突破（有界收敛，见下） |

选择 **方案 C**，理由：

1. **保持异步语义**：`Submit` 仍然立即返回、任务并发执行，与既有对外语义一致；方案 B
   会把 `Submit` 变成同步调用，改变时序语义并引入 A→B 依赖型死锁（方案 B 的"新死锁面"
   是方案 C 不存在的）。
2. **无循环等待**：worker 协程内的嵌套提交从此不再阻塞，worker 必然能推进到
   `revertWorker`，归纳可知任何有限嵌套深度都能完成；池外提交者的阻塞语义完全不变。
3. **容量收敛**：超额 worker 完成任务后在 `revertWorker()` 既有的超容量检查
   （`capacity > 0 && Running() > capacity`）处直接退役退出，池自动收敛回配置容量，
   不会留下常驻的超额 goroutine。
4. **不破坏既有契约**：`Nonblocking` 池依旧快速失败（显式选择，且是历史文档推荐的
   逃生口）；`MaxBlockingTasks` 仍只约束"会真正阻塞的"池外提交者——嵌套提交不再阻塞，
   不应占用该额度。

方案 C 的代价：嵌套提交瞬时让 `running` 超过 `capacity`（`Free()` 可能短暂为负）。这是
用"临时弹性"换"活性"的刻意取舍：容量限制的本质是控制稳态并发规模，而嵌套死锁场景下
坚持硬上限等于选择假死。超额部分随任务完成立即回收。

## 5. 实现要点

- `goid.go`（新增）：`curGoID()` 解析当前协程栈头获取 goroutine ID。仅在"池已满、
  即将阻塞"的冷路径上调用，热路径零开销。
- `ants.go`：
  - `poolCommon` 新增 `workerGoIDs sync.Map`，登记本池全部存活 worker 的协程 ID；
  - `registerWorker()`/`isPoolWorker()`：worker 协程启动时登记、退出时注销；
  - `retrieveWorker()` 阻塞分支拆分：`Nonblocking` 检查（不变）→ **嵌套检测：若提交者
    是本池 worker，直接派生临时 overflow worker 返回** → `MaxBlockingTasks` 检查
    （不变）→ `cond.Wait()`（不变）。
- `worker.go` / `worker_func.go` / `worker_func_generic.go`：三种 worker 的 `run()`
  在协程内登记/注销协程 ID。
- `pool.go` / `pool_func.go`：更新 `Submit`/`Invoke` 注释，说明嵌套提交的新行为。
- 超额 worker 的回收复用既有逻辑，未改动 `revertWorker`、`purge`、`Tune`、`Release`
  等任何账目与生命周期代码。

## 6. 验证

- 复现测试 `TestNestedSubmitSamePool`：改造前 3 秒 0/8 子任务执行（死锁确认）；改造后
  8/8 即时完成，且池在 3 秒内收敛回 `Running() <= capacity`。
- 新增回归测试（`nested_submit_test.go`）：
  - `TestNestedSubmitSamePool`：满池嵌套提交，8 个子任务限时完成 + 容量收敛；
  - `TestNestedInvokeSamePool`：`PoolWithFunc.Invoke` 同场景；
  - `TestNestedSubmitMultiLevel`：三层嵌套（1+2+4）全部完成；
  - `TestNestedSubmitNonblockingKeepsError`：`Nonblocking` 池嵌套提交仍快速返回
    `ErrPoolOverload`，契约不变。
- 全量既有测试 `go test ./...` 通过（含 `MultiPool`、`Tune`、`ReleaseTimeout`、
  `Reboot`、`Purge` 等），无回退。

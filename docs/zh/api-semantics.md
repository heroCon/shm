# API 语义

一切都在命名空间 `shm_pubsub` 中。`Topic<Config>` 映射一块命名共享内存；`Publisher` 和 `Subscriber` 是它上面的端点，各自占用一个槽，直到析构。

## 配置

`Config` 提供 `kBlockCount`、`kBlockSize`（含 64 字节块头）、`kQueueCapacity`、`kMaxPublishers`、`kMaxSubscribers`、`kMaxGroups`、每端点的贷款上限、`kTimestamps`，以及可选的第二块池 `kLargeBlockCount` / `kLargeBlockSize`（默认 0）。`loan()` 选择能装下的最小尺寸类。大于等于 `kBlockCount` 的 id 属于大块。`WriteLoan::capacity()` 返回这块的载荷上限。编译期的“不会耗尽”预算只覆盖小块；大块用完会返回 `kPoolExhausted`，除非 `kLargeBlockCount` 自己也满足同一预算。

从 `DefaultConfig` 派生并改写要改的常量。区域头里存着配置哈希和布局版本（6）。配置或布局不同的进程附加时抛 `std::runtime_error`。

## 发布

| 调用 | 结果 |
|---|---|
| `loan(size, write)` | 借出一块。`kLoanLimit`、`kMessageTooLarge`、`kInvalidArgument`、`kPoolExhausted`（修复之后仍然没有空闲块）。 |
| `publish(std::move(write))` | 只要贷款属于本发布者就消费它。对 `kBlock` 组最多等 `Options::block_timeout`。 |
| `publish(std::move(write), timeout)` | 同上，超时由参数给出。某个 `kBlock` 组一直满则 `kTimedOut`。 |
| `publish(writes, n)` | 一批贷款，一次钉住、每个队列最多唤醒一次。 |
| `publish_copy` | 没有订阅者时不分配，返回 `kNoSubscribers`。 |

`PublishResult::ok()` 对 `kOk`、`kNoSubscribers`、`kPartial` 为真。

## 订阅

`SubscriberOptions`：`group`（0 为私有队列）、`overflow`（`kDropNewest`、`kDropOldest`，组还可以 `kBlock`）、`durable`（最后一个成员离开后保留积压）、`delivery`（`kAtMostOnce` 或 `kAtLeastOnce`）、`max_deliveries`（0 为不限制，最大 254）。

组策略由第一个成员写定，后来的成员选项不一致则抛 `std::invalid_argument`。私有队列不能使用 `kBlock`、`durable`、投递策略或 `max_deliveries`。

`ReadLoan::delivery_count()` 首次为 1，再次投递后变大。`requeue()` 把组消息放回队列：`kQueueFull` 保留贷款，`kDeadLettered` 消费贷款并计入 `dead_lettered`。

非 durable 的组在最后一个成员离开或被回收时丢弃积压。成员关系是位图，`GroupInfo::members` 就是置位个数。

`take` 在队列空时不写活动字。`wait_take` 在 futex 上等待。`Options::self_heal`（默认开）让端点在队列卡住、队列满或池空时自己调用 `collect_garbage()`，每个进程每个 `self_heal_interval`（默认 10 ms）最多一次，而且只在没有别的线程正在回收时进行。

## 顺序

每个队列是 FIFO。同一发布者在同一队列上保持发布顺序。私有订阅者至多一次。组默认至多一次；`kAtLeastOnce` 会把成员死亡时拿着或已经声明但未转换的消息放回组队列，崩溃发生在这一步中间时可能投递两次。

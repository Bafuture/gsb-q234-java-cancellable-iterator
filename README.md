# 线程本地缓冲与批量合并刷出组件

Java 17 / Maven / JUnit 5。核心类：`com.example.gsb.buffer.BatchingBuffer`。

## 运行

```bash
./mvnw -q verify
```

## 用法

```java
BatchingBuffer<Record> buffer = BatchingBuffer.builder(downstreamSink)
        .maxBatchSize(500)                 // 单线程缓冲满 500 条触发刷出
        .flushInterval(Duration.ofMillis(200)) // 最早一条驻留超 200ms 触发刷出
        .maxBufferedItems(50_000)          // 全局缓冲硬上限
        .overflowPolicy(OverflowPolicy.FLUSH)  // 或 REJECT：超限直接拒绝
        .build();

buffer.write(record);            // 写入当前线程的本地缓冲
buffer.flushAll(Duration.ofSeconds(5)); // 显式刷出所有线程，返回 FlushReport
buffer.close();                  // 进程退出前调用，兜底刷出
```

## 语义说明

- **顺序保证**：同一线程的写入严格按提交顺序成批刷出，批次携带线程内单调递增的
  `sequence`。**不同线程之间的相对顺序不做保证**——各线程独立缓冲、独立刷出，
  到达下游的先后取决于调度；需要全局顺序请在下游按业务序号重排。
- **内存上限**：全局缓冲条数达到 `maxBufferedItems` 时，按策略触发刷出
  （先刷自己、再尽力刷别人）或抛出 `WriteRejectedException`，不会无限增长。
- **崩溃/重试安全**：每个批次带全局唯一 `batchId`（`实例:线程:序号`）。刷出失败
  时批次保留并以同一 `batchId` 重试；下游按 `batchId` 去重后，重复刷出与
  只刷一次的最终结果一致。
- **flushAll**：在时间预算内刷出所有已注册线程的缓冲；超时未完成（拿不到锁或
  下游失败）的线程列入 `FlushReport.pendingThreads()`。
- **线程退出**：后台扫描线程发现写入线程已死亡时兜底刷出其缓冲；线程池回收
  线程不会丢数据（缓冲挂在线程上，由批量/超时/退出/flushAll 保证最终刷出）。
- **统计**：`stats()` 返回刷出次数、合并比例（平均每批条数）、拒绝次数、
  各线程缓冲深度等快照。

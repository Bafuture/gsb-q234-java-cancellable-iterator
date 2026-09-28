package com.example.gsb.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class BatchingBufferTest {

    /** 记录每次到达的批次（不去重），用于观察重试/重复刷出行为。 */
    static final class RecordingSink<T> implements BatchSink<T> {
        final List<Batch<T>> batches = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void writeBatch(Batch<T> batch) {
            batches.add(batch);
        }

        List<T> items() {
            synchronized (batches) {
                return batches.stream().flatMap(b -> b.items().stream()).collect(Collectors.toList());
            }
        }
    }

    /** 模拟下游：按 batchId 幂等去重，重复刷出与只刷一次结果一致。 */
    static final class IdempotentSink<T> implements BatchSink<T> {
        final Set<String> seen = Collections.synchronizedSet(new HashSet<>());
        final List<T> committed = Collections.synchronizedList(new ArrayList<>());
        int deliveries;

        @Override
        public synchronized void writeBatch(Batch<T> batch) {
            deliveries++;
            if (seen.add(batch.batchId())) {
                committed.addAll(batch.items());
            }
        }
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + what);
            }
            Thread.sleep(10);
        }
    }

    // ------------------------------------------------------------------
    // 1) 批量触发 + 顺序保证
    // ------------------------------------------------------------------

    @Test
    void batchSizeTriggersFlushAndPreservesOrder() {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(5)
                .flushInterval(Duration.ofHours(1))
                .build()) {
            for (int i = 0; i < 12; i++) {
                buffer.write(i);
            }
            // 两次满批刷出 + 缓冲里剩 2 条
            assertThat(sink.batches).hasSize(2);
            assertThat(sink.items()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            assertThat(sink.batches.get(0).sequence()).isZero();
            assertThat(sink.batches.get(1).sequence()).isEqualTo(1);
            assertThat(buffer.stats().totalBuffered()).isEqualTo(2);

            buffer.flush();
            assertThat(sink.items()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        }
    }

    @Test
    void perThreadOrderPreservedAcrossThreads() throws Exception {
        RecordingSink<String> sink = new RecordingSink<>();
        int threads = 4, perThread = 50;
        try (BatchingBuffer<String> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(7)
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(100_000)
                .build()) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            for (int t = 0; t < threads; t++) {
                int id = t;
                pool.submit(() -> {
                    for (int j = 0; j < perThread; j++) {
                        buffer.write(id + "-" + j);
                    }
                });
            }
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(buffer.flushAll(Duration.ofSeconds(5)).complete()).isTrue();

            assertThat(sink.items()).hasSize(threads * perThread);
            // 同一线程的写入必须按提交顺序到达（跨线程相对顺序不做保证）
            for (int t = 0; t < threads; t++) {
                String prefix = t + "-";
                List<Integer> seq = sink.items().stream()
                        .filter(s -> s.startsWith(prefix))
                        .map(s -> Integer.parseInt(s.substring(prefix.length())))
                        .collect(Collectors.toList());
                assertThat(seq).containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, perThread).boxed().toList());
            }
            // 每个线程的批次序号从 0 开始单调递增
            sink.batches.stream().collect(Collectors.groupingBy(Batch::threadId))
                    .forEach((tid, bs) -> assertThat(bs.stream().map(Batch::sequence).toList())
                            .isSorted());
        }
    }

    // ------------------------------------------------------------------
    // 2) 超时触发
    // ------------------------------------------------------------------

    @Test
    void flushIntervalTriggersBackgroundFlush() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(1000)
                .flushInterval(Duration.ofMillis(80))
                .sweepInterval(Duration.ofMillis(20))
                .build()) {
            buffer.write(42);
            await("timeout-triggered flush", () -> !sink.batches.isEmpty());
            assertThat(sink.items()).containsExactly(42);
            assertThat(buffer.stats().totalBuffered()).isZero();
        }
    }

    // ------------------------------------------------------------------
    // 3) 内存上限：FLUSH 与 REJECT
    // ------------------------------------------------------------------

    @Test
    void memoryLimitTriggersFlushUnderFlushPolicy() {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(1000)
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(10)
                .overflowPolicy(OverflowPolicy.FLUSH)
                .build()) {
            for (int i = 0; i < 25; i++) {
                buffer.write(i);
                // 任何时刻全局缓冲都不超过上限
                assertThat(buffer.stats().totalBuffered()).isLessThanOrEqualTo(10);
            }
            assertThat(buffer.stats().flushCount()).isGreaterThanOrEqualTo(2);
            buffer.flush();
            assertThat(sink.items()).containsExactlyElementsOf(
                    java.util.stream.IntStream.range(0, 25).boxed().toList());
        }
    }

    @Test
    void memoryLimitRejectsWritesUnderRejectPolicy() {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(1000)
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(5)
                .overflowPolicy(OverflowPolicy.REJECT)
                .build()) {
            for (int i = 0; i < 5; i++) {
                buffer.write(i);
            }
            assertThatThrownBy(() -> buffer.write(5))
                    .isInstanceOf(WriteRejectedException.class);
            assertThat(buffer.stats().rejectedCount()).isEqualTo(1);
            assertThat(buffer.stats().totalBuffered()).isEqualTo(5);
        }
    }

    // ------------------------------------------------------------------
    // 4) 崩溃/重试安全：重复刷出幂等
    // ------------------------------------------------------------------

    @Test
    void duplicateDeliveryIsIdempotentAtSink() {
        IdempotentSink<Integer> downstream = new IdempotentSink<>();
        AtomicBoolean failFirstAck = new AtomicBoolean(true);
        // 模拟「下游已写入但应答丢失」：先真正投递，再抛异常让组件重试
        BatchSink<Integer> flakySink = batch -> {
            downstream.writeBatch(batch);
            if (failFirstAck.compareAndSet(true, false)) {
                throw new java.io.IOException("ack lost after commit");
            }
        };
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(flakySink)
                .maxBatchSize(3)
                .flushInterval(Duration.ofHours(1))
                .build()) {
            buffer.write(1);
            buffer.write(2);
            // 第三条触发满批刷出：下游已提交但应答丢失 → 组件视为失败并保留批次
            assertThatThrownBy(() -> buffer.write(3))
                    .isInstanceOf(FlushFailureException.class);
            assertThat(buffer.stats().totalBuffered()).isEqualTo(3);

            // 重试复用同一个 batchId，下游去重后结果与只刷一次一致
            assertThat(buffer.flushAll(Duration.ofSeconds(5)).complete()).isTrue();
            assertThat(downstream.deliveries).isEqualTo(2);
            assertThat(downstream.committed).containsExactly(1, 2, 3);
            assertThat(buffer.stats().flushCount()).isEqualTo(1);
        }
    }

    // ------------------------------------------------------------------
    // 5) flushAll：全部刷出 + 超时报告
    // ------------------------------------------------------------------

    @Test
    void flushAllFlushesEveryThread() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(1000)
                .flushInterval(Duration.ofHours(1))
                .build()) {
            ExecutorService pool = Executors.newFixedThreadPool(3);
            for (int t = 0; t < 3; t++) {
                int base = t * 100;
                pool.submit(() -> {
                    for (int i = 0; i < 7; i++) {
                        buffer.write(base + i);
                    }
                });
            }
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

            FlushReport report = buffer.flushAll(Duration.ofSeconds(5));
            assertThat(report.complete()).isTrue();
            assertThat(report.pendingThreads()).isEmpty();
            assertThat(report.flushedThreads()).hasSize(3);
            assertThat(sink.items()).containsExactlyInAnyOrderElementsOf(
                    java.util.stream.IntStream.range(0, 3)
                            .flatMap(t -> java.util.stream.IntStream.range(t * 100, t * 100 + 7))
                            .boxed().toList());
            assertThat(buffer.stats().totalBuffered()).isZero();
        }
    }

    @Test
    void flushAllTimeoutReportsPendingThreads() throws Exception {
        CountDownLatch sinkEntered = new CountDownLatch(1);
        CountDownLatch releaseSink = new CountDownLatch(1);
        BatchSink<Integer> blockingSink = batch -> {
            sinkEntered.countDown();
            assertThat(releaseSink.await(10, TimeUnit.SECONDS)).isTrue();
        };
        BatchingBuffer<Integer> buffer = BatchingBuffer.builder(blockingSink)
                .maxBatchSize(1) // 第一条写入即触发同步刷出，线程阻塞在 sink 内并持有缓冲锁
                .flushInterval(Duration.ofHours(1))
                .build();
        try {
            Thread blocked = new Thread(() -> buffer.write(1), "blocked-writer");
            blocked.start();
            assertThat(sinkEntered.await(5, TimeUnit.SECONDS)).isTrue();

            FlushReport report = buffer.flushAll(Duration.ofMillis(300));
            assertThat(report.complete()).isFalse();
            assertThat(report.pendingThreads()).contains("blocked-writer");

            releaseSink.countDown();
            blocked.join(5_000);
            assertThat(buffer.flushAll(Duration.ofSeconds(5)).complete()).isTrue();
        } finally {
            releaseSink.countDown();
            buffer.close();
        }
    }

    // ------------------------------------------------------------------
    // 6) 线程退出自动刷出 + 线程池复用不丢数据
    // ------------------------------------------------------------------

    @Test
    void threadExitTriggersAutoFlush() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(1000)
                .flushInterval(Duration.ofHours(1)) // 排除超时触发，只验证线程退出路径
                .sweepInterval(Duration.ofMillis(20))
                .build()) {
            Thread worker = new Thread(() -> {
                buffer.write(7);
                buffer.write(8);
            }, "short-lived-worker");
            worker.start();
            worker.join(5_000);

            await("flush after thread exit", () -> sink.items().size() == 2);
            assertThat(sink.items()).containsExactly(7, 8);
        }
    }

    @Test
    void pooledThreadsDoNotLoseData() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        int tasks = 20;
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(1000)
                .flushInterval(Duration.ofMillis(50))
                .sweepInterval(Duration.ofMillis(20))
                .build()) {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            for (int i = 0; i < tasks; i++) {
                int value = i;
                pool.submit(() -> buffer.write(value));
            }
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

            // 池线程退出后由扫描线程兜底刷出，一条不丢
            await("flush of pooled threads", () -> sink.items().size() == tasks);
            assertThat(sink.items()).containsExactlyInAnyOrderElementsOf(
                    java.util.stream.IntStream.range(0, tasks).boxed().toList());
        }
    }

    // ------------------------------------------------------------------
    // 7) 统计
    // ------------------------------------------------------------------

    @Test
    void statsReportFlushesMergeRatioRejectionsAndDepths() {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(10)
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(3)
                .overflowPolicy(OverflowPolicy.REJECT)
                .build()) {
            buffer.write(0);
            buffer.write(1);
            buffer.write(2);             // 全局占用 3，达到上限
            assertThatThrownBy(() -> buffer.write(3)).isInstanceOf(WriteRejectedException.class);

            BufferStats before = buffer.stats();
            assertThat(before.rejectedCount()).isEqualTo(1);
            assertThat(before.totalBuffered()).isEqualTo(3);
            assertThat(before.threadBufferDepths())
                    .containsEntry(Thread.currentThread().getName(), 3);

            buffer.flush();              // 3 条合并为一批刷出，合并比例 3.0
            BufferStats stats = buffer.stats();
            assertThat(stats.flushCount()).isEqualTo(1);
            assertThat(stats.itemsAccepted()).isEqualTo(3);
            assertThat(stats.itemsFlushed()).isEqualTo(3);
            assertThat(stats.mergeRatio()).isEqualTo(3.0);
            assertThat(stats.totalBuffered()).isZero();
        }
    }

    @Test
    void queueLikeStressKeepsEveryItem() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        int threads = 4, perThread = 500;
        try (BatchingBuffer<Integer> buffer = BatchingBuffer.builder(sink)
                .maxBatchSize(32)
                .flushInterval(Duration.ofMillis(30))
                .sweepInterval(Duration.ofMillis(10))
                .maxBufferedItems(1_000)
                .overflowPolicy(OverflowPolicy.FLUSH)
                .build()) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            ConcurrentLinkedQueue<Integer> expected = new ConcurrentLinkedQueue<>();
            for (int t = 0; t < threads; t++) {
                int base = t * perThread;
                pool.submit(() -> {
                    for (int i = 0; i < perThread; i++) {
                        buffer.write(base + i);
                        expected.add(base + i);
                    }
                });
            }
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(buffer.flushAll(Duration.ofSeconds(5)).complete()).isTrue();

            assertThat(sink.items()).containsExactlyInAnyOrderElementsOf(expected);
            assertThat(buffer.stats().itemsFlushed()).isEqualTo((long) threads * perThread);
        }
    }
}

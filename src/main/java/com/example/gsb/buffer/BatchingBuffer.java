package com.example.gsb.buffer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程本地缓冲 + 批量合并刷出组件。
 *
 * <p>核心语义：
 * <ul>
 *   <li><b>线程本地缓冲</b>：每个写入线程拥有独立缓冲，写路径只在「本线程缓冲」
 *       上加锁，线程之间互不阻塞。</li>
 *   <li><b>顺序保证</b>：同一线程的写入严格按提交顺序进入缓冲、按提交顺序成批刷出，
 *       且批次携带线程内单调递增的 {@code sequence}。<b>不同线程之间的相对顺序不做保证</b>：
 *       两个线程各自缓冲、各自刷出，到达下游的先后取决于调度与刷出时机，
 *       需要全局顺序的场景请在下游按业务时间戳/序号重排。</li>
 *   <li><b>触发条件</b>：缓冲达到 {@code maxBatchSize}、最早一条记录驻留超过
 *       {@code flushInterval}（后台扫描线程检测）、显式 {@link #flushAll}、
 *       线程退出（后台扫描发现线程已死亡时兜底刷出）或内存达到上限。</li>
 *   <li><b>内存上限</b>：全局缓冲总条数达到 {@code maxBufferedItems} 时，按
 *       {@link OverflowPolicy} 触发刷出或拒绝写入，缓冲不会无限增长。</li>
 *   <li><b>崩溃/重试安全</b>：每个批次带全局唯一 {@code batchId}；刷出失败时批次
 *       保留并以同一 {@code batchId} 重试，下游按 {@code batchId} 去重后，
 *       重复刷出与只刷一次的结果一致。</li>
 *   <li><b>线程池安全</b>：缓冲挂在「线程」而非「任务」上，线程被池化复用时数据
 *       仍留在该线程的缓冲里，由批量/超时/flushAll/线程退出等机制保证最终刷出，
 *       不会因任务结束而丢数据。</li>
 * </ul>
 */
public final class BatchingBuffer<T> implements AutoCloseable {

    private final BatchSink<T> sink;
    private final int maxBatchSize;
    private final long flushIntervalNanos;
    private final int maxBufferedItems;
    private final OverflowPolicy overflowPolicy;
    private final String instanceId;

    private final ConcurrentMap<Long, ThreadBuffer> buffers = new ConcurrentHashMap<>();
    private final ThreadLocal<ThreadBuffer> localBuffer = ThreadLocal.withInitial(this::registerBuffer);

    private final AtomicInteger totalBuffered = new AtomicInteger();
    private final AtomicLong flushCount = new AtomicLong();
    private final AtomicLong itemsAccepted = new AtomicLong();
    private final AtomicLong itemsFlushed = new AtomicLong();
    private final AtomicLong rejectedCount = new AtomicLong();

    private final ScheduledExecutorService sweeper;
    private volatile boolean closed;

    private BatchingBuffer(Builder<T> builder) {
        this.sink = builder.sink;
        this.maxBatchSize = builder.maxBatchSize;
        this.flushIntervalNanos = builder.flushInterval.toNanos();
        this.maxBufferedItems = builder.maxBufferedItems;
        this.overflowPolicy = builder.overflowPolicy;
        this.instanceId = UUID.randomUUID().toString().substring(0, 8);
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "batching-buffer-sweeper-" + instanceId);
            thread.setDaemon(true);
            return thread;
        };
        this.sweeper = Executors.newSingleThreadScheduledExecutor(factory);
        this.sweeper.scheduleWithFixedDelay(this::sweepSafely,
                builder.sweepInterval.toMillis(), builder.sweepInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static <T> Builder<T> builder(BatchSink<T> sink) {
        return new Builder<>(sink);
    }

    /**
     * 写入一条记录到当前线程的本地缓冲。
     *
     * @throws WriteRejectedException  内存达到上限且策略为 REJECT
     * @throws FlushFailureException   本次写入触发的同步刷出失败（批次保留，可重试）
     */
    public void write(T item) {
        Objects.requireNonNull(item, "item");
        if (closed) {
            throw new IllegalStateException("buffer is closed");
        }
        ThreadBuffer buffer = localBuffer.get();
        buffer.lock.lock();
        try {
            if (totalBuffered.get() >= maxBufferedItems) {
                handleOverflow(buffer);
            }
            if (buffer.items.isEmpty()) {
                buffer.oldestItemNanos = System.nanoTime();
            }
            buffer.items.add(item);
            totalBuffered.incrementAndGet();
            itemsAccepted.incrementAndGet();
            if (buffer.items.size() >= maxBatchSize) {
                flushLocked(buffer);
            }
        } finally {
            buffer.lock.unlock();
        }
    }

    /** 内存达到上限：按策略刷出或拒绝。 */
    private void handleOverflow(ThreadBuffer ownBuffer) {
        if (overflowPolicy == OverflowPolicy.REJECT) {
            rejectedCount.incrementAndGet();
            throw new WriteRejectedException(
                    "buffered item count reached limit " + maxBufferedItems + ", write rejected");
        }
        flushLocked(ownBuffer);
        if (totalBuffered.get() >= maxBufferedItems) {
            flushOthersBestEffort(ownBuffer);
        }
    }

    /** 尽力刷出其他线程的缓冲以腾出全局容量；拿不到锁的跳过，不阻塞写线程。 */
    private void flushOthersBestEffort(ThreadBuffer ownBuffer) {
        for (ThreadBuffer other : buffers.values()) {
            if (other == ownBuffer || totalBuffered.get() < maxBufferedItems) {
                continue;
            }
            if (other.lock.tryLock()) {
                try {
                    flushLocked(other);
                } catch (FlushFailureException ignored) {
                    // 保留该批次，由后续扫描重试
                } finally {
                    other.lock.unlock();
                }
            }
        }
    }

    /**
     * 刷出所有已注册线程的缓冲。
     *
     * @param timeout 整体时间预算；预算耗尽后仍未拿到锁或刷出失败的线程会被列入报告
     * @return 报告，包含已完成与未完成（超时）的线程列表
     */
    public FlushReport flushAll(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long deadline = System.nanoTime() + timeout.toNanos();
        List<String> flushed = new ArrayList<>();
        List<String> pending = new ArrayList<>();
        for (ThreadBuffer buffer : buffers.values()) {
            boolean done = tryFlushWithin(buffer, deadline);
            (done ? flushed : pending).add(buffer.ownerName());
        }
        return new FlushReport(pending.isEmpty(), flushed, pending);
    }

    private boolean tryFlushWithin(ThreadBuffer buffer, long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            return false;
        }
        boolean locked;
        try {
            locked = buffer.lock.tryLock(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        if (!locked) {
            return false;
        }
        try {
            flushLocked(buffer);
            return true;
        } catch (FlushFailureException e) {
            return false;
        } finally {
            buffer.lock.unlock();
        }
    }

    /** 当前线程缓冲的便捷刷出（等价于只 flush 自己）。 */
    public void flush() {
        ThreadBuffer buffer = localBuffer.get();
        buffer.lock.lock();
        try {
            flushLocked(buffer);
        } finally {
            buffer.lock.unlock();
        }
    }

    /** 统计快照。 */
    public BufferStats stats() {
        Map<String, Integer> depths = new HashMap<>();
        for (ThreadBuffer buffer : buffers.values()) {
            buffer.lock.lock();
            try {
                depths.merge(buffer.ownerName(), buffer.items.size(), Integer::sum);
            } finally {
                buffer.lock.unlock();
            }
        }
        long flushes = flushCount.get();
        long flushedItems = itemsFlushed.get();
        return new BufferStats(
                flushes,
                itemsAccepted.get(),
                flushedItems,
                rejectedCount.get(),
                flushes == 0 ? 0.0 : (double) flushedItems / flushes,
                totalBuffered.get(),
                depths);
    }

    /**
     * 关闭组件：停止后台扫描并尽力刷出全部剩余缓冲。
     * 进程退出前应调用本方法（或注册 shutdown hook 调用）以避免丢数据。
     */
    @Override
    public void close() {
        closed = true;
        sweeper.shutdownNow();
        flushAll(Duration.ofSeconds(5));
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    private ThreadBuffer registerBuffer() {
        Thread owner = Thread.currentThread();
        ThreadBuffer buffer = new ThreadBuffer(owner);
        buffers.put(owner.getId(), buffer);
        return buffer;
    }

    /**
     * 刷出前提：已持有 {@code buffer.lock}。失败时批次与序号原样保留，
     * 重试会复用同一个 batchId，保证下游幂等去重后结果一致。
     */
    private void flushLocked(ThreadBuffer buffer) {
        if (buffer.items.isEmpty()) {
            return;
        }
        Batch<T> batch = new Batch<>(
                instanceId + ":" + buffer.threadId + ":" + buffer.nextSequence,
                buffer.nextSequence,
                buffer.ownerName(),
                buffer.threadId,
                buffer.items,
                Instant.now());
        try {
            sink.writeBatch(batch);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FlushFailureException("flush failed, batch " + batch.batchId() + " retained for retry", e);
        }
        int size = buffer.items.size();
        buffer.items.clear();
        totalBuffered.addAndGet(-size);
        buffer.nextSequence++;
        flushCount.incrementAndGet();
        itemsFlushed.addAndGet(size);
    }

    /** 后台扫描：超时刷出 + 线程退出兜底刷出。 */
    private void sweepSafely() {
        try {
            long now = System.nanoTime();
            for (Map.Entry<Long, ThreadBuffer> entry : buffers.entrySet()) {
                ThreadBuffer buffer = entry.getValue();
                Thread owner = buffer.owner.get();
                boolean ownerGone = owner == null || !owner.isAlive();
                buffer.lock.lock();
                try {
                    if (ownerGone) {
                        flushQuietly(buffer);
                        buffers.remove(entry.getKey(), buffer);
                    } else if (!buffer.items.isEmpty()
                            && now - buffer.oldestItemNanos >= flushIntervalNanos) {
                        flushQuietly(buffer);
                    }
                } finally {
                    buffer.lock.unlock();
                }
            }
        } catch (Throwable ignored) {
            // 扫描线程绝不能死掉
        }
    }

    private void flushQuietly(ThreadBuffer buffer) {
        try {
            flushLocked(buffer);
        } catch (RuntimeException ignored) {
            // 批次保留，下一轮扫描以同一 batchId 重试
        }
    }

    private final class ThreadBuffer {
        private final java.lang.ref.WeakReference<Thread> owner;
        private final long threadId;
        private final ReentrantLock lock = new ReentrantLock();
        private final List<T> items = new ArrayList<>();
        private long nextSequence;
        private long oldestItemNanos;

        private ThreadBuffer(Thread owner) {
            this.owner = new java.lang.ref.WeakReference<>(owner);
            this.threadId = owner.getId();
        }

        private String ownerName() {
            Thread thread = owner.get();
            return thread != null ? thread.getName() : "terminated-" + threadId;
        }
    }

    // ------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------

    public static final class Builder<T> {
        private final BatchSink<T> sink;
        private int maxBatchSize = 100;
        private Duration flushInterval = Duration.ofSeconds(1);
        private int maxBufferedItems = 10_000;
        private OverflowPolicy overflowPolicy = OverflowPolicy.FLUSH;
        private Duration sweepInterval;

        private Builder(BatchSink<T> sink) {
            this.sink = Objects.requireNonNull(sink, "sink");
        }

        /** 单线程缓冲达到该条数即触发刷出。 */
        public Builder<T> maxBatchSize(int maxBatchSize) {
            if (maxBatchSize < 1) throw new IllegalArgumentException("maxBatchSize must be >= 1");
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        /** 最早一条记录驻留超过该时长即由后台线程触发刷出。 */
        public Builder<T> flushInterval(Duration flushInterval) {
            this.flushInterval = Objects.requireNonNull(flushInterval);
            return this;
        }

        /** 全局（所有线程合计）缓冲条数硬上限，达到后按 overflowPolicy 处理。 */
        public Builder<T> maxBufferedItems(int maxBufferedItems) {
            if (maxBufferedItems < 1) throw new IllegalArgumentException("maxBufferedItems must be >= 1");
            this.maxBufferedItems = maxBufferedItems;
            return this;
        }

        public Builder<T> overflowPolicy(OverflowPolicy overflowPolicy) {
            this.overflowPolicy = Objects.requireNonNull(overflowPolicy);
            return this;
        }

        /** 后台扫描周期，默认 flushInterval 的一半（最小 10ms）。 */
        public Builder<T> sweepInterval(Duration sweepInterval) {
            this.sweepInterval = Objects.requireNonNull(sweepInterval);
            return this;
        }

        public BatchingBuffer<T> build() {
            if (sweepInterval == null) {
                long millis = Math.max(10, flushInterval.toMillis() / 2);
                sweepInterval = Duration.ofMillis(millis);
            }
            return new BatchingBuffer<>(this);
        }
    }
}

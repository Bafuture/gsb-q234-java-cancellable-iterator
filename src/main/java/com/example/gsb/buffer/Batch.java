package com.example.gsb.buffer;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * 一次刷出动作携带的不可变批次。
 *
 * <p>{@code batchId} 全局唯一且在重试时保持不变，下游应按该 id 去重，从而保证
 * 「同一批次被刷出多次」与「只刷一次」的最终结果一致（见 {@link #batchId()}）。
 * {@code sequence} 是线程内单调递增的批次序号，可用于校验/排序同一线程的批次。
 */
public final class Batch<T> {

    private final String batchId;
    private final long sequence;
    private final String threadName;
    private final long threadId;
    private final List<T> items;
    private final Instant createdAt;

    public Batch(String batchId, long sequence, String threadName, long threadId,
                 List<T> items, Instant createdAt) {
        this.batchId = Objects.requireNonNull(batchId, "batchId");
        this.threadName = Objects.requireNonNull(threadName, "threadName");
        this.items = List.copyOf(items);
        this.sequence = sequence;
        this.threadId = threadId;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
    }

    /**
     * 全局唯一的批次标识，格式为 {@code <bufferInstanceId>:<threadId>:<sequence>}。
     * 下游必须按此 id 去重；批次重试（例如刷出后应答丢失）会复用同一个 id。
     */
    public String batchId() {
        return batchId;
    }

    /** 所属线程内单调递增的批次序号，从 0 开始。 */
    public long sequence() {
        return sequence;
    }

    public String threadName() {
        return threadName;
    }

    public long threadId() {
        return threadId;
    }

    /** 批次内元素，严格保持该线程的提交顺序。 */
    public List<T> items() {
        return items;
    }

    public Instant createdAt() {
        return createdAt;
    }

    @Override
    public String toString() {
        return "Batch{batchId='" + batchId + "', size=" + items.size() + "}";
    }
}

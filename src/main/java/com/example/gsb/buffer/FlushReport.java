package com.example.gsb.buffer;

import java.util.List;
import java.util.Objects;

/**
 * {@link BatchingBuffer#flushAll} 的结果报告。
 *
 * @param complete        是否所有线程的缓冲都已成功刷出
 * @param flushedThreads  已刷出线程名
 * @param pendingThreads  超时仍未完成（拿不到锁或下游失败）的线程名
 */
public record FlushReport(boolean complete, List<String> flushedThreads, List<String> pendingThreads) {

    public FlushReport {
        Objects.requireNonNull(flushedThreads);
        Objects.requireNonNull(pendingThreads);
        flushedThreads = List.copyOf(flushedThreads);
        pendingThreads = List.copyOf(pendingThreads);
    }
}

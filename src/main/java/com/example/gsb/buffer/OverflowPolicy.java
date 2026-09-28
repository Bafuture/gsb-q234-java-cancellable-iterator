package com.example.gsb.buffer;

/**
 * 全局缓冲内存达到上限（{@code maxBufferedItems}）时的处理策略。
 */
public enum OverflowPolicy {

    /**
     * 触发刷出：先同步刷出当前线程的缓冲；若总量仍超阈值，则尽力刷出其他
     * 线程的缓冲，直到占用降到阈值以下后再接受本次写入。
     */
    FLUSH,

    /**
     * 拒绝写入：直接抛出 {@link WriteRejectedException}，拒绝计数加一，
     * 缓冲内容保持不变。
     */
    REJECT
}

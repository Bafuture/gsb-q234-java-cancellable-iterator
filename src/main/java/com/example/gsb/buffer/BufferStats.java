package com.example.gsb.buffer;

import java.util.Map;

/**
 * 统计快照（不可变）。
 *
 * @param flushCount          成功刷出的批次数
 * @param itemsAccepted       接收到的写入总条数
 * @param itemsFlushed        已成功刷出的总条数
 * @param rejectedCount       因内存上限被拒绝的写入次数
 * @param mergeRatio          合并比例 = itemsFlushed / flushCount，即平均每批合并的条数
 * @param totalBuffered       当前所有线程缓冲中的总条数
 * @param threadBufferDepths  各线程名 → 当前缓冲深度（条数）
 */
public record BufferStats(long flushCount,
                          long itemsAccepted,
                          long itemsFlushed,
                          long rejectedCount,
                          double mergeRatio,
                          int totalBuffered,
                          Map<String, Integer> threadBufferDepths) {
}

package com.example.gsb.buffer;

/**
 * 刷出下游失败。批次并未丢失：缓冲内容与批次序号均保留，后续
 * （下一批量、超时扫描或 {@link BatchingBuffer#flushAll}）会以同一个
 * batchId 重试。
 */
public class FlushFailureException extends RuntimeException {

    public FlushFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}

package com.example.gsb.buffer;

/** 缓冲达到内存上限且策略为 {@link OverflowPolicy#REJECT} 时抛出。 */
public class WriteRejectedException extends RuntimeException {

    public WriteRejectedException(String message) {
        super(message);
    }
}

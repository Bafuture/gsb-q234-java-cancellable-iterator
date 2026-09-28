package com.example.gsb.buffer;

/**
 * 下游接收端：接收一批已排好序的记录。
 *
 * <p>实现方应当：
 * <ul>
 *   <li>按 {@link Batch#batchId()} 做幂等去重——同一个 batchId 可能因重试到达多次；</li>
 *   <li>批次内元素顺序即提交顺序，不要重排；</li>
 *   <li>处理失败时抛出异常，缓冲组件会保留本批次并在之后以同一个 batchId 重试。</li>
 * </ul>
 */
@FunctionalInterface
public interface BatchSink<T> {

    void writeBatch(Batch<T> batch) throws Exception;
}

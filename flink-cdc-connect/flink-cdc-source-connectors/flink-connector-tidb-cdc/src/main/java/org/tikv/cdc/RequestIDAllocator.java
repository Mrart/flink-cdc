package org.tikv.cdc;

import java.util.concurrent.atomic.AtomicLong;

public class RequestIDAllocator {
    // 定义一个 AtomicLong 用于存储当前的 request ID
    private static final AtomicLong currentRequestID = new AtomicLong(0);

    // allocateRequestID 方法，生成并返回一个唯一的 request ID
    public static long allocateRequestID() {
        return currentRequestID.incrementAndGet();
    }
}

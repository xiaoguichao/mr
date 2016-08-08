package com.alibaba.middleware.race.io;

import java.nio.ByteBuffer;

public class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {

    private final ByteBuffer originalByteBuffer;

    public ThreadLocalByteBuffer(ByteBuffer originalByteBuffer) {
        this.originalByteBuffer = originalByteBuffer;
    }

    @Override
    protected synchronized ByteBuffer initialValue() {
        return originalByteBuffer.duplicate();
    }

}
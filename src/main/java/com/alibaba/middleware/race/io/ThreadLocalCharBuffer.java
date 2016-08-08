package com.alibaba.middleware.race.io;

import java.nio.CharBuffer;

public class ThreadLocalCharBuffer extends ThreadLocal<CharBuffer> {
    private final CharBuffer originalCharBuffer;

    public ThreadLocalCharBuffer(CharBuffer originalCharBuffer) {
        this.originalCharBuffer = originalCharBuffer;
    }

    @Override
    protected synchronized CharBuffer initialValue() {
        return originalCharBuffer.duplicate();
    }
}
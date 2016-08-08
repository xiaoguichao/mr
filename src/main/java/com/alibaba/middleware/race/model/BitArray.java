package com.alibaba.middleware.race.model;

public class BitArray {
    private final long[] data;

    public BitArray(int bits) {
        if (bits < 1) bits = 1;
        this.data = new long[(bits - 1) / 64 + 1];
    }

    public boolean set(int index) {
        if (!get(index)) {
            data[(index >>> 6)] |= (1L << index);
            return true;
        }
        return false;
    }

    public boolean get(int index) {
        return (data[(index >>> 6)] & (1L << index)) != 0;
    }
}

package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.utils.Keys;

public class SimpleKV implements OrderSystem.KeyValue {

    private final int keyIndex;
    private final SimpleValue value;

    public SimpleKV(int keyIndex, SimpleValue value){
        this.keyIndex = keyIndex;
        this.value = value;
    }

    public SimpleValue getValue(){
        return value;
    }

    public int getKeyIndex(){
        return keyIndex;
    }

    @Override
    public String key() {
        return Keys.KEYS_STRING[keyIndex];
    }

    @Override
    public String valueAsString() {
        return value.stringValue();
    }

    @Override
    public long valueAsLong() throws OrderSystem.TypeException {
        return value.longValue();
    }

    @Override
    public double valueAsDouble() throws OrderSystem.TypeException {
        return value.doubleValue();
    }

    @Override
    public boolean valueAsBoolean() throws OrderSystem.TypeException {
        return value.booleanValue();
    }
}

package com.alibaba.middleware.race.model.values;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.db.LongStringCache;
import com.alibaba.middleware.race.model.SimpleValue;

public class LongStringValue implements SimpleValue {

    private String value;
    private boolean inited = false;
    private final long index;

    public LongStringValue(long index){
        this.index = index;
    }

    @Override
    public long longValue() throws OrderSystem.TypeException {
        throw TYPE_EXCEPTION;
    }

    @Override
    public boolean booleanValue() throws OrderSystem.TypeException {
        throw TYPE_EXCEPTION;
    }

    @Override
    public double doubleValue() throws OrderSystem.TypeException {
        throw TYPE_EXCEPTION;
    }

    @Override
    public String stringValue() {
        if(!inited){
            this.value = LongStringCache.INSTANCE.getString(index);
            this.inited = true;
        }
        return value;
    }
}

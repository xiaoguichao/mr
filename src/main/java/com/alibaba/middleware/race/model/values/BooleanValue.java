package com.alibaba.middleware.race.model.values;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.model.SimpleValue;

public class BooleanValue implements SimpleValue {

    public static final BooleanValue TRUE = new BooleanValue(true);
    public static final BooleanValue FALSE = new BooleanValue(false);

    private boolean value;

    private BooleanValue(boolean value){this.value = value;};

    @Override
    public long longValue() throws OrderSystem.TypeException {
        throw TYPE_EXCEPTION;
    }

    @Override
    public boolean booleanValue() throws OrderSystem.TypeException {
        return value;
    }

    @Override
    public double doubleValue() throws OrderSystem.TypeException {
        throw TYPE_EXCEPTION;
    }

    @Override
    public String stringValue() {
        return Boolean.toString(value);
    }
}

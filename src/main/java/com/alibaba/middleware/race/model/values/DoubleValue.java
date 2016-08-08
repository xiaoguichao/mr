package com.alibaba.middleware.race.model.values;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.model.SimpleValue;

public class DoubleValue implements SimpleValue {

    private double value;

    public DoubleValue(double value){
        this.value = value;
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
        return value;
    }

    @Override
    public String stringValue() {
        return Double.toString(value);
    }
}

package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.OrderSystem;

public interface SimpleValue {
    OrderSystem.TypeException TYPE_EXCEPTION = new OrderSystem.TypeException();
    long longValue() throws OrderSystem.TypeException;
    boolean booleanValue() throws OrderSystem.TypeException;
    double doubleValue() throws OrderSystem.TypeException;
    String stringValue();
}

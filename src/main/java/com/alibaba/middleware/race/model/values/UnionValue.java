package com.alibaba.middleware.race.model.values;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.model.SimpleValue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UnionValue implements SimpleValue {

    private String value;

    public UnionValue(ByteBuffer byteBuffer, int length){
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        this.value = new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public long longValue() throws OrderSystem.TypeException {
        try{
            return Long.parseLong(value);
        }catch (NumberFormatException e){
            throw TYPE_EXCEPTION;
        }
    }

    @Override
    public boolean booleanValue() throws OrderSystem.TypeException {
        if("true".equals(value))return true;
        else if("false".equals(value))return false;
        else throw TYPE_EXCEPTION;
    }

    @Override
    public double doubleValue() throws OrderSystem.TypeException {
        try{
            return Double.parseDouble(value);
        }catch (NumberFormatException e){
            throw TYPE_EXCEPTION;
        }
    }

    @Override
    public String stringValue() {
        return value;
    }
}

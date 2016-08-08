package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.model.values.*;
import com.alibaba.middleware.race.utils.Keys;

import java.nio.ByteBuffer;
import java.util.Collection;

public class SimpleRow implements OrderSystem.Result {

    private final SimpleValue[] values = new SimpleValue[Keys.SIZE];
    private long oid;
    private final BitArray queryIndices;
    private byte size = 0;
    private short total = 0;

    public SimpleRow(long oid, Collection<String> keys){
        this.oid = oid;
        if(keys!=null){
            this.queryIndices = new BitArray(Keys.SIZE);
            for (String key : keys){
                int index = Keys.indexOfString(key);
                if(index>=0){
                    queryIndices.set(index);
                    total++;
                }
            }
        }else{
            this.queryIndices = null;
            this.total = Keys.SIZE;
        }
    }

    public SimpleRow(int keyIndex){
        this.queryIndices = new BitArray(Keys.SIZE);
        queryIndices.set(keyIndex);
        this.total = 1;
    }

    public boolean completed(){
        return size>=total;
    }

    public void autoFillOid(){
        try {
            this.oid = values[Keys.ORDER_ID_INDEX].longValue();
        } catch (OrderSystem.TypeException e) {
            e.printStackTrace();
        }
    }

    public void join(ByteBuffer byteBuffer){
        while(byteBuffer.hasRemaining()){
            int index = byteBuffer.get()&0xFF;
            if(queryIndices==null || queryIndices.get(index)){
                switch(Keys.KEYS_TYPE[index]){
                    case 0:
                        values[index] = (byteBuffer.get() == 1) ? BooleanValue.TRUE : BooleanValue.FALSE;
                        break;
                    case 1:
                        values[index] = new LongValue(byteBuffer.getLong());
                        break;
                    case 2:
                    case 3:
                    case 4:
                        int length = byteBuffer.get()&0xFF;
                        values[index] = new UnionValue(byteBuffer, length);
                        break;
                    case 5:
                        values[index] = new LongStringValue(byteBuffer.getLong());
                        break;
                }
                size++;
                if(completed())return;
            }else{
                switch(Keys.KEYS_TYPE[index]){
                    case 0:
                        byteBuffer.get();
                        break;
                    case 1:
                    case 5:
                        byteBuffer.position(byteBuffer.position()+8);
                        break;
                    case 2:
                    case 3:
                    case 4:
                        int length = byteBuffer.get()&0xFF;
                        byteBuffer.position(byteBuffer.position()+length);
                        break;
                }
            }

        }
    }

    public void clear(){
        for(int i=0; i<Keys.SIZE; i++){
            values[i] = null;
        }
        this.size = 0;
    }

    public SimpleValue get(int index){
        return values[index];
    }

    @Override
    public OrderSystem.KeyValue get(String key) {
        int index = Keys.indexOfString(key);
        if(index>=0){
            return new SimpleKV(index, values[index]);
        }
        return null;
    }

    @Override
    public OrderSystem.KeyValue[] getAll() {
        SimpleKV[] r = new SimpleKV[size];
        int p = 0;
        for(int i=0; i<Keys.SIZE; i++){
            if(values[i]!=null)r[p++]=new SimpleKV(i, values[i]);
            if(p==size)break;
        }
        return r;
    }

    @Override
    public long orderId() {
        return oid;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(oid);
        sb.append('{');
        for(int i=0; i<Keys.SIZE; i++){
            if(values[i]!=null){
                sb.append(Keys.KEYS_STRING[i]);
                sb.append(":");
                sb.append(values[i].stringValue());
                sb.append(',');
            }
        }
        sb.append('}');
        return sb.toString();
    }
}

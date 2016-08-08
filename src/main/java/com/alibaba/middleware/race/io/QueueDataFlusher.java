package com.alibaba.middleware.race.io;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueDataFlusher {

    private static final DataFlushUnit END_UNIT = new DataFlushUnit(0);
    private static final int CAPACITY = 3000;

    private final BlockingQueue<DataFlushUnit> queueA = new ArrayBlockingQueue<DataFlushUnit>(CAPACITY);
    private final BlockingQueue<DataFlushUnit> queueB = new ArrayBlockingQueue<DataFlushUnit>(CAPACITY);

    public QueueDataFlusher(){
        for(int i=0; i<CAPACITY; i++)queueA.offer(new DataFlushUnit(0x10000));
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    try {
                        DataFlushUnit unit = queueB.take();
                        if(unit==END_UNIT)break;
                        unit.directByteBuffer.flip();
                        unit.fileChannel.write(unit.directByteBuffer);
                        queueA.put(unit);
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public DataFlushUnit getNext() throws InterruptedException {
        return queueA.take();
    }

    public void publish(DataFlushUnit unit) throws InterruptedException {
        queueB.put(unit);
    }

    public void shutdown() throws InterruptedException {
        int i = CAPACITY;
        while(i>0){
            queueA.take().clean();
            i--;
        }
        queueA.clear();
        queueB.put(END_UNIT);
    }

}

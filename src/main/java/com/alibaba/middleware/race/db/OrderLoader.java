package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.Params;
import com.alibaba.middleware.race.utils.Keys;
import com.alibaba.middleware.race.io.IOExecutors;
import com.alibaba.middleware.race.utils.IndexUtil;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OrderLoader {
    public static final OrderLoader INSTANCE = new OrderLoader();
    private static final int MAX_KEYS = 24;
    private static final int MAX_DATA_LENGTH = 1024;

    private static final String[] filesOne = new String[]{"/disk1/orders/order.0.0","/disk1/orders/order.0.3","/disk1/orders/order.0.6","/disk1/orders/order.0.9","/disk1/orders/order.0.12","/disk1/orders/order.0.15","/disk1/orders/order.0.18","/disk1/orders/order.0.21","/disk1/orders/order.0.24","/disk1/orders/order.0.27","/disk1/orders/order.0.30","/disk1/orders/order.0.33","/disk1/orders/order.0.36","/disk1/orders/order.0.39","/disk1/orders/order.0.42"};
    private static final String[] filesTwo = new String[]{"/disk2/orders/order.1.1","/disk2/orders/order.1.4","/disk2/orders/order.1.7","/disk2/orders/order.1.10","/disk2/orders/order.1.13","/disk2/orders/order.1.16","/disk2/orders/order.1.19","/disk2/orders/order.1.22","/disk2/orders/order.1.25","/disk2/orders/order.1.28","/disk2/orders/order.1.31","/disk2/orders/order.1.34","/disk2/orders/order.1.37","/disk2/orders/order.1.40"};
    private static final String[] filesThree = new String[]{"/disk3/orders/order.2.2","/disk3/orders/order.2.5","/disk3/orders/order.2.8","/disk3/orders/order.2.11","/disk3/orders/order.2.14","/disk3/orders/order.2.17","/disk3/orders/order.2.20","/disk3/orders/order.2.23","/disk3/orders/order.2.26","/disk3/orders/order.2.29","/disk3/orders/order.2.32","/disk3/orders/order.2.35","/disk3/orders/order.2.38","/disk3/orders/order.2.41"};

    private final EventFactory<RawOrder> RAW_FACTORY = new EventFactory<RawOrder>() {
        @Override
        public RawOrder newInstance() {
            return new RawOrder();
        }
    };

    private OrderLoader(){};

    public void load() throws InterruptedException, IOException {
        Disruptor<RawOrder> disruptor = new Disruptor<RawOrder>(RAW_FACTORY, 1024, DaemonThreadFactory.INSTANCE, ProducerType.MULTI, new BlockingWaitStrategy());
        //noinspection unchecked
        disruptor.handleEventsWith(new EventHandler<RawOrder>() {
            @Override
            public void onEvent(RawOrder record, long sequence, boolean endOfBatch) throws Exception {
                record.prepareKeys();
            }
        }).then(new EventHandler<RawOrder>() {
            @Override
            public void onEvent(RawOrder record, long sequence, boolean endOfBatch) throws Exception {
                record.prepareBuffer();
            }
        }).then(new EventHandler<RawOrder>() {
            @Override
            public void onEvent(RawOrder record, long sequence, boolean endOfBatch) throws Exception {
                OrderHouse.INSTANCE.receiveOrder(record.encodedBuffer, record.oid, record.buyerIndex, record.goodsIndex);
            }
        }).then(new EventHandler<RawOrder>() {
            @Override
            public void onEvent(RawOrder record, long sequence, boolean endOfBatch) throws Exception {
                GoodsHouse.INSTANCE.receiveOrder(record.encodedBuffer, record.oid, record.buyerIndex, record.goodsIndex);
            }
        }).then(new EventHandler<RawOrder>() {
            @Override
            public void onEvent(RawOrder record, long sequence, boolean endOfBatch) throws Exception {
                BuyerHouse.INSTANCE.receiveOrder(record.encodedBuffer, record.ctime, record.buyerIndex, record.goodsIndex);
            }
        });
        final RingBuffer<RawOrder> ringBuffer = disruptor.start();

        Future<?> future1 = IOExecutors.DISK_ONE_EXECUTOR.submit(new OrderLoaderTask(filesOne, ringBuffer));
        Future<?> future2 = IOExecutors.DISK_TWO_EXECUTOR.submit(new OrderLoaderTask(filesTwo, ringBuffer));
        Future<?> future3 = IOExecutors.DISK_THREE_EXECUTOR.submit(new OrderLoaderTask(filesThree, ringBuffer));
        try {
            future1.get();
            future2.get();
            future3.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        disruptor.shutdown();
        disruptor.halt();
        GoodsHouse.INSTANCE.flushData();
        BuyerHouse.INSTANCE.flushData();
        OrderHouse.INSTANCE.flushData();
    }

    private class RawOrder {
        private final int[] keys = new int[MAX_KEYS];
        private final int[] starts = new int[MAX_KEYS];
        private final int[] values = new int[MAX_KEYS];
        private int keySize = 0;
        private final byte[] data = new byte[MAX_DATA_LENGTH];
        private int dataSize;
        private final ByteBuffer encodedBuffer = ByteBuffer.allocate(MAX_DATA_LENGTH);
        private long oid;
        private int buyerIndex;
        private int goodsIndex;
        private long ctime;

        void init(){
            this.keySize = 0;
            this.dataSize = 0;
            encodedBuffer.clear();
        }

        void inToBuffer(byte[] bytes, int from, int length){
            System.arraycopy(bytes, from, data, dataSize, length);
            dataSize+=length;
        }

        void prepareKeys(){
            int pos = 0, keyLength = 0, length=0;
            for(int i=0; i<dataSize; i++){
                if(data[i]== Params.SEP_KEY_VALUE){
                    keyLength = length;
                    length = 0;
                }else if(data[i]==Params.SEP_KV){
                    endKV(data, pos, keyLength, length);
                    pos += keyLength + length + 2;
                    length = 0;
                }else{
                    length++;
                }
            }
            endKV(data, pos, keyLength, length);
        }

        private void endKV(byte[] bytes, int start, int keyLength, int valueLength){
            int index = Keys.indexOfBytes(bytes, start, start+keyLength);
            if(index<0)return;
            keys[keySize] = index;
            starts[keySize] = start+keyLength+1;
            values[keySize] = valueLength;
            keySize++;
        }

        void prepareBuffer(){
            for(int i=0; i<keySize; i++){
                int index = keys[i];
                if(index==Keys.GOODS_ID_INDEX){
                    this.goodsIndex = GoodsLoader.INSTANCE.indexOf(IndexUtil.StringIndexToLong(data, starts[i], values[i]));
                }else if(index==Keys.BUYER_ID_INDEX){
                    this.buyerIndex = BuyerLoader.INSTANCE.indexOf(IndexUtil.StringIndexToLong(data, starts[i], values[i]));
                }else if(index==Keys.ORDER_ID_INDEX){
                    this.oid = IndexUtil.LongIndexToLong(data, starts[i], values[i]);
                }else if(index==Keys.CREATE_TIME_INDEX){
                    this.ctime = IndexUtil.LongIndexToLong(data, starts[i], values[i]);
                }
                encodedBuffer.put((byte) index);
                switch (Keys.KEYS_TYPE[index]){
                    case 0:{
                        encodedBuffer.put((byte) (data[starts[i]]==116?1:0));
                        break;
                    }
                    case 1:{
                        encodedBuffer.putLong(IndexUtil.LongIndexToLong(data, starts[i], values[i]));
                        break;
                    }
                    default:{
                        encodedBuffer.put((byte) values[i]);
                        encodedBuffer.put(data, starts[i], values[i]);
                    }
                }
            }
            encodedBuffer.flip();
        }

    }

    private class OrderLoaderTask implements Runnable{

        private final String[] files;
        private final RingBuffer<RawOrder> ringBuffer;

        OrderLoaderTask(String[] files, RingBuffer<RawOrder> ringBuffer){
            this.files = files;
            this.ringBuffer = ringBuffer;
        }

        @Override
        public void run() {
            try{
                ByteBuffer byteBuffer = ByteBuffer.allocate(Params.BUFFER_SIZE_TABLE);
                final byte[] bytes = byteBuffer.array();
                for(String sourceFile : files){
                    Path path = Paths.get(sourceFile);
                    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
                    long size = channel.size(), finish = 0;
                    long sequence = ringBuffer.next();
                    RawOrder raw = ringBuffer.get(sequence);
                    raw.init();
                    int length;
                    while((length = channel.read(byteBuffer))>0){
                        int mark = 0;
                        for(int i=0; i<length; i++){
                            finish++;
                            if(bytes[i]== Params.SEP_ROW){
                                if(mark<i)raw.inToBuffer(bytes, mark, i-mark);
                                ringBuffer.publish(sequence);
                                if(finish<size){
                                    sequence = ringBuffer.next();
                                    raw = ringBuffer.get(sequence);
                                    raw.init();
                                }
                                mark = i+1;
                            }
                        }
                        if(mark<length){
                            raw.inToBuffer(bytes, mark, length-mark);
                        }
                        byteBuffer.clear();
                    }
                    channel.close();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}

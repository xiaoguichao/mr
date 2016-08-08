package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.Params;
import com.alibaba.middleware.race.io.IOExecutors;
import com.alibaba.middleware.race.io.ThreadLocalByteBuffer;
import com.alibaba.middleware.race.utils.IndexUtil;
import com.alibaba.middleware.race.utils.Keys;
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
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BuyerLoader {
    public static final BuyerLoader INSTANCE = new BuyerLoader();
    private static final int MAX_KEYS = 24;
    private static final int MAX_DATA_LENGTH = 1024;

    private static final String[] filesOne = new String[]{"/disk1/buyers/buyer.0.0", "/disk1/buyers/buyer.0.3"};
    private static final String[] filesTwo = new String[]{"/disk2/buyers/buyer.1.1", "/disk2/buyers/buyer.1.4"};
    private static final String[] filesThree = new String[]{"/disk3/buyers/buyer.2.2"};

    private final EventFactory<RawBuyer> RAW_FACTORY = new EventFactory<RawBuyer>() {
        @Override
        public RawBuyer newInstance() {
            return new RawBuyer();
        }
    };

    private final BuyerIndex[] buyerIndices = new BuyerIndex[Params.NUM_BUYER];
    private final long[] buyerIds = new long[Params.NUM_BUYER];
    private int cur = 0;
    private final ByteBuffer hugeBuffer = ByteBuffer.allocateDirect(1168768755);
    private final ThreadLocalByteBuffer localDataBuffer = new ThreadLocalByteBuffer(hugeBuffer);

    private BuyerLoader(){}

    public void load() throws InterruptedException {
        Disruptor<RawBuyer> disruptor = new Disruptor<RawBuyer>(RAW_FACTORY, 256, DaemonThreadFactory.INSTANCE, ProducerType.MULTI, new BlockingWaitStrategy());
        //noinspection unchecked
        disruptor.handleEventsWith(new EventHandler<RawBuyer>() {
            @Override
            public void onEvent(RawBuyer record, long sequence, boolean endOfBatch) throws Exception {
                record.prepareKeys();
            }
        }).then(new EventHandler<RawBuyer>() {
            @Override
            public void onEvent(RawBuyer record, long sequence, boolean endOfBatch) throws Exception {
                long position = record.outToBuffer();
                buyerIndices[cur++] = new BuyerIndex(record.bid, position);
            }
        });
        final RingBuffer<RawBuyer> ringBuffer = disruptor.start();

        Future<?> future1 = IOExecutors.DISK_ONE_EXECUTOR.submit(new BuyerLoaderTask(filesOne, ringBuffer));
        Future<?> future2 = IOExecutors.DISK_TWO_EXECUTOR.submit(new BuyerLoaderTask(filesTwo, ringBuffer));
        Future<?> future3 = IOExecutors.DISK_THREE_EXECUTOR.submit(new BuyerLoaderTask(filesThree, ringBuffer));
        try {
            future1.get();
            future2.get();
            future3.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        disruptor.shutdown();
        disruptor.halt();
        Arrays.sort(buyerIndices, 0, cur);
        for(int i=0; i<cur; i++){
            buyerIds[i] = buyerIndices[i].bid;
        }
        hugeBuffer.flip();
    }

    public ByteBuffer getDataBufferByIndex(int index){
        long i = buyerIndices[index].position;
        ByteBuffer dataBuffer = localDataBuffer.get();
        dataBuffer.limit((int)(i>>28)+(int)(i&0xFFFFFFF));
        dataBuffer.position((int)(i>>28));
        return dataBuffer;
    }

    public int indexOf(long bid){
        return Arrays.binarySearch(buyerIds, 0, cur, bid);
    }

    private static class BuyerIndex implements Comparable<BuyerIndex>{

        long bid;
        long position;

        BuyerIndex(long bid, long position){
            this.bid = bid;
            this.position = position;
        }

        @Override
        public int compareTo(BuyerIndex o) {
            return this.bid<o.bid?-1:(this.bid==o.bid?0:1);
        }

    }

    private class RawBuyer {
        private final int[] keys = new int[MAX_KEYS];
        private final int[] starts = new int[MAX_KEYS];
        private final int[] values = new int[MAX_KEYS];
        private int keySize = 0;
        private final byte[] data = new byte[MAX_DATA_LENGTH];
        private int dataSize;
        private long bid;

        void init(){
            this.keySize = 0;
            this.dataSize = 0;
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

        long outToBuffer(){
            long start = hugeBuffer.position();
            for(int i=0; i<keySize; i++){
                int index = keys[i];
                if(index==Keys.BUYER_ID_INDEX){
                    this.bid = IndexUtil.StringIndexToLong(data, starts[i], values[i]);
                    continue;
                }
                hugeBuffer.put((byte) index);
                switch (Keys.KEYS_TYPE[index]){
                    case 0:{
                        hugeBuffer.put((byte) (data[starts[i]]==116?1:0));
                        break;
                    }
                    case 1:{
                        hugeBuffer.putLong(IndexUtil.LongIndexToLong(data, starts[i], values[i]));
                        break;
                    }
                    default:{
                        hugeBuffer.put((byte) values[i]);
                        hugeBuffer.put(data, starts[i], values[i]);
                    }
                }
            }
            return (start<<28)|(hugeBuffer.position() - start);
        }

    }

    private class BuyerLoaderTask implements Runnable{

        private final String[] files;
        private final RingBuffer<RawBuyer> ringBuffer;

        BuyerLoaderTask(String[] files, RingBuffer<RawBuyer> ringBuffer){
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
                    RawBuyer raw = ringBuffer.get(sequence);
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
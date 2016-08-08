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

public class GoodsLoader {

    public static final GoodsLoader INSTANCE = new GoodsLoader();
    private static final int MAX_KEYS = 24;
    private static final int MAX_DATA_LENGTH = 80000;

    private static final String[] filesOne = new String[]{"/disk1/goods/good.0.0", "/disk1/goods/good.0.3"};
    private static final String[] filesTwo = new String[]{"/disk2/goods/good.1.1", "/disk2/goods/good.1.4"};
    private static final String[] filesThree = new String[]{"/disk3/goods/good.2.2"};

    private final EventFactory<RawGoods> RAW_FACTORY = new EventFactory<RawGoods>() {
        @Override
        public RawGoods newInstance() {
            return new RawGoods();
        }
    };

    private final GoodsIndex[] goodsIndices = new GoodsIndex[Params.NUM_GOODS];
    private final long[] goodsIds = new long[Params.NUM_GOODS];
    private int cur = 0;
    private final ByteBuffer hugeBuffer = ByteBuffer.allocateDirect(700301995);
    private final ThreadLocalByteBuffer localDataBuffer = new ThreadLocalByteBuffer(hugeBuffer);

    private GoodsLoader(){}

    void load() throws InterruptedException {
        Disruptor<RawGoods> disruptor = new Disruptor<RawGoods>(RAW_FACTORY, 256, DaemonThreadFactory.INSTANCE, ProducerType.MULTI, new BlockingWaitStrategy());
        //noinspection unchecked
        disruptor.handleEventsWith(new EventHandler<RawGoods>() {
            @Override
            public void onEvent(RawGoods record, long sequence, boolean endOfBatch) throws Exception {
                record.prepareKeys();
            }
        }).then(new EventHandler<RawGoods>() {
            @Override
            public void onEvent(RawGoods record, long sequence, boolean endOfBatch) throws Exception {
                long position = record.outToBuffer();
                goodsIndices[cur++] = new GoodsIndex(record.gid, position);
            }
        });
        final RingBuffer<RawGoods> ringBuffer = disruptor.start();

        Future<?> future1 = IOExecutors.DISK_ONE_EXECUTOR.submit(new GoodsLoaderTask(filesOne, ringBuffer));
        Future<?> future2 = IOExecutors.DISK_TWO_EXECUTOR.submit(new GoodsLoaderTask(filesTwo, ringBuffer));
        Future<?> future3 = IOExecutors.DISK_THREE_EXECUTOR.submit(new GoodsLoaderTask(filesThree, ringBuffer));
        try {
            future1.get();
            future2.get();
            future3.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        disruptor.shutdown();
        disruptor.halt();
        Arrays.sort(goodsIndices, 0, cur);
        for(int i=0; i<cur; i++){
            goodsIds[i] = goodsIndices[i].gid;
        }
        hugeBuffer.flip();
    }

    ByteBuffer getDataBufferByIndex(int index){
        long i = goodsIndices[index].position;
        ByteBuffer dataBuffer = localDataBuffer.get();
        dataBuffer.limit((int)(i>>28)+(int)(i&0xFFFFFFF));
        dataBuffer.position((int)(i>>28));
        return dataBuffer;
    }

    public int indexOf(long gid){
        return Arrays.binarySearch(goodsIds, 0, cur, gid);
    }

    private static class GoodsIndex implements Comparable<GoodsIndex>{

        long gid;
        long position;

        GoodsIndex(long gid, long position){
            this.gid = gid;
            this.position = position;
        }

        @Override
        public int compareTo(GoodsIndex o) {
            return this.gid<o.gid?-1:(this.gid==o.gid?0:1);
        }

    }

    private class RawGoods {
        private final int[] keys = new int[MAX_KEYS];
        private final int[] starts = new int[MAX_KEYS];
        private final int[] values = new int[MAX_KEYS];
        private int keySize = 0;
        private final byte[] data = new byte[MAX_DATA_LENGTH];
        private int dataSize;
        private long gid;

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
                if(index==Keys.GOODS_ID_INDEX){
                    this.gid = IndexUtil.StringIndexToLong(data, starts[i], values[i]);
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
                    case 2:
                    case 3:
                    case 4:
                        hugeBuffer.put((byte) values[i]);
                        hugeBuffer.put(data, starts[i], values[i]);
                        break;
                    default:{
                        hugeBuffer.putLong(LongStringCache.INSTANCE.receiveBytes(data, starts[i], values[i]));
                    }
                }
            }
            return (start<<28)|(hugeBuffer.position() - start);
        }

    }

    private class GoodsLoaderTask implements Runnable{

        private final String[] files;
        private final RingBuffer<RawGoods> ringBuffer;

        GoodsLoaderTask(String[] files, RingBuffer<RawGoods> ringBuffer){
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
                    RawGoods raw = ringBuffer.get(sequence);
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
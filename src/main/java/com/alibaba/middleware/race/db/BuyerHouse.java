package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Params;
import com.alibaba.middleware.race.utils.Keys;
import com.alibaba.middleware.race.io.DataFlushUnit;
import com.alibaba.middleware.race.io.QueueDataFlusher;
import com.alibaba.middleware.race.model.SimpleRow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public class BuyerHouse {
    public static final BuyerHouse INSTANCE = new BuyerHouse();
    private static final String HOUSE_CACHE_ROOT = "/disk3/stores/";
    //private static final String HOUSE_CACHE_ROOT = "C://disk3/stores/";
    private static final int MAX_ROW_LENGTH = 512;
    private static final int MAX_ROOM_SIZE = 0x4000000;
    private static final long MIN_TIME = 1400000000;
    private static final long MAX_TIME = 0xFFFFFFFFFL;
    private static final SimpleRow[] EMPTY_RESULT = new SimpleRow[0];
    private static final Comparator<SimpleRow> ROW_COMPARATOR = new Comparator<SimpleRow>() {
        @Override
        public int compare(SimpleRow o1, SimpleRow o2) {
            try {
                return o1.get(Keys.CREATE_TIME_INDEX).longValue()>o2.get(Keys.CREATE_TIME_INDEX).longValue()?-1:1;
            } catch (OrderSystem.TypeException e) {
                e.printStackTrace();
                return 0;
            }
        }
    };

    private final ArrayBlockingQueue<ByteBuffer> NEW_BUFFER_QUEUE = new ArrayBlockingQueue<ByteBuffer>(8);
    private final BuyerRoom[] buyerRooms = new BuyerRoom[1<< Params.BUYER_ROOM_POWER];
    private final int roomHash = (1<<Params.BUYER_ROOM_POWER)-1;

    public final QueueDataFlusher dataFlusher = new QueueDataFlusher();

    private BuyerHouse(){
        try{
            for(int i=0; i<buyerRooms.length; i++){
                buyerRooms[i] = new BuyerRoom(i);
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void receiveOrder(ByteBuffer orderBuffer, long createTime, int buyerIndex, int goodsIndex) throws InterruptedException {
        buyerRooms[buyerIndex&roomHash].receiveOrder(orderBuffer, buyerIndex>>Params.BUYER_ROOM_POWER, createTime, goodsIndex);
    }

    public void flushData() throws InterruptedException {
        for(BuyerRoom room : buyerRooms){
            room.finishReceiving();
        }
        dataFlusher.shutdown();
    }

    public void startSorting() throws IOException {
        final ArrayBlockingQueue<BuyerRoom> queueA = new ArrayBlockingQueue<BuyerRoom>(8);
        final ArrayBlockingQueue<BuyerRoom> queueB = new ArrayBlockingQueue<BuyerRoom>(8);
        for(int i=0; i<16; i++)NEW_BUFFER_QUEUE.offer(ByteBuffer.allocate(MAX_ROOM_SIZE));
        new Thread(new Runnable() {
            @Override
            public void run() {
                int c = buyerRooms.length;
                ByteBuffer tempBuffer = ByteBuffer.allocate(MAX_ROOM_SIZE);
                while(c>0){
                    try {
                        BuyerRoom room = queueA.take();
                        tempBuffer = room.sort(tempBuffer);
                        queueB.put(room);
                        c--;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                int c = buyerRooms.length;
                while(c>0){
                    try {
                        BuyerRoom room = queueB.take();
                        room.endSort();
                        c--;
                    } catch (InterruptedException|IOException e) {
                        e.printStackTrace();
                    }
                }
                DBService.sortCounter.countDown();
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                for(BuyerRoom room : buyerRooms){
                    try {
                        room.preSort();
                        queueA.put(room);
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                    }
                }
                for(int i=0; i<8; i++){
                    try {
                        NEW_BUFFER_QUEUE.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public Iterator<OrderSystem.Result> listRows(long bid, final long startTime, final long endTime) throws IOException {
        if(endTime<= MIN_TIME||startTime>MAX_TIME||startTime>=endTime)return DBService.EMPTY_ITERATOR;
        final int index = BuyerLoader.INSTANCE.indexOf(bid);
        if(index>=0){
            return new Iterator<OrderSystem.Result>() {

                private final SimpleRow[] rows = buyerRooms[index&roomHash].queryRows(index>>Params.GOODS_ROOM_POWER, startTime, endTime);
                private int idx = 0;

                @Override
                public boolean hasNext() {
                    return rows.length>idx;
                }

                @Override
                public OrderSystem.Result next() {
                    if(hasNext()){
                        return rows[idx++];
                    }
                    return null;
                }

                @Override
                public void remove(){

                }
            };
        }
        return DBService.EMPTY_ITERATOR;
    }

    public void destroy() throws IOException {
        for(BuyerRoom room : buyerRooms){
            room.destroy();
        }
    }

    private class BuyerRoom {
        private FileChannel cacheChannel;
        private final Path cachePath;

        private DataFlushUnit currentUnit;
        private ByteBuffer currentBuffer;

        private final int index;
        private final int[] sizes;
        private final int[] offsets;

        BuyerRoom(int index) throws IOException {
            this.index = index;
            this.cachePath = Paths.get(HOUSE_CACHE_ROOT, "bc"+index);
            this.cacheChannel = FileChannel.open(cachePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
            this.sizes = new int[(Params.NUM_BUYER>>Params.BUYER_ROOM_POWER)+1];
            this.offsets = new int[(Params.NUM_BUYER>>Params.BUYER_ROOM_POWER)+1];
        }

        void receiveOrder(ByteBuffer orderBuffer, int buyerIndex, long createTime, int goodsIndex) throws InterruptedException {
            if(currentBuffer==null || currentBuffer.remaining()<MAX_ROW_LENGTH){
                if(currentBuffer!=null)dataFlusher.publish(currentUnit);
                this.currentUnit = dataFlusher.getNext();
                currentUnit.fileChannel = cacheChannel;
                this.currentBuffer = currentUnit.directByteBuffer;
                currentBuffer.clear();
            }
            short length = (short) (orderBuffer.limit()+14);
            currentBuffer.putShort(length);
            currentBuffer.putLong(((long)buyerIndex<<40)|createTime);
            currentBuffer.putInt(goodsIndex);
            currentBuffer.put(orderBuffer);
            sizes[buyerIndex]+=length;
        }

        void finishReceiving() throws InterruptedException {
            if(currentBuffer!=null && currentBuffer.position()>0){
                dataFlusher.publish(currentUnit);
            }
            currentBuffer = null;
            currentUnit = null;
        }

        public void preSort() throws InterruptedException, IOException {
            this.currentBuffer = NEW_BUFFER_QUEUE.take();
            int size = (int) cacheChannel.size(), r = 0;
            cacheChannel.position(0);
            while(r<size)r+=cacheChannel.read(currentBuffer);
            for(int i=1; i<offsets.length;i++){
                offsets[i] = offsets[i-1]+sizes[i-1];
            }
        }

        public ByteBuffer sort(ByteBuffer tempBuffer) {
            ByteBuffer inBuffer = this.currentBuffer;
            try{
                tempBuffer.clear();
                int[] tempSizes = new int[(Params.NUM_BUYER>>Params.BUYER_ROOM_POWER)+1];
                int size = (int) cacheChannel.size();

                for(int i=0; i<size;){
                    short length = inBuffer.getShort(i);
                    int bi = (int) (inBuffer.getLong(i+2)>>40);
                    inBuffer.limit(i+length);
                    inBuffer.position(i);
                    tempBuffer.position(offsets[bi]+tempSizes[bi]);
                    tempBuffer.put(inBuffer);
                    tempSizes[bi]+=length;
                    i+=length;
                    inBuffer.limit(size);
                }
                tempBuffer.limit(size);
                tempBuffer.position(0);
                this.currentBuffer = tempBuffer;
            }catch (IOException e){
                e.printStackTrace();
            }
            return inBuffer;
        }

        void endSort() throws IOException, InterruptedException {
            cacheChannel.position(0);
            cacheChannel.write(currentBuffer);
            cacheChannel.close();
            cacheChannel = FileChannel.open(cachePath, StandardOpenOption.READ);
            currentBuffer.clear();
            NEW_BUFFER_QUEUE.put(currentBuffer);
            currentBuffer = null;
        }

        SimpleRow[] queryRows(int buyerIndex, long startTime, long endTime) throws IOException {
            if(sizes[buyerIndex]==0)return EMPTY_RESULT;
            ByteBuffer byteBuffer = ByteBuffer.allocate(sizes[buyerIndex]);
            synchronized (this){
                cacheChannel.position(offsets[buyerIndex]);
                cacheChannel.read(byteBuffer);
            }
            byteBuffer.flip();
            int offset = 0;
            ArrayList<SimpleRow> rowList = new ArrayList<>();
            ByteBuffer buyerBuffer = BuyerLoader.INSTANCE.getDataBufferByIndex((buyerIndex<<Params.BUYER_ROOM_POWER)|index);
            while(byteBuffer.hasRemaining()){
                short length = byteBuffer.getShort();
                long createTime = byteBuffer.getLong()&0xFFFFFFFFFFL;
                int goodsIndex = byteBuffer.getInt();
                if(createTime>=startTime&&createTime<endTime){
                    byteBuffer.limit(offset+length);
                    SimpleRow sr = new SimpleRow(0L, null);
                    sr.join(byteBuffer);
                    sr.join(GoodsLoader.INSTANCE.getDataBufferByIndex(goodsIndex));

                    buyerBuffer.mark();
                    sr.join(buyerBuffer);
                    buyerBuffer.reset();

                    sr.autoFillOid();
                    rowList.add(sr);
                }
                offset += length;
                byteBuffer.limit(sizes[buyerIndex]);
                byteBuffer.position(offset);
            }
            SimpleRow[] result = rowList.toArray(EMPTY_RESULT);
            Arrays.sort(result, ROW_COMPARATOR);
            return result;
        }

        void destroy() throws IOException {
            cacheChannel.close();
            cachePath.toFile().delete();
        }
    }
}

package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.Params;
import com.alibaba.middleware.race.io.DataFlushUnit;
import com.alibaba.middleware.race.io.QueueDataFlusher;
import com.alibaba.middleware.race.model.SimpleRow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;

public class OrderHouse {

    public static final OrderHouse INSTANCE = new OrderHouse();
    private static final String HOUSE_CACHE_ROOT = "/disk1/stores/";
    //private static final String HOUSE_ROOT = "C://disk1/stores/";
    private static final int MAX_ROW_LENGTH = 512;
    private static final int MAX_ROOM_SIZE = 0x4000000;
    private static final long MIN_ID = 500000000;
    private static final long MAX_ID = 0xFFFFFFFFFL;

    private final ArrayBlockingQueue<ByteBuffer> NEW_BUFFER_QUEUE = new ArrayBlockingQueue<ByteBuffer>(8);
    private final OrderRoom[] orderRooms = new OrderRoom[1<< Params.ORDER_ROOM_POWER];
    private final int roomHash = (1<<Params.ORDER_ROOM_POWER)-1;

    public final QueueDataFlusher dataFlusher = new QueueDataFlusher();

    private OrderHouse(){
        try{
            for(int i=0; i<orderRooms.length; i++){
                orderRooms[i] = new OrderRoom(i);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void receiveOrder(ByteBuffer orderBuffer, long oid, int buyerIndex, int goodsIndex) throws InterruptedException {
        orderRooms[(int) (oid&roomHash)].receiveOrder(orderBuffer, (int)(oid>>Params.ORDER_ROOM_POWER), buyerIndex, goodsIndex);
    }

    public void flushData() throws IOException, InterruptedException {
        for(OrderRoom room : orderRooms){
            room.finishReceiving();
        }
        dataFlusher.shutdown();
    }

    public void startSorting(){
        final ArrayBlockingQueue<OrderRoom> queueA = new ArrayBlockingQueue<OrderRoom>(8);
        final ArrayBlockingQueue<OrderRoom> queueB = new ArrayBlockingQueue<OrderRoom>(8);
        for(int i=0; i<8; i++)NEW_BUFFER_QUEUE.offer(ByteBuffer.allocate(MAX_ROOM_SIZE));
        new Thread(new Runnable() {
            @Override
            public void run() {
                int c = orderRooms.length;
                ByteBuffer tempBuffer = ByteBuffer.allocate(MAX_ROOM_SIZE);
                while(c>0){
                    try {
                        OrderRoom room = queueA.take();
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
                int c = orderRooms.length;
                while(c>0){
                    try {
                        OrderRoom room = queueB.take();
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
                for(OrderRoom room : orderRooms){
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

    public SimpleRow getRow(long oid, Collection<String> queryKeys) throws IOException {
        if(oid>MAX_ID||oid<MIN_ID)return null;
        return orderRooms[(int) (oid&roomHash)].getRow((int)(oid>>Params.ORDER_ROOM_POWER), queryKeys);
    }

    public void destroy() throws IOException {
        for(OrderRoom room : orderRooms){
            room.destroy();
        }
    }

    private class OrderRoom{

        private static final int UNIT_NUM = 1<<Params.ORDER_ROOM_UNIT_POWER;
        private static final int UNIT_HASH = UNIT_NUM-1;

        private FileChannel cacheChannel;
        private final Path cachePath;

        private DataFlushUnit currentUnit;
        private ByteBuffer currentBuffer;

        private final int index;
        private final int[] sizes;
        private final int[] offsets;

        OrderRoom(int index) throws IOException {
            this.index = index;
            this.cachePath = Paths.get(HOUSE_CACHE_ROOT, "oc"+index);
            this.cacheChannel = FileChannel.open(cachePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
            this.sizes = new int[UNIT_NUM];
            this.offsets = new int[UNIT_NUM];
        }

        public void receiveOrder(ByteBuffer orderBuffer, int oid, int buyerIndex, int goodsIndex) throws InterruptedException {
            if(currentBuffer==null || currentBuffer.remaining()<MAX_ROW_LENGTH){
                if(currentBuffer!=null)dataFlusher.publish(currentUnit);
                this.currentUnit = dataFlusher.getNext();
                currentUnit.fileChannel = cacheChannel;
                this.currentBuffer = currentUnit.directByteBuffer;
                currentBuffer.clear();
            }
            orderBuffer.mark();
            short length = (short) (orderBuffer.limit()+14);
            currentBuffer.putShort(length);
            currentBuffer.putInt(oid);
            currentBuffer.putInt(buyerIndex);
            currentBuffer.putInt(goodsIndex);
            currentBuffer.put(orderBuffer);
            orderBuffer.reset();
            sizes[oid&UNIT_HASH]+=length;
        }

        public void finishReceiving() throws InterruptedException {
            if(currentBuffer!=null && currentBuffer.position()>0){
                dataFlusher.publish(currentUnit);
            }
            currentBuffer = null;
            currentUnit = null;
        }

        public void preSort() throws InterruptedException, IOException {
            this.currentBuffer = NEW_BUFFER_QUEUE.take();
            int size = (int) cacheChannel.size(), r = 0;
            //System.out.println(size);
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
                int[] tempSizes = new int[UNIT_NUM];
                int size = (int) cacheChannel.size();

                for(int i=0; i<size;){
                    short length = inBuffer.getShort(i);
                    int oi = inBuffer.getInt(i+2)&UNIT_HASH;
                    inBuffer.limit(i+length);
                    inBuffer.position(i);
                    tempBuffer.position(offsets[oi]+tempSizes[oi]);
                    tempBuffer.put(inBuffer);
                    tempSizes[oi]+=length;
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

        public SimpleRow getRow(int oid, Collection<String> queryKeys) throws IOException {
            if(sizes[oid&UNIT_HASH]==0)return null;
            ByteBuffer byteBuffer = ByteBuffer.allocate(sizes[oid&UNIT_HASH]);
            synchronized (this){
                cacheChannel.position(offsets[oid&UNIT_HASH]);
                cacheChannel.read(byteBuffer);
            }
            byteBuffer.flip();
            int offset = 0;
            while(byteBuffer.hasRemaining()){
                short length = byteBuffer.getShort();
                int orderIndex = byteBuffer.getInt();
                if(oid==orderIndex){
                    int buyerIndex = byteBuffer.getInt();
                    int goodsIndex = byteBuffer.getInt();
                    byteBuffer.limit(offset+length);
                    SimpleRow sr = new SimpleRow(((long)oid<<Params.ORDER_ROOM_POWER)|index, queryKeys);
                    if(!sr.completed()){
                        sr.join(byteBuffer);
                    }
                    if(!sr.completed()){
                        sr.join(GoodsLoader.INSTANCE.getDataBufferByIndex(goodsIndex));
                    }
                    if(!sr.completed()){
                        sr.join(BuyerLoader.INSTANCE.getDataBufferByIndex(buyerIndex));
                    }
                    return sr;
                }
                offset += length;
                byteBuffer.limit(sizes[oid&UNIT_HASH]);
                byteBuffer.position(offset);
            }
            return  null;
        }

        public void destroy() throws IOException {
            cacheChannel.close();
            //noinspection ResultOfMethodCallIgnored
            cachePath.toFile().delete();
        }

    }
}

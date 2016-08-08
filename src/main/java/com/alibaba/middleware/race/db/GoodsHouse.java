package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Params;
import com.alibaba.middleware.race.utils.Keys;
import com.alibaba.middleware.race.io.DataFlushUnit;
import com.alibaba.middleware.race.io.IOExecutors;
import com.alibaba.middleware.race.io.QueueDataFlusher;
import com.alibaba.middleware.race.model.SimpleKV;
import com.alibaba.middleware.race.model.SimpleRow;
import com.alibaba.middleware.race.model.SimpleValue;
import com.alibaba.middleware.race.model.values.DoubleValue;
import com.alibaba.middleware.race.model.values.LongValue;
import com.alibaba.middleware.race.model.values.UnionValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public class GoodsHouse {
    public static final GoodsHouse INSTANCE = new GoodsHouse();
    private static final String HOUSE_CACHE_ROOT = "/disk2/stores/";
    //private static final String HOUSE_CACHE_ROOT = "C://disk2/stores/";
    private static final int MAX_ROW_LENGTH = 512;
    private static final int MAX_ROOM_SIZE = 0x4000000;
    private static final SimpleRow[] EMPTY_RESULT = new SimpleRow[0];
    private static final Comparator<SimpleRow> ROW_COMPARATOR = new Comparator<SimpleRow>() {
        @Override
        public int compare(SimpleRow o1, SimpleRow o2) {
            return o1.orderId()<o2.orderId()?-1:1;
        }
    };
    private final ArrayBlockingQueue<ByteBuffer> NEW_BUFFER_QUEUE = new ArrayBlockingQueue<ByteBuffer>(8);
    private final GoodsRoom[] goodsRooms = new GoodsRoom[1<< Params.GOODS_ROOM_POWER];
    private final int roomHash = (1<<Params.GOODS_ROOM_POWER)-1;

    private final QueueDataFlusher dataFlusher = new QueueDataFlusher();

    private GoodsHouse(){
        try{
            for(int i=0; i<goodsRooms.length; i++){
                goodsRooms[i] = new GoodsRoom(i);
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void receiveOrder(ByteBuffer orderBuffer, long oid, int buyerIndex, int goodsIndex) throws InterruptedException {
        goodsRooms[goodsIndex&roomHash].receiveOrder(orderBuffer, oid, buyerIndex, goodsIndex>>Params.GOODS_ROOM_POWER);
    }

    public void flushData() throws InterruptedException {
        for(GoodsRoom room : goodsRooms){
            room.finishReceiving();
        }
        dataFlusher.shutdown();
    }

    public void startSorting() throws IOException {
        final ArrayBlockingQueue<GoodsRoom> queueA = new ArrayBlockingQueue<GoodsRoom>(8);
        final ArrayBlockingQueue<GoodsRoom> queueB = new ArrayBlockingQueue<GoodsRoom>(8);
        for(int i=0; i<8; i++)NEW_BUFFER_QUEUE.offer(ByteBuffer.allocate(MAX_ROOM_SIZE));
        IOExecutors.DISK_ONE_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                int c = goodsRooms.length;
                ByteBuffer tempBuffer = ByteBuffer.allocate(MAX_ROOM_SIZE);
                while(c>0){
                    try {
                        GoodsRoom room = queueA.take();
                        tempBuffer = room.sort(tempBuffer);
                        queueB.put(room);
                        c--;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        IOExecutors.DISK_TWO_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                int c = goodsRooms.length;
                while(c>0){
                    try {
                        GoodsRoom room = queueB.take();
                        room.endSort();
                        c--;
                    } catch (InterruptedException|IOException e) {
                        e.printStackTrace();
                    }
                }
                DBService.sortCounter.countDown();
            }
        });
        IOExecutors.DISK_THREE_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                for(GoodsRoom room : goodsRooms){
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
        });
    }

    public Iterator<OrderSystem.Result> listRows(long gid, final Collection<String> queryKeys) throws IOException {
        final int index = GoodsLoader.INSTANCE.indexOf(gid);
        if(index>=0){
            return new Iterator<OrderSystem.Result>() {

                private final SimpleRow[] rows = goodsRooms[index&roomHash].queryRows(index>>Params.GOODS_ROOM_POWER, queryKeys);
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

    public SimpleKV sum(long gid, int keyIndex) throws IOException {
        int index = GoodsLoader.INSTANCE.indexOf(gid);
        if(index>=0){
            return goodsRooms[index&roomHash].sum(index>>Params.GOODS_ROOM_POWER, keyIndex);
        }
        return null;
    }

    public void destroy() throws IOException {
        for(GoodsRoom room : goodsRooms){
            room.destroy();
        }
    }

    private class GoodsRoom {

        private FileChannel cacheChannel;
        private final Path cachePath;

        private DataFlushUnit currentUnit;
        private ByteBuffer currentBuffer;

        private final int index;
        private final int[] sizes;
        private final int[] offsets;
        private final short[] numbers;

        GoodsRoom(int index) throws IOException {
            this.index = index;
            this.cachePath = Paths.get(HOUSE_CACHE_ROOT, "gc"+index);
            this.cacheChannel = FileChannel.open(cachePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
            this.sizes = new int[(Params.NUM_GOODS>>Params.GOODS_ROOM_POWER)+1];
            this.offsets = new int[(Params.NUM_GOODS>>Params.GOODS_ROOM_POWER)+1];
            this.numbers = new short[(Params.NUM_GOODS>>Params.GOODS_ROOM_POWER)+1];
        }

        void receiveOrder(ByteBuffer orderBuffer, long oid, int buyerIndex, int goodsIndex) throws InterruptedException {
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
            currentBuffer.putLong(((long)goodsIndex<<40)|oid);
            currentBuffer.putInt(buyerIndex);
            currentBuffer.put(orderBuffer);
            orderBuffer.reset();
            sizes[goodsIndex]+=length;
            numbers[goodsIndex]++;
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
                int[] tempSizes = new int[(Params.NUM_GOODS>>Params.GOODS_ROOM_POWER)+1];
                int size = (int) cacheChannel.size();

                for(int i=0; i<size;){
                    short length = inBuffer.getShort(i);
                    int gi = (int) (inBuffer.getLong(i+2)>>40);
                    inBuffer.limit(i+length);
                    inBuffer.position(i);
                    tempBuffer.position(offsets[gi]+tempSizes[gi]);
                    tempBuffer.put(inBuffer);
                    tempSizes[gi]+=length;
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

        SimpleRow[] queryRows(int goodsIndex, Collection<String> queryKeys) throws IOException {
            if(numbers[goodsIndex]==0)return EMPTY_RESULT;
            ByteBuffer byteBuffer = ByteBuffer.allocate(sizes[goodsIndex]);
            synchronized (this){
                cacheChannel.position(offsets[goodsIndex]);
                cacheChannel.read(byteBuffer);
            }
            byteBuffer.flip();
            int offset = 0;
            SimpleRow[] result = new SimpleRow[numbers[goodsIndex]];
            int i = 0;
            ByteBuffer goodsBuffer = GoodsLoader.INSTANCE.getDataBufferByIndex((goodsIndex<<Params.GOODS_ROOM_POWER)|index);
            while(byteBuffer.hasRemaining()){
                short length = byteBuffer.getShort();
                long goid = byteBuffer.getLong();
                int buyerIndex = byteBuffer.getInt();
                byteBuffer.limit(offset+length);
                SimpleRow sr = new SimpleRow(goid&0xFFFFFFFFFFL, queryKeys);
                if(!sr.completed()){
                    sr.join(byteBuffer);
                }
                if(!sr.completed()){
                    goodsBuffer.mark();
                    sr.join(goodsBuffer);
                    goodsBuffer.reset();
                }
                if(!sr.completed()){
                    sr.join(BuyerLoader.INSTANCE.getDataBufferByIndex(buyerIndex));
                }
                result[i++] = sr;
                offset += length;
                byteBuffer.limit(sizes[goodsIndex]);
                byteBuffer.position(offset);
            }
            Arrays.sort(result, ROW_COMPARATOR);
            return result;
        }

        SimpleKV sum(int goodsIndex, int keyIndex) throws IOException {
            byte keyType = Keys.KEYS_TYPE[keyIndex];
            switch (keyType){
                case Keys.TYPE_BOOLEAN:
                case Keys.TYPE_STRING:
                case Keys.TYPE_LONG_STRING:
                    return null;
                default:{
                    if(numbers[goodsIndex]==0)return null;
                    switch(Keys.KEYS_TABLE[keyIndex]){
                        case Keys.TABLE_GOODS: {
                            SimpleValue value = getValue(GoodsLoader.INSTANCE.getDataBufferByIndex((goodsIndex<<Params.GOODS_ROOM_POWER)|index), keyIndex);
                            if(value!=null){
                                try {
                                    return new SimpleKV(keyIndex, new LongValue(value.longValue()*numbers[goodsIndex]));
                                } catch (Exception e) {
                                }
                                try {
                                    return new SimpleKV(keyIndex, new DoubleValue(value.doubleValue()*numbers[goodsIndex]));
                                } catch (Exception e) {
                                }
                            }
                            return null;
                        }
                        default: {
                            ByteBuffer byteBuffer = ByteBuffer.allocate(sizes[goodsIndex]);
                            synchronized (this){
                                cacheChannel.position(offsets[goodsIndex]);
                                cacheChannel.read(byteBuffer);
                            }
                            try{
                                byteBuffer.flip();
                                int offset = 0;
                                long sumLong = 0;
                                boolean hasValue = false;
                                while(byteBuffer.hasRemaining()) {
                                    short length = byteBuffer.getShort();
                                    byteBuffer.getLong();
                                    int buyerIndex = byteBuffer.getInt();
                                    SimpleValue value = null;
                                    if(Keys.KEYS_TABLE[keyIndex]==Keys.TABLE_ORDER){
                                        byteBuffer.limit(offset + length);
                                        value = getValue(byteBuffer, keyIndex);
                                    }else{
                                        value = getValue(BuyerLoader.INSTANCE.getDataBufferByIndex(buyerIndex), keyIndex);
                                    }
                                    if(value!=null){
                                        sumLong += value.longValue();
                                        hasValue = true;
                                    }
                                    offset += length;
                                    byteBuffer.limit(sizes[goodsIndex]);
                                    byteBuffer.position(offset);
                                }
                                if(hasValue){
                                    return new SimpleKV(keyIndex, new LongValue(sumLong));
                                }
                            }catch(OrderSystem.TypeException e){}
                            try{
                                byteBuffer.clear();
                                int offset = 0;
                                double sumDouble = 0;
                                boolean hasValue = false;
                                while(byteBuffer.hasRemaining()) {
                                    short length = byteBuffer.getShort();
                                    byteBuffer.getLong();
                                    int buyerIndex = byteBuffer.getInt();
                                    SimpleValue value = null;
                                    if(Keys.KEYS_TABLE[keyIndex]==Keys.TABLE_ORDER){
                                        byteBuffer.limit(offset + length);
                                        value = getValue(byteBuffer, keyIndex);
                                    }else{
                                        value = getValue(BuyerLoader.INSTANCE.getDataBufferByIndex(buyerIndex), keyIndex);
                                    }
                                    if(value!=null){
                                        sumDouble += value.doubleValue();
                                        hasValue = true;
                                    }
                                    offset += length;
                                    byteBuffer.limit(sizes[goodsIndex]);
                                    byteBuffer.position(offset);
                                }
                                if(hasValue){
                                    return new SimpleKV(keyIndex, new DoubleValue(sumDouble));
                                }
                            }catch(OrderSystem.TypeException e){}
                            return null;
                        }
                    }
                }
            }
        }

        private SimpleValue getValue(ByteBuffer byteBuffer, int keyIndex){
            while(byteBuffer.hasRemaining()){
                int index = byteBuffer.get()&0xFF;
                if(index==keyIndex){
                    switch(Keys.KEYS_TYPE[index]){
                        case Keys.TYPE_LONG:
                            return new LongValue(byteBuffer.getLong());
                        case Keys.TYPE_DOUBLE:
                        case Keys.TYPE_UNION:
                        case Keys.TYPE_STRING:
                            int length = byteBuffer.get()&0xFF;
                            return new UnionValue(byteBuffer, length);
                        default:
                            return null;
                    }
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
            return null;
        }

        void destroy() throws IOException {
            cacheChannel.close();
            //noinspection ResultOfMethodCallIgnored
            cachePath.toFile().delete();
        }

    }
}
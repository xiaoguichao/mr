package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Params;
import com.alibaba.middleware.race.utils.Keys;
import com.alibaba.middleware.race.io.IOExecutors;
import com.alibaba.middleware.race.utils.IndexUtil;
import com.alibaba.middleware.race.utils.TimeRecorder;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DBService implements Params {

    public static final DBService INSTANCE = new DBService();
    static final CountDownLatch sortCounter = new CountDownLatch(3);
    static final Iterator<OrderSystem.Result> EMPTY_ITERATOR = new Iterator<OrderSystem.Result>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public void remove() {}

        @Override
        public OrderSystem.Result next() {
            return null;
        }
    };

    private DBService(){}

    public void setup() throws InterruptedException, IOException {
        TimeRecorder.start();

        GoodsLoader.INSTANCE.load();
        LongStringCache.INSTANCE.backSave();
        TimeRecorder.record("goods load finish");
        System.gc();

        BuyerLoader.INSTANCE.load();
        TimeRecorder.record("buyer load finish");
        System.gc();

        OrderLoader.INSTANCE.load();
        TimeRecorder.record("order load finish");
        System.gc();

        GoodsHouse.INSTANCE.startSorting();
        BuyerHouse.INSTANCE.startSorting();
        OrderHouse.INSTANCE.startSorting();

        IOExecutors.DISK_ONE_EXECUTOR.shutdown();
        IOExecutors.DISK_TWO_EXECUTOR.shutdown();
        IOExecutors.DISK_THREE_EXECUTOR.shutdown();
        TimeRecorder.record("sort prepared");

        sortCounter.await();
        LongStringCache.INSTANCE.preLoad();
        IndexUtil.initNone();
        TimeRecorder.record("construct over");
    }

    public OrderSystem.Result getOrder(long orderId, Collection<String> keys) throws IOException {
        return OrderHouse.INSTANCE.getRow(orderId, keys);
    }

    public Iterator<OrderSystem.Result> listOrdersOfBuyer(final long startTime, final long endTime, String buyerId) throws IOException {
        return BuyerHouse.INSTANCE.listRows(IndexUtil.StringIndexToLong(buyerId), startTime, endTime);
    }

    public Iterator<OrderSystem.Result> listOrdersOfGoods(String goodid, final Collection<String> qrKeys) throws IOException {
        return GoodsHouse.INSTANCE.listRows(IndexUtil.StringIndexToLong(goodid), qrKeys);
    }

    public OrderSystem.KeyValue sumOrdersByGood(String goodid, String key) throws IOException {
        int keyIndex = Keys.indexOfString(key);
        if(keyIndex>=0)return GoodsHouse.INSTANCE.sum(IndexUtil.StringIndexToLong(goodid), keyIndex);
        return null;
    }

    public void destroy() throws IOException {
        OrderHouse.INSTANCE.destroy();
        GoodsHouse.INSTANCE.destroy();
        BuyerHouse.INSTANCE.destroy();
        LongStringCache.INSTANCE.destroy();
    }
}
package com.alibaba.middleware.race;

import com.alibaba.middleware.race.db.DBService;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class OrderSystemImpl implements OrderSystem {

    private final DBService dbService = DBService.INSTANCE;

    @Override
    public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles, Collection<String> storeFolders) throws IOException, InterruptedException {
        dbService.setup();
    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        try {
            return dbService.getOrder(orderId, keys);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        try {
            return dbService.listOrdersOfBuyer(startTime, endTime, buyerid);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        try {
            return dbService.listOrdersOfGoods(goodid, keys);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        try {
            return dbService.sumOrdersByGood(goodid, key);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

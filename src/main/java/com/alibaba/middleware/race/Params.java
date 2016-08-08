package com.alibaba.middleware.race;

public interface Params {

    int BUFFER_SIZE_TABLE = 0x100000;

    int NUM_GOODS = 4000000;
    int NUM_BUYER = 8000000;
    int NUM_ORDER = 400000000;

    int ORDER_ROOM_POWER = 10;
    int ORDER_ROOM_UNIT_POWER = 12;
    int GOODS_ROOM_POWER = 10;
    int BUYER_ROOM_POWER = 10;

    byte SEP_KEY_VALUE = 58;
    byte SEP_KV = 9;
    byte SEP_ROW = 10;

}

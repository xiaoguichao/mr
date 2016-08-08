package com.alibaba.middleware.race.utils;

public class TimeRecorder {
    private static long start;
    private static long record;

    public static void start(){
        start = System.nanoTime();
        record = start;
    }

    public static void record(String event){
        long now = System.nanoTime();
        long gap = now - record;
        record = now;
        long total = now - start;
        System.out.println(event+" use time: "+gap/1000000+"ms"+(gap%1000000)/1000+"us, total time: "+total/1000000+"ms"+(total%1000000)/1000+"us");
    }
}

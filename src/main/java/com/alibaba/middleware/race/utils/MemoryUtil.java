package com.alibaba.middleware.race.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public class MemoryUtil {

    private static final boolean INITED;

    private static final Method directBufferCleaner;
    private static final Method directBufferCleanerClean;
    private static final Field maxDirectMemory;
    private static final Field reservedDirectMemory;

    static {
        Method directBufferCleanerX = null, directBufferCleanerCleanX = null;
        Field maxMemory = null, reservedMemory = null;
        boolean v;
        try {
            directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
            directBufferCleanerX.setAccessible(true);
            directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
            directBufferCleanerCleanX.setAccessible(true);
            Class<?> c = Class.forName("java.nio.Bits");
            maxMemory = c.getDeclaredField("maxMemory");
            maxMemory.setAccessible(true);
            reservedMemory = c.getDeclaredField("reservedMemory");
            reservedMemory.setAccessible(true);
            v = true;
        } catch (Exception e) {
            v = false;
        }
        directBufferCleaner = directBufferCleanerX;
        directBufferCleanerClean = directBufferCleanerCleanX;
        maxDirectMemory = maxMemory;
        reservedDirectMemory = reservedMemory;
        INITED = v;
    }

    public static void cleanDirect(ByteBuffer buffer) {
        if (buffer == null) return;
        if (INITED && buffer.isDirect()) {
            try {
                Object cleaner = directBufferCleaner.invoke(buffer);
                directBufferCleanerClean.invoke(cleaner);
            } catch (Exception ignored) {
            }
        }
    }

    public static void printMemory(){
        if(!INITED)return;
        Runtime runtime = Runtime.getRuntime();
        try {
            System.out.println("memory: max-"+runtime.maxMemory()/(1<<20)+"M, total-"+runtime.totalMemory()/(1<<20)+"M, used-"+(runtime.totalMemory()-runtime.freeMemory())/(1<<20)+"M; direct: max-"+(Long)maxDirectMemory.get(null)/(1<<20)+"M, used-"+(Long)reservedDirectMemory.get(null)/(1<<20)+"M");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}

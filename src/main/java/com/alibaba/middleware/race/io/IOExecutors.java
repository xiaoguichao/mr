package com.alibaba.middleware.race.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IOExecutors {
    public static final ExecutorService DISK_ONE_EXECUTOR = Executors.newSingleThreadExecutor();
    public static final ExecutorService DISK_TWO_EXECUTOR = Executors.newSingleThreadExecutor();
    public static final ExecutorService DISK_THREE_EXECUTOR = Executors.newSingleThreadExecutor();
}

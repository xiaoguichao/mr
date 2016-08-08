package com.alibaba.middleware.race.io;

import com.alibaba.middleware.race.utils.MemoryUtil;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataFlushUnit {

    public FileChannel fileChannel;
    public ByteBuffer directByteBuffer;

    DataFlushUnit(int size){
        this.directByteBuffer = ByteBuffer.allocateDirect(size);
    }

    void clean(){
        this.fileChannel = null;
        MemoryUtil.cleanDirect(directByteBuffer);
        this.directByteBuffer = null;
    }
}

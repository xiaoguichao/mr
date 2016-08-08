package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.io.ThreadLocalCharBuffer;
import com.alibaba.middleware.race.utils.MemoryUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LongStringCache {

    public static final LongStringCache INSTANCE;
    private static final String cacheFilePath = "/disk1/stores/long_string";
    //private static final String cacheFilePath = "C://disk1/stores/long_string";

    static {
        LongStringCache instance;
        try {
            instance = new LongStringCache();
        } catch (IOException e) {
            instance = null;
            e.printStackTrace();
        }
        INSTANCE = instance;
    }

    private int offset = 0;
    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private final FileChannel cacheChannel = FileChannel.open(Paths.get(cacheFilePath), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
    private MappedByteBuffer mappedByteBuffer = cacheChannel.map(FileChannel.MapMode.READ_WRITE, 0, 919296850<<1);
    private CharBuffer dataBuffer = mappedByteBuffer.asCharBuffer();
    private ThreadLocalCharBuffer localDataBuffer;

    private LongStringCache() throws IOException {}

    public long receiveBytes(byte[] bytes, int from, int length){
        decoder.decode(ByteBuffer.wrap(bytes, from, length), dataBuffer,false);
        int newSize = dataBuffer.position()-offset;
        long r = ((long)offset<<24)|newSize;
        offset += newSize;
        return r;
    }

    public void backSave(){
        mappedByteBuffer.force();
        MemoryUtil.cleanDirect(mappedByteBuffer);
        mappedByteBuffer = null;
        dataBuffer = null;
    }

    public void preLoad() throws IOException {
        mappedByteBuffer = cacheChannel.map(FileChannel.MapMode.READ_ONLY, 0, 919296850<<1);
        mappedByteBuffer.load();
        dataBuffer = mappedByteBuffer.asCharBuffer().asReadOnlyBuffer();
        this.localDataBuffer = new ThreadLocalCharBuffer(dataBuffer);
    }

    public String getString(long index){
        int from = (int) (index>>24);
        int size = (int) (index&0xFFFFFF);
        char[] data = new char[size];
        CharBuffer readBuffer = localDataBuffer.get();
        readBuffer.position(from);
        readBuffer.get(data);
        return new String(data);
    }

    public void destroy() throws IOException {
        MemoryUtil.cleanDirect(mappedByteBuffer);
        cacheChannel.close();
        new File(cacheFilePath).delete();
    }
}

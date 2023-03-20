package com.chail.oracle;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MmapFile {
    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;

    public MmapFile(File file, long fileSize) throws Exception {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0,  Integer.MAX_VALUE);
    }

    public void write(byte[] data, int offset, int length) {
        mappedByteBuffer.put(data, offset, length);
    }


    public void write(byte[] data) {
        mappedByteBuffer.put(data);
    }


    public byte[] read(int offset, int length) {
        byte[] data = new byte[length];
        mappedByteBuffer.get(data, offset, length);
        return data;
    }

    public void flush() {
        mappedByteBuffer.force();
    }

    public void close() throws Exception {
        fileChannel.close();
        randomAccessFile.close();
    }
}

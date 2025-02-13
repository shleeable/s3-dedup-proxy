package com.jortage.poolmgr.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;

public class PngSurgeon implements Closeable {

    /**
     * Exception thrown when a PNG chunk has an invalid CRC.
     */
    public static class CRCException extends IOException {
        public CRCException(String msg) { super(msg); }
    }

    /**
     * Defines constants for common PNG chunk types.
     */
    public static final class Chunk {
        public static final int IHDR = fourcc("IHDR");
        public static final int PLTE = fourcc("PLTE");
        public static final int IDAT = fourcc("IDAT");
        public static final int IEND = fourcc("IEND");
        public static final int tRNS = fourcc("tRNS");
        public static final int cHRM = fourcc("cHRM");
        public static final int gAMA = fourcc("gAMA");
        public static final int iCCP = fourcc("iCCP");
        public static final int sBIT = fourcc("sBIT");
        public static final int sRGB = fourcc("sRGB");
        public static final int cICP = fourcc("cICP");
        public static final int mDCv = fourcc("mDCv");
        public static final int cLLi = fourcc("cLLi");
        public static final int tEXt = fourcc("tEXt");
        public static final int zTXt = fourcc("zTXt");
        public static final int iTXt = fourcc("iTXt");
        public static final int bKGD = fourcc("bKGD");
        public static final int hIST = fourcc("hIST");
        public static final int pHYs = fourcc("pHYs");
        public static final int sPLT = fourcc("sPLT");
        public static final int eXIf = fourcc("eXIf");
        public static final int tIME = fourcc("tIME");
        public static final int acTL = fourcc("acTL");
        public static final int fcTL = fourcc("fcTL");
        public static final int fdAT = fourcc("fdAT");
    }

    public static final long PNG_MAGIC = 0x89504E470D0A1A0AL;

    private final DataInputStream in;
    private final DataOutputStream out, crcOut;
    private final CRC32 crc = new CRC32();

    private int chunkLength = -1;
    private int chunkType;

    /**
     * Creates a PNG parser that reads from an input stream and writes to an output stream.
     *
     * @param in  the input stream for reading PNG data
     * @param out the output stream for writing PNG data
     * @throws IOException if an I/O error occurs
     */
    public PngSurgeon(InputStream in, OutputStream out) throws IOException {
        this.in = new DataInputStream(new BufferedInputStream(in));
        OutputStream bout = new BufferedOutputStream(out);
        this.out = new DataOutputStream(bout);
        this.crcOut = new DataOutputStream(new CheckedOutputStream(bout, crc));
    }

    /**
     * Reads the next chunk type from the PNG file.
     *
     * @return the chunk type as an integer
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if a previous chunk has not been processed
     */
    public int readChunkType() throws IOException {
        if (chunkLength != -1) throw new IllegalStateException("Previous chunk not processed");
        chunkLength = in.readInt();
        chunkType = in.readInt();
        return chunkType;
    }

    /**
     * Reads the chunk data while validating its CRC checksum.
     *
     * @return the chunk data as a byte array
     * @throws IOException if an I/O error occurs or if CRC validation fails
     */
    public byte[] readChunkData() throws IOException {
        if (chunkLength == -1) throw new IllegalStateException("No chunk data available");
        byte[] data = new byte[chunkLength];
        chunkLength = -1;
        in.readFully(data);
        crc.reset();
        crc.update(Ints.toByteArray(chunkType));
        crc.update(data);
        int actual = in.readInt();
        int expected = (int) crc.getValue();
        if (actual != expected) {
            throw new CRCException("Bad CRC (" + toHexString(actual) + " != " + toHexString(expected) + ")");
        }
        return data;
    }

    /**
     * Skips the current chunk's data.
     *
     * @throws IOException if an I/O error occurs
     */
    public void skipChunkData() throws IOException {
        if (chunkLength == -1) throw new IllegalStateException("No chunk to skip");
        long remaining = chunkLength + 4;
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped <= 0) throw new IOException("Failed to skip bytes");
            remaining -= skipped;
        }
        chunkLength = -1;
    }

    @Override
    public void close() throws IOException {
        try {
            in.close();
        } finally {
            out.close();
        }
    }

    private static int fourcc(String str) {
        return Ints.fromByteArray(str.getBytes(Charsets.ISO_8859_1));
    }

    private static String toHexString(int i) {
        return Long.toHexString(((i) & 0xFFFFFFFFL) | 0xF00000000L).substring(1).toUpperCase(Locale.ROOT);
    }
}

package com.jortage.poolmgr.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;

public class MemoryByteSinkSource implements ByteSinkSource {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    /**
     * Creates an empty in-memory byte storage.
     */
    public MemoryByteSinkSource() {}

    /**
     * Initializes the in-memory storage with the given byte array.
     * The provided data will be copied into an internal buffer.
     *
     * @param bys The byte array used to initialize the storage.
     * @throws NullPointerException if the provided array is {@code null}.
     */
    public MemoryByteSinkSource(byte[] bys) {
        this(bys, 0, bys.length);
    }

    /**
     * Initializes the in-memory storage with a portion of a byte array.
     * The specified range of bytes will be copied into the internal buffer.
     *
     * @param bys The byte array to initialize the storage with.
     * @param ofs The starting offset in the byte array.
     * @param len The number of bytes to copy from the offset.
     * @throws NullPointerException if {@code bys} is {@code null}.
     * @throws IndexOutOfBoundsException if {@code ofs} or {@code len} are invalid.
     */
    public MemoryByteSinkSource(byte[] bys, int ofs, int len) {
        if (bys == null) {
            throw new NullPointerException("Byte array cannot be null");
        }
        if (ofs < 0 || len < 0 || ofs + len > bys.length) {
            throw new IndexOutOfBoundsException("Invalid offset/length for the provided byte array");
        }
        baos.write(bys, ofs, len);
    }

    /**
     * Returns a {@link ByteSink} that allows writing data to the internal buffer.
     * <p>
     * Note: Each time {@link #openStream()} is called, the buffer is cleared 
     * before new data is written.
     * </p>
     *
     * @return A {@link ByteSink} that writes to the in-memory buffer.
     */
    @Override
    public ByteSink getSink() {
        return new ByteSink() {
            @Override
            public OutputStream openStream() throws IOException {
                baos.reset(); // Clears previous content before writing
                return baos;
            }
        };
    }

    /**
     * Returns a {@link ByteSource} that allows reading data from the internal buffer.
     * <p>
     * The returned {@link ByteSource} provides multiple reading methods, 
     * including streaming, reading all bytes at once, and retrieving the buffer size.
     * </p>
     *
     * @return A {@link ByteSource} for reading data from memory.
     */
    @Override
    public ByteSource getSource() {
        return new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
                return new ByteArrayInputStream(baos.toByteArray());
            }

            @Override
            public InputStream openBufferedStream() throws IOException {
                return openStream(); // ByteArrayInputStream is already buffered
            }
		
            @Override
            public byte[] read() throws IOException {
                return baos.toByteArray();
            }

            @Override
            public long size() throws IOException {
                return baos.size();
            }
        };
    }

    /**
     * Clears the internal buffer, effectively erasing all stored data.
     * This method is useful when the buffer is no longer needed.
     */
    @Override
    public void close() {
        baos.reset();
    }

    /**
     * Returns a string representation of this instance, indicating the current buffer size.
     *
     * @return A string representation of the object.
     */
    @Override
    public String toString() {
        return "MemoryByteSinkSource[size=" + baos.size() + "]";
    }
}

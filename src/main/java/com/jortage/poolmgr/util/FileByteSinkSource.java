package com.jortage.poolmgr.util;

import java.io.File;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

public class FileByteSinkSource implements ByteSinkSource {

    private final File file;
    private final boolean deleteOnClose;

    /**
     * Constructs a new FileByteSinkSource.
     *
     * @param file The file used for byte storage.
     * @param deleteOnClose If true, deletes the file when close() is called.
     */
    public FileByteSinkSource(File file, boolean deleteOnClose) {
        this.file = file;
        this.deleteOnClose = deleteOnClose;
    }

    /**
     * Returns a ByteSink for writing to the file.
     *
     * @return a ByteSink for the file.
     */
    @Override
    public ByteSink getSink() {
        return Files.asByteSink(file);
    }

    /**
     * Returns a ByteSource for reading from the file.
     *
     * @return a ByteSource for the file.
     */
    @Override
    public ByteSource getSource() {
        return Files.asByteSource(file);
    }

    /**
     * Closes this resource, deleting the file if {@code deleteOnClose} is true.
     */
    @Override
    public void close() {
        if (deleteOnClose && file.exists()) {
            if (!file.delete()) {
                System.err.println("Warning: Failed to delete file " + file.getAbsolutePath());
            }
        }
    }
}

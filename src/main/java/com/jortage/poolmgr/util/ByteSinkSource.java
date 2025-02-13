package com.jortage.poolmgr.util;

import java.io.Closeable;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;

public interface ByteSinkSource extends Closeable {

    /**
     * Returns a ByteSink for writing data.
     *
     * @return a ByteSink instance.
     */
    ByteSink getSink();

    /**
     * Returns a ByteSource for reading data.
     *
     * @return a ByteSource instance.
     */
    ByteSource getSource();

    @Override
    void close();
}

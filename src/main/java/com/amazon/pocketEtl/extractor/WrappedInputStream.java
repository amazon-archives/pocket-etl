/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.extractor;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;

/**
 * Allows a class to masquerade as an InputStream by extending this class and providing an implementation of a method
 * to get the real InputStream it is masquerading as.
 *
 * Mark and reset are not intentionally not supported by this wrapper due to:
 * a) Not being needed.
 * b) Requiring remapping of IOException that might be thrown by getWrappedInputStream(). The reason
 *    getWrappedInputStream() might throw an IOException is to support implementation of eagerly buffered InputStream
 *    implementations such as the S3BufferedInputStream.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class WrappedInputStream extends InputStream {
    protected abstract InputStream getWrappedInputStream() throws IOException;

    @Override
    public int read(@Nonnull byte[] b) throws IOException {
        return getWrappedInputStream().read(b);
    }

    @Override
    public int read(@Nonnull byte[] b, int off, int len) throws IOException {
        return getWrappedInputStream().read(b, off, len);
    }

    @Override
    public int read() throws IOException {
        return getWrappedInputStream().read();
    }

    @Override
    public long skip(long n) throws IOException {
        return getWrappedInputStream().skip(n);
    }

    @Override
    public int available() throws IOException {
        return getWrappedInputStream().available();
    }

    @Override
    public void close() throws IOException {
        getWrappedInputStream().close();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void reset() {
        throw new UnsupportedOperationException();
    }
}

/*
 *   Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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

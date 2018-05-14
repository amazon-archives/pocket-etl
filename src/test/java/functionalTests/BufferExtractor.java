/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package functionalTests;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.Extractor;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

class BufferExtractor<T> implements Extractor<T> {

    private final Iterator<T> bufferIterator;

    BufferExtractor(List<T> buffer) {
        bufferIterator = buffer.iterator();
    }

    @Override
    public Optional<T> next() {
        if(bufferIterator.hasNext()) {
            return Optional.of(bufferIterator.next());
        }

        return Optional.empty();
    }

    @Override
    public void close() {
        //no-op
    }

    @Override
    public void open(EtlMetrics parentMetrics) {
        //no-op
    }
}

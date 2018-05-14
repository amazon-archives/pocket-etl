/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package functionalTests;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.Loader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BufferLoader<T> implements Loader<T> {
    private final ConcurrentLinkedQueue<T> bufferLinkedQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void load(T objectToLoad) {
        bufferLinkedQueue.add(objectToLoad);
    }

    @Override
    public void open(EtlMetrics parentMetrics) {
        //no-op
    }

    @Override
    public void close() throws Exception {
        //no-op
    }

    List<T> getBuffer() {
         return new ArrayList<>(bufferLinkedQueue);
    }
}

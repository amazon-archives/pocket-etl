/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.prefs.BackingStoreException;

/**
 * Interface for an extractor object that either extracts objects from some kind of storage.
 *
 * @param <T> Type of object that is extracted.
 */
@FunctionalInterface
public interface Extractor<T> extends AutoCloseable {
    /**
     * Attempts to extract the next object from the backing store.
     *
     * @return An optional object that contains the next extracted object or empty if there are no objects left to be
     * extracted.
     * @throws BackingStoreException If there was a problem extracting the object from the backing store.
     */
    Optional<T> next() throws BackingStoreException;

    /**
     * Signal the extractor to prepare to extract objects.
     *
     * @param parentMetrics A parent EtlMetrics object to record all timers and counters into, will be null if
     *                      profiling is not required.
     */
    default void open(@Nullable EtlMetrics parentMetrics) {
        //no-op
    }

    /**
     * Signal the extractor that it should free up any resources allocated to the extraction of objects as no more
     * requests will be made.
     *
     * @throws Exception if something goes wrong.
     */
    @Override
    default void close() throws Exception {
        //no-op
    }
}

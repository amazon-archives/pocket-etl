/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl;

import javax.annotation.Nullable;

/**
 * Interface for a Loader that loads (writes) objects into final its final destination.
 *
 * @param <T> Type of object this loader loads.
 */
@FunctionalInterface
public interface Loader<T> extends AutoCloseable {
    /**
     * Load a single object to to the destination store/service.
     *
     * @param objectToLoad The object to be loaded.
     */
    void load(T objectToLoad);

    /**
     * Signal the loader to prepare to load objects.
     *
     * @param parentMetrics An EtlMetrics object to attach any child threads created by load() to, will be null if
     *                      profiling is not required.
     */
    default void open(@Nullable EtlMetrics parentMetrics) {
        //no-op
    }

    /**
     * Free up any resources allocated for the purposes of loading objects.
     *
     * @throws Exception if something goes wrong.
     */
    @Override
    default void close() throws Exception {
        //no-op
    }
}

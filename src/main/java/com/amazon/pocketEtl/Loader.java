/*
 *   Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.pocketEtl;

import javax.annotation.Nullable;

import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

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
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     * detected and the stream needs to be aborted.
     */
    void load(T objectToLoad) throws UnrecoverableStreamFailureException;

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

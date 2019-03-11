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
import java.util.Optional;

import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

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
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     * detected and the stream needs to be aborted.
     */
    Optional<T> next() throws UnrecoverableStreamFailureException;

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

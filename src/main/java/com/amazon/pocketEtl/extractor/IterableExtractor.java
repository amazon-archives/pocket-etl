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

package com.amazon.pocketEtl.extractor;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Optional;

/**
 * An Extractor implementation that wraps any Java iterable object. The iterator will be created just once when the ETL
 * job is first run.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class IterableExtractor<T> implements Extractor<T> {
    private final Iterable<T> iterable;
    private Iterator<T> iterator = null;

    /**
     * Simple static builder that constructs an extractor around a Java Iterable object.
     * @param iterable An iterable object.
     * @param <T> Type of object the iterator iterates on.
     * @return An Extractor object that can be used in Pocket ETL jobs.
     */
    public static <T> Extractor<T> of(Iterable<T> iterable) {
        return new IterableExtractor<>(iterable);
    }

    /**
     * Get the next object from the wrapped iterable. If there are any problems, throw an
     * UnrecoverableStreamFailureException to force the ETL runner to pay attention.
     * @return An optional extracted object. Empty if there are no objects left to extract.
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     * detected and the stream needs to be aborted.
     */
    @Override
    public Optional<T> next() throws UnrecoverableStreamFailureException {
        if (iterator == null) {
            throw new IllegalStateException("next() called on an unopened extractor");
        }

        try {
            return iterator.hasNext() ? Optional.of(iterator.next()) : Optional.empty();
        } catch (RuntimeException e) {
            throw new UnrecoverableStreamFailureException(e);
        }
    }

    /**
     * Create the iterator from the wrapped iterable object and prepare to start iterating on it.
     * @param parentMetrics A parent EtlMetrics object to record all timers and counters into, will be null if
     *                      profiling is not required.
     */
    @Override
    public void open(@Nullable EtlMetrics parentMetrics) {
        iterator = iterable.iterator();
    }
}

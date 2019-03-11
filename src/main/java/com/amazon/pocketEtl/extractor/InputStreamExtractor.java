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

import java.io.InputStream;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * Extractor implementation that maps an input stream into objects to be extracted.
 *
 * Example usage:
 * InputStreamExtractor.of(S3BufferedInputStream.of("myBucket", "myFile"), CsvInputStreamMapper.of(My.class));
 *
 * This example will create an extractor that will read a CSV file called 'myFile' stored in the S3 bucket 'myBucket'
 * and map each row to a newly constructed My.class object.
 *
 * @param <T> Type of object to be extracted from the input stream.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class InputStreamExtractor<T> implements Extractor<T> {
    private final Supplier<InputStream> inputStreamSupplier;
    private final InputStreamMapper<T> inputStreamMapperFunction;

    private InputStream inputStream = null;
    private Iterator<T> mappingIterator = null;

    /**
     * Static constructor method to build a functioning extractor.
     * @param inputStreamProvider A function that supplies the inputStream to be extracted from.
     * @param inputStreamMapper A function that can map an open inputStream into an iterator of the objects being
     *                          extracted.
     * @param <T> The type of object being extracted/mapped-to.
     * @return A functioning extractor.
     */
    public static <T> Extractor<T> of(Supplier<InputStream> inputStreamProvider,
                                      InputStreamMapper<T> inputStreamMapper) {
        return new InputStreamExtractor<>(inputStreamProvider, inputStreamMapper);
    }

    /**
     * This implementation of open will construct an inputStream and then create an iterator from it.
     * @param parentMetrics A parent EtlMetrics object to record all timers and counters into, will be null if
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        inputStream = inputStreamSupplier.get();
        mappingIterator = inputStreamMapperFunction.apply(inputStream);
    }

    /**
     * Attempts to extract the next object from the inputStream.
     * @return A newly extracted object or an empty optional if the end of the stream has been reached.
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     * detected and the stream needs to be aborted.
     */
    @Override
    public Optional<T> next() throws UnrecoverableStreamFailureException {
        if (inputStream == null || mappingIterator == null) {
            throw new IllegalStateException("Attempt to call next() on an uninitialized stream");
        }

        try {
            if (mappingIterator.hasNext()) {
                T nextObject = mappingIterator.next();
                return Optional.of(nextObject);
            }
            else {
                return Optional.empty();
            }
        } catch (RuntimeException e) {
            throw new UnrecoverableStreamFailureException(e);
        }
    }

    /**
     * Signals the extractor that it should free up any resources used to extract new objects. This will close the
     * inputStream that was being wrapped by this extractor.
     * @throws Exception If something goes wrong.
     */
    @Override
    public void close() throws Exception {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}

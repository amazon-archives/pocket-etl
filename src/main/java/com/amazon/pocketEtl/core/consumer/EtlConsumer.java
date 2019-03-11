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

package com.amazon.pocketEtl.core.consumer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.core.EtlStreamObject;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import javax.annotation.Nullable;

/**
 * Common interface for all ETL objects that consume data from the ETL stream.
 */
public interface EtlConsumer extends AutoCloseable {
    /**
     * Consume a single object.
     *
     * @param objectToConsume The object to be consumed.
     * @throws IllegalStateException If the consumer is in a state that it cannot accept more objects to consume.
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     * detected and the stream needs to be aborted.
     */
    void consume(EtlStreamObject objectToConsume) throws IllegalStateException, UnrecoverableStreamFailureException;

    /**
     * Signal the consumer to prepare receiving work.
     *
     * @param parentMetrics An EtlMetrics object to store counters and timers. Will be null if profiling is not
     *                      required.
     */
    void open(@Nullable EtlMetrics parentMetrics);
}

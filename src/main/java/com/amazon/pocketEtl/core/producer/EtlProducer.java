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

package com.amazon.pocketEtl.core.producer;

import javax.annotation.Nullable;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

/**
 * Producer interface for ETL system. A producer will, once started, continue to produce work until the supply of
 * work has been exhausted. Typically a producer will send work it produces to a downstream consumer of some kind.
 */
public interface EtlProducer extends AutoCloseable {
    /**
     * Produce objects until the source of objects has been exhausted.
     *
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     * detected and the stream needs to be aborted.
     * @throws IllegalStateException If the producer is not in a state capable of producing new work.
     */
    void produce() throws UnrecoverableStreamFailureException, IllegalStateException;

    /**
     * Signal the producer to complete its work and free any resources allocated for the production of work.
     *
     * @throws Exception If something goes wrong.
     */
    @Override
    void close() throws Exception;

    /**
     * Signal the producer that it should prepare to produce work.
     * @param parentMetrics An EtlMetrics object to add counters and timers to, will be null if profiling is not
     *                      required.
     */
    void open(@Nullable EtlMetrics parentMetrics);
}
